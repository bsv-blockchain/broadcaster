package propagation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/teranode"
)

// These tests pin the convergence properties of the PENDING_RETRY path.
//
// #299 promotes that path from what it was on main — a rare poison-batch
// escape hatch, reached only when a tx burned its whole requeue budget on
// infra failures — to the DESIGNED steady-state landing spot for every
// cross-submission parent/child chain. Family partition keys co-locate a
// chain that arrives in ONE POST, but a chain split across submissions (or
// submitted via POST /tx, which still keys by bare txid) has no ordering
// guarantee: the child reaches teranode first, draws TX_MISSING_PARENT, and
// after retry_max_attempts × retry_backoff_ms parks at PENDING_RETRY.
//
// From there the reaper owns it. These tests assert the properties that role
// requires and the current implementation does not have:
//
//	convergence  — a parked chain must drain in bounded time, not one
//	               dependency level per 30s reaper tick
//	fairness     — a parked row must not be starved by newer eligible rows
//	liveness     — a parked row must never be silently abandoned
//	durability   — a park must record retry state, not just a status string
//
// The convergence math at shipped defaults (retry_max_attempts=5,
// retry_backoff_ms=2000, reaper_interval_ms=30000, stalePendingRetryAge=5m)
// is: T(D) ≈ 10s + 300s + (D−6) × 30s. Depth 100 is ~52 minutes.

// depAwareTeranode is a stateful fake teranode that accepts a tx only when
// every input's source tx was already accepted BEFORE the current request.
//
// The pre-request snapshot is the important detail, and it is deliberately
// stricter than tests/smoke's dependencyAwareTeranode (which accepts a child
// whose parent appears earlier in the same body). It models teranode's
// parallel bulk processing, which is BSV Broadcaster's own stated assumption in two
// places: handleAdmit's "Teranode's parallel bulk processing means we can't
// trust same-batch ordering", and #299's "no parent and child in one /txs
// batch" contract. A fake that resolved a whole chain from one batch would
// be asserting a guarantee BSV Broadcaster explicitly says it does not have.
type depAwareTeranode struct {
	server *httptest.Server

	mu        sync.Mutex
	accepted  map[string]bool
	requests  int            // POST /txs count — one per broadcast chunk
	submitted map[string]int // times each txid was sent
}

func newDepAwareTeranode(t *testing.T, preAccepted ...string) *depAwareTeranode {
	t.Helper()
	d := &depAwareTeranode{accepted: make(map[string]bool), submitted: make(map[string]int)}
	for _, txid := range preAccepted {
		d.accepted[strings.ToLower(txid)] = true
	}
	d.server = httptest.NewServer(http.HandlerFunc(d.handle))
	t.Cleanup(d.server.Close)
	return d
}

func (d *depAwareTeranode) URL() string { return d.server.URL }

func (d *depAwareTeranode) requestCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.requests
}

func (d *depAwareTeranode) submitCount(txid string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.submitted[strings.ToLower(txid)]
}

func (d *depAwareTeranode) isAccepted(txid string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.accepted[strings.ToLower(txid)]
}

func (d *depAwareTeranode) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost || (req.URL.Path != "/txs" && req.URL.Path != "/tx") {
		w.WriteHeader(http.StatusOK)
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	type parsedTx struct {
		txid    string
		parents []string
	}
	var txs []parsedTx
	for offset := 0; offset < len(body); {
		tx, used, perr := sdkTx.NewTransactionFromStream(body[offset:])
		if perr != nil || used == 0 {
			break
		}
		p := parsedTx{txid: tx.TxID().String()}
		for _, in := range tx.Inputs {
			if in != nil && in.SourceTXID != nil {
				p.parents = append(p.parents, in.SourceTXID.String())
			}
		}
		txs = append(txs, p)
		offset += used
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.requests++
	for _, p := range txs {
		d.submitted[strings.ToLower(p.txid)]++
	}

	// Snapshot BEFORE deciding anything, so siblings in this same request
	// cannot satisfy each other's dependencies.
	snapshot := make(map[string]bool, len(d.accepted))
	for k, v := range d.accepted {
		snapshot[k] = v
	}

	var failureLines []string
	for _, p := range txs {
		missing := ""
		for _, parent := range p.parents {
			if !snapshot[strings.ToLower(parent)] {
				missing = parent
				break
			}
		}
		if missing != "" {
			// Teranode's deepest-cause form. The [ProcessTransaction][<txid>]
			// wrapper is what txsWrappedTxidPattern keys the failure map on,
			// so the line is attributed to the child, not to the parent hash
			// that also appears in it.
			failureLines = append(failureLines, fmt.Sprintf(
				"TX_MISSING_PARENT (34): [ProcessTransaction][%s] error getting parent transaction %s",
				p.txid, missing,
			))
			continue
		}
		d.accepted[strings.ToLower(p.txid)] = true
	}

	if len(failureLines) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	// 422 is teranode's status for the missing-parent family.
	w.WriteHeader(http.StatusUnprocessableEntity)
	_, _ = io.WriteString(w, "Failed to process transactions:\n"+strings.Join(failureLines, "\n")+"\n")
}

// chainLink is one transaction in a linear parent→child chain.
type chainLink struct {
	txid string
	raw  []byte
}

// buildChain returns depth transactions where each spends the previous, the
// first spending rootTxID. Real bytes, real inputs — the reaper re-parses
// RawTx to discover parents, so synthetic bodies would not exercise it.
func buildChain(t *testing.T, rootTxID string, depth int) []chainLink {
	t.Helper()
	links := make([]chainLink, 0, depth)
	parent := rootTxID
	for i := 0; i < depth; i++ {
		txid, raw := spendingTx(t, parent, 0, uint32(i+1))
		links = append(links, chainLink{txid: txid, raw: raw})
		parent = txid
	}
	return links
}

// park seeds a chain link in the state parkExhaustedRequeues leaves behind.
// parkedAt doubles as the next-attempt time so the row is immediately due —
// these tests are about what the reaper does with a due row, not about the
// backoff schedule (nextPendingRetryDelay has its own coverage).
func park(ms *mockStore, link chainLink, parkedAt time.Time) {
	ms.parkTx(link.txid, link.raw, parkedAt, parkedAt)
}

// newReaperPropagator builds a propagator with a configurable rebroadcast cap
// against a dependency-aware teranode.
func newReaperPropagator(t *testing.T, teranodeURL string, st *mockStore, batchCap int) *Propagator {
	t.Helper()
	cfg := &config.Config{CallbackURL: testLocalCallbackURL}
	cfg.Propagation.MerkleConcurrency = 10
	if batchCap > 0 {
		cfg.Propagation.ReaperRebroadcastBatch = batchCap
	}
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, http.StatusOK)
	t.Cleanup(merkleSrv.Close)
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	return New(cfg, zap.NewNop(), nil, nil, st, nil, tc, mc)
}

// acceptedTxIDs collects every txid the store was told reached
// ACCEPTED_BY_NETWORK.
func acceptedTxIDs(ms *mockStore) map[string]bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make(map[string]bool)
	for _, u := range ms.updates {
		if u.Status == models.StatusAcceptedByNetwork {
			out[u.TxID] = true
		}
	}
	return out
}

// TestReapOnce_PendingRetryChain_ConvergesInOneTick is the headline
// convergence regression.
//
// A depth-5 chain is parked at PENDING_RETRY (the shape produced when a
// client submits a chain across several POSTs and the children outrun their
// parents across partitions). Its root is already on-chain. One reaper tick
// must drain the whole chain.
//
// It does not today, because reapOnce is dependency-blind in two ways:
// it builds propagationMsg{TXID, RawTx} with InputTXIDs left empty, and it
// hands the entire selected batch to broadcastInChunks as one /txs call with
// no topological layering. So exactly one dependency level clears per tick,
// and a parked chain of depth D costs (D−6) × reaper_interval_ms on top of
// the 5-minute park floor — ~52 minutes at D=100.
//
// The fix is to layer the batch by in-batch dependency and emit one chunk
// per layer within the tick, which also removes the parent-and-child-in-one-
// batch shape the design says teranode forbids.
func TestReapOnce_PendingRetryChain_ConvergesInOneTick(t *testing.T) {
	const depth = 5
	root := strings.Repeat("11", 32)
	chain := buildChain(t, root, depth)

	parkedAt := time.Now().Add(-10 * time.Minute)
	ms := newMockStore()
	for _, link := range chain {
		park(ms, link, parkedAt)
	}

	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, 0)

	p.reapOnce(context.Background())

	accepted := acceptedTxIDs(ms)
	var stuck []string
	for i, link := range chain {
		if !accepted[link.txid] {
			stuck = append(stuck, fmt.Sprintf("level %d (%s)", i+1, link.txid[:12]))
		}
	}
	if len(stuck) > 0 {
		t.Fatalf("one reaper tick accepted %d of %d chain levels; still parked: %s\n"+
			"reapOnce is dependency-blind: InputTXIDs is left empty and the whole batch "+
			"goes out as a single /txs call, so only one level can clear per tick. At "+
			"reaper_interval_ms=30000 a depth-%d chain needs %d ticks (%v).",
			len(accepted), depth, strings.Join(stuck, ", "),
			depth, depth, time.Duration(depth)*30*time.Second)
	}

	// And it must get there by submitting one layer at a time. A single /txs
	// call carrying the whole chain would put parents and children in one
	// batch, which is exactly the shape teranode's contract forbids and the
	// reason a dep-blind batch could only ever advance one level.
	if got := tn.requestCount(); got < depth {
		t.Errorf("teranode saw %d /txs requests for a depth-%d chain, want one per "+
			"dependency layer; the batch is not being layered", got, depth)
	}
}

// TestReapOnce_PendingRetry_StarvedByNewerBacklog pins the fairness property.
//
// Two things combine into starvation:
//
//  1. The reaper's missing-parent arm deliberately leaves the row alone, and
//     MarkMerkleRegisteredByTxIDs only writes merkle_registered_at — so a
//     failing row's timestamp_at is FROZEN at park time.
//  2. Selection is IterateStatusesSince, which Postgres serves as
//     "ORDER BY timestamp_at DESC", early-stopped at rebroadcastBatch.
//
// So an old parked row sinks below every newer eligible row and is never
// retried again. Here the cap is 2 and three newer rows are always eligible,
// so the oldest row is never even offered to teranode.
//
// (Pebble walks idx:tx:updated ascending, so the two backends starve in
// opposite directions — neither is FIFO. store.GetReadyRetries, which orders
// by next_retry_at and is already implemented and indexed, is.)
func TestReapOnce_PendingRetry_StarvedByNewerBacklog(t *testing.T) {
	root := strings.Repeat("22", 32)
	// One old orphan, plus newer rows that will always occupy the cap.
	old := buildChain(t, root, 1)[0]
	newer := buildChain(t, strings.Repeat("33", 32), 3)

	now := time.Now()
	ms := newMockStore()
	park(ms, old, now.Add(-6*time.Hour))
	for i, link := range newer {
		park(ms, link, now.Add(-time.Duration(10+i)*time.Minute))
	}

	// Root of the newer rows is NOT accepted, so they never resolve and never
	// stop being eligible — the steady-state backlog case.
	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, 2)

	for tick := 0; tick < 5; tick++ {
		p.reapOnce(context.Background())
	}

	if !tn.isAccepted(old.txid) {
		t.Fatalf("oldest parked row %s was never rebroadcast across 5 reaper ticks "+
			"(its parent is on-chain, so it would have been accepted on sight).\n"+
			"Newest-first selection + a frozen timestamp_at means a steady supply of "+
			"newer eligible rows starves it permanently; at 24h it falls out of the "+
			"staleScanLookback window and is silently abandoned.", old.txid[:12])
	}
}

// TestReapOnce_PendingRetry_NeverSilentlyAbandoned pins the liveness property.
//
// staleScanLookback bounds the walk at 24h. Because a failing PENDING_RETRY
// row's timestamp_at never moves, at park+24h it falls out of
// "WHERE timestamp_at >= $1" and stops being visible to the reaper entirely.
// It then stays PENDING_RETRY forever: no terminal status, no log line, no
// metric, and GET /tx never reaches a terminal answer for the client.
//
// A row that old must reach SOME resolution — either it is still retried, or
// it is terminalized with a reason. Silently vanishing is not an option.
func TestReapOnce_PendingRetry_NeverSilentlyAbandoned(t *testing.T) {
	root := strings.Repeat("44", 32)
	orphan := buildChain(t, root, 1)[0]

	ms := newMockStore()
	park(ms, orphan, time.Now().Add(-25*time.Hour)) // past staleScanLookback

	// Parent is on-chain, so a retry would succeed immediately.
	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, 0)

	p.reapOnce(context.Background())

	if tn.isAccepted(orphan.txid) {
		return // retried and resolved — acceptable outcome
	}
	last := ms.lastUpdateForTxid(orphan.txid)
	if last != nil && last.Status.IsTerminal() {
		return // terminalized with a verdict — also acceptable
	}
	t.Fatalf("a PENDING_RETRY row parked 25h ago was neither retried nor terminalized; "+
		"last status = %v.\nstaleScanLookback (24h) excludes it from the walk and nothing "+
		"else scans PENDING_RETRY, so it is abandoned in a non-terminal state forever — "+
		"invisible to the stuck census, which only tracks RECEIVED and ACCEPTED_BY_NETWORK.",
		statusOrNone(last))
}

func statusOrNone(st *models.TransactionStatus) models.Status {
	if st == nil {
		return models.StatusUnknown
	}
	return st.Status
}

// TestParkExhaustedRequeues_WritesDurableRetryFields pins the durability
// property.
//
// The schema already carries retry_count and next_retry_at, with
// idx_tx_retry_ready(next_retry_at) WHERE status='PENDING_RETRY' to serve
// them, and store exposes BumpRetryCount / SetPendingRetryFields /
// GetReadyRetries (ORDER BY next_retry_at LIMIT n FOR UPDATE SKIP LOCKED) /
// ClearRetryState. All of it is implemented on every backend, mocked in this
// package, and has zero callers outside store/ and its tests.
//
// parkExhaustedRequeues writes only a status row, so a parked tx has no
// attempt count and no scheduled retry time. That is why the reaper has to
// fall back to scanning by timestamp_at, which is what produces the
// starvation and abandonment above.
func TestParkExhaustedRequeues_WritesDurableRetryFields(t *testing.T) {
	root := strings.Repeat("55", 32)
	orphan := buildChain(t, root, 1)[0]

	ms := newMockStore()
	p := newReaperPropagator(t, "http://127.0.0.1:0", ms, 0)
	p.retryMaxAttempts = 2

	msg := propagationMsg{TXID: orphan.txid, RawTx: orphan.raw, retryReason: "TX_MISSING_PARENT (34): test"}
	p.parkExhaustedRequeues(context.Background(), []propagationMsg{msg}, p.defaultIO)

	ms.mu.Lock()
	pr := ms.pendingRetries[orphan.txid]
	count := ms.retryCounts[orphan.txid]
	ms.mu.Unlock()

	if pr == nil {
		t.Fatalf("park wrote no durable retry state for %s: SetPendingRetryFields was never called.\n"+
			"retry_count/next_retry_at stay NULL and idx_tx_retry_ready is dead for this path, "+
			"so the reaper can only find the row by scanning timestamp_at.", orphan.txid[:12])
	}
	if pr.NextRetryAt.IsZero() {
		t.Errorf("next_retry_at not set; the reaper cannot schedule a per-row retry")
	}
	if count < 1 {
		t.Errorf("retry_count = %d, want >= 1; without it there is no basis for backoff or for giving up", count)
	}
}

// TestReapOnce_PendingRetry_RecoversWhenParentAccepted is the durable happy
// path — a parked child whose parent has since landed must be lifted out of
// PENDING_RETRY and its retry state cleared.
//
// Worth stating that this path has no coverage at all today at any level:
// the unit tests stop at "the row is left alone", and the smoke test
// (TestSmoke_OrphanChild_ParksWithoutWedgingPipeline) runs with
// ReaperIntervalMs=60000 against a hardcoded 5-minute stalePendingRetryAge,
// so the parked row is provably unreachable inside its own test window.
func TestReapOnce_PendingRetry_RecoversWhenParentAccepted(t *testing.T) {
	root := strings.Repeat("66", 32)
	chain := buildChain(t, root, 1)
	child := chain[0]

	ms := newMockStore()
	park(ms, child, time.Now().Add(-10*time.Minute))

	tn := newDepAwareTeranode(t, root) // parent already on-chain
	p := newReaperPropagator(t, tn.URL(), ms, 0)

	p.reapOnce(context.Background())

	if !acceptedTxIDs(ms)[child.txid] {
		t.Fatalf("parked child was not lifted to ACCEPTED_BY_NETWORK after its parent landed")
	}

	// And it must leave the retry queue. Asserted behaviorally rather than by
	// watching for a ClearRetryState call: the resolving write goes through
	// BatchUpdateStatusReturning, and GetReadyRetries filters on
	// status='PENDING_RETRY', so a resolved row drops out on its own. Pinning
	// the method would pin an implementation detail; pinning the query result
	// pins the property that actually matters.
	ready, err := ms.GetReadyRetries(context.Background(), time.Now(), 100)
	if err != nil {
		t.Fatalf("GetReadyRetries: %v", err)
	}
	for _, r := range ready {
		if r.TxID == child.txid {
			t.Fatalf("resolved child %s is still served by the durable retry drain; "+
				"it would be rebroadcast to teranode every tick forever", child.txid[:12])
		}
	}
}

// TestRejectedAncestor_GrandparentRejected_Cascades pins the depth of the
// store-consult cascade.
//
// rejectedAncestor is documented as "the cross-partition analog of the
// dispatcher's same-partition cascade" and its reason string renders
// "parent rejected (ancestor %s)" — but cascadeReject is a recursive BFS
// over the waiter graph while this walks parentTxIDsOf(msg) once. Direct
// parents only.
//
// So with a REJECTED grandparent and a parent still at PENDING_RETRY, the
// grandchild is not condemned: it requeues, waits for the parent to cascade
// on some later tick, and only then cascades itself. The rejection path is
// therefore also O(depth) reaper ticks when it could be one bounded walk.
func TestRejectedAncestor_GrandparentRejected_Cascades(t *testing.T) {
	root := strings.Repeat("77", 32)
	chain := buildChain(t, root, 2)
	parent, child := chain[0], chain[1]

	ms := newMockStore()
	// Grandparent (the chain root) is terminally REJECTED; the parent between
	// it and the child is merely parked. The parent's row carries its raw
	// bytes, which is how the walk discovers the grandparent.
	ms.setStatus(root, models.StatusRejected)
	ms.setStatusRow(parent.txid, models.StatusPendingRetry, parent.raw)

	p := newReaperPropagator(t, "http://127.0.0.1:0", ms, 0)

	msg := propagationMsg{TXID: child.txid, RawTx: child.raw}
	ancestor, found := p.rejectedAncestor(context.Background(), msg)

	if !found {
		t.Fatalf("rejectedAncestor found no REJECTED ancestor for a grandchild of a REJECTED tx.\n"+
			"It walks direct parents only (parent %s is PENDING_RETRY, so the walk stops), "+
			"despite the reason string and doc comment both promising an ancestor walk. "+
			"The grandchild therefore burns a full reaper tick per generation before it "+
			"can be condemned.", parent.txid[:12])
	}
	if ancestor != root {
		t.Errorf("ancestor = %s, want the REJECTED root %s", ancestor, root)
	}
}
