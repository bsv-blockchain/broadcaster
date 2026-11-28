package propagation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
)

// orphanRetryReason is the last thing the network said about a parked orphan:
// a missing-parent line. schedulePendingRetry quotes it in the give-up reason.
const orphanRetryReason = "TX_MISSING_PARENT (34): [Validate][child] error getting parent transaction root"

// TestGiveUpReason pins what BSV Broadcaster tells a submitter when the durable-retry
// budget runs out. Nothing here is a verdict — the transaction exhausted its
// attempts precisely because no peer ever returned one — so the reason must
// quote what was actually heard rather than assert a cause.
//
// Before this, every exhausted transaction was told the same thing: that a
// parent it spends had never reached the network. That is true only for the
// missing-parent case the escape was built for. A transaction parked by a
// node-side storage fault, or by a verdict BSV Broadcaster could not attribute, was
// handed a confident explanation of something that never happened — the exact
// shape of "REJECTED with no reason why" that is worse than silence, because
// it sends the submitter to look for a parent that was never the problem.
func TestGiveUpReason(t *testing.T) {
	cases := []struct {
		name       string
		lastReason string
		want       []string
		absent     []string
	}{
		{
			name:       "missing parent keeps the diagnosis and quotes the line",
			lastReason: orphanRetryReason,
			want:       []string{"giving up", "a parent this transaction spends", orphanRetryReason},
		},
		{
			name:       "node fault quotes the fault instead of blaming a parent",
			lastReason: "STORAGE_ERROR (69): [ProcessTransaction][abc] failed to save transaction",
			want:       []string{"giving up", "last network response", "STORAGE_ERROR"},
			absent:     []string{"a parent this transaction spends"},
		},
		{
			name:       "nothing heard says so plainly",
			lastReason: "",
			want:       []string{"giving up", "no peer ever answered"},
			absent:     []string{"a parent this transaction spends"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := giveUpReason(7, tc.lastReason)
			if !strings.Contains(got, "7") {
				t.Errorf("reason = %q, want it to name the attempt budget", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("reason = %q, want it to contain %q", got, want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(got, absent) {
					t.Errorf("reason = %q, must NOT claim %q", got, absent)
				}
			}
		})
	}
}

// TestReapOnce_DeepChain_ConvergesInBoundedTicks is the scale answer to "how
// long can the PENDING_RETRY path take to resolve".
//
// A 500-deep chain is parked with a per-tick rebroadcast cap of 200, i.e. the
// chain is 2.5x the batch the reaper can move in one pass. Convergence must
// still be bounded and monotonic: every tick makes progress, and the whole
// chain drains in ceil(depth/cap) ticks or so.
//
// This is the case that previously could not converge at all. Parked rows were
// selected by scanning timestamp_at, all rows of one park batch share a single
// timestamp (parkExhaustedRequeues stamps one now for the whole slice), and a
// failed rebroadcast never rewrote the row — so the selection was a stable,
// arbitrary 200-row slice of the chain. If the resolvable frontier was not in
// that slice, the same 200 rows were re-broadcast every 30s forever, making
// zero progress, until the whole set aged past staleScanLookback and was
// abandoned. Draining by next_retry_at instead makes the frontier reachable by
// construction.
func TestReapOnce_DeepChain_ConvergesInBoundedTicks(t *testing.T) {
	const (
		depth    = 500
		batchCap = 200
	)
	root := strings.Repeat("88", 32)
	chain := buildChain(t, root, depth)

	ms := newMockStore()
	parkedAt := time.Now().Add(-time.Hour)
	for _, link := range chain {
		park(ms, link, parkedAt)
	}

	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, batchCap)
	// Compress the backoff so rescheduled rows are due again immediately;
	// this test is about how many drains convergence needs, not wall time.
	p.pendingRetryBackoff = time.Nanosecond
	p.pendingRetryMaxBackoff = time.Nanosecond

	// Generous ceiling: the point is that convergence is BOUNDED, not the
	// exact count. A tick can legitimately add nothing — rows that fail are
	// rescheduled behind the ones not yet tried, so the queue rotates — but
	// the rotation guarantees the frontier is reached. The old behavior
	// selected a stable arbitrary slice by frozen timestamp and could rotate
	// forever without ever including the frontier.
	const maxTicks = 25
	ticks := 0
	for ; ticks < maxTicks; ticks++ {
		p.reapOnce(context.Background())
		if len(acceptedTxIDs(ms)) == depth {
			break
		}
	}

	if got := len(acceptedTxIDs(ms)); got != depth {
		t.Fatalf("depth-%d chain drained only %d levels in %d ticks", depth, got, maxTicks)
	}
	t.Logf("depth-%d chain converged in %d reaper ticks at a %d-row cap "+
		"(~%v at reaper_interval_ms=30000)", depth, ticks+1, batchCap,
		time.Duration(ticks+1)*30*time.Second)
}

// TestNextPendingRetryDelay_ExponentialCappedJittered pins the durable retry
// schedule. Before it existed, eligibility was a flat hardcoded 5 minutes for
// every parked row regardless of how many times it had already failed.
func TestNextPendingRetryDelay_ExponentialCappedJittered(t *testing.T) {
	p := &Propagator{
		pendingRetryBackoff:    time.Second,
		pendingRetryMaxBackoff: 30 * time.Second,
	}

	// Jitter is +/-20%, so assert against the un-jittered target with slack.
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},  // capped
		{50, 30 * time.Second}, // stays capped
	} {
		got := p.nextPendingRetryDelay(tc.attempt)
		lo := time.Duration(float64(tc.want) * 0.79)
		hi := time.Duration(float64(tc.want) * 1.21)
		if got < lo || got > hi {
			t.Errorf("attempt %d: delay = %v, want %v +/-20%%", tc.attempt, got, tc.want)
		}
	}

	// Jitter must actually vary, or a batch parked together re-broadcasts in
	// lockstep every cycle — a synchronized burst at teranode and
	// merkle-service for as long as the shared parent is missing.
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 32; i++ {
		seen[p.nextPendingRetryDelay(4)] = struct{}{}
	}
	if len(seen) < 4 {
		t.Errorf("only %d distinct delays across 32 calls; retries are effectively unjittered", len(seen))
	}
}

// TestSchedulePendingRetry_GivesUpAfterMaxAttempts pins the terminal exit.
//
// A tx spending an outpoint that does not exist is the most common client
// mistake there is, and it is unresolvable in both directions: it can never be
// accepted, and rejectedAncestor can never condemn it because BSV Broadcaster has never
// seen the parent at all. Without a bound those rows accumulate forever,
// consume the reaper's per-tick batch, and re-POST to every healthy endpoint
// on every cycle indefinitely.
func TestSchedulePendingRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	root := strings.Repeat("99", 32)
	orphan := buildChain(t, root, 1)[0]

	ms := newMockStore()
	p := newReaperPropagator(t, "http://127.0.0.1:0", ms, 0)
	p.pendingRetryMaxAttempts = 3
	p.pendingRetryBackoff = time.Millisecond
	p.pendingRetryMaxBackoff = time.Millisecond

	before := testutil.ToFloat64(metrics.PropagationPendingRetryTotal.WithLabelValues("exhausted"))

	// Attempts 1..3 reschedule; the 4th exceeds the budget.
	for i := 0; i < 4; i++ {
		p.schedulePendingRetry(context.Background(), orphan.txid, orphan.raw, orphanRetryReason)
	}

	last := ms.lastUpdateForTxid(orphan.txid)
	if last == nil || last.Status != models.StatusRejected {
		t.Fatalf("orphan was not terminalized after exceeding the durable retry budget; "+
			"last status = %v. It would be re-broadcast to every endpoint forever.",
			statusOrNone(last))
	}
	if !strings.Contains(last.ExtraInfo, "giving up") {
		t.Errorf("ExtraInfo = %q, want a reason the submitter can act on", last.ExtraInfo)
	}

	ms.mu.Lock()
	_, stillQueued := ms.pendingRetries[orphan.txid]
	ms.mu.Unlock()
	if stillQueued {
		t.Errorf("orphan is still in the durable retry queue after being terminalized")
	}

	if got := testutil.ToFloat64(metrics.PropagationPendingRetryTotal.WithLabelValues("exhausted")); got != before+1 {
		t.Errorf("exhausted counter = %v, want %v", got, before+1)
	}
}

// TestLayerByDependency covers the topological split that lets one reaper tick
// advance a whole chain. Each layer must contain no tx that spends another in
// the same layer, since teranode processes a batch in parallel and cannot be
// relied on to order within it.
func TestLayerByDependency(t *testing.T) {
	msg := func(txid string, parents ...string) propagationMsg {
		return propagationMsg{TXID: txid, InputTXIDs: parents}
	}

	for _, tc := range []struct {
		name       string
		msgs       []propagationMsg
		wantLayers int
	}{
		{"empty", nil, 1},
		{"single", []propagationMsg{msg("a")}, 1},
		{"independent siblings share a layer", []propagationMsg{msg("a"), msg("b"), msg("c")}, 1},
		{"chain splits per level", []propagationMsg{msg("a"), msg("b", "a"), msg("c", "b")}, 3},
		{"chain in reverse input order", []propagationMsg{msg("c", "b"), msg("b", "a"), msg("a")}, 3},
		{"fan-out shares one child layer", []propagationMsg{msg("p"), msg("c1", "p"), msg("c2", "p")}, 2},
		{"fan-in waits for its deepest parent", []propagationMsg{msg("a"), msg("b", "a"), msg("z", "a", "b")}, 3},
		{"diamond", []propagationMsg{msg("a"), msg("b", "a"), msg("c", "a"), msg("d", "b", "c")}, 3},
		{"out-of-batch parents are ignored", []propagationMsg{msg("a", "outside"), msg("b", "elsewhere")}, 1},
		{"self-reference does not loop", []propagationMsg{msg("a", "a")}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layers := layerByDependency(tc.msgs)
			if len(layers) != tc.wantLayers {
				t.Fatalf("got %d layers, want %d: %v", len(layers), tc.wantLayers, layers)
			}

			// Every tx appears exactly once, and no layer contains a tx that
			// spends another member of the same layer.
			count := 0
			placed := make(map[string]int)
			for i, layer := range layers {
				for _, m := range layer {
					count++
					placed[m.TXID] = i
				}
			}
			if count != len(tc.msgs) {
				t.Errorf("layering moved %d of %d messages", count, len(tc.msgs))
			}
			for i, layer := range layers {
				inLayer := make(map[string]struct{}, len(layer))
				for _, m := range layer {
					inLayer[m.TXID] = struct{}{}
				}
				for _, m := range layer {
					for _, parent := range m.InputTXIDs {
						if parent == m.TXID {
							continue
						}
						if _, clash := inLayer[parent]; clash {
							t.Errorf("layer %d contains both %s and its parent %s", i, m.TXID, parent)
						}
					}
				}
			}
		})
	}
}

// TestLayerByDependency_CycleGuard: real transactions cannot form a cycle, but
// the layering must not hang or drop messages if fed one.
func TestLayerByDependency_CycleGuard(t *testing.T) {
	msgs := []propagationMsg{
		{TXID: "a", InputTXIDs: []string{"b"}},
		{TXID: "b", InputTXIDs: []string{"a"}},
	}
	layers := layerByDependency(msgs)
	total := 0
	for _, l := range layers {
		total += len(l)
	}
	if total != len(msgs) {
		t.Fatalf("cycle dropped messages: placed %d of %d", total, len(msgs))
	}
}

// TestConfigDefaults_PendingRetrySchedule pins that an unconfigured propagator
// gets the documented schedule, so "shipped cadence unchanged" claims are
// checkable rather than asserted in prose.
func TestConfigDefaults_PendingRetrySchedule(t *testing.T) {
	p := newPropagator("", "http://localhost:0", newMockStore())

	if p.pendingRetryBackoff != defaultPendingRetryBackoff {
		t.Errorf("pendingRetryBackoff = %v, want %v", p.pendingRetryBackoff, defaultPendingRetryBackoff)
	}
	if p.pendingRetryMaxBackoff != defaultPendingRetryMaxBackoff {
		t.Errorf("pendingRetryMaxBackoff = %v, want %v", p.pendingRetryMaxBackoff, defaultPendingRetryMaxBackoff)
	}
	if p.pendingRetryMaxAttempts != defaultPendingRetryMaxAttempts {
		t.Errorf("pendingRetryMaxAttempts = %d, want %d", p.pendingRetryMaxAttempts, defaultPendingRetryMaxAttempts)
	}

	// The cap must equal the flat eligibility age the reaper used before the
	// durable schedule existed, so the slowest retry cadence is unchanged.
	if defaultPendingRetryMaxBackoff != 5*time.Minute {
		t.Errorf("max backoff = %v; it should match the previous flat 5m eligibility age",
			defaultPendingRetryMaxBackoff)
	}
	// And the attempt bound should be about 24h at that cap — the age at which
	// rows previously fell out of staleScanLookback and were abandoned.
	span := time.Duration(defaultPendingRetryMaxAttempts) * defaultPendingRetryMaxBackoff
	if span < 20*time.Hour || span > 28*time.Hour {
		t.Errorf("give-up span = %v (%d attempts x %v), want ~24h to match the old "+
			"silent-abandonment horizon", span, defaultPendingRetryMaxAttempts, defaultPendingRetryMaxBackoff)
	}
}

// TestResolveMissingParents_ReaperPathIsNotLabelledRequeued keeps the
// fast-path signal clean. #295's dashboard guidance says a sustained
// missing_parent_total{outcome="requeued"} rate means family keying is not
// co-locating same-submission chains — which is only true if the reaper's
// durable retries are not also counted there.
func TestResolveMissingParents_ReaperPathIsNotLabelledRequeued(t *testing.T) {
	root := strings.Repeat("aa", 32)
	orphan := buildChain(t, root, 1)[0]

	ms := newMockStore()
	p := newReaperPropagator(t, "http://127.0.0.1:0", ms, 0)

	requeuedBefore := testutil.ToFloat64(metrics.PropagationMissingParentTotal.WithLabelValues("requeued"))
	reaperBefore := testutil.ToFloat64(metrics.PropagationMissingParentTotal.WithLabelValues("reaper_retry"))

	msgs := []propagationMsg{{TXID: orphan.txid, RawTx: orphan.raw}}
	line := []string{fmt.Sprintf("TX_MISSING_PARENT (34): [ProcessTransaction][%s] error getting parent transaction %s", orphan.txid, root)}
	p.resolveMissingParents(context.Background(), msgs, line, missingParentOutcomeReaperRetry)

	if got := testutil.ToFloat64(metrics.PropagationMissingParentTotal.WithLabelValues("requeued")); got != requeuedBefore {
		t.Errorf("reaper path incremented outcome=\"requeued\" (%v -> %v); a steady population "+
			"of unresolvable orphans would pin that counter forever and read as a "+
			"family-keying regression", requeuedBefore, got)
	}
	if got := testutil.ToFloat64(metrics.PropagationMissingParentTotal.WithLabelValues("reaper_retry")); got != reaperBefore+1 {
		t.Errorf("outcome=\"reaper_retry\" = %v, want %v", got, reaperBefore+1)
	}
}

// TestReapOnce_AdoptsLegacyParkedRows is the upgrade path.
//
// Rows parked by a build that predates durable retry scheduling carry no
// next_retry_at, and GetReadyRetries selects on "next_retry_at <= now" — so
// moving parked rows onto the durable queue would make every already-parked
// transaction invisible to both the walk and the drain. That is precisely the
// silently-abandoned state this change exists to remove, so the reaper adopts
// such rows into the queue when it encounters them.
func TestReapOnce_AdoptsLegacyParkedRows(t *testing.T) {
	root := strings.Repeat("bb", 32)
	legacy := buildChain(t, root, 1)[0]

	ms := newMockStore()
	// A legacy park: a PENDING_RETRY status row with raw bytes and NO entry in
	// the durable retry queue.
	ms.replayRows = []*models.TransactionStatus{{
		TxID:      legacy.txid,
		Status:    models.StatusPendingRetry,
		RawTx:     legacy.raw,
		Timestamp: time.Now().Add(-2 * time.Hour),
	}}

	ms.mu.Lock()
	queued := len(ms.pendingRetries)
	ms.mu.Unlock()
	if queued != 0 {
		t.Fatalf("fixture should start with an empty durable queue, got %d", queued)
	}

	tn := newDepAwareTeranode(t, root) // parent on-chain, so a retry succeeds
	p := newReaperPropagator(t, tn.URL(), ms, 0)
	p.pendingRetryBackoff = time.Nanosecond
	p.pendingRetryMaxBackoff = time.Nanosecond

	// First tick adopts it; a later tick drains it once its schedule is due.
	p.reapOnce(context.Background())
	ms.mu.Lock()
	_, adopted := ms.pendingRetries[legacy.txid]
	ms.mu.Unlock()
	if !adopted {
		t.Fatalf("legacy parked row was not adopted into the durable retry queue; " +
			"with no next_retry_at it is invisible to GetReadyRetries and would sit at " +
			"PENDING_RETRY forever after the upgrade")
	}

	// nextPendingRetryDelay floors at 1ms, so an adopted row is not due the
	// instant it is booked. Tick until it drains rather than assuming it is
	// ready on the very next pass.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !acceptedTxIDs(ms)[legacy.txid] {
		p.reapOnce(context.Background())
	}
	if !acceptedTxIDs(ms)[legacy.txid] {
		t.Errorf("adopted legacy row was not retried to ACCEPTED_BY_NETWORK")
	}
}

// TestReapOnce_ChildDueBeforeParkedParent_StillConverges covers a parked
// chain whose children are due BEFORE their parents.
//
// Nothing about parking puts a chain in parent-first order: children reach
// the network first and draw TX_MISSING_PARENT first, so they park first, and
// the jittered backoff reshuffles every reschedule after that. With more due
// rows than one batch holds, the reaper kept picking children whose parents
// were further back in the queue, broadcast them alone, and rebooked them
// still ahead of their parents. Thousand-deep chains advanced about one link
// per full pass and would have exhausted pending_retry_max_attempts.
//
// Parked here in exactly the wrong order (deepest link due first, depth ten
// times the batch), the chain must still drain in no more ticks than a
// parent-first queue would need.
func TestReapOnce_ChildDueBeforeParkedParent_StillConverges(t *testing.T) {
	const (
		depth    = 200
		batchCap = 20
	)
	root := strings.Repeat("77", 32)
	chain := buildChain(t, root, depth)

	ms := newMockStore()
	base := time.Now().Add(-time.Hour)
	for i := len(chain) - 1; i >= 0; i-- {
		// Deepest link gets the earliest next_retry_at.
		park(ms, chain[i], base.Add(-time.Duration(i)*time.Millisecond))
	}

	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, batchCap)
	p.pendingRetryBackoff = time.Nanosecond
	p.pendingRetryMaxBackoff = time.Nanosecond

	// The rate a parent-first queue drains at. Before the fix this order
	// managed 60 links in 30 ticks.
	const maxTicks = depth / batchCap
	ticks := 0
	for ; ticks < maxTicks; ticks++ {
		p.reapOnce(context.Background())
		if len(acceptedTxIDs(ms)) == depth {
			break
		}
	}

	if got := len(acceptedTxIDs(ms)); got != depth {
		t.Fatalf("depth-%d chain parked child-first drained only %d links in %d ticks", depth, got, maxTicks)
	}
	t.Logf("converged in %d ticks, %d broadcast requests", ticks+1, tn.requestCount())
}

// spendingTxFrom builds a transaction spending one output of each source,
// in order.
func spendingTxFrom(t *testing.T, nonce uint32, sources ...string) (txid string, raw []byte) {
	t.Helper()
	tx := sdkTx.NewTransaction()
	tx.LockTime = nonce
	for i, src := range sources {
		h, err := chainhash.NewHashFromHex(src)
		if err != nil {
			t.Fatalf("NewHashFromHex(%s): %v", src, err)
		}
		tx.Inputs = append(tx.Inputs, &sdkTx.TransactionInput{
			SourceTXID:       h,
			SourceTxOutIndex: uint32(i),
			UnlockingScript:  &script.Script{},
			SequenceNumber:   0xffffffff,
		})
	}
	return tx.TxID().String(), tx.Bytes()
}

// TestWithParkedAncestors_PullsDependencyClosedSet pins that the pull never
// takes an ancestor without the parked parents it spends.
//
// D spends U and V, and V spends U, so the walk finds U and V at the same
// distance from D. With room for one, taking V would be useless: it cannot be
// accepted before U. U must be the one pulled, and accepted.
func TestWithParkedAncestors_PullsDependencyClosedSet(t *testing.T) {
	root := strings.Repeat("55", 32)
	u, uRaw := spendingTxFrom(t, 1, root)
	v, vRaw := spendingTxFrom(t, 2, u)
	d, dRaw := spendingTxFrom(t, 3, u, v)

	ms := newMockStore()
	now := time.Now()
	ms.parkTx(u, uRaw, now.Add(-time.Hour), now.Add(time.Hour))
	ms.parkTx(v, vRaw, now.Add(-time.Hour), now.Add(time.Hour))
	ms.parkTx(d, dRaw, now.Add(-time.Hour), now.Add(-time.Minute))

	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, 1)

	p.reapOnce(context.Background())

	if !tn.isAccepted(u) {
		t.Fatalf("U, the only ancestor that can be accepted, was not broadcast")
	}
}

// TestWithParkedAncestors_FailedPullLeavesRowAlone pins that pulling a
// parked ancestor for a due descendant does not spend the ancestor's retry
// budget or rewrite its schedule and reason when the attempt settles nothing.
// Otherwise a busy chain's children would burn their parent's
// pending_retry_max_attempts long before its own backoff called for a retry.
func TestWithParkedAncestors_FailedPullLeavesRowAlone(t *testing.T) {
	missing := strings.Repeat("44", 32) // never reaches the network
	parent, parentRaw := spendingTxFrom(t, 1, missing)
	child, childRaw := spendingTxFrom(t, 2, parent)

	ms := newMockStore()
	now := time.Now()
	parentNext := now.Add(time.Hour)
	ms.parkTx(parent, parentRaw, now.Add(-time.Hour), parentNext)
	ms.parkTx(child, childRaw, now.Add(-time.Hour), now.Add(-time.Minute))
	const reason = "STORAGE_ERROR (69): earlier attempt"
	ms.mu.Lock()
	ms.pendingRetries[parent].LastReason = reason
	before := *ms.pendingRetries[parent]
	beforeCount := ms.retryCounts[parent]
	ms.mu.Unlock()

	tn := newDepAwareTeranode(t)
	p := newReaperPropagator(t, tn.URL(), ms, 10)

	p.reapOnce(context.Background())

	ms.mu.Lock()
	defer ms.mu.Unlock()
	after, ok := ms.pendingRetries[parent]
	if !ok {
		t.Fatalf("parked parent left the retry queue after a pull that settled nothing")
	}
	if ms.retryCounts[parent] != beforeCount {
		t.Errorf("retry_count = %d, want %d: the pull spent the parent's budget", ms.retryCounts[parent], beforeCount)
	}
	if !after.NextRetryAt.Equal(before.NextRetryAt) {
		t.Errorf("next_retry_at = %v, want %v", after.NextRetryAt, before.NextRetryAt)
	}
	if after.LastReason != reason {
		t.Errorf("retry_reason = %q, want %q", after.LastReason, reason)
	}
	if ms.retryCounts[child] == 1 {
		t.Errorf("the due child was not retried")
	}
}

// TestWithParkedAncestors_ClosureRunsThroughDueRows pins that a due row in
// the batch does not cut the dependency analysis short. In
// U1 -> U2 -> U3 -> A -> B -> D with A and D due and room for two
// ancestors, B is useless without U1..U3 behind A. The two pulled must be the
// top of the chain, U1 and U2, and both accepted.
func TestWithParkedAncestors_ClosureRunsThroughDueRows(t *testing.T) {
	root := strings.Repeat("33", 32)
	chain := buildChain(t, root, 6) // U1 U2 U3 A B D
	ms := newMockStore()
	now := time.Now()
	for i, link := range chain {
		next := now.Add(time.Hour)
		if i == 3 || i == 5 { // A, D
			next = now.Add(-time.Minute)
		}
		ms.parkTx(link.txid, link.raw, now.Add(-time.Hour), next)
	}

	tn := newDepAwareTeranode(t, root)
	p := newReaperPropagator(t, tn.URL(), ms, 2)

	p.reapOnce(context.Background())

	for i, name := range []string{"U1", "U2"} {
		if !tn.isAccepted(chain[i].txid) {
			t.Errorf("%s, at the top of the chain, was not accepted", name)
		}
	}
	if n := tn.submitCount(chain[4].txid); n != 0 {
		t.Errorf("B was sent %d times; it cannot be accepted before U3 and A", n)
	}
}

// TestWithParkedAncestors_PullCooldown pins that a parked ancestor that
// stays unresolved is not resubmitted on every tick just because another
// descendant fell due: it waits out the backoff its own next attempt would
// get.
func TestWithParkedAncestors_PullCooldown(t *testing.T) {
	missing := strings.Repeat("22", 32) // never reaches the network
	parent, parentRaw := spendingTxFrom(t, 1, missing)
	child1, child1Raw := spendingTxFrom(t, 2, parent)
	child2, child2Raw := spendingTxFrom(t, 3, parent)

	ms := newMockStore()
	now := time.Now()
	ms.parkTx(parent, parentRaw, now.Add(-time.Hour), now.Add(time.Hour))
	ms.parkTx(child1, child1Raw, now.Add(-time.Hour), now.Add(-time.Minute))

	tn := newDepAwareTeranode(t)
	p := newReaperPropagator(t, tn.URL(), ms, 10)
	p.pendingRetryBackoff = time.Hour
	p.pendingRetryMaxBackoff = time.Hour

	p.reapOnce(context.Background())
	if n := tn.submitCount(parent); n == 0 {
		t.Fatalf("parent was never pulled for its due child")
	}
	first := tn.submitCount(parent)

	// A second descendant falls due on the next tick.
	ms.parkTx(child2, child2Raw, now.Add(-time.Hour), now.Add(-time.Minute))
	p.reapOnce(context.Background())

	if n := tn.submitCount(parent); n != first {
		t.Errorf("parent sent %d times after the second tick, want %d: pulled again inside its cooldown", n, first)
	}
	if tn.submitCount(child2) == 0 {
		t.Errorf("the newly due child was not retried")
	}
}
