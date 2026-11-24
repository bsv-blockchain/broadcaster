package propagation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
)

// scriptedMerkle fails /watch with HTTP 500 for a per-txid quota, then
// returns 200. The quota is the number of failures still owed, so a tx
// with failsLeft=6 is registered on the seventh call.
type scriptedMerkle struct {
	mu        sync.Mutex
	failsLeft map[string]int
	calls     map[string]int
	when      []time.Time
	status    int
}

func (s *scriptedMerkle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TxID string `json:"txid"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	if s.calls == nil {
		s.calls = map[string]int{}
	}
	s.calls[req.TxID]++
	s.when = append(s.when, time.Now())
	left := s.failsLeft[req.TxID]
	if left > 0 {
		s.failsLeft[req.TxID] = left - 1
	}
	status := s.status
	s.mu.Unlock()
	if status == 0 {
		status = http.StatusInternalServerError
	}
	if left > 0 {
		w.WriteHeader(status)
		_, _ = w.Write([]byte("merkle watch unavailable"))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *scriptedMerkle) callCount(txid string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[txid]
}

// countingTeranode records how many times each raw-tx marker was submitted.
// Markers are unique per tx so a duplicate broadcast is visible even when
// several txs share one POST /txs body.
type countingTeranode struct {
	mu        sync.Mutex
	posts     int
	hits      map[string]int
	failPosts int
}

func (c *countingTeranode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.posts++
	for marker := range c.hits {
		if bytes.Contains(body, []byte(marker)) {
			c.hits[marker]++
		}
	}
	fail := c.failPosts
	if fail > 0 {
		c.failPosts--
	}
	c.mu.Unlock()
	if fail > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream unavailable"))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *countingTeranode) count(marker string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits[marker]
}

func (c *countingTeranode) postsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.posts
}

func retryCount(stage string) float64 {
	return testutil.ToFloat64(metrics.PropagationRetryTotal.WithLabelValues(stage))
}

func propMarker(i int) string {
	return fmt.Sprintf("MK%04d", i)
}

func propPayload(i int) (txid string, raw []byte) {
	raw = []byte(propMarker(i))
	return fmt.Sprintf("tx-%04d", i), raw
}

// pumpUntilTerminal flushes until every tx has a terminal propagation status
// or the deadline passes. Merkle and network requeues both land back on the
// dispatcher after requeueDelay, so an empty flush while work is still
// in flight just waits for that delay.
func pumpUntilTerminal(t *testing.T, p *Propagator, ms *mockStore, txids []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := flushSync(t, p); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if allPropagationTerminal(ms, txids) {
			return
		}
		wait := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(wait) && time.Now().Before(deadline) {
			if p.pendingDepth.Load() > 0 || allPropagationTerminal(ms, txids) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if allPropagationTerminal(ms, txids) {
			return
		}
	}
	t.Fatalf("txs not terminal after %s (pending=%d inflight=%d)", timeout, p.pendingDepth.Load(), p.inflightDepth.Load())
}

func allPropagationTerminal(ms *mockStore, txids []string) bool {
	for _, txid := range txids {
		st := ms.lastUpdateForTxid(txid)
		if st == nil {
			return false
		}
		switch st.Status {
		case models.StatusAcceptedByNetwork, models.StatusPendingRetry, models.StatusRejected:
		default:
			return false
		}
	}
	return true
}

func countStatus(ms *mockStore, txid string, status models.Status) int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	n := 0
	for _, upd := range ms.updates {
		if upd.TxID == txid && upd.Status == status {
			n++
		}
	}
	return n
}

// TestMerkleWatch_ExceedingSharedBudget_StillReachesNetwork is the
// production-shaped regression: six Merkle /watch HTTP 500s is enough to
// exceed propagation.retry_max_attempts (the fast path parks when the
// shared counter passes 5). Registration then succeeds and Teranode
// accepts. The tx must be ACCEPTED_BY_NETWORK, not parked, and Teranode
// must see it exactly once.
//
// On the coupled budget this parks at PENDING_RETRY on the sixth /watch
// failure, before any broadcast.
func TestMerkleWatch_ExceedingSharedBudget_StillReachesNetwork(t *testing.T) {
	const txid = "tx-six-watch-failures"
	marker := "MK-SIX"
	merkle := &scriptedMerkle{
		failsLeft: map[string]int{txid: 6},
		calls:     map[string]int{},
	}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{marker: 0}}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = 5 * time.Millisecond
	defer func() {
		p.dispatcherCancel()
		<-p.dispatcherDone
	}()

	merkleBefore := retryCount("merkle")
	networkBefore := retryCount("network")

	raw := []byte(marker)
	if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, raw))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	pumpUntilTerminal(t, p, ms, []string{txid}, 5*time.Second)

	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusAcceptedByNetwork {
		got := "<nil>"
		if st != nil {
			got = string(st.Status) + " " + st.ExtraInfo
		}
		t.Fatalf("status = %s, want ACCEPTED_BY_NETWORK (a /watch burst past five attempts must not park the tx before Teranode)", got)
	}
	if got := countStatus(ms, txid, models.StatusPendingRetry); got != 0 {
		t.Errorf("PENDING_RETRY writes = %d, want 0", got)
	}
	if got := countStatus(ms, txid, models.StatusAcceptedByNetwork); got != 1 {
		t.Errorf("ACCEPTED_BY_NETWORK writes = %d, want 1 (no duplicate terminalization)", got)
	}
	if got := node.count(marker); got != 1 {
		t.Errorf("teranode submissions of %s = %d, want 1", marker, got)
	}
	if got := merkle.callCount(txid); got != 7 {
		t.Errorf("/watch calls = %d, want 7 (six failures then success)", got)
	}
	if got := retryCount("merkle") - merkleBefore; got != 6 {
		t.Errorf("merkle retry decisions = %v, want 6", got)
	}
	if got := retryCount("network") - networkBefore; got != 0 {
		t.Errorf("network retry decisions = %v, want 0 before and after the single broadcast", got)
	}
}

func mustPropMsg(txid string, raw []byte) []byte {
	b, err := json.Marshal(propagationMsg{TXID: txid, RawTx: raw})
	if err != nil {
		panic(err)
	}
	return b
}

func stopTestPropagator(p *Propagator) {
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
		if p.dispatcherDone != nil {
			<-p.dispatcherDone
		}
		p.dispatcherCancel = nil
	}
}

// TestMerkleRetryBudget_IsIndependentOfNetworkBudget pins the ceilings:
// the merkle fast-path budget is not propagation.retry_max_attempts, and
// it stays strictly above that budget so a /watch burst that used to
// exhaust the shared counter still reaches broadcast.
func TestMerkleRetryBudget_IsIndependentOfNetworkBudget(t *testing.T) {
	if defaultMerkleRetryMaxAttempts <= defaultRetryMaxAttempts {
		t.Fatalf("merkle budget %d must stay above network budget %d", defaultMerkleRetryMaxAttempts, defaultRetryMaxAttempts)
	}
	p := newPropagator("", "http://127.0.0.1:1", newMockStore())
	defer stopTestPropagator(p)
	if p.retryMaxAttempts != defaultRetryMaxAttempts {
		t.Errorf("network budget = %d, want %d", p.retryMaxAttempts, defaultRetryMaxAttempts)
	}
	if p.merkleRetryMaxAttempts != defaultMerkleRetryMaxAttempts {
		t.Errorf("merkle budget = %d, want %d", p.merkleRetryMaxAttempts, defaultMerkleRetryMaxAttempts)
	}
	if p.merkleRetryMaxAttempts == p.retryMaxAttempts {
		t.Fatal("merkle and network budgets must not be the same counter ceiling")
	}
}

// TestMerkleWatch_FiveFailuresThenNetworkSuccess is the core regression:
// five /watch HTTP 500s, then /watch success, then Teranode HTTP 200.
// Merkle retries = 5. Network propagation retries consumed before
// broadcast = 0. Final status = ACCEPTED_BY_NETWORK.
func TestMerkleWatch_FiveFailuresThenNetworkSuccess(t *testing.T) {
	const txid = "tx-five-watch"
	marker := "MK-FIVE"
	merkle := &scriptedMerkle{failsLeft: map[string]int{txid: 5}, calls: map[string]int{}}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{marker: 0}}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = 5 * time.Millisecond
	defer stopTestPropagator(p)

	merkleBefore := retryCount("merkle")
	networkBefore := retryCount("network")
	http5xxBefore := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("http_5xx"))
	exhaustedBefore := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal)

	raw := []byte(marker)
	if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, raw))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	for i := range 5 {
		if i > 0 {
			waitForPending(t, p, 1)
		}
		if err := flushSync(t, p); err != nil {
			t.Fatal(err)
		}
		if got := node.count(marker); got != 0 {
			t.Fatalf("F-024: teranode saw the tx during merkle failures (%d)", got)
		}
		if res := p.admitToDispatcher(propagationMsg{TXID: txid, RawTx: raw}, 50); !res.duplicate {
			t.Fatalf("tx must stay in-flight while merkle registration is unresolved; admit = %+v", res)
		}
	}
	waitForPending(t, p, 1)
	if err := flushSync(t, p); err != nil {
		t.Fatal(err)
	}
	pumpUntilTerminal(t, p, ms, []string{txid}, 3*time.Second)

	if got := retryCount("merkle") - merkleBefore; got != 5 {
		t.Errorf("merkle retries = %v, want 5", got)
	}
	if got := retryCount("network") - networkBefore; got != 0 {
		t.Errorf("network retries consumed before broadcast = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("http_5xx")) - http5xxBefore; got != 5 {
		t.Errorf("http_5xx failures = %v, want 5", got)
	}
	if got := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal) - exhaustedBefore; got != 0 {
		t.Errorf("network exhaustion counter moved by %v on a merkle-only failure", got)
	}
	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("status = %v, want ACCEPTED_BY_NETWORK", st)
	}
	if got := countStatus(ms, txid, models.StatusAcceptedByNetwork); got != 1 {
		t.Errorf("ACCEPTED writes = %d, want 1", got)
	}
	if got := node.count(marker); got != 1 {
		t.Errorf("broadcasts = %d, want 1", got)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && p.inflightDepth.Load() != 0 {
		time.Sleep(time.Millisecond)
	}
	if got := p.inflightDepth.Load(); got != 0 {
		t.Errorf("inflight depth = %d, want 0 after accept (offset no longer pinned)", got)
	}
}

// TestMerkleWatch_Outage_BoundedRecoverableWithoutNetworkCharge simulates
// a prolonged Merkle outage. The fast path must not busy-loop, must park
// so Kafka can commit, must leave the network budget untouched, and must
// stay recoverable through the durable rebroadcast once /watch succeeds.
func TestMerkleWatch_Outage_BoundedRecoverableWithoutNetworkCharge(t *testing.T) {
	const txid = "tx-merkle-outage"
	marker := "MK-OUTAGE"
	merkle := &scriptedMerkle{
		failsLeft: map[string]int{txid: 1000},
		calls:     map[string]int{},
	}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{marker: 0}}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = 15 * time.Millisecond
	p.merkleRetryMaxAttempts = 3
	defer stopTestPropagator(p)

	quiesceRequeues(t)
	goroutinesBefore := runtime.NumGoroutine()
	networkBefore := retryCount("network")
	exhaustedNetBefore := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal)
	exhaustedMerkleBefore := testutil.ToFloat64(metrics.PropagationMerkleRetryExhaustedTotal)
	requeuesBefore := testutil.ToFloat64(metrics.PropagationPendingRequeues)

	claim, stopClaim := runDispatcherWithClaim(t, p)
	defer stopClaim()
	claim.ch <- kafkaMessage(mustPropMsg(txid, []byte(marker)), 7)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !claim.isMarked(7) {
		time.Sleep(5 * time.Millisecond)
	}
	if !claim.isMarked(7) {
		t.Fatal("offset stayed unmarked through a merkle outage; Kafka watermark would wedge")
	}
	// The first /watch must not have been able to commit the offset. The
	// mark above is the park, which is what releases it.
	if got := merkle.callCount(txid); got != 4 {
		t.Errorf("/watch calls = %d, want 4 (3 requeues + the exhausting attempt)", got)
	}
	merkle.mu.Lock()
	times := append([]time.Time(nil), merkle.when...)
	merkle.mu.Unlock()
	if len(times) >= 2 {
		gap := times[1].Sub(times[0])
		if gap < p.requeueDelay {
			t.Errorf("retry gap %s < requeue delay %s (busy loop)", gap, p.requeueDelay)
		}
	}
	if got := node.postsCount(); got != 0 {
		t.Errorf("F-024: teranode posts during outage = %d, want 0", got)
	}
	if got := retryCount("network") - networkBefore; got != 0 {
		t.Errorf("network retries = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal) - exhaustedNetBefore; got != 0 {
		t.Errorf("network exhaustion counter = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PropagationMerkleRetryExhaustedTotal) - exhaustedMerkleBefore; got != 1 {
		t.Errorf("merkle exhaustion counter = %v, want 1", got)
	}
	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusPendingRetry {
		t.Fatalf("status = %v, want PENDING_RETRY", st)
	}
	if st.Status == models.StatusRejected {
		t.Fatal("merkle outage must not terminalize REJECTED")
	}
	settleRequeues := time.Now().Add(time.Second)
	for time.Now().Before(settleRequeues) && testutil.ToFloat64(metrics.PropagationPendingRequeues) != requeuesBefore {
		time.Sleep(time.Millisecond)
	}
	if got := testutil.ToFloat64(metrics.PropagationPendingRequeues) - requeuesBefore; got != 0 {
		t.Errorf("pending requeue goroutines = %v, want 0 after park", got)
	}
	if got := runtime.NumGoroutine(); got > goroutinesBefore+20 {
		t.Errorf("goroutines = %d, baseline %d; outage grew unbounded", got, goroutinesBefore)
	}

	merkle.mu.Lock()
	merkle.failsLeft[txid] = 0
	merkle.mu.Unlock()
	ready, err := ms.GetReadyRetries(context.Background(), time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].TxID != txid {
		t.Fatalf("durable queue = %+v, want the parked tx", ready)
	}
	p.rebroadcastStuck(context.Background(), []propagationMsg{{
		TXID:  ready[0].TxID,
		RawTx: ready[0].RawTx,
	}}, true)
	st = ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("after merkle recovery status = %v, want ACCEPTED_BY_NETWORK", st)
	}
	if got := node.count(marker); got != 1 {
		t.Errorf("broadcasts after recovery = %d, want 1", got)
	}
	if got := retryCount("network") - networkBefore; got != 0 {
		t.Errorf("network retries after recovery = %v, want 0", got)
	}
	if got := countStatus(ms, txid, models.StatusRejected); got != 0 {
		t.Errorf("REJECTED writes = %d, want 0", got)
	}
}

// TestMerkleWatch_AuthFailure_StaysBoundedAndDiagnosable keeps 401/403 on
// the bounded merkle requeue. Bad credentials must not spin without the
// requeue delay, must not charge the network budget, and must keep the
// operator-facing auth warning.
func TestMerkleWatch_AuthFailure_StaysBoundedAndDiagnosable(t *testing.T) {
	const txid = "tx-merkle-auth"
	merkle := &scriptedMerkle{
		failsLeft: map[string]int{txid: 1000},
		calls:     map[string]int{},
		status:    http.StatusUnauthorized,
	}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{"MK-AUTH": 0}}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	core, logs := observer.New(zapcore.WarnLevel)
	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.logger = zap.New(core).Named("propagation")
	p.requeueDelay = 20 * time.Millisecond
	p.merkleRetryMaxAttempts = 3
	defer stopTestPropagator(p)

	networkBefore := retryCount("network")
	authBefore := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("auth_error"))
	start := time.Now()
	if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, []byte("MK-AUTH")))); err != nil {
		t.Fatal(err)
	}
	pumpUntilTerminal(t, p, ms, []string{txid}, 3*time.Second)
	elapsed := time.Since(start)

	if got := node.postsCount(); got != 0 {
		t.Errorf("F-024: broadcast under 401 = %d, want 0", got)
	}
	if got := retryCount("network") - networkBefore; got != 0 {
		t.Errorf("network retries = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("auth_error")) - authBefore; got != 4 {
		t.Errorf("auth_error = %v, want 4", got)
	}
	// Three delays separate four attempts. A tight loop would finish
	// well under one delay.
	if elapsed < 3*p.requeueDelay {
		t.Errorf("auth retries finished in %s, want at least %s (no tight loop)", elapsed, 3*p.requeueDelay)
	}
	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusPendingRetry {
		t.Fatalf("status = %v, want PENDING_RETRY", st)
	}
	if st.Status == models.StatusRejected {
		t.Fatal("auth failure must not terminalize REJECTED")
	}
	warns := logs.FilterMessageSnippet("401/403").All()
	if len(warns) == 0 {
		t.Fatal("expected the operator-facing 401/403 warning")
	}
}

// TestNetworkRetry_ExhaustsPropagationBudgetAfterMerkleSuccess proves the
// poison-batch protection is unchanged once registration has succeeded.
func TestNetworkRetry_ExhaustsPropagationBudgetAfterMerkleSuccess(t *testing.T) {
	const txid = "tx-network-exhaust"
	marker := "MK-NET"
	merkle := &scriptedMerkle{failsLeft: map[string]int{}, calls: map[string]int{}}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{marker: 0}, failPosts: 100}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = 5 * time.Millisecond
	defer stopTestPropagator(p)

	merkleBefore := retryCount("merkle")
	networkBefore := retryCount("network")
	exhaustedBefore := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal)

	if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, []byte(marker)))); err != nil {
		t.Fatal(err)
	}
	pumpUntilTerminal(t, p, ms, []string{txid}, 3*time.Second)

	if got := retryCount("merkle") - merkleBefore; got != 0 {
		t.Errorf("merkle retries = %v, want 0", got)
	}
	if got := retryCount("network") - networkBefore; got != float64(defaultRetryMaxAttempts) {
		t.Errorf("network retries = %v, want %d", got, defaultRetryMaxAttempts)
	}
	if got := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal) - exhaustedBefore; got != 1 {
		t.Errorf("network exhaustion = %v, want 1", got)
	}
	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusPendingRetry {
		t.Fatalf("status = %v, want PENDING_RETRY", st)
	}
	if got := node.postsCount(); got != defaultRetryMaxAttempts+1 {
		t.Errorf("teranode posts = %d, want %d", got, defaultRetryMaxAttempts+1)
	}
}

// TestMerkleAndNetworkRetries_AreIndependent fails Merkle N times, then
// Teranode M times, and requires the network retry count to be M rather
// than N+M.
func TestMerkleAndNetworkRetries_AreIndependent(t *testing.T) {
	const (
		txid = "tx-mixed"
		n    = 4
		m    = 5
	)
	marker := "MK-MIX"
	merkle := &scriptedMerkle{failsLeft: map[string]int{txid: n}, calls: map[string]int{}}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{marker: 0}, failPosts: m}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = 5 * time.Millisecond
	defer stopTestPropagator(p)

	merkleBefore := retryCount("merkle")
	networkBefore := retryCount("network")
	if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, []byte(marker)))); err != nil {
		t.Fatal(err)
	}
	pumpUntilTerminal(t, p, ms, []string{txid}, 5*time.Second)

	if got := retryCount("merkle") - merkleBefore; got != n {
		t.Errorf("merkle retries = %v, want %d", got, n)
	}
	if got := retryCount("network") - networkBefore; got != m {
		t.Errorf("network retries = %v, want %d (not %d)", got, m, n+m)
	}
	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("status = %v, want ACCEPTED_BY_NETWORK", st)
	}
	if got := countStatus(ms, txid, models.StatusAcceptedByNetwork); got != 1 {
		t.Errorf("ACCEPTED writes = %d, want 1", got)
	}
}

// TestMerkleWatch_P100IncidentShape is the 100-tx reproduction: 66 txs see
// at least one /watch HTTP 500, and 20 of those fail often enough to have
// exceeded the old shared five-attempt budget. After Merkle recovers,
// every tx is accepted once, nothing is parked, and the network budget
// stays at zero.
func TestMerkleWatch_P100IncidentShape(t *testing.T) {
	const total = 100
	merkle := &scriptedMerkle{failsLeft: map[string]int{}, calls: map[string]int{}}
	node := &countingTeranode{hits: map[string]int{}}
	txids := make([]string, 0, total)
	var wantMerkle float64
	for i := range total {
		txid, raw := propPayload(i)
		txids = append(txids, txid)
		node.hits[string(raw)] = 0
		switch {
		case i < 20:
			merkle.failsLeft[txid] = 6
			wantMerkle += 6
		case i < 66:
			merkle.failsLeft[txid] = 1
			wantMerkle++
		}
	}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = time.Millisecond
	defer stopTestPropagator(p)

	quiesceRequeues(t)
	goroutinesBefore := runtime.NumGoroutine()
	merkleBefore := retryCount("merkle")
	networkBefore := retryCount("network")
	start := time.Now()
	for i, txid := range txids {
		if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, []byte(propMarker(i))))); err != nil {
			t.Fatal(err)
		}
	}
	pumpUntilTerminal(t, p, ms, txids, 8*time.Second)
	elapsed := time.Since(start)

	if got := retryCount("network") - networkBefore; got != 0 {
		t.Errorf("network retries = %v, want 0", got)
	}
	if got := retryCount("merkle") - merkleBefore; got != wantMerkle {
		t.Errorf("merkle retries = %v, want %v", got, wantMerkle)
	}
	for i, txid := range txids {
		st := ms.lastUpdateForTxid(txid)
		if st == nil || st.Status != models.StatusAcceptedByNetwork {
			t.Fatalf("%s status = %v, want ACCEPTED_BY_NETWORK", txid, st)
		}
		if got := countStatus(ms, txid, models.StatusPendingRetry); got != 0 {
			t.Errorf("%s parked %d times", txid, got)
		}
		if got := countStatus(ms, txid, models.StatusAcceptedByNetwork); got != 1 {
			t.Errorf("%s ACCEPTED writes = %d, want 1", txid, got)
		}
		if got := node.count(propMarker(i)); got != 1 {
			t.Errorf("%s broadcasts = %d, want 1", txid, got)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (p.inflightDepth.Load() != 0 || p.pendingDepth.Load() != 0) {
		time.Sleep(time.Millisecond)
	}
	if p.inflightDepth.Load() != 0 || p.pendingDepth.Load() != 0 {
		t.Errorf("dispatcher not drained inflight=%d pending=%d", p.inflightDepth.Load(), p.pendingDepth.Load())
	}
	if got := runtime.NumGoroutine(); got > goroutinesBefore+30 {
		t.Errorf("goroutines = %d, baseline %d", got, goroutinesBefore)
	}
	t.Logf("P100 merkle-recovery elapsed=%s goroutine_delta=%d", elapsed, runtime.NumGoroutine()-goroutinesBefore)
	if elapsed > 5*time.Second {
		t.Errorf("P100 elapsed %s, want under 5s", elapsed)
	}
}

func TestMerkleRegisterFailureReason_Classifies(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"cancel", context.Canceled, "claim_revoked"},
		{"401", &merkleservice.RegisterError{StatusCode: http.StatusUnauthorized}, "auth_error"},
		{"403", &merkleservice.RegisterError{StatusCode: http.StatusForbidden}, "auth_error"},
		{"500", &merkleservice.RegisterError{StatusCode: http.StatusInternalServerError}, "http_5xx"},
		{"400", &merkleservice.RegisterError{StatusCode: http.StatusBadRequest}, "register_error"},
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"net timeout", &net.DNSError{IsTimeout: true, Err: "i/o timeout"}, "timeout"},
		{"transport", errors.New("connection refused"), "transport"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := merkleRegisterFailureReason(ctx, tc.err); got != tc.want {
				t.Errorf("reason = %s, want %s", got, tc.want)
			}
		})
	}
	t.Run("dead ctx", func(t *testing.T) {
		dead, cancel := context.WithCancel(context.Background())
		cancel()
		if got := merkleRegisterFailureReason(dead, errors.New("wrapped")); got != "claim_revoked" {
			t.Errorf("reason = %s, want claim_revoked", got)
		}
	})
}

func kafkaMessage(payload []byte, offset int64) *kafka.Message {
	return &kafka.Message{Value: payload, Offset: offset}
}
