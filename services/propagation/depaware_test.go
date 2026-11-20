package propagation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/teranode"
)

// makePropMsgWithParents builds a propagationMsg envelope with explicit
// InputTXIDs.
func makePropMsgWithParents(txid string, parents []string) []byte {
	msg := propagationMsg{
		TXID:       txid,
		RawTx:      []byte{0xde, 0xad, 0xbe, 0xef},
		InputTXIDs: parents,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

// newPropagatorForDepTest constructs a Propagator with a minimal mock
// store. Its dispatcher goroutine starts inside New; the returned
// cleanup cancels it and waits for the goroutine to exit before the
// test returns (otherwise it can outlive the mockStore the test owns).
func newPropagatorForDepTest(t *testing.T, ms *mockStore) (*Propagator, func()) {
	t.Helper()
	cfg := &config.Config{}
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)
	return p, func() {
		if p.dispatcherCancel != nil {
			p.dispatcherCancel()
			if p.dispatcherDone != nil {
				<-p.dispatcherDone
			}
		}
	}
}

// drainSet snapshots the dispatcher's pendingMsgs as a set of txids
// AND clears the dispatcher's pending state (moving the txids from
// "queued for next batch" to "broadcasting"). Tests calling this
// must then re-think subsequent admission decisions accordingly.
func drainSet(p *Propagator) map[string]bool {
	batch := p.drainPending()
	out := make(map[string]bool, len(batch))
	for _, m := range batch {
		out[m.TXID] = true
	}
	return out
}

// TestHandleMessage_HoldsChildWhenParentInFlight verifies the
// dep-aware admission rule for Teranode's parallel /txs processing:
// even if a parent and child both arrive before the next flush, they
// must NOT be co-batched. The child is held while the parent is
// in-flight and can only be broadcast in a later batch.
func TestHandleMessage_HoldsChildWhenParentInFlight(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	pending := drainSet(p)
	if !pending["parent"] {
		t.Errorf("parent should be in pending batch, got %v", pending)
	}
	if pending["child"] {
		t.Errorf("child should be HELD while parent is in-flight (Teranode parallel processing forbids same-batch parent+child); got %v", pending)
	}
}

// TestHandleMessage_HoldsChildWhenParentInDifferentBatch verifies the
// "different in-flight batch → hold" rule. A child arriving after its
// parent has already been drained (and is now broadcasting) must be
// held; Teranode can't coordinate across separate batches.
func TestHandleMessage_HoldsChildWhenParentInDifferentBatch(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	// Drain — parent moves from inPending to inFlight-but-not-inPending
	// (broadcasting from this point until terminal).
	if got := drainSet(p); !got["parent"] {
		t.Fatalf("parent should drain, got %v", got)
	}

	// Now child arrives. Parent is in inFlight but not in inPending →
	// child must hold.
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	if got := drainSet(p); got["child"] {
		t.Errorf("child should be held (parent in different batch), not in pending; got %v", got)
	}
}

// TestApplyTerminalStatuses_ReleasesWaitersOnAccepted verifies the
// release path: child held on a broadcasting parent gets released to
// the pending batch when the parent terminalizes ACCEPTED.
func TestApplyTerminalStatuses_ReleasesWaitersOnAccepted(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	_ = drainSet(p) // parent now broadcasting

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	// Confirm child is held, not in pending.
	if got := drainSet(p); got["child"] {
		t.Fatalf("child should be held before parent ACCEPTED; got %v", got)
	}

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0, p.defaultIO)

	if got := drainSet(p); !got["child"] {
		t.Errorf("child should be released into pending batch after parent ACCEPTED; got %v", got)
	}
}

// TestApplyTerminalStatuses_CascadesRejectedChildren verifies that
// when a broadcasting parent terminalizes REJECTED, every held
// descendant gets a terminal REJECTED row written and is removed
// from in-flight state without ever broadcasting.
func TestApplyTerminalStatuses_CascadesRejectedChildren(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	_ = drainSet(p) // parent broadcasting; subsequent children of it must hold

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("grandchild", []string{"child"}))); err != nil {
		t.Fatalf("grandchild admit: %v", err)
	}
	if got := drainSet(p); len(got) != 0 {
		t.Fatalf("child and grandchild should both be held; got %v", got)
	}

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusRejected, Timestamp: time.Now(), ExtraInfo: "bad parent"},
	}, 0, 1, p.defaultIO)

	if got := drainSet(p); got["child"] || got["grandchild"] {
		t.Errorf("cascaded descendants should NOT enter pending batch; got %v", got)
	}

	ms.mu.Lock()
	rejected := map[string]string{}
	for _, st := range ms.updates {
		if st.Status == models.StatusRejected && (st.TxID == "child" || st.TxID == "grandchild") {
			rejected[st.TxID] = st.ExtraInfo
		}
	}
	ms.mu.Unlock()
	if len(rejected) != 2 {
		t.Errorf("expected 2 cascade-rejection rows (child + grandchild), got %d: %v", len(rejected), rejected)
	}
	for txid, reason := range rejected {
		// The stable "parent rejected" prefix stays for existing consumers;
		// the suffix must name the rejected ancestor and state the recovery
		// path so a cascaded child reads as retryable queue state, not a
		// verdict about its own bytes.
		if !strings.HasPrefix(reason, "parent rejected") {
			t.Errorf("%s ExtraInfo should keep the \"parent rejected\" prefix, got %q", txid, reason)
		}
		if !strings.Contains(reason, "ancestor parent") {
			t.Errorf("%s ExtraInfo should name the rejected ancestor, got %q", txid, reason)
		}
		if !strings.Contains(reason, "resubmit") {
			t.Errorf("%s ExtraInfo should state the resubmit recovery path, got %q", txid, reason)
		}
	}
}

// TestSequentialReleaseDeepChain verifies that a held chain releases
// one link at a time. Under Teranode's parallel bulk processing,
// parent and child can't share a batch — so when grandparent
// ACCEPTED, only parent (one level down) releases. Child stays held
// on parent until parent ACCEPTED in its own batch.
func TestSequentialReleaseDeepChain(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("grandparent"))); err != nil {
		t.Fatalf("grandparent admit: %v", err)
	}
	_ = drainSet(p) // grandparent broadcasting

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("parent", []string{"grandparent"}))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	if got := drainSet(p); len(got) != 0 {
		t.Fatalf("parent and child should both be held; got %v", got)
	}

	// grandparent ACCEPTED — releases parent only. Child still held
	// because parent is still in-flight (just queued for broadcast).
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "grandparent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0, p.defaultIO)

	got := drainSet(p)
	if !got["parent"] {
		t.Errorf("parent should release after grandparent ACCEPTED; got %v", got)
	}
	if got["child"] {
		t.Errorf("child should NOT release yet — parent still in-flight; got %v", got)
	}

	// parent ACCEPTED in its own batch — now child can release.
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0, p.defaultIO)

	got = drainSet(p)
	if !got["child"] {
		t.Errorf("child should release after parent ACCEPTED; got %v", got)
	}
}

// TestHandleMessage_NoParents_AdmitsNormally is the trivial path: a
// tx with no InputTXIDs lands in the pending batch.
func TestHandleMessage_NoParents_AdmitsNormally(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("lone"))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	if got := drainSet(p); !got["lone"] {
		t.Errorf("lone tx should be in pending batch; got %v", got)
	}
}

// TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly verifies
// that a child whose declared parent isn't tracked by BSV Broadcaster at all
// (mined long ago, never seen, whatever) is admitted normally — only
// IN-FLIGHT parents block.
func TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"someParentNotInFlight"}))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	if got := drainSet(p); !got["child"] {
		t.Errorf("child should be admitted directly when parent is not in flight; got %v", got)
	}
}

// TestHandleMessage_MaxPendingFull_BlocksUntilDrained verifies the
// pending-cap backpressure: handleMessage BLOCKS when pending is at
// its configured cap, then unblocks after a drain frees capacity.
// No DLQ, no error to the consumer wrapper — just natural
// backpressure flowing back to the broker.
func TestHandleMessage_MaxPendingFull_BlocksUntilDrained(t *testing.T) {
	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MaxPending = 1
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)
	defer p.dispatcherCancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx1"))); err != nil {
		t.Fatalf("first admit: %v", err)
	}

	tx2Done := make(chan error, 1)
	go func() {
		tx2Done <- p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx2")))
	}()

	select {
	case err := <-tx2Done:
		t.Fatalf("tx2 admit should have blocked while pending was full; returned err=%v", err)
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked
	}

	pending := drainSet(p)
	if !pending["tx1"] {
		t.Errorf("tx1 should be in pending batch; got %v", pending)
	}
	if pending["tx2"] {
		t.Errorf("tx2 should not have been in the drained batch yet; got %v", pending)
	}

	select {
	case err := <-tx2Done:
		if err != nil {
			t.Errorf("tx2 admit should complete after drain; got err=%v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("tx2 admit didn't unblock after pending drain")
	}

	if got := drainSet(p); !got["tx2"] {
		t.Errorf("tx2 should be in pending batch after unblocking; got %v", got)
	}
}

// TestHandleAdmit_DuplicateTxid_DoesNotStrandOffset guards the
// Kafka-redelivery edge case: if the same txid is admitted twice, the
// second admission must not overwrite the inFlight entry's offset.
// Doing so would leave the first offset on the tracker with no path to
// Done — LowestUnfinished would pin to it forever and freeze the
// commit watermark. The dispatcher's contract is that the new offset
// is bookkept on the tracker (added and immediately marked done) so
// the consumer's offset commit can advance past the redelivery once
// the original terminalizes, but the broadcast / waiter graph is
// untouched.
func TestHandleAdmit_DuplicateTxid_DoesNotStrandOffset(t *testing.T) {
	inFlight := make(map[string]int64)
	waiters := make(map[string]map[string]struct{})
	heldMsgs := make(map[string]propagationMsg)
	var pendingMsgs []propagationMsg
	tracker := newOffsetTracker()

	msg := propagationMsg{TXID: "X", RawTx: []byte{0xde, 0xad}}

	first := handleAdmit(msg, 100, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
	if !first.admitted || first.held || first.duplicate {
		t.Fatalf("first admit should be admitted; got %+v", first)
	}
	if got := inFlight["X"]; got != 100 {
		t.Fatalf("inFlight[X] should be 100 after first admit; got %d", got)
	}
	if len(pendingMsgs) != 1 {
		t.Fatalf("pendingMsgs should have 1 entry after first admit; got %d", len(pendingMsgs))
	}

	second := handleAdmit(msg, 200, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
	if !second.duplicate || second.admitted || second.held {
		t.Fatalf("second admit should be duplicate; got %+v", second)
	}
	if got := inFlight["X"]; got != 100 {
		t.Fatalf("inFlight[X] must still point at the original offset 100; got %d", got)
	}
	if len(pendingMsgs) != 1 {
		t.Fatalf("pendingMsgs should still have 1 entry after duplicate; got %d", len(pendingMsgs))
	}
	if low, ok := tracker.LowestUnfinished(); !ok || low != 100 {
		t.Fatalf("LowestUnfinished must remain 100 while original is in-flight; got (%d, %v)", low, ok)
	}

	// Terminalize the original. Both offsets should now be free —
	// the duplicate's offset was already marked done at admit time,
	// and the original's Done call here clears the last anchor.
	tracker.Done(inFlight["X"])
	if !tracker.Empty() {
		t.Fatal("tracker must be empty after the original terminalizes; the duplicate's offset must not strand")
	}
}

// TestRequeueAfterDelay_PendingRequeuesGauge pins the Inc/Dec contract on
// PropagationPendingRequeues. The gauge needs to reflect parked
// requeue goroutines so a sustained upstream outage shows up in
// dashboards. Without the dec on early-exit via ctx.Done, the gauge
// would drift up forever after every shutdown / claim revocation.
func TestRequeueAfterDelay_PendingRequeuesGauge(t *testing.T) {
	p, cleanup := newPropagatorForDepTest(t, &mockStore{})
	t.Cleanup(cleanup)

	quiesceRequeues(t)
	startVal := testutil.ToFloat64(metrics.PropagationPendingRequeues)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	msgs := []propagationMsg{{TXID: "a", RawTx: []byte{0x01}}}
	p.requeueAfterDelay(ctx, msgs, p.defaultIO)

	if got := testutil.ToFloat64(metrics.PropagationPendingRequeues); got != startVal+1 {
		t.Fatalf("after requeueAfterDelay gauge = %v, want %v (inc by 1)", got, startVal+1)
	}

	// Cancel context — the goroutine bails before the timer fires and
	// the defer must dec the gauge back. Poll for ≤500ms so a slow
	// scheduler doesn't make the test flake.
	cancel()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(metrics.PropagationPendingRequeues) == startVal {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := testutil.ToFloat64(metrics.PropagationPendingRequeues); got != startVal {
		t.Fatalf("after ctx.Done gauge = %v, want %v (dec back to baseline) — defer is not reconciling the goroutine exit", got, startVal)
	}
}

// TestRequeueAfterDelay_EmptyMsgs_NoGaugeChange ensures the cheap
// early-return path (no msgs) does not perturb the gauge — important
// because the dispatcher path can call requeueAfterDelay with an empty
// slice and we shouldn't pollute the metric in that case.
func TestRequeueAfterDelay_EmptyMsgs_NoGaugeChange(t *testing.T) {
	p, cleanup := newPropagatorForDepTest(t, &mockStore{})
	t.Cleanup(cleanup)

	quiesceRequeues(t)
	startVal := testutil.ToFloat64(metrics.PropagationPendingRequeues)
	p.requeueAfterDelay(t.Context(), nil, p.defaultIO)
	if got := testutil.ToFloat64(metrics.PropagationPendingRequeues); got != startVal {
		t.Fatalf("empty-msgs early-return mutated gauge: got %v, want %v", got, startVal)
	}
}
