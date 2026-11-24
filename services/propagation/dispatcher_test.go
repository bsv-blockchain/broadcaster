package propagation

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
)

// These tests pin the dispatcher's Kafka offset accounting: every offset
// Add'ed to the offsetTracker on admission must be matched by a Done on
// the tx's terminal outcome, or LowestUnfinished() — the commit
// watermark — pins and the bsv-broadcaster-propagation consumer group never
// commits (it shows "-" in kafka-consumer-groups --describe and re-reads
// the whole topic on every restart).
//
// Two layers of coverage:
//
//   - Pure-function tests call handleAdmit / handleTerminal /
//     handleRequeue / cascadeReject directly against an offsetTracker so
//     the Add/Done bookkeeping is asserted in isolation.
//
//   - Integration tests drive a real runDispatcher loop against a
//     fakeClaim and assert claim.MarkMessage — the actual Kafka commit
//     signal — fires for terminalized offsets. These are the regression
//     guard for the applyTerminalStatuses offset-leak: a tx replayed
//     from Kafka after a rebalance is already at its terminal status, so
//     the store reports a lattice no-op; the dispatcher must still be
//     told so it releases the offset.

// --- pure-function offset accounting -------------------------------------

// dispatcherState is the bundle of maps runDispatcher owns as locals.
// The handle* functions take them as explicit params, so a test can
// build the bundle, drive a sequence of calls, and inspect the tracker.
type dispatcherState struct {
	inFlight    map[string]int64
	waiters     map[string]map[string]struct{}
	heldMsgs    map[string]propagationMsg
	pendingMsgs []propagationMsg
	tracker     *offsetTracker
}

func newDispatcherState() *dispatcherState {
	return &dispatcherState{
		inFlight: make(map[string]int64),
		waiters:  make(map[string]map[string]struct{}),
		heldMsgs: make(map[string]propagationMsg),
		tracker:  newOffsetTracker(),
	}
}

func (s *dispatcherState) admit(txid string, offset int64, parents ...string) admitResult {
	return handleAdmit(
		propagationMsg{TXID: txid, RawTx: []byte{0x01}, InputTXIDs: parents},
		offset, s.inFlight, s.waiters, s.heldMsgs, &s.pendingMsgs, s.tracker,
	)
}

func (s *dispatcherState) terminal(txid string, status models.Status) terminalResult {
	return handleTerminal(
		terminalEvent{txid: txid, status: status},
		s.inFlight, s.waiters, s.heldMsgs, &s.pendingMsgs, s.tracker,
	)
}

// TestDispatcherOffsets_AdmitThenTerminal_ReleasesWatermark is the
// happy path: a tx admitted at offset N pins the watermark at N until
// it terminalizes, then the tracker drains.
func TestDispatcherOffsets_AdmitThenTerminal_ReleasesWatermark(t *testing.T) {
	s := newDispatcherState()

	s.admit("a", 10)
	if low, ok := s.tracker.LowestUnfinished(); !ok || low != 10 {
		t.Fatalf("after admit, LowestUnfinished = (%d, %v), want (10, true)", low, ok)
	}

	s.terminal("a", models.StatusAcceptedByNetwork)
	if !s.tracker.Empty() {
		t.Fatal("tracker must be empty after the only in-flight tx terminalizes — offset stranded")
	}
	if _, ok := s.inFlight["a"]; ok {
		t.Fatal("terminalized tx must be removed from inFlight")
	}
}

// TestDispatcherOffsets_RejectedTerminal_DonesOffset confirms the
// REJECTED branch of handleTerminal also Dones the offset, not just the
// ACCEPTED branch.
func TestDispatcherOffsets_RejectedTerminal_DonesOffset(t *testing.T) {
	s := newDispatcherState()
	s.admit("bad", 42)
	s.terminal("bad", models.StatusRejected)
	if !s.tracker.Empty() {
		t.Fatal("REJECTED terminal must Done the offset; tracker should be empty")
	}
}

// TestDispatcherOffsets_OutOfOrderTerminal_PinsAtLowest verifies the
// watermark holds at the lowest un-terminalized offset even when later
// offsets terminalize first — txs finish in broadcast order, not Kafka
// order, and committing past an un-terminalized offset would lose it
// on a crash.
func TestDispatcherOffsets_OutOfOrderTerminal_PinsAtLowest(t *testing.T) {
	s := newDispatcherState()
	s.admit("a", 1)
	s.admit("b", 2)
	s.admit("c", 3)

	s.terminal("c", models.StatusAcceptedByNetwork)
	s.terminal("b", models.StatusAcceptedByNetwork)
	if low, ok := s.tracker.LowestUnfinished(); !ok || low != 1 {
		t.Fatalf("watermark must hold at offset 1 while 'a' is in-flight; got (%d, %v)", low, ok)
	}

	s.terminal("a", models.StatusAcceptedByNetwork)
	if !s.tracker.Empty() {
		t.Fatal("tracker must drain once the lowest offset terminalizes")
	}
}

// TestDispatcherOffsets_Requeue_KeepsOffsetPinned is the contract that
// makes the requeue path safe: a requeue must NOT Done the offset. The
// tx is still in-flight (it'll be re-broadcast), so its offset has to
// keep pinning the watermark until it reaches a real terminal verdict.
// Done'ing it on requeue would commit past an un-broadcast tx and lose
// it on a crash.
func TestDispatcherOffsets_Requeue_KeepsOffsetPinned(t *testing.T) {
	s := newDispatcherState()
	s.admit("r", 7)
	s.pendingMsgs = nil // simulate the drain into a broadcasting batch

	handleRequeue(
		propagationMsg{TXID: "r", RawTx: []byte{0x01}},
		s.inFlight, s.waiters, s.heldMsgs, &s.pendingMsgs,
	)
	if low, ok := s.tracker.LowestUnfinished(); !ok || low != 7 {
		t.Fatalf("requeue must leave the offset pinned; LowestUnfinished = (%d, %v), want (7, true)", low, ok)
	}
	if len(s.pendingMsgs) != 1 {
		t.Fatalf("requeue should put the tx back on pendingMsgs; got %d", len(s.pendingMsgs))
	}

	// The requeued tx eventually terminalizes — only then does the
	// offset release.
	s.terminal("r", models.StatusAcceptedByNetwork)
	if !s.tracker.Empty() {
		t.Fatal("offset must release once the requeued tx finally terminalizes")
	}
}

// TestDispatcherOffsets_CascadeReject_DonesEveryDescendant verifies a
// rejected parent releases the offsets of every cascade-rejected
// descendant. Those children never broadcast, so cascadeReject is their
// only path to Done — miss one and its offset pins forever.
func TestDispatcherOffsets_CascadeReject_DonesEveryDescendant(t *testing.T) {
	s := newDispatcherState()

	// parent admitted and broadcasting; child + grandchild held behind it.
	if r := s.admit("parent", 1); !r.admitted {
		t.Fatalf("parent should admit; got %+v", r)
	}
	s.pendingMsgs = nil
	if r := s.admit("child", 2, "parent"); !r.held {
		t.Fatalf("child should be held behind parent; got %+v", r)
	}
	if r := s.admit("grandchild", 3, "child"); !r.held {
		t.Fatalf("grandchild should be held behind child; got %+v", r)
	}

	res := s.terminal("parent", models.StatusRejected)
	if len(res.cascaded) != 2 {
		t.Fatalf("rejecting parent should cascade to 2 descendants; got %v", res.cascaded)
	}
	if !s.tracker.Empty() {
		t.Fatal("cascadeReject must Done the offset of every descendant; tracker should be empty")
	}
}

// TestDispatcherOffsets_HeldTxReleasedThenTerminal verifies a held tx's
// offset survives the held→pending release and is only Done'd when the
// released tx itself terminalizes.
func TestDispatcherOffsets_HeldTxReleasedThenTerminal(t *testing.T) {
	s := newDispatcherState()
	s.admit("parent", 1)
	s.pendingMsgs = nil
	s.admit("child", 2, "parent")

	// child is held; its offset 2 is in-flight behind parent's offset 1.
	if low, _ := s.tracker.LowestUnfinished(); low != 1 {
		t.Fatalf("watermark should be 1 (parent); got %d", low)
	}

	// parent ACCEPTED releases child into pendingMsgs but does NOT
	// terminalize the child — child's offset 2 must still pin.
	s.terminal("parent", models.StatusAcceptedByNetwork)
	if low, ok := s.tracker.LowestUnfinished(); !ok || low != 2 {
		t.Fatalf("after parent ACCEPTED, watermark should move to child's offset 2; got (%d, %v)", low, ok)
	}

	s.terminal("child", models.StatusAcceptedByNetwork)
	if !s.tracker.Empty() {
		t.Fatal("tracker must drain once the released child terminalizes")
	}
}

// --- runDispatcher integration: the commit watermark --------------------

// fakeClaim is a kafka.Claim that feeds messages from a channel and
// records every offset MarkMessage is called for. MarkMessage is the
// real Kafka commit-watermark signal, so a test can assert the
// dispatcher actually advanced the watermark.
type fakeClaim struct {
	ch chan *kafka.Message
	// a kafka.Claim must expose Context(); fakeClaim has to store the
	// claim context to return it, exactly as the real saramaClaim does.
	ctx context.Context //nolint:containedctx // satisfies the kafka.Claim interface

	mu     sync.Mutex
	marked map[int64]bool
}

func newFakeClaim(ctx context.Context) *fakeClaim {
	return &fakeClaim{
		ch:     make(chan *kafka.Message, 64),
		ctx:    ctx,
		marked: make(map[int64]bool),
	}
}

func (c *fakeClaim) Messages() <-chan *kafka.Message { return c.ch }
func (c *fakeClaim) Context() context.Context        { return c.ctx }

func (c *fakeClaim) MarkMessage(m *kafka.Message) {
	c.mu.Lock()
	c.marked[m.Offset] = true
	c.mu.Unlock()
}

func (c *fakeClaim) isMarked(offset int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.marked[offset]
}

var _ kafka.Claim = (*fakeClaim)(nil)

// runDispatcherWithClaim swaps out the test-mode dispatcher New() starts
// and runs the production runDispatcher loop against a fakeClaim
// instead, so a test can feed real Kafka messages and observe the
// commit watermark. Returns the claim and a stop func.
func runDispatcherWithClaim(t *testing.T, p *Propagator) (*fakeClaim, func()) {
	t.Helper()
	// The test-mode and production dispatcher loops share p.defaultIO
	// and must never run concurrently — cancel the test-mode one
	// New() spawned and wait for it to exit first. The production loop
	// reuses defaultIO so the batch-pipeline helpers tests reach through
	// the Propagator keep routing to this dispatcher.
	p.dispatcherCancel()
	<-p.dispatcherDone
	p.dispatcherCancel = nil

	claimCtx, cancel := context.WithCancel(context.Background())
	claim := newFakeClaim(claimCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = p.runDispatcher(claimCtx, claim, dispatcherConfig{maxPending: 1000}, p.defaultIO)
	}()
	return claim, func() {
		cancel()
		<-done
		p.WaitForBatches()
	}
}

// waitForMark polls until claim has marked offset, or fails after ~3s.
func waitForMark(t *testing.T, claim *fakeClaim, offset int64, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if claim.isMarked(offset) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("offset %d was never MarkMessage'd: %s", offset, msg)
}

// TestRunDispatcher_TerminalTx_AdvancesCommitWatermark is the happy
// path / harness sanity check: a tx that broadcasts and is accepted
// has its Kafka offset committed.
func TestRunDispatcher_TerminalTx_AdvancesCommitWatermark(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, newMockStore())
	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 11, Value: makePropMsg("fresh-tx")}
	waitForMark(t, claim, 11, "an accepted tx must advance the commit watermark")
}

// TestRunDispatcher_AlreadyTerminalTx_AdvancesCommitWatermark is the
// core offset-leak regression test. A tx replayed from Kafka after a
// rebalance is already at its terminal status, so
// BatchUpdateStatusReturning reports a lattice no-op (prev.Status ==
// new.Status). Pre-fix, applyTerminalStatuses filtered those out of the
// dispatcher notification, so handleTerminal never ran, the offset was
// never Done'd, and LowestUnfinished() pinned forever — the
// bsv-broadcaster-propagation group's committed offset stuck at "-".
func TestRunDispatcher_AlreadyTerminalTx_AdvancesCommitWatermark(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	// Every status write is reported as a lattice no-op: the row is
	// already at exactly the status being written.
	ms.returningPrev = func(s *models.TransactionStatus) *models.TransactionStatus {
		return &models.TransactionStatus{
			TxID:      s.TxID,
			Status:    s.Status,
			Timestamp: time.Now().Add(-time.Hour),
		}
	}

	p := newPropagator("", teranodeSrv.URL, ms)
	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 7, Value: makePropMsg("replayed-mined-tx")}
	waitForMark(t, claim, 7,
		"a lattice-no-op terminal must still notify the dispatcher — otherwise the commit watermark pins forever")
}

// TestRunDispatcher_ReapedRowTerminal_AdvancesCommitWatermark covers the
// other skipped path: BatchUpdateStatusReturning returns a nil prev row
// (the txid is unknown — the status row was reaped between RECEIVED and
// broadcast). The offset must still be released.
func TestRunDispatcher_ReapedRowTerminal_AdvancesCommitWatermark(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	ms.returningPrev = func(*models.TransactionStatus) *models.TransactionStatus {
		return nil // unknown / reaped row
	}

	p := newPropagator("", teranodeSrv.URL, ms)
	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 5, Value: makePropMsg("reaped-row-tx")}
	waitForMark(t, claim, 5,
		"a terminal for a reaped (nil-prev) row must still notify the dispatcher")
}

// --- claim revocation: fail fast, leave the batch uncommitted ------------

// TestRunDispatcher_ClaimRevokedMidBatch_FailsFastAndLeavesUncommitted pins
// the fail-fast contract from the 2026-08-10 load test (>1,900 TPS macro-
// batch cycles overran the consumer-group session and the broker revoked the
// claim mid-batch): when the claim ctx dies while a batch is mid-pipeline,
// the batch must stop where it stands — no teranode submit under the dead
// ctx, no 2s-parked requeue goroutine, no store writes — and the tx's Kafka
// offset must stay unmarked so the next claim replays it. Pre-fix the batch
// dragged the dead ctx through register → requeue → broadcast: every /watch
// "failure" counted as register_error (39,189 in one observed batch), the
// requeue parked 2s and dropped, and retry batches logged a misleading
// "batch propagated" accepted=0.
func TestRunDispatcher_ClaimRevokedMidBatch_FailsFastAndLeavesUncommitted(t *testing.T) {
	// A merkle server that parks the first /watch call until released lets
	// the test revoke the claim while the batch is provably mid-
	// registerBatch. The cancel aborts the ctx-bound client call with
	// context.Canceled even though the handler stays parked.
	registerStarted := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		startedOnce.Do(func() { close(registerStarted) })
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	teranodeLog := &eventLog{}
	teranodeSrv := newTeranodeServer(teranodeLog, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	quiesceRequeues(t)
	revokedBefore := testutil.ToFloat64(metrics.PropagationClaimRevokedBatchesTotal)
	requeuesBefore := testutil.ToFloat64(metrics.PropagationPendingRequeues)
	claimRevokedBefore := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("claim_revoked"))
	registerErrBefore := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("register_error"))
	merkleRetryBefore := testutil.ToFloat64(metrics.PropagationRetryTotal.WithLabelValues("merkle"))
	networkRetryBefore := testutil.ToFloat64(metrics.PropagationRetryTotal.WithLabelValues("network"))

	claim, stop := runDispatcherWithClaim(t, p)
	claim.ch <- &kafka.Message{Offset: 21, Value: makePropMsg("revoked-mid-batch-tx")}

	select {
	case <-registerStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("batch never reached merkle /watch")
	}

	// Revoke the claim while /watch is in flight. stop() cancels claimCtx,
	// waits for the dispatcher loop to exit AND for the processBatch
	// goroutine to unwind (WaitForBatches), so every assertion below runs
	// against a fully-settled aborted batch.
	stop()
	close(release) // unpark the handler so merkleSrv.Close() doesn't hang

	if got := teranodeLog.count("broadcast"); got != 0 {
		t.Errorf("no teranode submit may run under a revoked claim; got %d", got)
	}
	if claim.isMarked(21) {
		t.Error("the tx's offset must stay unmarked so the next claim replays it")
	}
	if got := ms.updateCount(); got != 0 {
		t.Errorf("no status writes may land for an aborted batch; got %d", got)
	}
	if got := ms.markCount(); got != 0 {
		t.Errorf("no merkle-registered marks may be written for an aborted batch; got %d", got)
	}
	if d := testutil.ToFloat64(metrics.PropagationClaimRevokedBatchesTotal) - revokedBefore; d != 1 {
		t.Errorf("claim-revoked batch counter delta = %v, want 1", d)
	}
	if d := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("claim_revoked")) - claimRevokedBefore; d != 1 {
		t.Errorf("claim_revoked register-failure delta = %v, want 1 (the parked /watch collapsed with context canceled)", d)
	}
	if d := testutil.ToFloat64(metrics.PropagationMerkleRegisterFailures.WithLabelValues("register_error")) - registerErrBefore; d != 0 {
		t.Errorf("a rebalance must not pollute register_error; delta = %v", d)
	}
	if d := testutil.ToFloat64(metrics.PropagationPendingRequeues) - requeuesBefore; d != 0 {
		t.Errorf("no delayed-requeue goroutine may park under a revoked claim; gauge delta = %v", d)
	}
	if d := testutil.ToFloat64(metrics.PropagationRetryTotal.WithLabelValues("merkle")) - merkleRetryBefore; d != 0 {
		t.Errorf("claim revocation must not charge the merkle retry budget; delta = %v", d)
	}
	if d := testutil.ToFloat64(metrics.PropagationRetryTotal.WithLabelValues("network")) - networkRetryBefore; d != 0 {
		t.Errorf("claim revocation must not charge the network retry budget; delta = %v", d)
	}
}

// TestProcessBatch_ClaimRevokedBeforeStart_NoSideEffects covers the other
// revocation window: the dispatcher's flush tick detaches the processBatch
// goroutine, so the claim can already be dead before the batch STARTS. The
// entry checkpoint must stop it before any merkle /watch or teranode submit
// is even attempted, leaving the store untouched and the offsets uncommitted
// for replay.
func TestProcessBatch_ClaimRevokedBeforeStart_NoSideEffects(t *testing.T) {
	httpLog := &eventLog{}
	merkleSrv := newMerkleServer(httpLog, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(httpLog, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	quiesceRequeues(t)
	revokedBefore := testutil.ToFloat64(metrics.PropagationClaimRevokedBatchesTotal)
	requeuesBefore := testutil.ToFloat64(metrics.PropagationPendingRequeues)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the claim died before the detached batch goroutine ran

	p.processBatch(ctx, []propagationMsg{
		{TXID: "revoked-1", RawTx: []byte{0x01}},
		{TXID: "revoked-2", RawTx: []byte{0x02}},
	}, p.defaultIO)

	if got := httpLog.count("register:"); got != 0 {
		t.Errorf("no merkle /watch call may run under a revoked claim; got %d", got)
	}
	if got := httpLog.count("broadcast"); got != 0 {
		t.Errorf("no teranode submit may run under a revoked claim; got %d", got)
	}
	if got := ms.updateCount(); got != 0 {
		t.Errorf("store must stay untouched (txs left for replay); got %d status writes", got)
	}
	if got := ms.markCount(); got != 0 {
		t.Errorf("no merkle-registered marks may be written; got %d", got)
	}
	if d := testutil.ToFloat64(metrics.PropagationClaimRevokedBatchesTotal) - revokedBefore; d != 1 {
		t.Errorf("claim-revoked batch counter delta = %v, want 1", d)
	}
	if d := testutil.ToFloat64(metrics.PropagationPendingRequeues) - requeuesBefore; d != 0 {
		t.Errorf("no delayed-requeue goroutine may park; gauge delta = %v", d)
	}
}

// --- store-error guard: offsets must not release on a failed write ------

// TestApplyTerminalStatuses_StoreError_DoesNotReleaseOffset is the
// at-least-once guard. When BatchUpdateStatusReturning fails the terminal
// status did not durably persist, so the dispatcher must NOT be notified —
// otherwise the tx's Kafka offset would be marked Done and the commit
// watermark would advance past a tx whose status never reached the store.
// The tx must stay in-flight so the offset stays uncommitted and the next
// claim replays it.
func TestApplyTerminalStatuses_StoreError_DoesNotReleaseOffset(t *testing.T) {
	ms := newMockStore()
	ms.returningErr = errors.New("store write failed")
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	// Admit a tx and move it onto the broadcasting path.
	if res := p.admitToDispatcher(propagationMsg{TXID: "x", RawTx: []byte{0x01}}, 5); !res.admitted {
		t.Fatalf("first admit should be admitted; got %+v", res)
	}
	_ = p.drainPending()

	// Terminalize it — but the store write fails.
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "x", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0, p.defaultIO)

	// The dispatcher must NOT have been notified: "x" is still in flight,
	// so a re-admission is detected as a duplicate (offset bookkept, not a
	// fresh admit). Had the notify fired, "x" would be gone from inFlight
	// and this would come back admitted.
	if res := p.admitToDispatcher(propagationMsg{TXID: "x", RawTx: []byte{0x01}}, 6); !res.duplicate {
		t.Fatalf("after a failed status write the tx must stay in-flight (offset uncommitted for replay); re-admit got %+v, want duplicate", res)
	}
}

// TestApplyTerminalStatuses_StoreOK_ReleasesOffset is the positive control:
// a successful status write DOES notify the dispatcher, so the tx leaves
// the in-flight set and a re-admission is a fresh admit.
func TestApplyTerminalStatuses_StoreOK_ReleasesOffset(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if res := p.admitToDispatcher(propagationMsg{TXID: "y", RawTx: []byte{0x01}}, 5); !res.admitted {
		t.Fatalf("first admit should be admitted; got %+v", res)
	}
	_ = p.drainPending()

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "y", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0, p.defaultIO)

	if res := p.admitToDispatcher(propagationMsg{TXID: "y", RawTx: []byte{0x01}}, 6); !res.admitted {
		t.Fatalf("after a successful terminal write the tx must be released from in-flight; re-admit got %+v, want admitted", res)
	}
}
