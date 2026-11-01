package bump_builder

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
)

// Anchor-reconciler tests (issue #279) run against a REAL pebble store so
// the whole re-anchor/revert path — block-hash index maintenance, orphaned-
// anchor history, reconciled_at bookkeeping, crash-resume idempotency — is
// exercised end to end, not mocked.

const (
	recOrphan    = "0d6be57a5be1776686ee44a4c93623940463ec598104734a723d66c43efc5100"
	recCanonical = "7abcd229ab91a477e553deb8534d64a756410b6210af438b5b8c86eb4b5d6200"
	recNeighbor  = "13cd9751d86c0831b029719d681c17683960a5a54de4c931a943695e86f1e200"

	recShared1 = "624b7c58572e84d651f778a6142adb359e74786f12540c52cf39ff1ebc2dd700"
	recShared2 = "40626a72baa61c5e9cad3a3b15e9e32f29ebb8760001cfe7707afa798d9dc900"
	recAOnly   = "adba6f6c1840ccf5c17afdaec99c7467a525a951505bbcd34ad198a3fcfd8d00"
	recBOnly   = "65bbf60c689560769e4cbc7fcb65a2a2c2a67c789c6fee4502ce70ad11427400"
	recRebin   = "07722a1642bf1d059dfebb0b3a0200c4c600acdbc6c622b008e11a2f40fd7100"
)

func newPebbleForTest(t *testing.T) store.Store {
	t.Helper()
	st, err := pebble.New(config.Pebble{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// makeCompoundForTest builds a parseable BRC-74 compound with every txid as
// a flagged level-0 leaf. Root validity doesn't matter here — the
// reconciler only parses leaves and tests membership.
func makeCompoundForTest(t *testing.T, blockHeight uint32, txids ...string) []byte {
	t.Helper()
	leaves := make([]*transaction.PathElement, 0, len(txids))
	isTxid := true
	for i, id := range txids {
		h, err := sdkchainhash.NewHashFromHex(id)
		if err != nil {
			t.Fatalf("parse txid %s: %v", id, err)
		}
		leaves = append(leaves, &transaction.PathElement{
			Offset: uint64(i),
			Hash:   h,
			Txid:   &isTxid,
		})
	}
	return transaction.NewMerklePath(blockHeight, [][]*transaction.PathElement{leaves}).Bytes()
}

// seedMined registers rows and anchors them to blockHash at height.
func seedMined(t *testing.T, st store.Store, blockHash string, height uint64, txids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range txids {
		if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: id, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	if _, _, err := st.SetMinedByTxIDs(ctx, blockHash, height, txids); err != nil {
		t.Fatalf("seed mined: %v", err)
	}
}

func seedSeen(t *testing.T, st store.Store, txids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range txids {
		if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: id, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

func newTestReconciler(st store.Store, pub *capturePublisher, stub *stubChaintracks, tweak func(*config.ReconcilerConfig)) *Reconciler {
	cfg := &config.Config{BumpBuilder: config.BumpBuilderConfig{
		AnchorGuardEnabled: true,
		Reconciler: config.ReconcilerConfig{
			Enabled:           true,
			IntervalMs:        50,
			BatchSize:         100,
			BlocksPerTick:     10,
			NeighborhoodDepth: 6,
			MaxDeferAttempts:  3,
		},
	}}
	if tweak != nil {
		tweak(&cfg.BumpBuilder.Reconciler)
	}
	return &Reconciler{
		cfg:           cfg,
		logger:        zap.NewNop(),
		store:         st,
		publisher:     pub,
		chainHeader:   stub,
		defers:        make(map[string]int),
		rewatched:     make(map[string]time.Time),
		pendingRemine: make(map[string]uint64),
		now:           time.Now,
		done:          make(chan struct{}),
	}
}

func statusOf(t *testing.T, st store.Store, txid string) *models.TransactionStatus {
	t.Helper()
	got, err := st.GetStatus(context.Background(), txid)
	if err != nil || got == nil {
		t.Fatalf("GetStatus %s: %v (nil=%v)", txid, err, got == nil)
	}
	return got
}

// TestReconciler_MixedReanchorAndRevert is the incident in miniature: the
// orphan's shared txs re-anchor to the canonical block (whose stored BUMP
// is the fuel), the guard-denied canonical-only tx finally mines, and the
// orphan-only tx reverts to SEEN_ON_NETWORK — with corrected events,
// orphaned-anchor history, STUMP cleanup, and a reconciled_at stamp. A
// second pass proves idempotency (crash-resume contract).
func TestReconciler_MixedReanchorAndRevert(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	// State as the reorg left it: shared+bOnly MINED@orphan, aOnly SEEN
	// (the write guard refused it while the canonical block was an
	// alternate), canonical BUMP stored, orphan row queued.
	seedMined(t, st, recOrphan, 10, recShared1, recShared2, recBOnly)
	seedSeen(t, st, recAOnly)
	if err := st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1, recShared2, recAOnly)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	if err := st.InsertStump(ctx, &models.Stump{BlockHash: recOrphan, SubtreeIndex: 0, StumpData: []byte{0x01}}); err != nil {
		t.Fatalf("insert stump: %v", err)
	}
	if err := st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()); err != nil {
		t.Fatalf("upsert orphan row: %v", err)
	}
	if _, err := st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now()); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	// Shared txs re-anchored with the orphan preserved as history.
	for _, id := range []string{recShared1, recShared2} {
		got := statusOf(t, st, id)
		if got.Status != models.StatusMined || got.BlockHash != recCanonical {
			t.Fatalf("%s: want MINED@%s, got %s@%s", id, recCanonical, got.Status, got.BlockHash)
		}
		if len(got.OrphanedProofs) != 1 || got.OrphanedProofs[0].BlockHash != recOrphan {
			t.Fatalf("%s: want orphaned-anchor history [%s], got %+v", id, recOrphan, got.OrphanedProofs)
		}
	}
	// The guard-denied canonical-only tx finally mined.
	if got := statusOf(t, st, recAOnly); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("aOnly: want MINED@%s, got %s@%s", recCanonical, got.Status, got.BlockHash)
	}
	// The orphan-only tx reverted, history preserved, block fields cleared.
	if got := statusOf(t, st, recBOnly); got.Status != models.StatusSeenOnNetwork || got.BlockHash != "" ||
		len(got.OrphanedProofs) != 1 || got.OrphanedProofs[0].BlockHash != recOrphan {
		t.Fatalf("bOnly: want reverted SEEN with history, got %+v", got)
	}
	// STUMPs pruned; the orphan's BUMP retained (historical proofs).
	if stumps, _ := st.GetStumpsByBlockHash(ctx, recOrphan); len(stumps) != 0 {
		t.Fatalf("orphan STUMPs must be pruned, got %d", len(stumps))
	}
	// Row stamped reconciled and off the queue.
	if rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 0 {
		t.Fatalf("queue must be empty after reconcile: rows=%v err=%v", rows, err)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.ReconciledAt == nil {
		t.Fatalf("reconciled_at must be stamped, got %+v err=%v", bp, err)
	}

	// Corrected events: one MINED bulk (re-anchor marker, canonical block,
	// covering shared+aOnly) and one SEEN bulk (revert marker, bOnly).
	var minedEv, seenEv *models.TransactionStatus
	for _, ev := range pub.bulkEvents() {
		switch ev.ExtraInfo {
		case models.ExtraInfoReorgReanchor:
			minedEv = ev
		case models.ExtraInfoReorgUnmined:
			seenEv = ev
		}
	}
	if minedEv == nil || minedEv.BlockHash != recCanonical || len(minedEv.TxIDs) != 3 {
		t.Fatalf("re-anchor event wrong: %+v", minedEv)
	}
	if seenEv == nil || len(seenEv.TxIDs) != 1 || seenEv.TxIDs[0] != recBOnly {
		t.Fatalf("revert event wrong: %+v", seenEv)
	}

	// Idempotency: a second tick finds an empty queue and publishes nothing.
	before := len(pub.bulkEvents())
	r.tick(ctx)
	if after := len(pub.bulkEvents()); after != before {
		t.Fatalf("second tick must be a no-op, events %d → %d", before, after)
	}
}

// TestReconciler_ResurrectedBlockResetsRow: a stale orphan mark on a block
// that IS the active-chain block at its height resets the row to active
// without touching transactions.
func TestReconciler_ResurrectedBlockResetsRow(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10)) // the "orphan" is actually active

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("resurrected block's txs must be untouched, got %s@%s", got.Status, got.BlockHash)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must be reset to active with orphan/reconcile marks cleared, got %+v err=%v", bp, err)
	}
}

// TestReconciler_DefersUntilCanonicalBUMPArrives: no canonical BUMP yet →
// the orphan stays queued and its txs untouched; once the BUMP lands the
// next tick heals.
func TestReconciler_DefersUntilCanonicalBUMPArrives(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, st, recShared1); got.BlockHash != recOrphan {
		t.Fatalf("deferred orphan's txs must be untouched, got anchored to %s", got.BlockHash)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
		t.Fatalf("deferred orphan must stay queued, got %d rows", len(rows))
	}

	if err := st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	r.tick(ctx)
	if got := statusOf(t, st, recShared1); got.BlockHash != recCanonical {
		t.Fatalf("post-BUMP tick must re-anchor, got %s", got.BlockHash)
	}
}

// TestReconciler_DeferCapFallsBackToRevert: the OPT-IN legacy fallback
// (RevertWhenUnreconcilable) — when the canonical BUMP never arrives, the
// defer cap converts the orphan to revert-all. Default behavior is now to
// park instead (see TestReconciler_DeferCapParksByDefault, issue #282).
func TestReconciler_DeferCapFallsBackToRevert(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.MaxDeferAttempts = 1
		c.RevertWhenUnreconcilable = true // opt into the legacy revert-all fallback
	})
	r.tick(ctx) // defer 1/1
	r.tick(ctx) // cap reached → revert-all

	if got := statusOf(t, st, recShared1); got.Status != models.StatusSeenOnNetwork || got.BlockHash != "" {
		t.Fatalf("defer-cap fallback must revert, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("orphan must be reconciled after the fallback, got %d rows", len(rows))
	}
}

// TestReconciler_DeferCapParksByDefault: when the canonical BUMP never
// arrives, the DEFAULT fallback is to PARK the block (issue #282) — the txs
// stay MINED against the orphan (NOT reverted to SEEN_ON_NETWORK), no revert
// event is published, and the block leaves the queue (bounded, no infinite
// retry). Because the txs are left in place, a later stored/rebuilt canonical
// BUMP re-anchors them through the normal mine path.
func TestReconciler_DeferCapParksByDefault(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	// The canonical block at height 10 exists on-chain, but its BUMP is NOT
	// stored — the reconciler cannot prove where the txs belong.
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 1 })
	r.tick(ctx) // defer 1/1
	r.tick(ctx) // cap reached → PARK (default)

	// The tx stays MINED against the orphan — the #282 invariant: no un-mine.
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("parked orphan's tx must stay MINED@orphan, got %s@%s", got.Status, got.BlockHash)
	}
	// No revert event published.
	for _, ev := range pub.bulkEvents() {
		if ev.ExtraInfo == models.ExtraInfoReorgUnmined {
			t.Fatalf("park must not publish a revert event, got %+v", ev)
		}
	}
	// Block off the queue (bounded — no infinite retry) and stamped.
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("parked block must leave the reconcile queue, got %d rows", len(rows))
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.ReconciledAt == nil {
		t.Fatalf("parked block must be stamped reconciled_at, got %+v err=%v", bp, err)
	}

	// The txs were left in place, so once the canonical block IS processed
	// (its BUMP stored → normal mine path), they re-anchor to canonical.
	if _, _, err := st.SetMinedByTxIDs(ctx, recCanonical, 10, []string{recShared1}); err != nil {
		t.Fatalf("SetMinedByTxIDs (build-path re-anchor): %v", err)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("a later canonical BUMP must re-anchor the parked tx, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_NeighborhoodReanchor: a deep-reorg tx re-binned into a
// LATER canonical block re-anchors there via the membership check.
func TestReconciler_NeighborhoodReanchor(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))

	seedMined(t, st, recOrphan, 10, recRebin)
	// Canonical block at 10 does NOT contain the tx; the block at 11 does.
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recNeighbor || got.BlockHeight != 11 {
		t.Fatalf("re-binned tx must anchor to the neighbor block, got %s@%s h%d", got.Status, got.BlockHash, got.BlockHeight)
	}
}

// TestReconciler_FullScanDetectsStaleAnchors: the startup sweep finds an
// 'active' row whose height is held by a different block — the recovery
// path for incidents that predate the detection edges (the height-764
// scale-cluster block).
func TestReconciler_FullScanDetectsStaleAnchors(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) // still 'active' — nothing ever detected it
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.fullScan(ctx)
	r.tick(ctx)

	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("full-scan must route the stale anchor into healing, got %s@%s", got.Status, got.BlockHash)
	}
	if len(pub.bulkEvents()) == 0 {
		t.Fatal("expected corrected events from the full-scan heal")
	}
}

// TestReconciler_FullScanRespectsHorizon: the startup sweep is bounded to
// within FullScanDepth of the active tip (issue #282) — a stale off-chain
// row far below the tip is NOT re-orphaned by the default depth, but an
// explicit target range picks it up. This is what stops the deep backstop
// from grinding a long chain's whole history oldest-first and starving the
// actual recent incident.
func TestReconciler_FullScanRespectsHorizon(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}

	// Active tip at height 200 (on-chain → never flagged; sets GetActiveTip).
	stub.setHeightHeader(200, headerWithHash(t, recCanonical, 200))
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 200, time.Now())
	// A stale off-chain 'active' row far below the tip: the active chain
	// holds a DIFFERENT block at height 10.
	stub.setHeightHeader(10, headerWithHash(t, recNeighbor, 10))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())

	// Default depth (144): height 10 < tip-144 (=56) ⇒ out of horizon, so it
	// must NOT be marked.
	r := newTestReconciler(st, pub, stub, nil)
	r.fullScan(ctx)
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("horizon must skip height 10 (tip 200, depth 144), got %d queued", len(rows))
	}
	if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("out-of-horizon row must stay active, got %+v err=%v", bp, err)
	}

	// An explicit target range covering height 10 overrides the depth horizon
	// ⇒ the stale anchor is now marked orphaned and queued.
	r2 := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.FullScanMinHeight = 1
		c.FullScanMaxHeight = 50
	})
	r2.fullScan(ctx)
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].BlockHash != recOrphan {
		t.Fatalf("targeted scan must mark height 10 orphaned, got %+v err=%v", rows, err)
	}
}

// TestReconciler_FullScanResurrectsReconciledOrphan is issue #339 at the
// deep-backstop layer. The tie loser was orphaned AND reconciled — off the
// queue, so the resurrection short-circuit can never see it again — and
// then the competition flipped: it is the active-chain block at its height.
// The full-scan must reset its row to active with both marks cleared, orphan
// the competitor that still reads active in the same pass, and re-mine the
// resurrected block's txs from its retained compound BUMP so the heal does
// not depend on the competitor's row existing at all.
func TestReconciler_FullScanResurrectsReconciledOrphan(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	resurrected, competitor := recOrphan, recCanonical
	stub.setHeightHeader(10, headerWithHash(t, resurrected, 10))

	// The resurrected block's txs were reverted to SEEN when it lost the tie;
	// its compound BUMP is retained (they always are) and lists them.
	seedSeen(t, st, recShared1, recBOnly)
	if err := st.InsertBUMP(ctx, resurrected, 10, makeCompoundForTest(t, 10, recShared1, recBOnly)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = st.UpsertBlockHeaderSeen(ctx, competitor, 10, time.Now()) // still reads active
	_ = st.UpsertBlockHeaderSeen(ctx, resurrected, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{resurrected}, time.Now())
	_, _ = st.MarkBlockReconciled(ctx, resurrected, time.Time{}, time.Now()) // the trap: off the queue
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("precondition: resurrected block must be off the reconcile queue, got %d", len(rows))
	}

	r := newTestReconciler(st, pub, stub, nil)
	r.defers[resurrected] = 2
	r.fullScan(ctx)

	bp, err := st.GetBlockProcessingStatus(ctx, resurrected)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("resurrected row must be active with marks cleared, got %+v err=%v", bp, err)
	}
	if _, deferred := r.defers[resurrected]; deferred {
		t.Fatal("reactivation must clear the defer counter")
	}
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].BlockHash != competitor {
		t.Fatalf("competitor must be orphaned and queued in the same pass, got %+v err=%v", rows, err)
	}
	for _, id := range []string{recShared1, recBOnly} {
		if got := statusOf(t, st, id); got.Status != models.StatusMined || got.BlockHash != resurrected {
			t.Fatalf("%s: want MINED@%s after resurrection, got %s@%s", id, resurrected, got.Status, got.BlockHash)
		}
	}
	if len(pub.bulkEvents()) == 0 {
		t.Fatal("expected a corrected MINED event for the re-mined txs")
	}
}

// TestReconciler_FullScanReactivationRespectsHorizon: the reactivation
// direction honors the same bounds as the orphan direction — a resurrected
// row far below the tip is untouched by the default depth and picked up by
// an explicit target range (the operator lever for an old incident).
func TestReconciler_FullScanReactivationRespectsHorizon(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}

	stub.setHeightHeader(200, headerWithHash(t, recCanonical, 200))
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 200, time.Now())
	// An orphaned+reconciled row at height 10 that IS the active block there.
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = st.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())

	r := newTestReconciler(st, pub, stub, nil)
	r.fullScan(ctx)
	if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
		t.Fatalf("out-of-horizon row must stay orphaned under the default depth, got %+v err=%v", bp, err)
	}

	r2 := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.FullScanMinHeight = 1
		c.FullScanMaxHeight = 50
	})
	r2.fullScan(ctx)
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("targeted scan must reactivate the resurrected row, got %+v err=%v", bp, err)
	}
}

// hookedStore wraps the real pebble store so a test can inject a store
// failure or a concurrent writer at a precise point of the reconcile path.
// DeleteStumpsByBlockHash is the last store call before the reconciled_at
// stamp on both the healed and the parked paths, so beforeStamp runs "while
// the tick is finishing" — where the block-status tracker's reactivation
// can race it.
type hookedStore struct {
	store.Store

	failMined   bool
	beforeStamp func()
	// failGetBUMP makes every GetBUMP fail with a transient backend error —
	// distinct from "no BUMP stored", which is store.ErrNotFound.
	failGetBUMP bool
	// failGetBUMPFor fails GetBUMP for that one block only, so a test can
	// break a neighbor's read while the canonical block's succeeds.
	failGetBUMPFor string
	// beforeMined fires once at the start of SetMinedByTxIDs with the call's
	// context, so a test can inject the reorg that lands while a re-mine is
	// in flight, or hold the re-mine open and watch what the reconciler does
	// around it.
	beforeMined func(context.Context)
	// beforeReactivate fires once, right before ReactivateBlock, so a test
	// can inject the concurrent write that races a reactivation.
	beforeReactivate func()
	// orphanPartialErr makes MarkBlocksOrphaned apply its writes and then
	// report this error alongside the count that landed — the shape the
	// backends produce when a later hash or chunk fails.
	orphanPartialErr error
	// onMinedCall fires before every SetMinedByTxIDs with the 1-based call
	// index, so a test can act between two batches of one re-mine — flip
	// failMined, inject the tracker's reactivation.
	onMinedCall func(call int)
	minedCalls  int
	// failHandOffN refuses the next N hand-off writes (RequeueOrphanedBlock
	// and MarkBlocksOrphaned) before they touch the store — a store that is
	// down for single-row writes. Negative = refuse them all.
	failHandOffN int
	handOffCalls int
	// failOrphanN refuses only the next N MarkBlocksOrphaned writes, so a
	// test can let the hand-off's requeue and read succeed and refuse the
	// re-orphan itself. Negative = refuse them all.
	failOrphanN int
	// beforeGetBUMP fires once, right before the next GetBUMP, so a test can
	// inject a concurrent write between the full-scan's paging read and its
	// BUMP read.
	beforeGetBUMP func()
}

// refuseHandOff reports whether this hand-off write is refused, consuming
// one of failHandOffN.
func (h *hookedStore) refuseHandOff() bool {
	h.handOffCalls++
	if h.failHandOffN < 0 {
		return true
	}
	if h.failHandOffN > 0 {
		h.failHandOffN--
		return true
	}
	return false
}

func (h *hookedStore) MarkBlocksOrphaned(ctx context.Context, hashes []string, at time.Time) (int, error) {
	if h.refuseHandOff() {
		return 0, errors.New("injected: store refuses writes")
	}
	if h.failOrphanN < 0 || h.failOrphanN > 0 {
		if h.failOrphanN > 0 {
			h.failOrphanN--
		}
		return 0, errors.New("injected: store refuses the re-orphan")
	}
	n, err := h.Store.MarkBlocksOrphaned(ctx, hashes, at)
	if err == nil && h.orphanPartialErr != nil {
		return n, h.orphanPartialErr
	}
	return n, err
}

func (h *hookedStore) ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error) {
	if fn := h.beforeReactivate; fn != nil {
		h.beforeReactivate = nil
		fn()
	}
	return h.Store.ReactivateBlock(ctx, blockHash, blockHeight, orphanedAt)
}

// RequeueOrphanedBlock fires beforeReactivate too: both are the write a
// full-scan hand-off ends in, and a test injects the tracker's concurrent
// reactivation right before whichever one runs.
func (h *hookedStore) RequeueOrphanedBlock(ctx context.Context, blockHash string, orphanedAt time.Time) (bool, error) {
	if fn := h.beforeReactivate; fn != nil {
		h.beforeReactivate = nil
		fn()
	}
	if h.refuseHandOff() {
		return false, errors.New("injected: store refuses writes")
	}
	return h.Store.RequeueOrphanedBlock(ctx, blockHash, orphanedAt)
}

func (h *hookedStore) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	if fn := h.beforeGetBUMP; fn != nil {
		h.beforeGetBUMP = nil
		fn()
	}
	if h.failGetBUMP || (h.failGetBUMPFor != "" && blockHash == h.failGetBUMPFor) {
		return 0, nil, errors.New("injected: backend read failure")
	}
	return h.Store.GetBUMP(ctx, blockHash)
}

func (h *hookedStore) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	if fn := h.beforeMined; fn != nil {
		h.beforeMined = nil
		fn(ctx)
	}
	h.minedCalls++
	if fn := h.onMinedCall; fn != nil {
		fn(h.minedCalls)
	}
	if h.failMined {
		return nil, nil, errors.New("injected: store unavailable")
	}
	return h.Store.SetMinedByTxIDs(ctx, blockHash, blockHeight, txids)
}

func (h *hookedStore) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	if fn := h.beforeStamp; fn != nil {
		h.beforeStamp = nil
		fn()
	}
	return h.Store.DeleteStumpsByBlockHash(ctx, blockHash)
}

// TestReconciler_RemineBatchFailureKeepsRowQueued: a failed SetMinedByTxIDs
// batch while re-mining the canonical BUMP must not fall through to the
// revert (which would un-mine txs that ARE in the canonical block) or stamp
// reconciled_at. The row stays queued and the next tick heals once the
// store recovers.
func TestReconciler_RemineBatchFailureKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failMined: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, base, recOrphan, 10, recShared1, recBOnly)
	if err := base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(hs, pub, stub, nil)
	r.tick(ctx)

	for _, id := range []string{recShared1, recBOnly} {
		if got := statusOf(t, base, id); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("%s: must be untouched after a failed re-mine batch, got %s@%s", id, got.Status, got.BlockHash)
		}
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 1 {
		t.Fatalf("row must stay queued for retry, got %+v err=%v", rows, err)
	}

	hs.failMined = false
	r.tick(ctx)
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("shared: want MINED@%s after retry, got %s@%s", recCanonical, got.Status, got.BlockHash)
	}
	if got := statusOf(t, base, recBOnly); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("bOnly: want reverted SEEN after retry, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once healed, got %+v err=%v", rows, err)
	}
}

// TestReconciler_FullScanRemineFailureLeavesRowForRetry: the full-scan
// reactivates a resurrected row only once its txs are re-mined. A failed
// batch leaves the row orphaned (and off the tick's queue, as it was) so the
// next full-scan retries the whole heal.
func TestReconciler_FullScanRemineFailureLeavesRowForRetry(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failMined: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))

	seedSeen(t, base, recShared1)
	if err := base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())

	r := newTestReconciler(hs, pub, stub, nil)
	r.fullScan(ctx)
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
		t.Fatalf("row must stay orphaned when the re-mine failed, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("tx must be untouched after the failed re-mine, got %s@%s", got.Status, got.BlockHash)
	}

	hs.failMined = false
	r.fullScan(ctx)
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("retry must reactivate the row, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("retry must re-mine the tx, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_StaleReconcileDoesNotStampResurrectedRow: the block-status
// tracker reactivates the row while a tick is mid-reconcile (the tick
// observed the older competitor as canonical). The final stamp must
// recognise that the orphan generation it processed is gone and leave the
// resurrected row clean instead of writing reconciled_at onto an active row.
func TestReconciler_StaleReconcileDoesNotStampResurrectedRow(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, base, recOrphan, 10, recShared1)
	if err := base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}
	// The tracker wins the race right before the stamp: the row is active again.
	hs.beforeStamp = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }

	r := newTestReconciler(hs, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "stale" {
		t.Fatalf("outcome = %q, want stale", outcome)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("resurrected row must stay active and clean, got %+v err=%v", bp, err)
	}
}

// TestReconciler_StaleReconcileDoesNotStampReorphanedRow: the row was
// resurrected AND orphaned again (a newer generation) while the tick ran.
// The stamp for the old generation must not apply, so the new generation
// stays queued for its own reconciliation.
func TestReconciler_StaleReconcileDoesNotStampReorphanedRow(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, base, recOrphan, 10, recShared1)
	if err := base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}
	hs.beforeStamp = func() {
		_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now().Add(time.Second))
	}

	r := newTestReconciler(hs, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "stale" {
		t.Fatalf("outcome = %q, want stale", outcome)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("re-orphaned row must stay queued (unstamped), got %+v err=%v", bp, err)
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 1 {
		t.Fatalf("new generation must remain in the queue, got %+v err=%v", rows, err)
	}
}

// TestReconciler_StartupScanDeferredUntilChaintracksReady pins the fix: the
// startup full-scan must not run while the embedded chaintracks is still
// resyncing from genesis (every GetHeaderByHeight returns nil), because it
// would fail open on every row and heal nothing. It must instead self-heal on
// a later tick once chaintracks catches up to the active tip — and run at most
// once (startupScanDone). Covers (a) unready ⇒ marks nothing, (b) ready ⇒
// scans + reconciles, (d) one-shot idempotency.
func TestReconciler_StartupScanDeferredUntilChaintracksReady(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setUnready()                                             // resyncing from genesis
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10)) // the real header, revealed once ready

	// Same shape as the plain full-scan test: a stale 'active' row at height 10
	// whose canonical competitor holds the height, canonical BUMP stored so the
	// heal can re-anchor. Both rows active ⇒ active tip is 10.
	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.StartupFullScan = true
		c.FullScanChaintracksReadyTimeoutMs = 50
	})

	// (a) While chaintracks is unready the startup scan marks nothing.
	r.tick(ctx)
	if got := statusOf(t, st, recShared1); got.BlockHash != recOrphan {
		t.Fatalf("unready: stale anchor must be untouched, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("unready: nothing may be orphaned, got %d queued", len(rows))
	}
	if r.startupScanDone {
		t.Fatal("unready: startupScanDone must stay false")
	}

	// (b) Once chaintracks catches up, a later tick runs the deferred scan
	// (marking the off-chain block) and the same tick reconciles it as usual.
	stub.setReady()
	r.tick(ctx)
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("ready: stale anchor must heal to canonical, got %s@%s", got.Status, got.BlockHash)
	}
	if !r.startupScanDone {
		t.Fatal("ready: startupScanDone must be set after a successful scan")
	}
	if len(pub.bulkEvents()) == 0 {
		t.Fatal("ready: expected corrected events from the heal")
	}

	// (d) One-shot: a NEW stale row introduced afterwards is NOT swept, because
	// the startup full-scan does not re-run once startupScanDone is set.
	_ = st.UpsertBlockHeaderSeen(ctx, recNeighbor, 11, time.Now())
	stub.setHeightHeader(11, headerWithHash(t, recCanonical, 11)) // height 11 held by a DIFFERENT block
	r.tick(ctx)
	if bp, err := st.GetBlockProcessingStatus(ctx, recNeighbor); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("one-shot: post-scan stale row must stay active (scan must not re-run), got %+v err=%v", bp, err)
	}
	if !r.startupScanDone {
		t.Fatal("one-shot: startupScanDone must remain true")
	}
}

// TestReconciler_StartupScanReadinessWaitIsBounded covers (c): with chaintracks
// never becoming ready the readiness wait returns false without hanging, ctx
// cancellation aborts it, Start logs the timeout path and enters its loop
// without blocking or marking anything, and the deferred scan still self-heals
// once chaintracks finally catches up.
func TestReconciler_StartupScanReadinessWaitIsBounded(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setUnready() // never becomes ready during the wait phase

	// Active tip at 10 so readiness is a real comparison (unready ⇒ nil header).
	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.StartupFullScan = true
		c.FullScanChaintracksReadyTimeoutMs = 100
	})

	// The wait is bounded: never-ready ⇒ false, and it does not hang.
	start := time.Now()
	if r.waitForChaintracksReady(ctx) {
		t.Fatal("wait must report not-ready when chaintracks never syncs")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("readiness wait must be bounded, took %s", elapsed)
	}

	// ctx cancellation short-circuits the wait.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if r.waitForChaintracksReady(cctx) {
		t.Fatal("canceled ctx must abort the wait as not-ready")
	}

	// A timed-out initial wait is NOT fatal: Start logs and enters its loop
	// without hanging, and marks nothing while chaintracks stays unready.
	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Start(runCtx) }()
	time.Sleep(300 * time.Millisecond) // well past the 100ms readiness timeout
	runCancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after ctx cancel — the readiness wait hung")
	}
	if r.startupScanDone {
		t.Fatal("scan must not have run while chaintracks stayed unready")
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("nothing may be orphaned while unready, got %d queued", len(rows))
	}

	// Self-heal: once chaintracks catches up a later tick runs the deferred
	// scan exactly once and heals.
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())
	stub.setReady()
	r.tick(ctx)
	if !r.startupScanDone {
		t.Fatal("self-heal: startupScanDone must be set once the deferred scan runs")
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("self-heal: deferred scan must eventually heal, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_FullScanBUMPReadFailureLeavesRowForRetry: a transient
// GetBUMP failure is not "no BUMP stored". Collapsing the two would have
// the full-scan reactivate a row whose txs were never re-mined and which is
// already off the tick's queue — stranding exactly the deep incident the
// scan exists for. The row must stay orphaned, and the scan must report
// itself incomplete so the one-shot is retried.
func TestReconciler_FullScanBUMPReadFailureLeavesRowForRetry(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failGetBUMP: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))

	seedSeen(t, base, recShared1)
	if err := base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())

	r := newTestReconciler(hs, pub, stub, nil)
	if r.fullScan(ctx) {
		t.Fatal("a scan whose repair failed must report itself incomplete")
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
		t.Fatalf("row must stay orphaned when the BUMP could not be read, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("tx must be untouched, got %s@%s", got.Status, got.BlockHash)
	}

	hs.failGetBUMP = false
	if !r.fullScan(ctx) {
		t.Fatal("the retry must complete once the backend recovers")
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("retry must reactivate the row, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("retry must re-mine the tx, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_FullScanReorgDuringRemineLeavesRowOrphaned: the canonical
// judgement that queues a resurrection is made during the paging walk, and
// the re-mine that follows can run for minutes. If a reorg makes the block
// non-canonical in that window, the scan must NOT go on to reactivate it —
// doing so would clear the fresh orphan generation and take the row off the
// reconcile queue while it sits off-chain. And because the re-mine already
// landed, its txs are MINED against a block that is now off-chain while the
// row — stamped by its previous reconciliation — is off the tick's queue;
// leaving it there would strand them. The scan must put the row back on the
// queue (fresh generation, stamp cleared) so the tick re-anchors them to the
// new canonical block. A repair handed off that way does not leave the scan
// incomplete.
func TestReconciler_FullScanReorgDuringRemineLeavesRowOrphaned(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))

	seedSeen(t, base, recShared1)
	if err := base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())

	// A reorg hands height 10 to a different block while the re-mine runs.
	hs.beforeMined = func(context.Context) { stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10)) }

	r := newTestReconciler(hs, pub, stub, nil)
	if !r.fullScan(ctx) {
		t.Fatal("a repair handed off to the queue must not leave the scan incomplete")
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("premise: the re-mine landed before the reorg check, got %s@%s", got.Status, got.BlockHash)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("a block that lost its height mid-repair must stay orphaned and be unstamped (requeued), got %+v err=%v", bp, err)
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 1 || rows[0].BlockHash != recOrphan {
		t.Fatalf("row must be back on the reconcile queue, got %+v err=%v", rows, err)
	}

	// The tick heals the txs against the new canonical block.
	_ = base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	r.tick(ctx)
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("tick must re-anchor the tx to the new canonical block, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once healed, got %+v err=%v", rows, err)
	}
}

// TestReconciler_StartupFullScanRetriesUntilComplete: an incomplete startup
// full-scan must not retire the one-shot. A row that is orphaned AND
// already reconciled_at-stamped is off the tick's durable queue, so the
// scan is the only thing that can heal it — marking it done after a
// transient failure would strand it until someone restarts the process.
// Retries are capped so a persistent failure cannot re-page forever.
func TestReconciler_StartupFullScanRetriesUntilComplete(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failGetBUMP: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))

	seedSeen(t, base, recShared1)
	if err := base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })

	r.tick(ctx)
	if r.startupScanDone {
		t.Fatal("an incomplete scan must leave the one-shot armed")
	}
	if r.startupScanAttempts != 1 {
		t.Fatalf("attempts = %d, want 1", r.startupScanAttempts)
	}

	hs.failGetBUMP = false
	r.tick(ctx)
	if !r.startupScanDone {
		t.Fatal("a completed retry must retire the one-shot")
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("the retry must heal the row, got %+v err=%v", bp, err)
	}

	// A persistently failing store retires the one-shot at the cap rather
	// than re-paging the window on every tick for the life of the process.
	r2 := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })
	hs.failGetBUMP = true
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())
	for i := 0; i < maxStartupFullScanAttempts; i++ {
		if r2.startupScanDone {
			t.Fatalf("one-shot retired after %d attempts, want %d", i, maxStartupFullScanAttempts)
		}
		r2.tick(ctx)
	}
	if !r2.startupScanDone {
		t.Fatalf("one-shot must be retired at the %d-attempt cap", maxStartupFullScanAttempts)
	}
}

// TestReconciler_FullScanOrphanMetricCountsAppliedTransitions: the scan
// pages the whole window before it writes, so the block-status tracker can
// orphan a candidate in between. The transition series promises APPLIED
// transitions, so that candidate must not be counted — while every
// candidate is still passed to the store.
func TestReconciler_FullScanOrphanMetricCountsAppliedTransitions(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	// The active chain holds a third block at height 10, so both rows below
	// are off-chain candidates.
	stub.setHeightHeader(10, headerWithHash(t, recNeighbor, 10))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())
	// One of them was already orphaned by the tracker: re-marking it is not
	// a transition.
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recCanonical}, time.Now())

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan,
	)
	before := testutil.ToFloat64(counter)

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) {
		c.FullScanMinHeight = 1
		c.FullScanMaxHeight = 50
	})
	r.fullScanMarkOrphaned(ctx, map[string]uint64{recOrphan: 10, recCanonical: 10})

	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("full-scan orphaned transitions = %v, want 1 (a re-mark is not a transition)", got)
	}
	for _, h := range []string{recOrphan, recCanonical} {
		bp, err := st.GetBlockProcessingStatus(ctx, h)
		if err != nil || bp.Status != models.BlockStatusOrphaned {
			t.Fatalf("%s must end orphaned, got %+v err=%v", h, bp, err)
		}
	}
}

// partialRevertStore applies the revert and then reports an error, the shape
// SetStatusByBlockHash uses when a walk fails partway through or a block keeps
// taking mines faster than it can be retired: rows written, error returned.
type partialRevertStore struct {
	store.Store

	applied []string
}

func (s *partialRevertStore) SetStatusByBlockHash(ctx context.Context, blockHash string, st models.Status) ([]string, error) {
	applied, err := s.Store.SetStatusByBlockHash(ctx, blockHash, st)
	if err != nil {
		return applied, err
	}
	s.applied = applied
	return applied, errors.New("rows still arriving after 4 passes")
}

// A revert that returns rows AND an error has already written those rows, and
// they have left the block's index — a retry will not find them again. Their
// correction event must still go out, or subscribers keep believing the txs
// are MINED forever. The block itself must stay on the queue.
func TestReconciler_PublishesRevertedTxsWhenTheStoreAlsoErrors(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	st := &partialRevertStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, base, recOrphan, 10, recBOnly)
	if err := base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	if err := base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()); err != nil {
		t.Fatalf("upsert orphan row: %v", err)
	}
	if _, err := base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now()); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	newTestReconciler(st, pub, stub, nil).tick(ctx)

	if len(st.applied) == 0 {
		t.Fatal("test premise: the revert must have written at least one row")
	}
	var seenEv *models.TransactionStatus
	for _, ev := range pub.bulkEvents() {
		if ev.ExtraInfo == models.ExtraInfoReorgUnmined {
			seenEv = ev
		}
	}
	if seenEv == nil {
		t.Fatal("the rows the store did write must still be published; their event is never retried")
	}
	if len(seenEv.TxIDs) != len(st.applied) || seenEv.TxIDs[0] != st.applied[0] {
		t.Fatalf("published %v, want the rows the store wrote %v", seenEv.TxIDs, st.applied)
	}
	// The block stays queued: the error means the store is not finished.
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.ReconciledAt != nil {
		t.Fatalf("reconciled_at must NOT be stamped after a revert error: %+v err=%v", bp, err)
	}
}

// failedMineStore reports an error from SetMinedByTxIDs with NO rows written
// — the shape of a mine that failed on its first row (a primary step-down, a
// pool exhausted), as distinct from partialMineStore, where every row landed
// before the error.
type failedMineStore struct {
	store.Store
}

func (s *failedMineStore) SetMinedByTxIDs(
	_ context.Context, _ string, _ uint64, _ []string,
) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	return nil, nil, errors.New("primary stepped down before any row landed")
}

// TestReconciler_PartialCanonicalRemineDoesNotRevert: a canonical re-mine
// that does not land in full must NOT let the orphan reconcile.
//
// The txs it failed to write are provably IN the canonical BUMP and are still
// anchored to the orphan. The neighborhood pass only walks heights ABOVE the
// orphan, so it cannot claim them. If the re-mine still reported ready, the
// revert would take them to SEEN_ON_NETWORK and reconciled_at would be
// stamped — un-mining transactions that are demonstrably mined, with nothing
// left to re-drive them: the canonical block already has its BUMP and its
// processed_at, and the orphan has left the queue.
func TestReconciler_PartialCanonicalRemineDoesNotRevert(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, base, recOrphan, 10, recShared1)
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	// The canonical BUMP IS stored and DOES contain the tx — so the only
	// reason the re-anchor does not happen is the store failure.
	if err := base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}

	r := newTestReconciler(&failedMineStore{Store: base}, pub, stub, nil)
	r.tick(ctx)

	// The tx must still be MINED@orphan — not reverted, and not silently
	// treated as belonging nowhere.
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("a failed canonical re-mine must leave the tx MINED@orphan, got %s@%s", got.Status, got.BlockHash)
	}
	for _, ev := range pub.bulkEvents() {
		if ev.Status == models.StatusSeenOnNetwork {
			t.Fatalf("no revert event may be published for a tx still provably in the canonical BUMP: %+v", ev)
		}
	}
	// And the block must stay queued so a later tick can retry.
	if rows, _ := base.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
		t.Fatalf("orphan must stay queued after a failed canonical re-mine, got %d rows", len(rows))
	}

	// Once the store recovers, the ordinary path completes the reconcile.
	r.store = base
	r.tick(ctx)
	if got := statusOf(t, base, recShared1); got.BlockHash != recCanonical {
		t.Fatalf("recovered tick must re-anchor to canonical, got %s@%s", got.Status, got.BlockHash)
	}
}

// blockFailMineStore fails SetMinedByTxIDs for one target block and passes
// every other block through, so a test can fail the neighborhood re-anchor
// while the canonical re-mine succeeds.
type blockFailMineStore struct {
	store.Store

	failFor string
}

func (s *blockFailMineStore) SetMinedByTxIDs(
	ctx context.Context, blockHash string, blockHeight uint64, txids []string,
) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	if blockHash == s.failFor {
		return nil, nil, errors.New("primary stepped down before any row landed")
	}
	return s.Store.SetMinedByTxIDs(ctx, blockHash, blockHeight, txids)
}

// TestReconciler_PartialNeighborhoodReanchorDoesNotRevert: the same invariant
// as the canonical re-mine, one height up. A tx the neighborhood pass failed
// to move is a proven member of that NEIGHBOR's BUMP and is still anchored to
// the orphan, so letting the pass report success would hand it to the revert
// and un-mine it. Shrinking `affected` is not what protects it — the revert
// works off the store's own block index — so the completion result has to
// reach reconcileBlock.
func TestReconciler_PartialNeighborhoodReanchorDoesNotRevert(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))

	seedMined(t, base, recOrphan, 10, recRebin)
	// Canonical at 10 does not contain the tx (so the canonical re-mine
	// succeeds trivially and canonicalReady is true); the block at 11 does.
	_ = base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = base.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(&blockFailMineStore{Store: base, failFor: recNeighbor}, pub, stub, nil)
	r.tick(ctx)

	if got := statusOf(t, base, recRebin); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("a failed neighborhood re-anchor must leave the tx MINED@orphan, got %s@%s", got.Status, got.BlockHash)
	}
	for _, ev := range pub.bulkEvents() {
		if ev.Status == models.StatusSeenOnNetwork {
			t.Fatalf("no revert event may be published for a tx the neighbor's BUMP proves: %+v", ev)
		}
	}
	if rows, _ := base.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
		t.Fatalf("orphan must stay queued after a failed neighborhood re-anchor, got %d rows", len(rows))
	}

	// Recovered: the ordinary neighborhood path completes.
	r.store = base
	r.tick(ctx)
	if got := statusOf(t, base, recRebin); got.BlockHash != recNeighbor {
		t.Fatalf("recovered tick must re-anchor to the neighbor, got %s@%s", got.Status, got.BlockHash)
	}
}

// fakeLeaser is a store.Leaser that grants the lease to any caller until
// told it was lost, and records every acquire/renew so a test can watch the
// heartbeat.
type fakeLeaser struct {
	mu    sync.Mutex
	calls int
	ttls  []time.Duration
	lost  bool
	// expiry, when set, is the expiry the store reports on acquisition
	// instead of now+ttl — a slow acquire or a skewed backend clock makes
	// the real lease shorter than the local estimate.
	expiry time.Duration
	// renewErr, when set, fails every call after the first, as a backend
	// outage during the pass does.
	renewErr error
	// renewGate, when set, makes every call after the first block until the
	// gate is closed — a renewal RPC that hangs. It ignores the caller's
	// context on purpose: the store's leaser may not honour one either, and
	// the reconciler must cope regardless. When it finally returns, the
	// result is a success (now+ttl), the "late but successful" shape.
	renewGate chan struct{}
	// honourCtx makes a gated renewal return ctx.Err() as soon as the
	// caller's context ends — a leaser that does honour the deadline.
	honourCtx bool
	// renewDeadlines records, per renewal call, the deadline the caller's
	// context carried (zero when it had none).
	renewDeadlines []time.Time
	// granted records every expiry this leaser handed back.
	granted []time.Time
	// results records, per renewal call, the error it returned (nil on a
	// success) — so a test can tell a renewal cut by its own deadline
	// (DeadlineExceeded) from one ended by the pass's cancellation.
	results []error
}

func (l *fakeLeaser) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	until, err := l.renew(ctx, name, holder, ttl)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.calls > 1 { // the acquire is call 1; everything after it is a renewal
		l.results = append(l.results, err)
	}
	return until, err
}

func (l *fakeLeaser) renew(ctx context.Context, _, _ string, ttl time.Duration) (time.Time, error) {
	l.mu.Lock()
	l.calls++
	call := l.calls
	l.ttls = append(l.ttls, ttl)
	if call > 1 {
		dl, _ := ctx.Deadline()
		l.renewDeadlines = append(l.renewDeadlines, dl)
	}
	gate, honour := l.renewGate, l.honourCtx
	l.mu.Unlock()

	if call > 1 && gate != nil {
		if honour {
			select {
			case <-gate:
			case <-ctx.Done():
				return time.Time{}, ctx.Err()
			}
		} else {
			<-gate
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if call > 1 && l.renewErr != nil {
		return time.Time{}, l.renewErr
	}
	if l.lost {
		return time.Time{}, nil
	}
	until := time.Now().Add(ttl)
	if l.expiry > 0 {
		until = time.Now().Add(l.expiry)
	}
	l.granted = append(l.granted, until)
	return until, nil
}

// renewalResults returns the error of every RENEWAL call so far (the
// acquire is not a renewal).
func (l *fakeLeaser) renewalResults() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.results...)
}

func (l *fakeLeaser) renewalDeadlines() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Time(nil), l.renewDeadlines...)
}

func (l *fakeLeaser) Release(context.Context, string, string) error { return nil }

func (l *fakeLeaser) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *fakeLeaser) grantedTTLs() []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Duration(nil), l.ttls...)
}

func (l *fakeLeaser) lose() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lost = true
}

// waitFor polls cond until it holds or timeout passes.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// seedResurrectable puts the store in the full-scan's resurrect shape: an
// orphaned row, already stamped reconciled (so off the tick's queue), that
// IS the active-chain block at its height, with a retained BUMP listing a tx
// still at SEEN — so the re-mine has work to do.
func seedResurrectable(t *testing.T, st store.Store, stub *stubChaintracks) {
	t.Helper()
	ctx := context.Background()
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	seedSeen(t, st, recShared1)
	if err := st.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = st.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())
}

// malformedBUMP is a stored blob that is not a BRC-74 compound: a read
// succeeds, the parse does not. Distinct from store.ErrNotFound.
var malformedBUMP = []byte{0xde, 0xad, 0xbe, 0xef}

// TestReconciler_LeaseRenewedWhileFullScanRuns: the lease is acquired once
// per tick, but a full-scan re-mine can run for minutes against a 90 s TTL,
// so without renewal another replica takes the lease mid-scan and runs the
// same repair concurrently. The heartbeat must keep renewing, at TTL/3, for
// as long as the pass runs.
func TestReconciler_LeaseRenewedWhileFullScanRuns(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	seedResurrectable(t, base, stub)

	fl := &fakeLeaser{}
	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })
	r.leaser = fl
	r.holderID = "replica-a"
	r.leaseTTLOverride = 60 * time.Millisecond // heartbeat every 20 ms
	// The re-mine "takes a while": well past the 60 ms the acquire granted,
	// so only renewals moving the expiry forward keep the pass alive — at
	// least five of them after the tick's own acquire.
	hs.beforeMined = func(passCtx context.Context) {
		if !waitFor(2*time.Second, func() bool { return fl.callCount() >= 6 }) {
			t.Error("the lease was not renewed while the re-mine ran")
		}
		if passCtx.Err() != nil {
			t.Error("the pass was cancelled although every renewal succeeded")
		}
	}

	r.tick(ctx)

	if !r.startupScanDone {
		t.Fatal("the scan must complete while the lease is held")
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("row must be reactivated by the completed scan, got %+v err=%v", bp, err)
	}
	for _, ttl := range fl.grantedTTLs() {
		if ttl != 60*time.Millisecond {
			t.Fatalf("renewal asked for a %v TTL, want the same %v the acquire used", ttl, 60*time.Millisecond)
		}
	}
}

// TestReconciler_LeaseLossAbandonsFullScan: when a renewal reports the lease
// held by another replica, the pass must stop rather than finish alongside
// the new holder's. The store calls in flight see a cancelled context, the
// row stays orphaned for the new leader's scan, and the interrupted pass
// does not count as a failed attempt toward the retry cap.
func TestReconciler_LeaseLossAbandonsFullScan(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	seedResurrectable(t, base, stub)

	fl := &fakeLeaser{}
	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })
	r.leaser = fl
	r.holderID = "replica-a"
	r.leaseTTLOverride = 60 * time.Millisecond
	// Another replica takes the lease while the re-mine is in flight; the
	// heartbeat must cancel this pass.
	hs.beforeMined = func(passCtx context.Context) {
		fl.lose()
		select {
		case <-passCtx.Done():
		case <-time.After(2 * time.Second):
			t.Error("the pass was not cancelled after the lease was lost")
		}
	}

	r.tick(ctx)

	if r.startupScanDone {
		t.Fatal("an abandoned scan must not retire the one-shot")
	}
	if r.startupScanAttempts != 0 {
		t.Fatalf("attempts = %d, want 0 (an interrupted pass is not a failed attempt)", r.startupScanAttempts)
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
		t.Fatalf("row must be left for the new lease holder, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("no store write may land after the lease is lost, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_NeighborhoodBUMPReadFailureKeepsRowQueued: a transient
// GetBUMP failure on a canonical neighbor is not "no BUMP". Treated as
// absence, the neighbor's txs would stay in the revert set — un-mining txs
// that ARE in a canonical block — and the stamp would retire the row with
// the retry lost. The block must stay queued and heal once the read works.
func TestReconciler_NeighborhoodBUMPReadFailureKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failGetBUMPFor: recNeighbor}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))

	seedMined(t, base, recOrphan, 10, recRebin)
	_ = base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = base.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(hs, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	if got := statusOf(t, base, recRebin); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must not be reverted while the neighbor's BUMP is unreadable, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 1 {
		t.Fatalf("row must stay queued for retry, got %+v err=%v", rows, err)
	}

	hs.failGetBUMPFor = ""
	r.tick(ctx)
	if got := statusOf(t, base, recRebin); got.Status != models.StatusMined || got.BlockHash != recNeighbor {
		t.Fatalf("retry must re-anchor to the neighbor, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once healed, got %+v err=%v", rows, err)
	}
}

// TestReconciler_NeighborhoodMalformedBUMPKeepsRowQueued: a stored neighbor
// BUMP that does not parse may well contain the affected txs, so it must not
// be read as "this neighbor claims nothing" either. The block stays queued
// (loudly) until the BUMP is rebuilt, then heals.
func TestReconciler_NeighborhoodMalformedBUMPKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))

	seedMined(t, st, recOrphan, 10, recRebin)
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.InsertBUMP(ctx, recNeighbor, 11, malformedBUMP)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must not be reverted while the neighbor's BUMP is corrupt, got %s@%s", got.Status, got.BlockHash)
	}

	// The rebuilt BUMP heals on the next tick.
	_ = st.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	r.tick(ctx)
	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recNeighbor {
		t.Fatalf("rebuilt BUMP must re-anchor the tx to the neighbor, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_MalformedCanonicalBUMPDefers: on the tick path a corrupt
// canonical BUMP proves nothing, like a missing one, so the block defers —
// its /reprocess poke is what rebuilds the BUMP — rather than sitting in
// "error" with no remedy in flight. Nothing is reverted or stamped, and the
// rebuilt BUMP heals on a later tick.
func TestReconciler_MalformedCanonicalBUMPDefers(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.InsertBUMP(ctx, recCanonical, 10, malformedBUMP)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "deferred" {
		t.Fatalf("outcome = %q, want deferred", outcome)
	}
	if r.defers[recOrphan] != 1 {
		t.Fatalf("defer count = %d, want 1", r.defers[recOrphan])
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must be untouched while the canonical BUMP is corrupt, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 1 {
		t.Fatalf("row must stay queued, got %+v err=%v", rows, err)
	}

	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	r.tick(ctx)
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("rebuilt BUMP must re-anchor the tx, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once healed, got %+v err=%v", rows, err)
	}
}

// TestReconciler_FullScanReactivatedMetricCountsAppliedTransitions: the
// full-scan judges during the paging walk and re-mines for minutes before it
// writes; the tracker can reactivate the row in between. The write is a
// generation CAS, so it applies nothing, the row keeps the tracker's clean
// active state, and the full_scan/reactivated series does not count it. The
// scan is still complete: nothing is left for it to retry.
func TestReconciler_FullScanReactivatedMetricCountsAppliedTransitions(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	seedResurrectable(t, base, stub)
	hs.beforeReactivate = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceFullScan,
	)
	before := testutil.ToFloat64(counter)

	r := newTestReconciler(hs, pub, stub, nil)
	if !r.fullScan(ctx) {
		t.Fatal("a row another edge already reactivated leaves nothing for the scan to retry")
	}
	if got := testutil.ToFloat64(counter) - before; got != 0 {
		t.Fatalf("full-scan reactivated transitions = %v, want 0 (the row was already active)", got)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must keep the tracker's clean active state, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the re-mine still runs, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_ShortCircuitReactivationIsGenerationChecked: the
// resurrection short-circuit's canonicality check and its write are not
// atomic. If a reorg orphans the row AGAIN — a newer generation — between
// them, the write must not clear that generation (it is queued for its own
// pass): the outcome is stale and the reconciler/reactivated series does not
// count it. The newer generation then resurrects on its own pass, counted
// once. A row the tracker reactivated in the gap is likewise left alone.
func TestReconciler_ShortCircuitReactivationIsGenerationChecked(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10)) // the "orphan" is canonical

	seedMined(t, base, recOrphan, 10, recShared1)
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReconciler,
	)
	before := testutil.ToFloat64(counter)

	// Re-orphaned with a newer generation right before the write.
	hs.beforeReactivate = func() {
		_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now().Add(time.Second))
	}
	r := newTestReconciler(hs, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "stale" {
		t.Fatalf("outcome = %q, want stale", outcome)
	}
	if got := testutil.ToFloat64(counter) - before; got != 0 {
		t.Fatalf("reconciler reactivated transitions = %v, want 0 (a newer generation is not ours to clear)", got)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("the newer generation must stay orphaned and queued, got %+v err=%v", bp, err)
	}
	rows, err = base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("the newer generation must remain in the queue, got %+v err=%v", rows, err)
	}

	// Its own pass finds the block still canonical: resurrected, counted once.
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "resurrected" {
		t.Fatalf("outcome = %q, want resurrected", outcome)
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("reconciler reactivated transitions = %v, want 1", got)
	}
	bp, err = base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must be active and clean, got %+v err=%v", bp, err)
	}

	// A row the tracker reactivated in the gap is left alone too.
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now().Add(2*time.Second))
	rows, err = base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}
	hs.beforeReactivate = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "stale" {
		t.Fatalf("outcome = %q, want stale", outcome)
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("reconciler reactivated transitions = %v, want still 1 (the tracker's write is not ours)", got)
	}
	bp, err = base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must keep the tracker's clean active state, got %+v err=%v", bp, err)
	}
}

// reprocessStub is a merkle-service /reprocess endpoint that records the
// block hashes it was asked to redeliver. Other endpoints (the reconciler's
// orphan re-watch /watch calls) are accepted and not recorded.
type reprocessStub struct {
	mu     sync.Mutex
	hashes []string
	srv    *httptest.Server
}

func newReprocessStub(t *testing.T) *reprocessStub {
	t.Helper()
	s := &reprocessStub{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/reprocess" {
			w.WriteHeader(http.StatusOK)
			return
		}
		var body struct {
			BlockHash string `json:"blockHash"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.hashes = append(s.hashes, body.BlockHash)
		s.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *reprocessStub) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.hashes...)
}

// withReprocess wires a reconciler to a /reprocess stub the way production
// does: a merkle client plus the callback URL the poke forwards.
func withReprocess(r *Reconciler, stub *reprocessStub) {
	r.merkle = merkleservice.NewClient(stub.srv.URL, "", time.Second)
	r.cfg.CallbackURL = "http://broadcaster.test/callback"
}

// TestReconciler_LeaseHeartbeatHonoursAcquireExpiry: the heartbeat must
// judge "still held" against the expiry the STORE granted, not a local
// now+TTL. Here the acquire reports a lease that lapses almost at once (a
// slow acquire, a skewed backend clock) and every renewal fails: the pass
// must be abandoned at that granted expiry — by the expiry timer, which
// fires before the first heartbeat is even due, or at the latest by the
// first failed renewal — whereas a local now+TTL estimate would have kept
// it running for three renewals, overlapping whoever took the lease.
func TestReconciler_LeaseHeartbeatHonoursAcquireExpiry(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	seedResurrectable(t, base, stub)

	fl := &fakeLeaser{expiry: 20 * time.Millisecond, renewErr: errors.New("injected: lease backend down")}
	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })
	r.leaser = fl
	r.holderID = "replica-a"
	r.leaseTTLOverride = 300 * time.Millisecond // heartbeat every 100 ms; local now+TTL would last 3 renewals
	hs.beforeMined = func(passCtx context.Context) {
		select {
		case <-passCtx.Done():
		case <-time.After(2 * time.Second):
			t.Error("the pass was not cancelled once renewals failed past the granted expiry")
		}
	}

	start := time.Now()
	r.tick(ctx)

	// Abandoned at the 20 ms grant, not the 300 ms estimate: at most the
	// acquire plus one failed renewal, and long before the third heartbeat.
	if got := fl.callCount(); got > 2 {
		t.Fatalf("lease calls = %d, want at most 2 (acquire + at most one failed renewal)", got)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("the pass ran %v after a 20 ms grant; it must be abandoned at the granted expiry", elapsed)
	}
	if r.startupScanDone || r.startupScanAttempts != 0 {
		t.Fatalf("an abandoned scan must stay armed without burning an attempt, got done=%v attempts=%d",
			r.startupScanDone, r.startupScanAttempts)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("no store write may land after the lease lapsed, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_FullScanUnjudgeableHeightKeepsOneShotArmed: a candidate row
// the chain-header source cannot judge (a per-height read failure, an
// embedded chaintracks a header behind) is never orphaned or reactivated on
// that absence of evidence — but it is not a verdict either. Retiring the
// one-shot over it would leave a stamped orphan untouched for the life of
// the process; the scan must report itself incomplete so the bounded retry
// runs, and complete once the height can be judged.
func TestReconciler_FullScanUnjudgeableHeightKeepsOneShotArmed(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	seedResurrectable(t, st, stub) // judgeable at 10, and healed by the scan
	// The active tip at 12 is served, so the readiness gate opens; an
	// active row at 11 below it is not — a per-height gap in the header
	// source.
	stub.setHeightHeader(12, headerWithHash(t, recCanonical, 12))
	_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 12, time.Now())
	_ = st.UpsertBlockHeaderSeen(ctx, recNeighbor, 11, time.Now())

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })
	r.tick(ctx)
	if r.startupScanDone {
		t.Fatal("a scan with an unjudgeable candidate must not retire the one-shot")
	}
	if r.startupScanAttempts != 1 {
		t.Fatalf("attempts = %d, want 1", r.startupScanAttempts)
	}
	// What could be judged was still repaired; what could not was left alone.
	if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("the judgeable row must still heal, got %+v err=%v", bp, err)
	}
	if bp, err := st.GetBlockProcessingStatus(ctx, recNeighbor); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("the unjudgeable row must be untouched, got %+v err=%v", bp, err)
	}

	// chaintracks catches up: the retry judges every row and completes.
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))
	r.tick(ctx)
	if !r.startupScanDone {
		t.Fatal("once every candidate is judgeable the scan must complete and retire the one-shot")
	}
}

// TestReconciler_NeighborhoodEmptyBUMPKeepsRowQueued: every backend reports
// a missing BUMP as store.ErrNotFound, so an empty stored blob is a
// stored-but-unusable one, not absence. Read as absence, the neighbor's txs
// would stay in the revert set and be un-mined. The block stays queued until
// the blob is rebuilt, then heals.
func TestReconciler_NeighborhoodEmptyBUMPKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))

	seedMined(t, st, recOrphan, 10, recRebin)
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.InsertBUMP(ctx, recNeighbor, 11, []byte{})
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must not be reverted while the neighbor's blob is empty, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10); err != nil || len(rows) != 1 {
		t.Fatalf("row must stay queued for retry, got %+v err=%v", rows, err)
	}

	_ = st.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	r.tick(ctx)
	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recNeighbor {
		t.Fatalf("rebuilt blob must re-anchor the tx to the neighbor, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_EmptyCanonicalBUMPDefers: the same for the canonical
// block's own blob on the tick path — an empty blob takes the malformed
// route (defer and request a rebuild), never the "not stored yet" one, and
// nothing is reverted or stamped.
func TestReconciler_EmptyCanonicalBUMPDefers(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	reprocess := newReprocessStub(t)

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.InsertBUMP(ctx, recCanonical, 10, []byte{})
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	withReprocess(r, reprocess)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "deferred" {
		t.Fatalf("outcome = %q, want deferred", outcome)
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recCanonical {
		t.Fatalf("the canonical block's rebuild must be requested, got %v", got)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must be untouched, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_FullScanOrphanMetricCountsPartialTransitionsOnError: the
// backends report the transitions that landed before a failure alongside the
// error, and those rows ARE orphaned in the store. The metric promises
// applied transitions, so the count must be observed even when the call
// also failed — and the scan still reports itself incomplete for the retry.
func TestReconciler_FullScanOrphanMetricCountsPartialTransitionsOnError(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, orphanPartialErr: errors.New("injected: later chunk failed")}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recNeighbor, 10)) // both rows below are off-chain
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_ = base.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan,
	)
	before := testutil.ToFloat64(counter)

	r := newTestReconciler(hs, pub, stub, nil)
	if r.fullScanMarkOrphaned(ctx, map[string]uint64{recOrphan: 10, recCanonical: 10}) {
		t.Fatal("a failed write must report the scan incomplete")
	}
	if got := testutil.ToFloat64(counter) - before; got != 2 {
		t.Fatalf("full-scan orphaned transitions = %v, want 2 (the rows that landed before the failure)", got)
	}
}

// seedQueuedCanonical puts a queued (orphaned, unstamped) row in the store
// for a block that IS the active-chain block at height 10, with a SEEN tx
// and the given stored BUMP blob — the resurrection short-circuit's input.
func seedQueuedCanonical(t *testing.T, st store.Store, stub *stubChaintracks, bumpBlob []byte) *models.BlockProcessingStatus {
	t.Helper()
	ctx := context.Background()
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	seedSeen(t, st, recShared1)
	if bumpBlob != nil {
		if err := st.InsertBUMP(ctx, recOrphan, 10, bumpBlob); err != nil {
			t.Fatalf("insert BUMP: %v", err)
		}
	}
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].BlockHash != recOrphan {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}
	return rows[0]
}

// TestReconciler_ShortCircuitReminesFromStoredBUMP: the resurrection
// short-circuit heals the block's own txs from its retained BUMP before it
// returns the row to active — the same repair the full-scan applies — so a
// resurrected block's heal does not depend on the competitor's row existing.
func TestReconciler_ShortCircuitReminesFromStoredBUMP(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	row := seedQueuedCanonical(t, st, stub, makeCompoundForTest(t, 10, recShared1))

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReconciler,
	)
	before := testutil.ToFloat64(counter)

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, row); outcome != "resurrected" {
		t.Fatalf("outcome = %q, want resurrected", outcome)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the short-circuit must re-mine the block's txs, got %s@%s", got.Status, got.BlockHash)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must be active and clean, got %+v err=%v", bp, err)
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("reconciler reactivated transitions = %v, want 1", got)
	}
	if len(pub.bulkEvents()) == 0 {
		t.Fatal("expected a corrected MINED event for the re-mined tx")
	}
}

// TestReconciler_ShortCircuitMalformedBUMPDefersInsteadOfReactivating: a
// queued canonical row whose stored BUMP does not parse must NOT be returned
// to active with its txs un-remined and nothing left to retry. The
// short-circuit takes the deferral the canonical path takes for an unusable
// BUMP — row still queued, /reprocess asked to rebuild the block — and only
// reactivates (re-mining) once the BUMP parses. The reactivated series counts
// nothing until then.
func TestReconciler_ShortCircuitMalformedBUMPDefersInsteadOfReactivating(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	row := seedQueuedCanonical(t, st, stub, malformedBUMP)

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReconciler,
	)
	before := testutil.ToFloat64(counter)

	r := newTestReconciler(st, pub, stub, nil)
	withReprocess(r, reprocess)
	if outcome := r.reconcileBlock(ctx, row); outcome != "deferred" {
		t.Fatalf("outcome = %q, want deferred", outcome)
	}
	if r.defers[recOrphan] != 1 {
		t.Fatalf("defer count = %d, want 1", r.defers[recOrphan])
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("the block's rebuild must be requested, got %v", got)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("row must stay orphaned and queued (unstamped), got %+v err=%v", bp, err)
	}
	if rows, lerr := st.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 1 {
		t.Fatalf("row must stay on the reconcile queue, got %+v err=%v", rows, lerr)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("nothing is re-mined from a corrupt BUMP, got %s@%s", got.Status, got.BlockHash)
	}
	if got := testutil.ToFloat64(counter) - before; got != 0 {
		t.Fatalf("reconciler reactivated transitions = %v, want 0 (nothing was reactivated)", got)
	}

	// The rebuild lands: the next tick re-mines and reactivates, counted once.
	_ = st.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1))
	r.tick(ctx)
	bp, err = st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must be reactivated once the BUMP parses, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the rebuilt BUMP must re-mine the tx, got %s@%s", got.Status, got.BlockHash)
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("reconciler reactivated transitions = %v, want 1", got)
	}
	if _, deferred := r.defers[recOrphan]; deferred {
		t.Fatal("reactivation must clear the defer counter")
	}
}

// TestReconciler_ShortCircuitMalformedBUMPReactivatesAtDeferCap: when the
// BUMP never becomes usable, the deferral is bounded like every other. At
// the cap the row IS reactivated — parking it (stamped, still orphaned)
// would leave a canonical block orphaned, the bug this PR closes — and the
// txs stay on the redelivery path.
func TestReconciler_ShortCircuitMalformedBUMPReactivatesAtDeferCap(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	row := seedQueuedCanonical(t, st, stub, malformedBUMP)

	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReconciler,
	)
	before := testutil.ToFloat64(counter)

	r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 2 })
	for i := 1; i <= 2; i++ {
		if outcome := r.reconcileBlock(ctx, row); outcome != "deferred" {
			t.Fatalf("pass %d: outcome = %q, want deferred", i, outcome)
		}
	}
	if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
		t.Fatalf("row must stay orphaned while under the cap, got %+v err=%v", bp, err)
	}
	if outcome := r.reconcileBlock(ctx, row); outcome != "resurrected" {
		t.Fatalf("at the cap: outcome = %q, want resurrected", outcome)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("a canonical block must not be left orphaned at the cap, got %+v err=%v", bp, err)
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("reconciler reactivated transitions = %v, want 1 (the cap reactivation is real)", got)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("a corrupt BUMP re-mines nothing; the tx stays on the redelivery path, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_FullScanMalformedBUMPRequeuesForTick is the hand-off end to
// end: the full-scan cannot fix a corrupt blob by retrying, so it requeues
// the row and reports complete; the tick's short-circuit then hits the same
// corrupt BUMP and DEFERS (requesting the rebuild) rather than reactivating
// with the txs un-remined; once the BUMP parses, the tick re-mines and
// reactivates. The scan itself requests nothing — the deferral is the one
// owner of the rebuild.
func TestReconciler_FullScanMalformedBUMPRequeuesForTick(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	reprocess := newReprocessStub(t)

	seedSeen(t, st, recShared1)
	_ = st.InsertBUMP(ctx, recOrphan, 10, malformedBUMP)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = st.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("precondition: the stamped row must be off the queue, got %d", len(rows))
	}

	r := newTestReconciler(st, pub, stub, nil)
	withReprocess(r, reprocess)
	if !r.fullScan(ctx) {
		t.Fatal("a repair handed to the tick must not leave the scan incomplete")
	}
	if got := reprocess.requested(); len(got) != 0 {
		t.Fatalf("the scan itself must not request a rebuild (the tick's deferral does), got %v", got)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("row must be orphaned and unstamped (requeued), got %+v err=%v", bp, err)
	}
	if rows, lerr := st.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 1 || rows[0].BlockHash != recOrphan {
		t.Fatalf("row must be back on the reconcile queue, got %+v err=%v", rows, lerr)
	}

	// The tick defers rather than reactivating a row whose txs it cannot
	// re-mine, and asks for the rebuild.
	r.tick(ctx)
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("the tick's deferral must request the rebuild once, got %v", got)
	}
	bp, err = st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("the tick must leave the row queued while the BUMP is corrupt, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("nothing is re-mined from a corrupt BUMP, got %s@%s", got.Status, got.BlockHash)
	}

	// The rebuild lands: the next tick heals and reactivates.
	_ = st.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1))
	r.tick(ctx)
	bp, err = st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("the tick must reactivate the row once the BUMP parses, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the rebuilt BUMP must re-mine the tx, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, lerr := st.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once reactivated, got %+v err=%v", rows, lerr)
	}
}

// TestReconciler_FullScanRequeueNeverReorphansAReactivatedRow: the tracker
// runs alongside the full-scan and can legitimately reactivate the canonical
// row between the scan's paging read and its hand-off. The requeue is a
// compare-and-set on the generation the scan read, never a transition, so
// that active row must stay active and clean, nothing may be counted on the
// orphaned/full_scan series, and the scan is complete — there is nothing
// left to hand off. Both hand-off branches are covered: the malformed-BUMP
// one and the lost-its-height one.
func TestReconciler_FullScanRequeueNeverReorphansAReactivatedRow(t *testing.T) {
	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan,
	)

	t.Run("MalformedBUMP", func(t *testing.T) {
		ctx := context.Background()
		base := newPebbleForTest(t)
		hs := &hookedStore{Store: base}
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
		seedSeen(t, base, recShared1)
		_ = base.InsertBUMP(ctx, recOrphan, 10, malformedBUMP)
		_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
		_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())
		// The tracker reactivates the row right before the hand-off write.
		hs.beforeReactivate = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }
		before := testutil.ToFloat64(counter)

		r := newTestReconciler(hs, pub, stub, nil)
		if !r.fullScan(ctx) {
			t.Fatal("a row another edge already reactivated leaves nothing for the scan to retry")
		}
		bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
		if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
			t.Fatalf("the reactivated row must stay active and clean, got %+v err=%v", bp, err)
		}
		if rows, lerr := base.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 0 {
			t.Fatalf("an active row must not be on the queue, got %+v err=%v", rows, lerr)
		}
		if got := testutil.ToFloat64(counter) - before; got != 0 {
			t.Fatalf("orphaned/full_scan = %v, want 0 (the hand-off is never a transition)", got)
		}
	})

	t.Run("LostItsHeight", func(t *testing.T) {
		ctx := context.Background()
		base := newPebbleForTest(t)
		hs := &hookedStore{Store: base}
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		seedResurrectable(t, base, stub)
		// A reorg hands height 10 to a competitor during the re-mine, and
		// the tracker (on its own view) reactivates the row before the
		// scan's hand-off write.
		hs.beforeMined = func(context.Context) { stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10)) }
		hs.beforeReactivate = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }
		before := testutil.ToFloat64(counter)

		r := newTestReconciler(hs, pub, stub, nil)
		if !r.fullScan(ctx) {
			t.Fatal("a row another edge already reactivated leaves nothing for the scan to retry")
		}
		bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
		if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
			t.Fatalf("the reactivated row must stay active and clean, got %+v err=%v", bp, err)
		}
		if got := testutil.ToFloat64(counter) - before; got != 0 {
			t.Fatalf("orphaned/full_scan = %v, want 0 (the hand-off is never a transition)", got)
		}
	})
}

// TestReconciler_PlaceholderHeightBUMPReadFailureKeepsRowQueued: a height-0
// placeholder row (MarkBlockProcessed before any header) resolves its height
// from its stored BUMP. A transient failure of THAT read must not be taken
// as "no height": with the canonical block unidentifiable the pass would
// park the row — stamped, off the queue, its txs still anchored to the
// orphan. Only a positively missing BUMP is absence; a read failure keeps
// the row queued, and the pass completes once the read works.
func TestReconciler_PlaceholderHeightBUMPReadFailureKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failGetBUMP: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	// The orphan's row was created by the BLOCK_PROCESSED callback before its
	// header arrived, so it carries no height; its BUMP knows it is 10.
	seedMined(t, base, recOrphan, 10, recShared1)
	if err := base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert orphan BUMP: %v", err)
	}
	if err := base.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	if err := base.MarkBlockProcessed(ctx, recOrphan, 0, time.Now()); err != nil {
		t.Fatalf("placeholder row: %v", err)
	}
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := base.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].BlockHeight != 0 {
		t.Fatalf("precondition: one queued height-0 row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(hs, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("row must stay orphaned and unstamped (not parked), got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must be untouched, got %s@%s", got.Status, got.BlockHash)
	}

	hs.failGetBUMP = false
	r.tick(ctx)
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("retry must re-anchor to the canonical block, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, lerr := base.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once healed, got %+v err=%v", rows, lerr)
	}
}

// TestReconciler_HeaderLookupErrorKeepsRowQueued: a FAILED chain-header
// lookup at the orphan's height is not "no canonical block". With nothing to
// re-anchor to, the pass would fall through to the park — stamped, off the
// queue, txs still anchored to the orphan — over a transient read. The
// block must stay queued and heal once the lookup works.
func TestReconciler_HeaderLookupErrorKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightErr(10, errors.New("injected: header source unavailable"))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("row must stay orphaned and unstamped (not parked), got %+v err=%v", bp, err)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must be untouched, got %s@%s", got.Status, got.BlockHash)
	}

	stub.setHeightErr(10, nil)
	r.tick(ctx)
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("retry must re-anchor to the canonical block, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_NeighborhoodHeaderErrorKeepsRowQueued: the neighborhood
// walk ends at the first height the header source cannot serve — the end of
// the chain. A lookup that FAILS at an intermediate height is not that: a
// later neighbor's BUMP may prove the remaining tx mined, and treating the
// failure as end-of-chain would revert that tx and stamp the row. The block
// must stay queued and heal once the lookup works; a genuine missing header
// still ends the walk (TestReconciler_MixedReanchorAndRevert, whose heights
// above 10 resolve to nothing, completes with the revert as before).
func TestReconciler_NeighborhoodHeaderErrorKeepsRowQueued(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	stub.setHeightHeader(11, headerWithHash(t, recNeighbor, 11))
	stub.setHeightErr(11, errors.New("injected: header source unavailable"))

	seedMined(t, st, recOrphan, 10, recRebin)
	// Canonical at 10 does not contain the tx (so the canonical re-mine is
	// ready and the revert would run); the block at 11 does.
	_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
	_ = st.InsertBUMP(ctx, recNeighbor, 11, makeCompoundForTest(t, 11, recRebin))
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("tx must not be reverted while a neighbor cannot be looked up, got %s@%s", got.Status, got.BlockHash)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("row must stay queued and unstamped, got %+v err=%v", bp, err)
	}
	for _, ev := range pub.bulkEvents() {
		if ev.Status == models.StatusSeenOnNetwork {
			t.Fatalf("no revert event may be published, got %+v", ev)
		}
	}

	stub.setHeightErr(11, nil)
	r.tick(ctx)
	if got := statusOf(t, st, recRebin); got.Status != models.StatusMined || got.BlockHash != recNeighbor {
		t.Fatalf("retry must re-anchor the tx to the neighbor, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, lerr := st.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 0 {
		t.Fatalf("row must leave the queue once healed, got %+v err=%v", rows, lerr)
	}
}

// seedQueuedCanonicalTwoTxs is seedQueuedCanonical with a two-tx BUMP, so a
// batchSize of 1 splits the re-mine into two batches a test can act between.
func seedQueuedCanonicalTwoTxs(t *testing.T, st store.Store, stub *stubChaintracks) *models.BlockProcessingStatus {
	t.Helper()
	ctx := context.Background()
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	seedSeen(t, st, recShared1, recShared2)
	if err := st.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1, recShared2)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].BlockHash != recOrphan {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}
	return rows[0]
}

// TestReconciler_ShortCircuitPartialRemineSurvivesConcurrentReactivation:
// batch 1 of the resurrection re-mine lands, then the tracker reactivates
// the row and batch 2 fails (and keeps failing through the in-process
// retries). The row is now active and off the durable queue with one tx
// still SEEN, and "error, the row is still queued" would be wrong. The
// remainder must land on a path that WILL retry it: the row is re-orphaned
// (fresh generation, so the next tick's short-circuit re-mines and
// reactivates it), the event is counted and a rebuild requested. Once the
// store recovers the next tick heals both txs and returns the row to active.
func TestReconciler_ShortCircuitPartialRemineSurvivesConcurrentReactivation(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	row := seedQueuedCanonicalTwoTxs(t, base, stub)
	// Between the two batches: the tracker reactivates the row, and the
	// store starts failing.
	hs.onMinedCall = func(call int) {
		if call == 2 {
			_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
			hs.failMined = true
		}
	}
	before := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	withReprocess(r, reprocess)
	if outcome := r.reconcileBlock(ctx, row); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	// Batch 1 landed, batch 2 did not.
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("premise: batch 1 must have landed, got %s@%s", got.Status, got.BlockHash)
	}
	if got := statusOf(t, base, recShared2); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("premise: batch 2 must have failed, got %s@%s", got.Status, got.BlockHash)
	}
	// The durable hand-off: back on the queue, counted, rebuild requested.
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("the reactivated row must be re-orphaned onto the queue, got %+v err=%v", bp, err)
	}
	if rows, lerr := base.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 1 {
		t.Fatalf("row must be on the reconcile queue, got %+v err=%v", rows, lerr)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - before; got != 1 {
		t.Fatalf("remine_requeue_total = %v, want 1", got)
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("a rebuild must be requested as well, got %v", got)
	}

	// The store recovers: the next tick re-mines the remainder and reactivates.
	hs.failMined = false
	r.tick(ctx)
	for _, id := range []string{recShared1, recShared2} {
		if got := statusOf(t, base, id); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("%s: want MINED@%s after the retry, got %s@%s", id, recOrphan, got.Status, got.BlockHash)
		}
	}
	bp, err = base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("row must be active and clean after the retry, got %+v err=%v", bp, err)
	}
}

// TestReconciler_ShortCircuitPartialRemineStillOursStaysQueued: the same
// partial failure with no concurrent reactivation. The row is still the
// judged orphan, so it simply stays queued at its generation — no re-orphan,
// no count — and the next tick finishes the re-mine.
func TestReconciler_ShortCircuitPartialRemineStillOursStaysQueued(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	row := seedQueuedCanonicalTwoTxs(t, base, stub)
	hs.onMinedCall = func(call int) {
		if call == 2 {
			hs.failMined = true
		}
	}
	before := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	if outcome := r.reconcileBlock(ctx, row); outcome != "error" {
		t.Fatalf("outcome = %q, want error", outcome)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil || !bp.OrphanedAt.Equal(*row.OrphanedAt) {
		t.Fatalf("row must stay queued at its own generation, got %+v err=%v", bp, err)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - before; got != 0 {
		t.Fatalf("remine_requeue_total = %v, want 0 (nothing was re-orphaned)", got)
	}

	hs.failMined = false
	r.tick(ctx)
	if got := statusOf(t, base, recShared2); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the retry must finish the re-mine, got %s@%s", got.Status, got.BlockHash)
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("row must be reactivated after the retry, got %+v err=%v", bp, err)
	}
}

// TestReconciler_FullScanPartialRemineHandsOffToTick: the full-scan's
// resurrect re-mine has the same exposure — the tracker can reactivate the
// row while its batches run, after which a scan retry would no longer
// select it. A partial failure is handed to the tick through the same
// generation-aware path (here: re-orphaned, counted, rebuild requested), and
// the scan is complete for it.
func TestReconciler_FullScanPartialRemineHandsOffToTick(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	seedSeen(t, base, recShared1, recShared2)
	if err := base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1, recShared2)); err != nil {
		t.Fatalf("insert BUMP: %v", err)
	}
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now()) // off the tick's queue
	hs.onMinedCall = func(call int) {
		if call == 2 {
			_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
			hs.failMined = true
		}
	}
	before := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	withReprocess(r, reprocess)
	if !r.fullScan(ctx) {
		t.Fatal("a repair handed to the tick must not leave the scan incomplete")
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("the reactivated row must be re-orphaned onto the queue, got %+v err=%v", bp, err)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - before; got != 1 {
		t.Fatalf("remine_requeue_total = %v, want 1", got)
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("a rebuild must be requested, got %v", got)
	}

	hs.failMined = false
	r.tick(ctx)
	for _, id := range []string{recShared1, recShared2} {
		if got := statusOf(t, base, id); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("%s: want MINED@%s after the tick, got %s@%s", id, recOrphan, got.Status, got.BlockHash)
		}
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("row must be active after the tick, got %+v err=%v", bp, err)
	}
}

// TestReconciler_PartialRemineHandOffRetriedInProcess: the hand-off writes
// are single-row store writes, so a blip is ridden out in-process. Batch 1
// lands, the tracker reactivates, batch 2 fails; the first two hand-off
// writes are refused, the third lands: the row is re-orphaned onto the
// queue within the same pass, the requeue counter moves once (from the
// transition the write reported, not before it), and nothing is held in
// memory.
func TestReconciler_PartialRemineHandOffRetriedInProcess(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	row := seedQueuedCanonicalTwoTxs(t, base, stub)
	hs.onMinedCall = func(call int) {
		if call == 2 {
			_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
			hs.failMined = true
			hs.failHandOffN = 2 // the requeue attempt and the first re-orphan attempt are refused
		}
	}
	requeued := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)
	failed := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal)

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	if outcome := r.reconcileBlock(ctx, row); outcome != outcomeError {
		t.Fatalf("outcome = %q, want %s", outcome, outcomeError)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("the hand-off must land within the pass once the store answers, got %+v err=%v", bp, err)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - requeued; got != 1 {
		t.Fatalf("remine_requeue_total = %v, want 1 (the transition the successful write reported)", got)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal) - failed; got != 0 {
		t.Fatalf("remine_handoff_failed_total = %v, want 0 (the retry landed)", got)
	}
	if len(r.pendingRemine) != 0 {
		t.Fatalf("nothing may be held in memory once the hand-off landed, got %v", r.pendingRemine)
	}
}

// TestReconciler_PartialRemineHandOffFailureIsRetriedNextTick: the tracker
// reactivated the row mid-re-mine and the store refuses every hand-off
// write, so the row is active and off every in-store queue with one tx still
// SEEN — the state no scan revisits. The block must be held in memory
// (counted, gauge up, requeue counter unmoved because no re-orphan landed)
// and retried on the next tick: with the store back, the tick re-mines the
// remainder directly, the row stays active and clean, and the set drains.
func TestReconciler_PartialRemineHandOffFailureIsRetriedNextTick(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	row := seedQueuedCanonicalTwoTxs(t, base, stub)
	hs.onMinedCall = func(call int) {
		if call == 2 {
			_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
			hs.failMined = true
			hs.failHandOffN = -1 // every hand-off write refused
		}
	}
	requeued := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)
	failed := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal)

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	withReprocess(r, reprocess)
	if outcome := r.reconcileBlock(ctx, row); outcome != outcomeError {
		t.Fatalf("outcome = %q, want %s", outcome, outcomeError)
	}
	// The store refused everything: the row is active, off the queue, half re-mined.
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("premise: the row is active after the tracker's reactivation, got %+v err=%v", bp, err)
	}
	if got := statusOf(t, base, recShared2); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("premise: batch 2 must not have landed, got %s@%s", got.Status, got.BlockHash)
	}
	if rows, lerr := base.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 0 {
		t.Fatalf("premise: no in-store queue holds the row, got %+v err=%v", rows, lerr)
	}
	// The observable fallback: held in memory, counted, no phantom requeue.
	if h, held := r.pendingRemine[recOrphan]; !held || h != 10 {
		t.Fatalf("the block must be held in the pending set with its height, got %v", r.pendingRemine)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal) - failed; got != 1 {
		t.Fatalf("remine_handoff_failed_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffPending); got != 1 {
		t.Fatalf("remine_handoff_pending = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - requeued; got != 0 {
		t.Fatalf("remine_requeue_total = %v, want 0 (no re-orphan landed)", got)
	}
	if hs.handOffCalls < handOffAttempts {
		t.Fatalf("the hand-off must be retried in-process, got %d write attempts", hs.handOffCalls)
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("a rebuild must be requested at hand-off time, got %v", got)
	}

	// Store still down: the tick keeps the block, unhealed.
	r.tick(ctx)
	if _, held := r.pendingRemine[recOrphan]; !held {
		t.Fatal("the block must stay pending while the store is down")
	}

	// Store back: the tick re-mines the remainder directly and drains the set.
	hs.failMined = false
	hs.failHandOffN = 0
	r.tick(ctx)
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("the pending set must drain once the block heals, got %v", r.pendingRemine)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffPending); got != 0 {
		t.Fatalf("remine_handoff_pending = %v, want 0", got)
	}
	for _, id := range []string{recShared1, recShared2} {
		if got := statusOf(t, base, id); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("%s: want MINED@%s after the drain, got %s@%s", id, recOrphan, got.Status, got.BlockHash)
		}
	}
	bp, err = base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("the canonical row must stay active and clean, got %+v err=%v", bp, err)
	}
}

// TestReconciler_PendingRemineHandsOffWhenRemineKeepsFailing: the store is
// back for the single-row writes but SetMinedByTxIDs still fails. The drain
// cannot heal the block, so it lands the durable hand-off instead (the row
// is re-orphaned onto the queue) and drops the block from memory; the
// queue's next pass re-mines and reactivates it once the mines work.
func TestReconciler_PendingRemineHandsOffWhenRemineKeepsFailing(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	row := seedQueuedCanonicalTwoTxs(t, base, stub)
	hs.onMinedCall = func(call int) {
		if call == 2 {
			_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
			hs.failMined = true
			hs.failHandOffN = -1
		}
	}
	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	_ = r.reconcileBlock(ctx, row)
	if _, held := r.pendingRemine[recOrphan]; !held {
		t.Fatal("premise: the block is pending")
	}
	requeued := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)

	// Single-row writes work again; mines still fail.
	hs.failHandOffN = 0
	r.tick(ctx)
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("once the hand-off lands the queue owns the block; the set must drop it, got %v", r.pendingRemine)
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusOrphaned || bp.ReconciledAt != nil {
		t.Fatalf("the block must be re-orphaned onto the durable queue, got %+v err=%v", bp, err)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - requeued; got != 1 {
		t.Fatalf("remine_requeue_total = %v, want 1", got)
	}

	// Mines work: the queue's pass finishes the heal.
	hs.failMined = false
	r.tick(ctx)
	if got := statusOf(t, base, recShared2); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the queued pass must re-mine the remainder, got %s@%s", got.Status, got.BlockHash)
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("the row must be reactivated, got %+v err=%v", bp, err)
	}
}

// TestReconciler_PartialRemineRequeueCounterFollowsTheWrite: the requeue
// counter must report re-orphans that LANDED, taken from the transition the
// write reports — not the attempts. Here the hand-off's requeue and read
// succeed (the row is active) but the re-orphan write itself is refused on
// every attempt: the counter must not move, the block is held pending; once
// the write is accepted (mines still failing, so the drain hands off) the
// counter moves exactly once, from the transition that landed.
func TestReconciler_PartialRemineRequeueCounterFollowsTheWrite(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	row := seedQueuedCanonicalTwoTxs(t, base, stub)
	hs.onMinedCall = func(call int) {
		if call == 2 {
			_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
			hs.failMined = true
			hs.failOrphanN = -1 // requeue + read succeed; the re-orphan is refused
		}
	}
	requeued := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)
	orphaned := testutil.ToFloat64(metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceReconciler,
	))

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.BatchSize = 1 })
	if outcome := r.reconcileBlock(ctx, row); outcome != outcomeError {
		t.Fatalf("outcome = %q, want %s", outcome, outcomeError)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - requeued; got != 0 {
		t.Fatalf("remine_requeue_total = %v, want 0 (every re-orphan write was refused)", got)
	}
	if got := testutil.ToFloat64(metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceReconciler,
	)) - orphaned; got != 0 {
		t.Fatalf("orphaned/reconciler = %v, want 0 (no transition landed)", got)
	}
	if _, held := r.pendingRemine[recOrphan]; !held {
		t.Fatal("the block must be held pending after the refused re-orphan")
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("premise: the row is still active, got %+v err=%v", bp, err)
	}

	// The re-orphan is accepted; mines still fail, so the drain hands off.
	hs.failOrphanN = 0
	r.tick(ctx)
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - requeued; got != 1 {
		t.Fatalf("remine_requeue_total = %v, want 1 (the one re-orphan that landed)", got)
	}
	if got := testutil.ToFloat64(metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceReconciler,
	)) - orphaned; got != 1 {
		t.Fatalf("orphaned/reconciler = %v, want 1", got)
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
		t.Fatalf("the row must be re-orphaned onto the queue, got %+v err=%v", bp, err)
	}
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("the queue owns the block now; the set must drop it, got %v", r.pendingRemine)
	}
}

// TestReconciler_LeaseExpiryTimerCancelsWhenRenewalHangs: a renewal RPC
// that never returns must not keep the pass alive past the lease. The fake
// leaser hangs every renewal and ignores the caller's context altogether, so
// the only thing that can end the pass is the independent expiry timer:
// leaseCtx must be cancelled at heldUntil while the heartbeat is still
// blocked inside the RPC. The RPC did carry a deadline no later than the
// lease (the other guard). When the hung renewal finally returns — late,
// and successfully — it must not resurrect the pass, and the heartbeat must
// exit rather than keep renewing.
func TestReconciler_LeaseExpiryTimerCancelsWhenRenewalHangs(t *testing.T) {
	gate := make(chan struct{})
	fl := &fakeLeaser{renewGate: gate}
	r := newTestReconciler(newPebbleForTest(t), &capturePublisher{}, &stubChaintracks{}, nil)
	r.leaser = fl
	r.holderID = "replica-a"
	r.leaseTTLOverride = 300 * time.Millisecond // heartbeat every 100 ms

	if _, ok := r.tryLease(context.Background()); !ok {
		t.Fatal("acquire")
	}
	// The pass is told the lease lapses between the first two heartbeats.
	heldUntil := time.Now().Add(150 * time.Millisecond)
	leaseCtx, release := r.holdLease(context.Background(), heldUntil)
	defer release()

	select {
	case <-leaseCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("leaseCtx was not cancelled at the lease expiry while the renewal hung")
	}
	if got := fl.callCount(); got != 2 {
		t.Fatalf("lease calls = %d, want 2 (the acquire and the one renewal still hanging)", got)
	}
	for _, dl := range fl.renewalDeadlines() {
		if dl.IsZero() || dl.After(heldUntil.Add(time.Millisecond)) {
			t.Fatalf("the renewal RPC must carry a deadline no later than the lease expiry %v, got %v", heldUntil, dl)
		}
	}

	// The hung renewal returns late and successfully: ignored.
	close(gate)
	time.Sleep(350 * time.Millisecond) // > 3 heartbeats
	if leaseCtx.Err() == nil {
		t.Fatal("a late successful renewal must not resurrect a cancelled pass")
	}
	if got := fl.callCount(); got != 2 {
		t.Fatalf("the heartbeat must exit after the pass was cancelled, but kept renewing: %d calls", got)
	}
}

// TestReconciler_LeaseRenewalBoundedByDeadline: each renewal RPC runs under
// a deadline of min(heldUntil, now+TTL/3), so a leaser that does honour its
// context returns DeadlineExceeded within one heartbeat instead of blocking
// the goroutine for the rest of the lease. Here the lease is long, so the
// bound is the heartbeat: the hung renewal must come back with
// DeadlineExceeded (not the pass's own cancellation) well before the lease
// expires, and the pass keeps running meanwhile.
func TestReconciler_LeaseRenewalBoundedByDeadline(t *testing.T) {
	fl := &fakeLeaser{renewGate: make(chan struct{}), honourCtx: true, expiry: 5 * time.Second}
	r := newTestReconciler(newPebbleForTest(t), &capturePublisher{}, &stubChaintracks{}, nil)
	r.leaser = fl
	r.holderID = "replica-a"
	r.leaseTTLOverride = 300 * time.Millisecond // heartbeat every 100 ms

	heldUntil, ok := r.tryLease(context.Background())
	if !ok {
		t.Fatal("acquire")
	}
	leaseCtx, release := r.holdLease(context.Background(), heldUntil)
	defer release()

	// The first renewal must come back — with DeadlineExceeded — while the
	// lease is still held and the pass still alive.
	if !waitFor(2*time.Second, func() bool { return len(fl.renewalResults()) >= 1 }) {
		t.Fatal("the hung renewal never returned: the RPC carried no effective deadline")
	}
	if leaseCtx.Err() != nil {
		t.Fatal("the pass must keep running while the lease is still held")
	}
	res := fl.renewalResults()[0]
	if !errors.Is(res, context.DeadlineExceeded) {
		t.Fatalf("the renewal must be cut by its own deadline, got %v", res)
	}
	for _, dl := range fl.renewalDeadlines() {
		if dl.IsZero() || dl.After(heldUntil) {
			t.Fatalf("renewal deadline %v must be set and no later than the lease expiry %v", dl, heldUntil)
		}
		if dl.After(time.Now().Add(r.leaseTTL())) {
			t.Fatalf("renewal deadline %v must be bounded by one heartbeat, not the whole lease", dl)
		}
	}
}

// TestReconciler_FullScanBUMPReadFailureAfterReactivationHoldsPending: the
// tracker reactivates the canonical row between the full-scan's paging read
// and its BUMP read, and that read then fails transiently. A scan retry
// would skip the row — active rows are never candidates — so its txs would
// never be re-mined once the attempt cap retired the one-shot. The scan
// must re-check the row at the generation it read and, finding it active
// with nothing written, hold it in the pending set: the row stays active and
// clean (this path never re-orphans a canonical row), the rebuild is
// requested, the durable-hand-off failure counter does not move (nothing
// failed), the scan still reports itself incomplete, and the next tick
// re-mines the block directly and drains the set.
func TestReconciler_FullScanBUMPReadFailureAfterReactivationHoldsPending(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failGetBUMP: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	seedResurrectable(t, base, stub)
	// Between the paging read and the BUMP read: the tracker reactivates it.
	hs.beforeGetBUMP = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }
	requeued := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal)
	failed := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal)
	orphaned := testutil.ToFloat64(metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan,
	))

	r := newTestReconciler(hs, pub, stub, nil)
	withReprocess(r, reprocess)
	if r.fullScan(ctx) {
		t.Fatal("a scan whose BUMP read failed must report itself incomplete")
	}
	bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("the reactivated canonical row must stay active and clean, got %+v err=%v", bp, err)
	}
	if h, held := r.pendingRemine[recOrphan]; !held || h != 10 {
		t.Fatalf("the block must be held in the pending set with its height, got %v", r.pendingRemine)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffPending); got != 1 {
		t.Fatalf("remine_handoff_pending = %v, want 1", got)
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("a rebuild must be requested when the block is held, got %v", got)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineRequeuedTotal) - requeued; got != 0 {
		t.Fatalf("remine_requeue_total = %v, want 0 (nothing was re-orphaned)", got)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal) - failed; got != 0 {
		t.Fatalf("remine_handoff_failed_total = %v, want 0 (the hand-off succeeded, into the pending set)", got)
	}
	if got := testutil.ToFloat64(metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan,
	)) - orphaned; got != 0 {
		t.Fatalf("orphaned/full_scan = %v, want 0", got)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("premise: nothing was re-mined, got %s@%s", got.Status, got.BlockHash)
	}

	// The store recovers: the next tick re-mines the block directly.
	hs.failGetBUMP = false
	r.tick(ctx)
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("the pending set must drain once the block heals, got %v", r.pendingRemine)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the next tick must re-mine the tx, got %s@%s", got.Status, got.BlockHash)
	}
	bp, err = base.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
		t.Fatalf("the row must still be active and clean, got %+v err=%v", bp, err)
	}
}

// TestReconciler_FullScanBUMPReadFailureHandOffRefusedStillHeld: the same
// race, but the store also refuses the hand-off's own reads and writes. The
// block must still land in the pending set — now counted as a refused
// durable hand-off — and the next tick, store back, re-mines it directly.
func TestReconciler_FullScanBUMPReadFailureHandOffRefusedStillHeld(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base, failGetBUMP: true}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	seedResurrectable(t, base, stub)
	hs.beforeGetBUMP = func() {
		_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		hs.failHandOffN = -1 // every hand-off write refused
	}
	failed := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal)

	r := newTestReconciler(hs, pub, stub, nil)
	if r.fullScan(ctx) {
		t.Fatal("a scan whose BUMP read failed must report itself incomplete")
	}
	if h, held := r.pendingRemine[recOrphan]; !held || h != 10 {
		t.Fatalf("the block must be held in the pending set with its height, got %v", r.pendingRemine)
	}
	if got := testutil.ToFloat64(metrics.ReconcilerRemineHandoffFailedTotal) - failed; got != 1 {
		t.Fatalf("remine_handoff_failed_total = %v, want 1", got)
	}
	if hs.handOffCalls < handOffAttempts {
		t.Fatalf("the hand-off must be retried in-process, got %d write attempts", hs.handOffCalls)
	}

	hs.failGetBUMP = false
	hs.failHandOffN = 0
	r.tick(ctx)
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("the pending set must drain once the block heals, got %v", r.pendingRemine)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the drain must re-mine the tx directly, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_FullScanMalformedBUMPAfterReactivationHoldsPending: the
// same race on the malformed-BUMP branch. A plain requeue would find the row
// active and leave it — a canonical row with a corrupt BUMP, un-remined txs
// and no rebuild requested — while re-orphaning it would flip a canonical
// row back to orphaned, which this path must never do. The block is held
// pending with the rebuild requested; each drain asks again under the defer
// cap; once the BUMP is rebuilt the drain re-mines directly, the row having
// stayed active throughout.
func TestReconciler_FullScanMalformedBUMPAfterReactivationHoldsPending(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	seedSeen(t, base, recShared1)
	_ = base.InsertBUMP(ctx, recOrphan, 10, malformedBUMP)
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())
	hs.beforeGetBUMP = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }

	r := newTestReconciler(hs, pub, stub, nil)
	withReprocess(r, reprocess)
	if !r.fullScan(ctx) {
		t.Fatal("a repair handed to the pending set must not leave the scan incomplete")
	}
	assertActive := func(when string) {
		t.Helper()
		bp, err := base.GetBlockProcessingStatus(ctx, recOrphan)
		if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil || bp.ReconciledAt != nil {
			t.Fatalf("%s: the canonical row must stay active and clean, got %+v err=%v", when, bp, err)
		}
	}
	assertActive("after the scan")
	if _, held := r.pendingRemine[recOrphan]; !held {
		t.Fatalf("the block must be held pending, got %v", r.pendingRemine)
	}
	if got := reprocess.requested(); len(got) != 1 || got[0] != recOrphan {
		t.Fatalf("a rebuild must be requested when the block is held, got %v", got)
	}

	// The drain keeps asking, under the defer cap, without touching the row.
	r.tick(ctx)
	assertActive("after a drain on the corrupt BUMP")
	if got := reprocess.requested(); len(got) != 2 {
		t.Fatalf("the drain must ask for the rebuild again, got %v", got)
	}
	if _, held := r.pendingRemine[recOrphan]; !held {
		t.Fatal("the block must stay pending while the BUMP is corrupt")
	}

	// The rebuild lands: the drain re-mines directly.
	_ = base.InsertBUMP(ctx, recOrphan, 10, makeCompoundForTest(t, 10, recShared1))
	r.tick(ctx)
	assertActive("after the heal")
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("the pending set must drain once the block heals, got %v", r.pendingRemine)
	}
	if got := statusOf(t, base, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("the rebuilt BUMP must re-mine the tx, got %s@%s", got.Status, got.BlockHash)
	}
	if _, deferred := r.defers[recOrphan]; deferred {
		t.Fatal("a healed block must clear its rebuild-request count")
	}
}

// TestReconciler_PendingRemineMalformedGivesUpAtDeferCap: a pending block
// whose BUMP stays corrupt is asked to be rebuilt once per tick under the
// same cap the tick's deferral uses, then dropped — loudly, the row still
// active — rather than requested without bound from memory forever.
func TestReconciler_PendingRemineMalformedGivesUpAtDeferCap(t *testing.T) {
	ctx := context.Background()
	base := newPebbleForTest(t)
	hs := &hookedStore{Store: base}
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	reprocess := newReprocessStub(t)
	stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
	seedSeen(t, base, recShared1)
	_ = base.InsertBUMP(ctx, recOrphan, 10, malformedBUMP)
	_ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = base.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	_, _ = base.MarkBlockReconciled(ctx, recOrphan, time.Time{}, time.Now())
	hs.beforeGetBUMP = func() { _ = base.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()) }

	r := newTestReconciler(hs, pub, stub, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 2 })
	withReprocess(r, reprocess)
	_ = r.fullScan(ctx)
	if _, held := r.pendingRemine[recOrphan]; !held {
		t.Fatal("premise: the block is pending")
	}
	for i := 1; i <= 2; i++ {
		r.tick(ctx)
		if _, held := r.pendingRemine[recOrphan]; !held {
			t.Fatalf("drain %d: the block must stay pending under the cap", i)
		}
	}
	r.tick(ctx) // at the cap
	if _, held := r.pendingRemine[recOrphan]; held {
		t.Fatalf("at the cap the block must be dropped from the pending set, got %v", r.pendingRemine)
	}
	if _, deferred := r.defers[recOrphan]; deferred {
		t.Fatal("the rebuild-request count must be cleared with the block")
	}
	// One request when held, then one per drain under the cap.
	if got := len(reprocess.requested()); got != 3 {
		t.Fatalf("rebuild requests = %d, want 3 (held + 2 drains)", got)
	}
	if bp, err := base.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
		t.Fatalf("the canonical row must never have been re-orphaned, got %+v err=%v", bp, err)
	}
}

// TestReconciler_FullScanMarkOrphanedRechecksCanonicality: the active→orphaned
// candidates are judged during the paging walk, and MarkBlocksOrphaned has
// no canonicality guard of its own. A reorg during the walk can make a
// candidate canonical again; marking it on the walk's stale verdict would
// demote a canonical row (and regress a reactivation the tracker applied
// meanwhile). Each candidate is therefore re-checked against the active
// chain immediately before the write: a candidate that is canonical NOW is
// dropped and the scan still completes for the rest; a candidate whose
// height the header source can no longer judge — a lookup failure, or a
// height it cannot serve — is dropped as well and the scan reported
// incomplete, since absence of evidence never orphans. Only rows the header
// source positively places off-chain are marked, and the applied count is
// what the write reports.
func TestReconciler_FullScanMarkOrphanedRechecksCanonicality(t *testing.T) {
	counter := metrics.BlockStatusTransitionsTotal.WithLabelValues(
		metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan,
	)

	t.Run("CandidateBecameCanonical", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		// The walk judged both rows at height 10 off-chain (a third block
		// held the height); before the write, a reorg hands the height to
		// recOrphan.
		stub.setHeightHeader(10, headerWithHash(t, recOrphan, 10))
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())
		before := testutil.ToFloat64(counter)

		r := newTestReconciler(st, pub, stub, nil)
		if !r.fullScanMarkOrphaned(ctx, map[string]uint64{recOrphan: 10, recCanonical: 10}) {
			t.Fatal("a candidate that turned canonical is dropped, not a reason to retry: the scan must complete")
		}
		bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
		if err != nil || bp.Status != models.BlockStatusActive || bp.OrphanedAt != nil {
			t.Fatalf("the now-canonical candidate must not be orphaned, got %+v err=%v", bp, err)
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recCanonical); err != nil || bp.Status != models.BlockStatusOrphaned {
			t.Fatalf("the candidate still off-chain must be orphaned, got %+v err=%v", bp, err)
		}
		if got := testutil.ToFloat64(counter) - before; got != 1 {
			t.Fatalf("orphaned/full_scan = %v, want 1 (only the row actually marked)", got)
		}
	})

	t.Run("LookupErrorLeavesCandidateForRetry", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		stub.setHeightHeader(10, headerWithHash(t, recNeighbor, 10)) // a third block holds the height
		stub.setHeightErr(10, errors.New("injected: header source unavailable"))
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		before := testutil.ToFloat64(counter)

		r := newTestReconciler(st, pub, stub, nil)
		if r.fullScanMarkOrphaned(ctx, map[string]uint64{recOrphan: 10}) {
			t.Fatal("a candidate that could not be re-judged must leave the scan incomplete")
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
			t.Fatalf("absence of evidence must never orphan, got %+v err=%v", bp, err)
		}
		if got := testutil.ToFloat64(counter) - before; got != 0 {
			t.Fatalf("orphaned/full_scan = %v, want 0", got)
		}

		// The header source recovers: the retry marks it.
		stub.setHeightErr(10, nil)
		if !r.fullScanMarkOrphaned(ctx, map[string]uint64{recOrphan: 10}) {
			t.Fatal("the retry must complete once the height can be judged")
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusOrphaned {
			t.Fatalf("the retry must orphan the off-chain row, got %+v err=%v", bp, err)
		}
		if got := testutil.ToFloat64(counter) - before; got != 1 {
			t.Fatalf("orphaned/full_scan = %v, want 1", got)
		}
	})

	t.Run("UnservedHeightLeavesCandidateForRetry", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		pub := &capturePublisher{}
		stub := &stubChaintracks{} // nothing served at 10: cannot judge
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())

		r := newTestReconciler(st, pub, stub, nil)
		if r.fullScanMarkOrphaned(ctx, map[string]uint64{recOrphan: 10}) {
			t.Fatal("a height the header source cannot serve must leave the scan incomplete")
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
			t.Fatalf("absence of evidence must never orphan, got %+v err=%v", bp, err)
		}
	})
}

// TestReconciler_HeaderNotFoundAboveTipIsEndOfChain reproduces the e2e
// failure the round-7 tri-state introduced. go-chaintracks' ChainManager
// answers every height at or beyond its tip with
// chaintracks.ErrHeaderNotFound — not (nil, nil) — so the neighborhood walk
// above an orphan meets that error on every tick. Treated as a lookup
// FAILURE it kept the orphan in "error" forever and its orphan-only tx
// MINED against a dead block; it is the ordinary end of the chain, and the
// walk must finish and revert the remainder exactly as it does for a nil
// header. The resurrection short-circuit's own lookup at the orphan's
// height goes through the same classification.
func TestReconciler_HeaderNotFoundAboveTipIsEndOfChain(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	stub := &stubChaintracks{}
	stub.setNotFoundErrors()
	stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10)) // the tip: 11+ are "header not found"

	// The e2e shape: the orphan lost a same-height tie; the canonical
	// block's BUMP holds the shared tx, nothing above holds bOnly.
	seedMined(t, st, recOrphan, 10, recShared1, recBOnly)
	if err := st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1)); err != nil {
		t.Fatalf("insert canonical BUMP: %v", err)
	}
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
	}

	r := newTestReconciler(st, pub, stub, nil)
	if outcome := r.reconcileBlock(ctx, rows[0]); outcome != outcomeMixed {
		t.Fatalf("outcome = %q, want %s (shared re-anchored, bOnly reverted)", outcome, outcomeMixed)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recCanonical {
		t.Fatalf("shared: want MINED@%s, got %s@%s", recCanonical, got.Status, got.BlockHash)
	}
	if got := statusOf(t, st, recBOnly); got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("bOnly must revert to SEEN_ON_NETWORK once the walk reaches the end of the chain, got %s@%s",
			got.Status, got.BlockHash)
	}
	bp, err := st.GetBlockProcessingStatus(ctx, recOrphan)
	if err != nil || bp.ReconciledAt == nil {
		t.Fatalf("the orphan must be stamped reconciled, got %+v err=%v", bp, err)
	}
	if rows, lerr := st.ListOrphanedBlocksToReconcile(ctx, 10); lerr != nil || len(rows) != 0 {
		t.Fatalf("the orphan must leave the queue, got %+v err=%v", rows, lerr)
	}
}

// TestReconciler_HeaderNotFoundIsAbsenceEverywhere: the same sentinel at
// the other classification sites. At the orphan's own height it is
// "cannot judge" — the pass parks (canonical unknown), not "error"; in the
// full-scan's per-row judgement and its pre-write re-check it leaves the
// row unjudged and the scan incomplete, never orphaning on it; a transport
// error at the same heights is still a failure (the round-7 rule).
func TestReconciler_HeaderNotFoundIsAbsenceEverywhere(t *testing.T) {
	t.Run("OrphanHeightUnknownParksInsteadOfErroring", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		stub.setNotFoundErrors() // nothing served: every height is "header not found"
		seedMined(t, st, recOrphan, 10, recShared1)
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
		rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
		}
		r := newTestReconciler(st, pub, stub, nil)
		// No canonical block can be named, so nothing to defer on: the
		// pre-existing fail-open path parks the block (txs left MINED@O).
		if outcome := r.reconcileBlock(ctx, rows[0]); outcome != outcomeParked {
			t.Fatalf("outcome = %q, want %s (absence is the fail-open path, not a lookup failure)", outcome, outcomeParked)
		}
		if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("parked txs stay MINED@orphan, got %s@%s", got.Status, got.BlockHash)
		}
	})

	t.Run("FullScanLeavesUnservedRowUnjudged", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		stub.setNotFoundErrors()
		seedResurrectable(t, st, stub)                                // judgeable at 10
		stub.setHeightHeader(12, headerWithHash(t, recCanonical, 12)) // the tip
		_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 12, time.Now())
		_ = st.UpsertBlockHeaderSeen(ctx, recNeighbor, 11, time.Now()) // 11 is "header not found"

		r := newTestReconciler(st, pub, stub, func(c *config.ReconcilerConfig) { c.StartupFullScan = true })
		r.tick(ctx)
		if r.startupScanDone {
			t.Fatal("an unserved candidate height must keep the one-shot armed")
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recNeighbor); err != nil || bp.Status != models.BlockStatusActive {
			t.Fatalf("absence of evidence must never orphan, got %+v err=%v", bp, err)
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recOrphan); err != nil || bp.Status != models.BlockStatusActive {
			t.Fatalf("the judgeable row must still heal, got %+v err=%v", bp, err)
		}
		// The pre-write re-check reads the sentinel the same way.
		if r.fullScanMarkOrphaned(ctx, map[string]uint64{recNeighbor: 11}) {
			t.Fatal("a candidate at an unserved height must leave the scan incomplete")
		}
		if bp, err := st.GetBlockProcessingStatus(ctx, recNeighbor); err != nil || bp.Status != models.BlockStatusActive {
			t.Fatalf("absence of evidence must never orphan, got %+v err=%v", bp, err)
		}
	})

	t.Run("TransportErrorIsStillAFailure", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		pub := &capturePublisher{}
		stub := &stubChaintracks{}
		stub.setNotFoundErrors()
		stub.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
		stub.setHeightErr(11, errors.New("injected: header source unavailable")) // not the sentinel
		seedMined(t, st, recOrphan, 10, recBOnly)
		_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared1))
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
		rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("precondition: one queued row, got %+v err=%v", rows, err)
		}
		r := newTestReconciler(st, pub, stub, nil)
		if outcome := r.reconcileBlock(ctx, rows[0]); outcome != outcomeError {
			t.Fatalf("outcome = %q, want %s (a real lookup failure keeps the block queued)", outcome, outcomeError)
		}
		if got := statusOf(t, st, recBOnly); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("nothing may be reverted on a failed lookup, got %s@%s", got.Status, got.BlockHash)
		}
	})
}
