package bump_builder

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// merkleTTLStub models the merkle-service registration lifetime that the
// mainnet 973522 incident (2026-10-31) turned on: a /watch registration
// lives until the txid is seen in a block, after which merkle-service
// touches it with block.postMineTTLSec (1800 s in every deployed
// configmap) — so a tx re-mined more than 30 minutes after its first mine
// produces no STUMP, and the broadcaster never learns of the second block.
// A fresh /watch rewrites the record and clears that TTL.
type merkleTTLStub struct {
	mu          sync.Mutex
	postMineTTL time.Duration
	// expiresAt is the registration's expiry; the zero time means the
	// registration does not expire (registered, not yet seen mined).
	expiresAt map[string]time.Time
	watches   map[string]int
	fail      bool
	failTx    map[string]bool // /watch refused for these txids only
	reprocess []string
	srv       *httptest.Server
}

func newMerkleTTLStub(t *testing.T) *merkleTTLStub {
	t.Helper()
	s := &merkleTTLStub{
		postMineTTL: 30 * time.Minute,
		expiresAt:   make(map[string]time.Time),
		watches:     make(map[string]int),
	}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			TxID      string `json:"txid"`
			BlockHash string `json:"blockHash"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if req.URL.Path == "/reprocess" {
			s.reprocess = append(s.reprocess, body.BlockHash)
			w.WriteHeader(http.StatusAccepted) // accepted, nothing to redeliver
			return
		}
		if s.fail || s.failTx[body.TxID] {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s.expiresAt[body.TxID] = time.Time{}
		s.watches[body.TxID]++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// register is the propagator's original /watch, before broadcast.
func (s *merkleTTLStub) register(txids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range txids {
		s.expiresAt[id] = time.Time{}
	}
}

// sawMined is merkle-service's subtree processor finding the txids in a
// block at `at`: their registrations now expire postMineTTL later.
func (s *merkleTTLStub) sawMined(at time.Time, txids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range txids {
		s.expiresAt[id] = at.Add(s.postMineTTL)
	}
}

// watching reports whether merkle-service still holds a registration for
// txid at `at` — i.e. whether a block processed then emits a STUMP for it.
func (s *merkleTTLStub) watching(txid string, at time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.expiresAt[txid]
	return ok && (exp.IsZero() || at.Before(exp))
}

func (s *merkleTTLStub) watchCount(txid string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watches[txid]
}

func (s *merkleTTLStub) setFailTx(txid string, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failTx == nil {
		s.failTx = make(map[string]bool)
	}
	s.failTx[txid] = fail
}

func (s *merkleTTLStub) reprocessed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.reprocess...)
}

func (s *merkleTTLStub) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func withMerkleTTLStub(r *Reconciler, stub *merkleTTLStub) {
	r.merkle = merkleservice.NewClient(stub.srv.URL, "", time.Second)
	r.cfg.CallbackURL = "http://broadcaster.test/callback"
}

// TestReconciler_OrphanTxsReminedAfterPostMineTTL_ReanchorToNewBlock is the
// mainnet 973522 incident (arcade-v2-us-1, 2026-10-31) end to end:
//
//   - 22:00:21 the tx is mined in block O (…f979) at 973522; merkle-service
//     starts its 30-minute post-mine TTL on the registration.
//   - 22:09:32 the tie-scan orphans O — the competitor C (…83b8) won 973522.
//     C holds none of our txs, so it never gets a BUMP: the reconciler defers
//     to the cap and then walks the neighborhood.
//   - 22:31:40 the tx is re-mined in N (…f02a) at 973524 — 80 s after its
//     registration expired. Without a fresh /watch merkle-service emits no
//     STUMP for it, N's compound BUMP is built without it, and it stays
//     MINED@O when the reconciler parks O at 22:41:14 (~8,750 txs, still
//     MINED against the orphan the next morning).
//
// The reconciler must refresh the merkle-service watch for every tx still
// anchored to an orphan, so the block that re-mines them reports them.
func TestReconciler_OrphanTxsReminedAfterPostMineTTL_ReanchorToNewBlock(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	chain := &stubChaintracks{}
	merkle := newMerkleTTLStub(t)

	const (
		orphanHeight   = 10 // 973522
		reminedHeight  = 12 // 973524
		canonicalOther = recShared2
	)
	minedAt := time.Date(2026, 10, 31, 22, 0, 21, 0, time.UTC)

	// The tx was registered, broadcast, and mined in O.
	merkle.register(recRebin)
	seedMined(t, st, recOrphan, orphanHeight, recRebin)
	merkle.sawMined(minedAt, recRebin)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, orphanHeight, minedAt)

	// O loses its height to C, which carries no tracked tx (no BUMP).
	chain.setHeightHeader(orphanHeight, headerWithHash(t, recCanonical, orphanHeight))
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, minedAt.Add(9*time.Minute))

	r := newTestReconciler(st, pub, chain, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 2 })
	withMerkleTTLStub(r, merkle)

	r.tick(ctx) // first pass over the orphan, ~22:09:43: defer 1/2

	// 22:31:40: N re-mines the tx. merkle-service reports it only if it is
	// still watching; either way N gets a BUMP from its other tracked txs.
	reminedAt := minedAt.Add(31*time.Minute + 19*time.Second)
	chain.setHeightHeader(reminedHeight, headerWithHash(t, recNeighbor, reminedHeight))
	inN := []string{canonicalOther}
	if merkle.watching(recRebin, reminedAt) {
		inN = append(inN, recRebin)
	}
	_ = st.InsertBUMP(ctx, recNeighbor, reminedHeight, makeCompoundForTest(t, reminedHeight, inN...))
	if _, _, err := st.SetMinedByTxIDs(ctx, recNeighbor, reminedHeight, inN); err != nil {
		t.Fatalf("build-path mine of N: %v", err)
	}

	r.tick(ctx) // defer 2/2
	r.tick(ctx) // cap → neighborhood walk → park

	got := statusOf(t, st, recRebin)
	if got.Status != models.StatusMined || got.BlockHash != recNeighbor || got.BlockHeight != reminedHeight {
		t.Fatalf("a tx re-mined after merkle-service's post-mine TTL must end MINED@N (h%d), got %s@%s h%d — "+
			"the reconciler never refreshed its merkle-service watch, so N's STUMP never reported it",
			reminedHeight, got.Status, got.BlockHash, got.BlockHeight)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("O must leave the reconcile queue, got %d rows", len(rows))
	}
}

// TestReconciler_OrphanRewatchOncePerGenerationRetriesOnFailure: the
// re-watch is a one-off per orphan generation — the defer ticks that follow
// must not re-POST every anchored txid every 30 s — but a failed one is
// retried on the next tick instead of being lost. While it is outstanding
// the pass is held, and a hold does not spend the defer budget.
func TestReconciler_OrphanRewatchOncePerGenerationRetriesOnFailure(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	chain := &stubChaintracks{}
	merkle := newMerkleTTLStub(t)
	chain.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1, recAOnly)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, chain, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 5 })
	withMerkleTTLStub(r, merkle)

	merkle.setFail(true)
	r.tick(ctx) // re-watch fails; the pass is held
	if n := merkle.watchCount(recShared1); n != 0 {
		t.Fatalf("failing merkle-service must not record a watch, got %d", n)
	}
	if r.defers[recOrphan] != 0 {
		t.Fatalf("a hold must not spend the defer budget, defers=%d", r.defers[recOrphan])
	}

	merkle.setFail(false)
	r.tick(ctx) // retried
	r.tick(ctx) // already refreshed: no new watch
	r.tick(ctx)
	for _, id := range []string{recShared1, recAOnly} {
		if n := merkle.watchCount(id); n != 1 {
			t.Fatalf("tx %s: want exactly one re-watch for the orphan generation, got %d", id, n)
		}
	}
}

// TestReconciler_NoParkUntilOrphanRewatched: merkle-service unreachable
// must not let the block park straight away — a park with the watches lapsed
// strands its txs exactly as at 973522, because nothing re-registers them
// once the block is off the queue. The block is held while the re-watch
// retries, then defers and parks as usual once it lands.
func TestReconciler_NoParkUntilOrphanRewatched(t *testing.T) {
	ctx := context.Background()
	st, merkle, r := newRewatchFixture(t, 2, recShared1)

	merkle.setFail(true)
	r.tick(ctx) // held 1/2
	r.tick(ctx) // held 2/2
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
		t.Fatalf("an orphan whose txs were never re-watched must stay queued, got %d rows", len(rows))
	}

	merkle.setFail(false)
	r.tick(ctx) // re-watch lands → defer 1/2
	r.tick(ctx) // defer 2/2
	r.tick(ctx) // cap → park
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("once re-watched the orphan must park and leave the queue, got %d rows", len(rows))
	}
	if n := merkle.watchCount(recShared1); n != 1 {
		t.Fatalf("want one re-watch before the park, got %d", n)
	}
	if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
		t.Fatalf("parked tx stays MINED@orphan, got %s@%s", got.Status, got.BlockHash)
	}
}

// TestReconciler_RewatchHoldIsBounded: a merkle-service that never accepts
// the re-watch (an outage, a rotated token) must not pin the orphan in the
// queue forever — the queue is oldest-first, so a few pinned rows would
// starve every newer orphan. Past the defer cap's worth of holds the block
// is retired anyway.
func TestReconciler_RewatchHoldIsBounded(t *testing.T) {
	ctx := context.Background()
	st, merkle, r := newRewatchFixture(t, 2, recShared1)

	merkle.setFail(true)
	for range 5 { // 2 holds, then 2 defers, then retired
		r.tick(ctx)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("a re-watch that never lands must not hold the orphan past the cap, got %d rows", len(rows))
	}
}

// TestReconciler_NoCanonicalReprocessUntilRewatched: the defer path's
// /reprocess of the canonical block must wait for the re-watch, or
// merkle-service rebuilds the canonical BUMP without the orphan's unwatched
// txs — which the next pass then reads as proof they are in no canonical
// block and reverts.
func TestReconciler_NoCanonicalReprocessUntilRewatched(t *testing.T) {
	ctx := context.Background()
	_, merkle, r := newRewatchFixture(t, 5, recShared1)

	merkle.setFail(true)
	r.tick(ctx)
	if got := merkle.reprocessed(); len(got) != 0 {
		t.Fatalf("no canonical /reprocess while the orphan's txs are unwatched, got %v", got)
	}

	merkle.setFail(false)
	r.tick(ctx)
	if got := merkle.reprocessed(); len(got) != 1 || got[0] != recCanonical {
		t.Fatalf("once re-watched the canonical block must be reprocessed, got %v", got)
	}
}

// TestReconciler_RewatchRetriesOnlyFailedTxs: a partial failure retries the
// txids that failed, not the whole orphan — and each tx counts once.
func TestReconciler_RewatchRetriesOnlyFailedTxs(t *testing.T) {
	ctx := context.Background()
	_, merkle, r := newRewatchFixture(t, 5, recShared1, recAOnly)

	merkle.setFailTx(recAOnly, true)
	r.tick(ctx)
	merkle.setFailTx(recAOnly, false)
	r.tick(ctx)
	r.tick(ctx)
	for _, id := range []string{recShared1, recAOnly} {
		if n := merkle.watchCount(id); n != 1 {
			t.Fatalf("tx %s: want exactly one successful /watch, got %d", id, n)
		}
	}
	if !r.orphanWatched(recOrphan, *mustOrphanedAt(t, r)) {
		t.Fatal("the orphan must count as re-watched once the failed tx landed")
	}
}

// newRewatchFixture is an orphan at height 10 holding txids, whose canonical
// competitor has no stored BUMP (the defer path), with a reconciler wired to
// a merkleTTLStub.
func newRewatchFixture(t *testing.T, maxDefer int, txids ...string) (store.Store, *merkleTTLStub, *Reconciler) {
	t.Helper()
	ctx := context.Background()
	st := newPebbleForTest(t)
	chain := &stubChaintracks{}
	merkle := newMerkleTTLStub(t)
	chain.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
	seedMined(t, st, recOrphan, 10, txids...)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
	r := newTestReconciler(st, &capturePublisher{}, chain, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = maxDefer })
	withMerkleTTLStub(r, merkle)
	return st, merkle, r
}

func mustOrphanedAt(t *testing.T, r *Reconciler) *time.Time {
	t.Helper()
	bp, err := r.store.GetBlockProcessingStatus(context.Background(), recOrphan)
	if err != nil || bp.OrphanedAt == nil {
		t.Fatalf("orphan row: %+v err=%v", bp, err)
	}
	return bp.OrphanedAt
}

// TestReconciler_RewatchLandingLateStillReprocessesCanonical: the re-watch
// lands only after as many failing ticks as the defer cap. Holds must not
// have spent the defer budget, or the pass parks straight away without ever
// asking merkle-service to rebuild the canonical block — and merkle-service
// has already processed that block, so no STUMP for the re-watched txs ever
// arrives and the ones in it stay MINED@orphan for good.
func TestReconciler_RewatchLandingLateStillReprocessesCanonical(t *testing.T) {
	ctx := context.Background()
	st, merkle, r := newRewatchFixture(t, 2, recShared1)

	merkle.setFail(true)
	r.tick(ctx)
	r.tick(ctx)
	merkle.setFail(false)
	for range 4 {
		r.tick(ctx)
	}
	if got := merkle.reprocessed(); len(got) == 0 || got[0] != recCanonical {
		t.Fatalf("the canonical block must be reprocessed once the re-watch lands, got %v", got)
	}
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("the orphan must still retire after its defers, got %d rows", len(rows))
	}
}

// TestReconciler_PermanentTxRewatchFailureDoesNotStarveReprocess: one txid
// merkle-service refuses every time must not keep the canonical /reprocess
// from ever being sent for the txs that did register. Past the hold cap the
// pass goes on: it defers with the /reprocess, and keeps retrying the one
// txid on the way.
func TestReconciler_PermanentTxRewatchFailureDoesNotStarveReprocess(t *testing.T) {
	ctx := context.Background()
	st, merkle, r := newRewatchFixture(t, 2, recShared1, recAOnly)

	merkle.setFailTx(recAOnly, true)
	r.tick(ctx) // held 1/2
	r.tick(ctx) // held 2/2
	if got := merkle.reprocessed(); len(got) != 0 {
		t.Fatalf("no /reprocess while held, got %v", got)
	}
	r.tick(ctx) // hold cap → defer 1/2 with /reprocess
	if got := merkle.reprocessed(); len(got) != 1 || got[0] != recCanonical {
		t.Fatalf("past the hold cap the canonical block must be reprocessed, got %v", got)
	}
	r.tick(ctx) // defer 2/2
	r.tick(ctx) // cap → park
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 0 {
		t.Fatalf("the orphan must retire, got %d rows", len(rows))
	}
	if n := merkle.watchCount(recShared1); n != 1 {
		t.Fatalf("the registered tx is not re-POSTed, got %d", n)
	}
}

// TestReconciler_RewatchHoldOutcome: a held pass reports "held", not "error"
// — it is a planned wait, and must not feed reconciler-error alerting.
func TestReconciler_RewatchHoldOutcome(t *testing.T) {
	ctx := context.Background()
	st, merkle, r := newRewatchFixture(t, 2, recShared1)
	merkle.setFail(true)
	rows, err := st.ListOrphanedBlocksToReconcile(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("queue: %d rows err=%v", len(rows), err)
	}
	if got := r.reconcileBlock(ctx, rows[0]); got != outcomeHeld {
		t.Fatalf("outcome = %q, want %q", got, outcomeHeld)
	}
}

// TestReconciler_PrunesStateOfRowsThatLeftTheQueue: per-orphan bookkeeping
// of a block that left the queue without passing through forgetOrphan (the
// block-status tracker reactivated it) is dropped once a listing shows the
// whole queue — a re-watch entry can hold a large orphan's whole txid list.
// A pending re-mine's defer counter is that path's own and stays.
func TestReconciler_PrunesStateOfRowsThatLeftTheQueue(t *testing.T) {
	ctx := context.Background()
	_, _, r := newRewatchFixture(t, 2, recShared1)
	r.rewatch["gone"] = &orphanRewatch{pending: []string{"a", "b"}}
	r.defers["gone"] = 3
	r.defers["pending-remine"] = 1
	r.pendingRemine["pending-remine"] = 7

	r.tick(ctx)

	if _, ok := r.rewatch["gone"]; ok {
		t.Fatal("re-watch state of a departed row must be pruned")
	}
	if _, ok := r.defers["gone"]; ok {
		t.Fatal("defer count of a departed row must be pruned")
	}
	if r.defers["pending-remine"] != 1 {
		t.Fatal("a pending re-mine's defer counter must be kept")
	}
	if _, ok := r.rewatch[recOrphan]; !ok {
		t.Fatal("a still-queued orphan's re-watch state must be kept")
	}
}

// TestReconciler_HeaderSourceBehindStoreKeepsQueued: absence from the
// header source is believed only above the store's active tip. At or below
// it the store holds an active block there, so "no header" means the source
// is behind or misconfigured — a chaintracks pod resyncing from genesis, a
// remote URL missing /chaintracks that 404s every height — and acting on it
// would park the orphan (no canonical hash at O) or end the neighborhood
// walk early ahead of a revert.
func TestReconciler_HeaderSourceBehindStoreKeepsQueued(t *testing.T) {
	t.Run("no header at the orphan's height", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		chain := &stubChaintracks{notFoundErr: true} // nothing at any height
		seedMined(t, st, recOrphan, 10, recShared1)
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now()) // store tip 10: the canonical block
		_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
		r := newTestReconciler(st, &capturePublisher{}, chain, func(c *config.ReconcilerConfig) {
			c.MaxDeferAttempts = 1
			c.NeighborhoodDepth = 0 // only the lookup at O is in play
		})

		for range 4 {
			r.tick(ctx)
		}
		if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
			t.Fatalf("a header source with nothing at or below the store's tip must not park the orphan, got %d rows", len(rows))
		}
		if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("tx must be untouched, got %s@%s", got.Status, got.BlockHash)
		}
	})

	t.Run("no header at a neighbor the store holds", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		chain := &stubChaintracks{notFoundErr: true}
		chain.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
		seedMined(t, st, recOrphan, 10, recShared1)
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())
		_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared2))
		_ = st.UpsertBlockHeaderSeen(ctx, recNeighbor, 11, time.Now()) // store tip 11, source tip 10
		_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
		r := newTestReconciler(st, &capturePublisher{}, chain, nil)

		rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10)
		if len(rows) != 1 {
			t.Fatalf("queue: %d rows", len(rows))
		}
		if got := r.reconcileBlock(ctx, rows[0]); got != outcomeError {
			t.Fatalf("outcome = %q, want %q — the walk must not end at a height the store holds", got, outcomeError)
		}
		if got := statusOf(t, st, recShared1); got.Status != models.StatusMined || got.BlockHash != recOrphan {
			t.Fatalf("tx must not be reverted, got %s@%s", got.Status, got.BlockHash)
		}
	})

	t.Run("absence above the store's tip ends the walk", func(t *testing.T) {
		ctx := context.Background()
		st := newPebbleForTest(t)
		chain := &stubChaintracks{notFoundErr: true}
		chain.setHeightHeader(10, headerWithHash(t, recCanonical, 10))
		seedMined(t, st, recOrphan, 10, recShared1)
		_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
		_ = st.UpsertBlockHeaderSeen(ctx, recCanonical, 10, time.Now())
		_ = st.InsertBUMP(ctx, recCanonical, 10, makeCompoundForTest(t, 10, recShared2))
		_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())
		r := newTestReconciler(st, &capturePublisher{}, chain, nil)

		rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10)
		if got := r.reconcileBlock(ctx, rows[0]); got == outcomeError {
			t.Fatalf("absence above the store's tip is the end of the chain, not a failure; got %q", got)
		}
	})
}

// TestReconciler_RewatchConcurrency: the reconciler's own setting wins; unset,
// it falls back to the propagation service's merkle_concurrency, which in
// microservice mode is only set on the propagation deployment.
func TestReconciler_RewatchConcurrency(t *testing.T) {
	r := newTestReconciler(newPebbleForTest(t), &capturePublisher{}, &stubChaintracks{}, nil)
	r.cfg.Propagation.MerkleConcurrency = 50
	if got := r.rewatchConcurrency(); got != 50 {
		t.Fatalf("fallback = %d, want 50", got)
	}
	r.cfg.BumpBuilder.Reconciler.RewatchConcurrency = 8
	if got := r.rewatchConcurrency(); got != 8 {
		t.Fatalf("reconciler setting = %d, want 8", got)
	}
}
