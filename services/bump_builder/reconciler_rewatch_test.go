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
		if req.URL.Path != "/watch" {
			w.WriteHeader(http.StatusAccepted) // /reprocess: accepted, nothing to redeliver
			return
		}
		var body struct {
			TxID string `json:"txid"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.fail {
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
// retried on the next tick instead of being lost, and it never holds the
// reconcile pass up.
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
	r.tick(ctx) // re-watch fails; the pass still defers as usual
	if n := merkle.watchCount(recShared1); n != 0 {
		t.Fatalf("failing merkle-service must not record a watch, got %d", n)
	}
	if r.defers[recOrphan] != 1 {
		t.Fatalf("a failed re-watch must not hold up the pass, defers=%d", r.defers[recOrphan])
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

// TestReconciler_NoParkUntilOrphanRewatched: merkle-service unreachable for
// the whole defer window must not let the cap park the block — a park with
// the watches lapsed strands its txs exactly as at 973522, because nothing
// re-registers them once the block is off the queue. The block stays queued
// until the re-watch lands, then parks as usual.
func TestReconciler_NoParkUntilOrphanRewatched(t *testing.T) {
	ctx := context.Background()
	st := newPebbleForTest(t)
	pub := &capturePublisher{}
	chain := &stubChaintracks{}
	merkle := newMerkleTTLStub(t)
	chain.setHeightHeader(10, headerWithHash(t, recCanonical, 10))

	seedMined(t, st, recOrphan, 10, recShared1)
	_ = st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now())
	_, _ = st.MarkBlocksOrphaned(ctx, []string{recOrphan}, time.Now())

	r := newTestReconciler(st, pub, chain, func(c *config.ReconcilerConfig) { c.MaxDeferAttempts = 1 })
	withMerkleTTLStub(r, merkle)

	merkle.setFail(true)
	r.tick(ctx) // defer 1/1
	r.tick(ctx) // cap reached, re-watch still failing → must NOT park
	r.tick(ctx)
	if rows, _ := st.ListOrphanedBlocksToReconcile(ctx, 10); len(rows) != 1 {
		t.Fatalf("an orphan whose txs were never re-watched must stay queued, got %d rows", len(rows))
	}

	merkle.setFail(false)
	r.tick(ctx) // re-watch lands → park
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
