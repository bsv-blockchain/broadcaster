package propagation

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
)

// testBroadcasterCallbackURL and testBroadcasterCallbackToken are the callback_url and
// callback_token the propagation tests configure. testLocalCallbackURL is the
// callback_url the reaper and lifecycle tests configure.
const (
	testBroadcasterCallbackURL   = "http://broadcaster/cb"
	testBroadcasterCallbackToken = "tok"
	testLocalCallbackURL         = "http://localhost:8080/callback"
)

// A tx is registered once any endpoint accepts it: with one merkle-service
// unreachable the batch still lands as fully_ok and every tx is broadcast
// (F-024 is satisfied by the surviving endpoint).
func TestRegisterBatch_PoolWithOneDeadEndpoint_FullyOK(t *testing.T) {
	log := &eventLog{}
	live := newMerkleServer(log, http.StatusOK)
	defer live.Close()

	pool := merkleservice.NewPool([]string{live.URL, "http://127.0.0.1:1"}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
	cfg.Propagation.MerkleConcurrency = 4
	ms := newMockStore()
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool)

	before := testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok"))
	batch := []propagationMsg{{TXID: "tx-a"}, {TXID: "tx-b"}}
	registered, failed := p.registerBatch(context.Background(), batch)
	if len(registered) != 2 || len(failed) != 0 {
		t.Fatalf("registered=%d failed=%d want 2/0", len(registered), len(failed))
	}
	if got := testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok")) - before; got != 1 {
		t.Fatalf("fully_ok delta=%v want 1", got)
	}
	if log.count("register:") != 2 {
		t.Fatalf("live endpoint registrations=%d want 2", log.count("register:"))
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.merkleMarks) != 1 || len(ms.merkleMarks[0]) != 2 {
		t.Fatalf("merkle_registered_at must be stamped for both txs, marks=%v", ms.merkleMarks)
	}
	// The dead endpoint still owes both registrations; the pool re-sends
	// them once it answers again.
	for _, st := range pool.EndpointStatuses() {
		if st.URL == live.URL && st.CatchupPending != 0 {
			t.Fatalf("live endpoint must owe nothing, got %+v", st)
		}
		if st.URL != live.URL && st.CatchupPending != 2 {
			t.Fatalf("dead endpoint must owe both txs, got %+v", st)
		}
	}
}

// When an endpoint's catch-up queue overflowed, the resync replay
// re-registers the whole lookback window with THAT endpoint only, ignoring
// merkle_registered_at (the stamp is pool-wide and cannot say which endpoint
// missed the row).
func TestRunResyncReplay_TargetsOnlyThatEndpoint(t *testing.T) {
	logA, logB := &eventLog{}, &eventLog{}
	srvA := newMerkleServer(logA, http.StatusOK)
	defer srvA.Close()
	srvB := newMerkleServer(logB, http.StatusOK)
	defer srvB.Close()

	pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
	cfg.Propagation.MerkleConcurrency = 4
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-recent", Status: models.StatusSeenOnNetwork, MerkleRegisteredAt: time.Now()},
		{TxID: "tx-older", Status: models.StatusAcceptedByNetwork},
		{TxID: "tx-mined", Status: models.StatusMined}, // terminal: skipped
	}
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool)

	if err := p.runResyncReplay(context.Background(), pool, srvB.URL); err != nil {
		t.Fatalf("resync: %v", err)
	}

	if n := logB.count("register:"); n != 2 {
		t.Fatalf("resynced endpoint registrations=%d want 2 (recent stamp must NOT skip)", n)
	}
	if n := logA.count("register:"); n != 0 {
		t.Fatalf("other endpoint must not be re-registered, got %d", n)
	}
}

// scheduleResync coalesces: a second request for the same endpoint while a
// pass is running triggers exactly one more pass, and nothing runs after
// Stop flagged the propagator.
func TestScheduleResync_CoalescesAndHonorsStop(t *testing.T) {
	logB := &eventLog{}
	srvB := newMerkleServer(logB, http.StatusOK)
	defer srvB.Close()
	logA := &eventLog{}
	srvA := newMerkleServer(logA, http.StatusOK)
	defer srvA.Close()

	pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{{TxID: "tx-1", Status: models.StatusSeenOnNetwork}}
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool)

	ctx := context.Background()
	p.scheduleResync(ctx, pool, srvB.URL)
	p.scheduleResync(ctx, pool, srvB.URL)
	p.scheduleResync(ctx, pool, srvB.URL)
	p.backgroundWG.Wait()

	// At most two passes (the running one, plus one coalesced rerun), never
	// three; at least one.
	if n := logB.count("register:"); n < 1 || n > 2 {
		t.Fatalf("resync passes registered tx-1 %d times, want 1..2", n)
	}

	p.resyncMu.Lock()
	p.resyncStopped = true
	p.resyncMu.Unlock()
	before := logB.count("register:")
	p.scheduleResync(ctx, pool, srvB.URL)
	p.backgroundWG.Wait()
	if logB.count("register:") != before {
		t.Fatal("no resync pass may start after Stop")
	}
}

// With several merkle-services an unset skip window (-1) means no skip: the
// stamp is set once any endpoint accepted, and the queue of registrations
// owed to a lagging endpoint did not survive the restart, so every
// non-terminal row must go to every endpoint again. One merkle-service keeps
// the 30-minute default.
func TestRunMerkleReplay_DefaultSkipDependsOnEndpointCount(t *testing.T) {
	rows := func() []*models.TransactionStatus {
		return []*models.TransactionStatus{
			{TxID: "tx-recent", Status: models.StatusSeenOnNetwork, MerkleRegisteredAt: time.Now().Add(-time.Minute)},
			{TxID: "tx-unstamped", Status: models.StatusAcceptedByNetwork},
		}
	}

	t.Run("several endpoints", func(t *testing.T) {
		logA, logB := &eventLog{}, &eventLog{}
		srvA := newMerkleServer(logA, http.StatusOK)
		defer srvA.Close()
		srvB := newMerkleServer(logB, http.StatusOK)
		defer srvB.Close()
		pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second)
		cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
		cfg.Propagation.MerkleReplaySkipRecentMinutes = -1
		ms := newMockStore()
		ms.replayRows = rows()
		New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool).runMerkleReplay(context.Background())

		if a, b := logA.count("register:"), logB.count("register:"); a != 2 || b != 2 {
			t.Fatalf("registrations a=%d b=%d, want 2 each (recent stamp must not skip)", a, b)
		}
	})

	t.Run("one endpoint", func(t *testing.T) {
		log := &eventLog{}
		srv := newMerkleServer(log, http.StatusOK)
		defer srv.Close()
		ms := newMockStore()
		ms.replayRows = rows()
		p := replayPropagator(t, ms, srv.URL, func(cfg *config.Config) {
			cfg.Propagation.MerkleReplaySkipRecentMinutes = -1
		})
		p.runMerkleReplay(context.Background())

		if got := log.count("register:"); got != 1 {
			t.Fatalf("registrations=%d, want 1 (recent stamp skipped by the 30m default)", got)
		}
	})
}

// sinceIgnoringStore returns every replay row regardless of since, as the
// Aerospike backend does.
type sinceIgnoringStore struct{ *mockStore }

func (s sinceIgnoringStore) IterateStatusesSince(ctx context.Context, _ time.Time, fn func(*models.TransactionStatus) error) error {
	return s.mockStore.IterateStatusesSince(ctx, time.Time{}, fn)
}

// The replay bounds the lookback window itself, so a backend that ignores
// since does not turn a resync into a re-registration of the whole history.
func TestReplay_BoundsWindowWhenStoreIgnoresSince(t *testing.T) {
	log := &eventLog{}
	srv := newMerkleServer(log, http.StatusOK)
	defer srv.Close()
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-in-window", Status: models.StatusSeenOnNetwork, Timestamp: time.Now().Add(-time.Hour)},
		{TxID: "tx-ancient", Status: models.StatusSeenOnNetwork, Timestamp: time.Now().Add(-30 * 24 * time.Hour)},
	}
	cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
	cfg.Propagation.MerkleReplaySkipRecentMinutes = 0
	p := New(cfg, zap.NewNop(), nil, nil, sinceIgnoringStore{ms}, nil, nil, merkleservice.NewClient(srv.URL, "", 2*time.Second))
	p.runMerkleReplay(context.Background())

	if got := log.count("register:"); got != 1 {
		t.Fatalf("registrations=%d, want 1 (row older than the lookback must be skipped)", got)
	}
}

// flakyScanStore fails the first failures IterateStatusesSince calls.
type flakyScanStore struct {
	*mockStore

	failures atomic.Int32
}

func (s *flakyScanStore) IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error {
	if s.failures.Add(-1) >= 0 {
		return errors.New("store unavailable")
	}
	return s.mockStore.IterateStatusesSince(ctx, since, fn)
}

// A resync pass whose store scan failed is not consumed: it runs again after
// a backoff until a scan completes.
func TestResyncLoop_RetriesFailedScan(t *testing.T) {
	prev := resyncRetryBackoff
	resyncRetryBackoff = 10 * time.Millisecond
	t.Cleanup(func() { resyncRetryBackoff = prev })

	logA, logB := &eventLog{}, &eventLog{}
	srvA := newMerkleServer(logA, http.StatusOK)
	defer srvA.Close()
	srvB := newMerkleServer(logB, http.StatusOK)
	defer srvB.Close()
	pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second)
	cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{{TxID: "tx-1", Status: models.StatusSeenOnNetwork}}
	st := &flakyScanStore{mockStore: ms}
	st.failures.Store(2)
	p := New(cfg, zap.NewNop(), nil, nil, st, nil, nil, pool)

	p.scheduleResync(context.Background(), pool, srvB.URL)
	p.backgroundWG.Wait()

	if n := logB.count("register:"); n != 1 {
		t.Fatalf("resynced endpoint registrations=%d, want 1 after two failed scans", n)
	}
}

// A startup-replay tx that no endpoint accepted is handed back to the pool,
// which queues it for every endpoint and re-sends it once they answer,
// instead of dropping it until the next restart. It is not stamped
// merkle_registered_at: it has not landed anywhere yet.
func TestRunMerkleReplay_RetainsTxsEveryEndpointRefused(t *testing.T) {
	logA, logB := &eventLog{}, &eventLog{}
	srvA := newMerkleServer(logA, http.StatusServiceUnavailable)
	defer srvA.Close()
	srvB := newMerkleServer(logB, http.StatusServiceUnavailable)
	defer srvB.Close()
	pool := merkleservice.NewPool([]string{srvA.URL, srvB.URL}, "", 2*time.Second, merkleservice.WithFailureThreshold(100))
	cfg := &config.Config{CallbackURL: testBroadcasterCallbackURL, CallbackToken: testBroadcasterCallbackToken}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-a", Status: models.StatusSeenOnNetwork},
		{TxID: "tx-b", Status: models.StatusAcceptedByNetwork},
	}
	New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, pool).runMerkleReplay(context.Background())

	for _, st := range pool.EndpointStatuses() {
		if st.CatchupPending != 2 {
			t.Fatalf("endpoint %s must owe both refused txs, got %+v", st.URL, st)
		}
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.merkleMarks) != 0 {
		t.Fatalf("refused txs must not be stamped merkle_registered_at, marks=%v", ms.merkleMarks)
	}
}
