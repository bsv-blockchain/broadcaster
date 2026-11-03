package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

const txA = "txA"

// fakeStore implements just enough of store.Store for these tests.
//
// Embeds store.Store so a method added to the interface does not break this
// fake at compile time; anything not explicitly implemented below panics if a
// test actually calls it. Matches propagation/sse/watchdog's fakes.
type fakeStore struct {
	store.Store

	mu   sync.Mutex
	subs map[string][]*models.Submission

	deliveries []deliveryRecord
	attempts   []attemptRecord
}

type deliveryRecord struct {
	SubmissionID string
	LastStatus   models.Status
	RetryCount   int
	NextRetryAt  *time.Time
}

type attemptRecord struct {
	SubmissionID string
	Result       string
}

func (s *fakeStore) GetSubmissionsByTxID(_ context.Context, txid string) ([]*models.Submission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Return value-copies so callers don't share state with the store's
	// internal map — mirrors Postgres semantics and prevents the data race
	// between concurrent deliver() reads and CAS/UpdateDeliveryStatus writes
	// in TestDeliver_ExactlyOnceAcrossConcurrentReplicas.
	list := s.subs[txid]
	out := make([]*models.Submission, len(list))
	for i, sub := range list {
		cp := *sub
		out[i] = &cp
	}
	return out, nil
}

func (s *fakeStore) UpdateDeliveryStatus(_ context.Context, id string, last models.Status, retry int, nextRetry *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append(s.deliveries, deliveryRecord{
		SubmissionID: id,
		LastStatus:   last,
		RetryCount:   retry,
		NextRetryAt:  nextRetry,
	})
	// Mutate the in-memory submission so subsequent lookups reflect retry
	// state — mirrors what a real store would do.
	for _, list := range s.subs {
		for _, sub := range list {
			if sub.SubmissionID == id {
				sub.LastDeliveredStatus = last
				sub.RetryCount = retry
				sub.NextRetryAt = nextRetry
			}
		}
	}
	return nil
}

// RecordDeliveryAttempt mirrors the real stores: attempts increments and the
// last-attempt bookkeeping is overwritten, on every POST regardless of outcome.
func (s *fakeStore) RecordDeliveryAttempt(_ context.Context, id string, at time.Time, result string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, attemptRecord{SubmissionID: id, Result: result})
	for _, list := range s.subs {
		for _, sub := range list {
			if sub.SubmissionID == id {
				sub.Attempts++
				t := at
				sub.LastAttemptAt = &t
				sub.LastResult = result
			}
		}
	}
	return nil
}

// UpdateDeliveryStatusCAS mirrors the Postgres semantics: the row is updated
// iff its current LastDeliveredStatus matches `expected`. Concurrent callers
// in the issue-#166 regression test rely on this behaving as a single-writer
// CAS, so the mutation runs under s.mu.
func (s *fakeStore) UpdateDeliveryStatusCAS(_ context.Context, id string, expected, next models.Status) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, list := range s.subs {
		for _, sub := range list {
			if sub.SubmissionID != id {
				continue
			}
			if sub.LastDeliveredStatus != expected {
				return false, nil
			}
			sub.LastDeliveredStatus = next
			sub.RetryCount = 0
			sub.NextRetryAt = nil
			s.deliveries = append(s.deliveries, deliveryRecord{
				SubmissionID: id,
				LastStatus:   next,
			})
			return true, nil
		}
	}
	return false, nil
}

// Stubs to satisfy the store.Store interface without referencing the full set.
func (s *fakeStore) GetOrInsertStatus(context.Context, *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	return nil, false, nil
}

func (s *fakeStore) BatchGetOrInsertStatus(context.Context, []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return nil, nil
}

func (s *fakeStore) GetTxIDsByBlockHash(context.Context, string) ([]string, error) {
	return nil, nil
}
func (s *fakeStore) DeleteBUMPByBlockHash(context.Context, string) error { return nil }
func (s *fakeStore) MarkBlockReconciled(context.Context, string, time.Time, time.Time) (bool, error) {
	return true, nil
}

func (s *fakeStore) ListOrphanedBlocksToReconcile(context.Context, int) ([]*models.BlockProcessingStatus, error) {
	return nil, nil
}
func (s *fakeStore) UpdateStatus(context.Context, *models.TransactionStatus) error { return nil }
func (s *fakeStore) BatchUpdateStatus(context.Context, []*models.TransactionStatus) error {
	return nil
}

func (s *fakeStore) BatchUpdateStatusReturning(context.Context, []*models.TransactionStatus) ([]store.StatusUpdate, error) {
	return nil, nil
}

//nolint:nilnil // unused stub; safe to return the zero pair.
func (s *fakeStore) GetStatus(context.Context, string) (*models.TransactionStatus, error) {
	return nil, nil
}

func (s *fakeStore) EnrichMerklePath(context.Context, *models.TransactionStatus) {}

func (s *fakeStore) GetStatusesSince(context.Context, time.Time) ([]*models.TransactionStatus, error) {
	return nil, nil
}

func (s *fakeStore) IterateStatusesSince(context.Context, time.Time, func(*models.TransactionStatus) error) error {
	return nil
}

func (s *fakeStore) CensusStatusesSince(context.Context, time.Time, time.Time, []models.Status) (map[models.Status]store.StatusCensus, error) {
	return map[models.Status]store.StatusCensus{}, nil
}

func (s *fakeStore) SetStatusByBlockHash(context.Context, string, models.Status) ([]string, error) {
	return nil, nil
}
func (s *fakeStore) InsertBUMP(context.Context, string, uint64, []byte) error { return nil }
func (s *fakeStore) GetBUMP(context.Context, string) (uint64, []byte, error)  { return 0, nil, nil }
func (s *fakeStore) SetMinedByTxIDs(context.Context, string, uint64, []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	return nil, nil, nil
}

func (s *fakeStore) MarkMerkleRegisteredByTxIDs(context.Context, []string, time.Time) error {
	return nil
}
func (s *fakeStore) InsertSubmission(context.Context, *models.Submission) error { return nil }
func (s *fakeStore) GetSubmissionsByToken(context.Context, string) ([]*models.Submission, error) {
	return nil, nil
}

func (s *fakeStore) IterateStatusesByToken(context.Context, string, time.Time, []models.Status, func(*models.TransactionStatus) error) error {
	return nil
}

func (s *fakeStore) TokensForTxIDs(context.Context, []string) (map[string][]string, error) {
	return map[string][]string{}, nil
}
func (s *fakeStore) InsertStump(context.Context, *models.Stump) error { return nil }
func (s *fakeStore) GetStumpsByBlockHash(context.Context, string) ([]*models.Stump, error) {
	return nil, nil
}
func (s *fakeStore) DeleteStumpsByBlockHash(context.Context, string) error { return nil }
func (s *fakeStore) BumpRetryCount(context.Context, string) (int, error)   { return 0, nil }
func (s *fakeStore) SetPendingRetryFields(context.Context, string, []byte, time.Time, string) error {
	return nil
}

func (s *fakeStore) GetReadyRetries(context.Context, time.Time, int) ([]*store.PendingRetry, error) {
	return nil, nil
}

// ListSubmissionsReadyForRetry returns submissions with retry_count > 0 and
// NextRetryAt <= now, ordered by NextRetryAt ASC. Mirrors the production
// backends' contract so reaper-driven tests exercise the same code path.
func (s *fakeStore) ListSubmissionsReadyForRetry(_ context.Context, now time.Time, limit int) ([]*models.Submission, error) {
	if limit <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*models.Submission
	for _, list := range s.subs {
		for _, sub := range list {
			if sub.RetryCount <= 0 || sub.NextRetryAt == nil || sub.NextRetryAt.After(now) {
				continue
			}
			out = append(out, sub)
		}
	}
	// Stable insertion order is enough for the existing tests; we don't
	// need a real sort here.
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeStore) ClearRetryState(context.Context, string, models.Status, string) error {
	return nil
}
func (s *fakeStore) EnsureIndexes(context.Context) error { return nil }
func (s *fakeStore) UpsertDatahubEndpoint(context.Context, store.DatahubEndpoint) error {
	return nil
}

func (s *fakeStore) ListDatahubEndpoints(context.Context, string) ([]store.DatahubEndpoint, error) {
	return nil, nil
}

func (s *fakeStore) UpsertPeerPolicy(context.Context, store.PeerPolicy) error {
	return nil
}

func (s *fakeStore) ListPeerPolicies(context.Context, string) ([]store.PeerPolicy, error) {
	return nil, nil
}

func (s *fakeStore) UpsertBlockHeaderSeen(context.Context, string, uint64, time.Time) error {
	return nil
}

func (s *fakeStore) MarkBlockProcessed(context.Context, string, uint64, time.Time) error {
	return nil
}

func (s *fakeStore) MarkBlockBUMPBuilt(context.Context, string, uint64, time.Time) error {
	return nil
}

func (s *fakeStore) MarkBlocksOrphaned(context.Context, []string, time.Time) (int, error) {
	return 0, nil
}

func (s *fakeStore) MarkBlocksParked(context.Context, []string) error { return nil }

//nolint:nilnil // unused stub.
func (s *fakeStore) GetBlockProcessingStatus(context.Context, string) (*models.BlockProcessingStatus, error) {
	return nil, nil
}

func (s *fakeStore) ListBlockProcessingStatus(context.Context, uint64, int) ([]*models.BlockProcessingStatus, error) {
	return nil, nil
}
func (s *fakeStore) GetActiveTipBlockHeight(context.Context) (uint64, error) { return 0, nil }
func (s *fakeStore) ListStaleBlockProcessingStatus(context.Context, time.Time, uint64, int) ([]*models.BlockProcessingStatus, error) {
	return nil, nil
}
func (s *fakeStore) Close() error { return nil }

// recordingPub captures published statuses but doesn't actually subscribe —
// the webhook tests drive handleUpdate directly.
type recordingPub struct{}

func (recordingPub) Publish(context.Context, *models.TransactionStatus) error     { return nil }
func (recordingPub) PublishBulk(context.Context, *models.TransactionStatus) error { return nil }
func (recordingPub) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("not used in tests")
}
func (recordingPub) Close() error { return nil }

// scriptedPub serves a caller-supplied channel from Subscribe so tests can
// drive Service.Start with synthetic status updates and observe how the
// channel reader / worker pool route them downstream.
type scriptedPub struct {
	ch chan *models.TransactionStatus
}

func (p *scriptedPub) Publish(context.Context, *models.TransactionStatus) error     { return nil }
func (p *scriptedPub) PublishBulk(context.Context, *models.TransactionStatus) error { return nil }
func (p *scriptedPub) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return p.ch, nil
}
func (p *scriptedPub) Close() error { return nil }

// TestDeliverSuccess covers the happy path: matching submission, terminal
// status, callback URL gets POSTed with the bearer token, store records
// LastDeliveredStatus.
func TestDeliverSuccess(t *testing.T) {
	var receivedAuth, receivedUA, receivedCT string
	var receivedBody []byte
	var hits atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		receivedUA = r.Header.Get("User-Agent")
		receivedCT = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID:  "sub-1",
				TxID:          txA,
				CallbackURL:   srv.URL,
				CallbackToken: "tok-A",
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1},
		// httptest.Server listens on 127.0.0.1 — opt into private dials so
		// the SSRF guard doesn't block the test client.
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	if hits.Load() != 1 {
		t.Fatalf("expected 1 callback hit, got %d", hits.Load())
	}
	if receivedAuth != "Bearer tok-A" {
		t.Errorf("Authorization = %q, want %q", receivedAuth, "Bearer tok-A")
	}
	// The documented allowlistable UA (issue #249) — receivers key WAF rules
	// on the "bsv-broadcaster-webhook/" prefix, so it must never regress to Go's
	// default UA. version.Version is "dev" outside ldflags builds.
	if receivedUA != "bsv-broadcaster-webhook/dev" {
		t.Errorf("User-Agent = %q, want %q", receivedUA, "bsv-broadcaster-webhook/dev")
	}
	if receivedCT != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", receivedCT)
	}
	var payload map[string]any
	if err := json.Unmarshal(receivedBody, &payload); err != nil {
		t.Fatalf("decoding callback body: %v", err)
	}
	if payload["txid"] != txA || payload["txStatus"] != string(models.StatusMined) {
		t.Errorf("unexpected payload: %+v", payload)
	}
	if len(st.deliveries) != 1 || st.deliveries[0].LastStatus != models.StatusMined {
		t.Errorf("expected one MINED delivery record, got %+v", st.deliveries)
	}
	// Successful POST stamps the lifetime attempt bookkeeping.
	if len(st.attempts) != 1 || st.attempts[0].Result != "delivered" {
		t.Errorf("expected one delivered attempt record, got %+v", st.attempts)
	}
	if sub := st.subs[txA][0]; sub.Attempts != 1 || sub.LastResult != "delivered" || sub.LastAttemptAt == nil {
		t.Errorf("submission attempt state not stamped: %+v", sub)
	}
}

// TestSkipIntermediateWhenNotFullUpdates verifies the FullStatusUpdates
// gating: a SEEN_ON_NETWORK update should NOT fire a callback when the
// submission opted out of full updates.
func TestSkipIntermediateWhenNotFullUpdates(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID:      "sub-1",
				TxID:              txA,
				CallbackURL:       srv.URL,
				FullStatusUpdates: false,
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	})

	if hits.Load() != 0 {
		t.Fatalf("expected 0 hits for intermediate status, got %d", hits.Load())
	}
}

// TestRetryOnFailure asserts the failure path schedules a retry: RetryCount
// is incremented and NextRetryAt is in the future.
func TestRetryOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{SubmissionID: "sub-1", TxID: txA, CallbackURL: srv.URL}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 5, InitialBackoffMs: 50, MaxBackoffMs: 1000},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil,
	)

	before := time.Now()
	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	// CAS claim writes first (advances LastDeliveredStatus, clears retry
	// state), then recordFailure writes the retry bookkeeping. Both are
	// captured in st.deliveries by the fakeStore; the retry record is the
	// last one.
	if len(st.deliveries) != 2 {
		t.Fatalf("expected 2 delivery records (CAS claim + retry write), got %d", len(st.deliveries))
	}
	d := st.deliveries[len(st.deliveries)-1]
	if d.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", d.RetryCount)
	}
	if d.NextRetryAt == nil || !d.NextRetryAt.After(before) {
		t.Errorf("NextRetryAt = %v, expected after %v", d.NextRetryAt, before)
	}
	// The failed POST stamps the lifetime attempt bookkeeping with the same
	// reason string the failure log carries — this is what GET /tx's
	// callbacks[].lastResult surfaces for receiver self-diagnosis (#249).
	if len(st.attempts) != 1 || st.attempts[0].Result != "status 500" {
		t.Errorf("expected one 'status 500' attempt record, got %+v", st.attempts)
	}
	if sub := st.subs[txA][0]; sub.Attempts != 1 || sub.LastResult != "status 500" {
		t.Errorf("submission attempt state not stamped on failure: %+v", sub)
	}
}

// TestDedupOnRepeatedStatus verifies that a status matching
// LastDeliveredStatus is suppressed (no second POST).
func TestDedupOnRepeatedStatus(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID:        "sub-1",
				TxID:                txA,
				CallbackURL:         srv.URL,
				LastDeliveredStatus: models.StatusMined,
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusMined, // same as LastDeliveredStatus
		Timestamp: time.Now(),
	})

	if hits.Load() != 0 {
		t.Errorf("expected 0 hits (deduped), got %d", hits.Load())
	}
}

// TestSSRFGuardBlocksLoopbackDial confirms the dial-time SSRF guard:
// with AllowPrivateIPs=false (the default), a delivery whose target is
// 127.0.0.1 — i.e. an httptest.Server — is refused at dial time, the
// callback never reaches the server, and the failure is recorded as a
// retryable delivery (RetryCount bumped).
//
// This is the second layer of defense: registration-time validation
// catches IP-literal callback URLs, and this dial-time check catches
// the DNS-rebinding case where a hostname resolved to a private IP.
func TestSSRFGuardBlocksLoopbackDial(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID: "sub-1",
				TxID:         txA,
				CallbackURL:  srv.URL, // 127.0.0.1:<port>
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1, MaxBackoffMs: 100},
		// Default-safe: SSRF guard ON.
		config.CallbackConfig{AllowPrivateIPs: false},
		zap.NewNop(), recordingPub{}, st, nil,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	if hits.Load() != 0 {
		t.Errorf("expected 0 hits (dial refused), got %d", hits.Load())
	}
	// CAS claim runs before the dial-time SSRF guard fires, so we observe
	// two records: the claim, then the retry write from recordFailure.
	if len(st.deliveries) != 2 {
		t.Fatalf("expected 2 delivery records (CAS claim + retry write), got %d", len(st.deliveries))
	}
	if st.deliveries[len(st.deliveries)-1].RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", st.deliveries[len(st.deliveries)-1].RetryCount)
	}
}

// TestStartDecouplesSlowDelivery is the regression test for the worker-pool
// fix. The previous implementation called handleUpdate (synchronous DB
// lookup + synchronous HTTP POST) on the channel-reader goroutine, so a
// single slow callback target stalled draining of the upstream
// events.Publisher subscriber channel and triggered drops there. The fix
// hands each status to a bounded worker pool; the reader stays drainable
// while workers are blocked on slow targets.
//
// The test stalls one delivery (server never responds, only ctx-cancel
// unblocks it) and asserts that subsequent statuses still reach
// handleUpdate via other workers — i.e. the reader didn't block on the
// stalled delivery.
func TestStartDecouplesSlowDelivery(t *testing.T) {
	stallStarted := make(chan struct{}, 1)
	releaseStall := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Notify on first hit, then block until releaseStall fires or the
		// request context cancels (which will happen at test cleanup).
		select {
		case stallStarted <- struct{}{}:
		default:
		}
		select {
		case <-releaseStall:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(releaseStall)

	var fastHits atomic.Int32
	fastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fastHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer fastSrv.Close()

	// One stalled submission plus several distinct fast submissions: each
	// fast tx has its own LastDeliveredStatus so shouldDeliver doesn't
	// dedup them away after the first success.
	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txStall": {{SubmissionID: "stall", TxID: "txStall", CallbackURL: srv.URL}},
			"txFast0": {{SubmissionID: "fast0", TxID: "txFast0", CallbackURL: fastSrv.URL}},
			"txFast1": {{SubmissionID: "fast1", TxID: "txFast1", CallbackURL: fastSrv.URL}},
			"txFast2": {{SubmissionID: "fast2", TxID: "txFast2", CallbackURL: fastSrv.URL}},
			"txFast3": {{SubmissionID: "fast3", TxID: "txFast3", CallbackURL: fastSrv.URL}},
		},
	}

	pub := &scriptedPub{ch: make(chan *models.TransactionStatus, 16)}

	// Pool of 2 workers: enough to keep one stalled delivery from blocking
	// every worker, while still small enough to make the test fast.
	svc := New(
		config.WebhookConfig{
			HTTPTimeoutMs:           60_000, // long, so the stall actually stalls
			MaxRetries:              3,
			InitialBackoffMs:        1,
			MaxBackoffMs:            10,
			MaxConcurrentDeliveries: 2,
		},
		config.CallbackConfig{AllowPrivateIPs: true}, // server is on 127.0.0.1
		zap.NewNop(), pub, st, nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErr := make(chan error, 1)
	go func() { startErr <- svc.Start(ctx) }()

	// First, send the stall-bound status. Wait until the stalled HTTP
	// handler has actually been entered — this proves a worker has picked
	// up the status and is blocked.
	pub.ch <- &models.TransactionStatus{TxID: "txStall", Status: models.StatusMined, Timestamp: time.Now()}
	select {
	case <-stallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("stalled HTTP server was never hit; worker pool didn't pick up the status")
	}

	// With one worker stalled, send several fast-bound statuses. If the
	// channel reader were still synchronous (pre-fix), the second worker
	// would also be busy serving handleUpdate and the reader would
	// eventually block on the stalled HTTP call. With the worker-pool
	// decoupling, the reader stays free and the second worker handles
	// these promptly.
	for i := 0; i < 4; i++ {
		pub.ch <- &models.TransactionStatus{
			TxID:      fmt.Sprintf("txFast%d", i),
			Status:    models.StatusMined,
			Timestamp: time.Now(),
		}
	}

	// All four fast deliveries should land within a generous timeout.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fastHits.Load() >= 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := fastHits.Load(); got < 4 {
		t.Fatalf("fast deliveries got through: %d, want 4 — channel reader appears to be blocked by stalled delivery", got)
	}

	// Cleanly stop the service.
	cancel()
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
}

// TestDeliver_ExactlyOnceAcrossConcurrentReplicas is the canonical regression
// for issue #166. Two Service instances share a single fakeStore — the
// production equivalent of two api-server pods talking to the same Postgres.
// Both receive the same status update concurrently. Exactly one POST must
// reach the receiver; the other instance must record a CAS-lost.
//
// Before the CAS fix this test would observe 2 POSTs (one per replica). The
// shouldDeliver in-memory dedup catches sequential repeats but not concurrent
// processing across pods.
func TestDeliver_ExactlyOnceAcrossConcurrentReplicas(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Single shared fakeStore — both Service instances see (and mutate) the
	// same submission row, which is the actual sharing model in production.
	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID: "sub-1",
				TxID:         txA,
				CallbackURL:  srv.URL,
			}},
		},
	}

	cfg := config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1}
	cb := config.CallbackConfig{AllowPrivateIPs: true}
	svcA := New(cfg, cb, zap.NewNop(), recordingPub{}, st, nil)
	svcB := New(cfg, cb, zap.NewNop(), recordingPub{}, st, nil)

	status := &models.TransactionStatus{
		TxID:      txA,
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	}

	// Race both replicas through handleUpdate against the same status.
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for _, svc := range []*Service{svcA, svcB} {
		go func() {
			defer wg.Done()
			<-start
			svc.handleUpdate(t.Context(), status)
		}()
	}
	close(start)
	wg.Wait()

	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly 1 callback POST, got %d (CAS fix regressed — issue #166)", got)
	}
	// Exactly one delivery record (the winner's CAS-success path appends one).
	if len(st.deliveries) != 1 || st.deliveries[0].LastStatus != models.StatusMined {
		t.Errorf("expected one MINED delivery record, got %+v", st.deliveries)
	}
}

// fakeLeaser is a minimal store.Leaser stand-in for the reaper tests.
// grant=true returns a non-zero held-until time (lease acquired);
// grant=false returns the zero time (another holder owns it).
type fakeLeaser struct {
	grant bool
}

func (l *fakeLeaser) TryAcquireOrRenew(context.Context, string, string, time.Duration) (time.Time, error) {
	if !l.grant {
		return time.Time{}, nil
	}
	return time.Now().Add(time.Minute), nil
}
func (l *fakeLeaser) Release(context.Context, string, string) error { return nil }

// TestReaper_LeaseHeld_RefiresReadySubmission asserts the retry-sweep path
// closes the failure-path regression rafa-js called out on PR #170: a row
// whose POST failed has a NextRetryAt that, before this fix, nothing
// consumed. With the reaper running and the lease granted, reapOnce should
// re-POST to the receiver and clear the retry state.
func TestReaper_LeaseHeld_RefiresReadySubmission(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	past := time.Now().Add(-1 * time.Second)
	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID:        "sub-retry",
				TxID:                txA,
				CallbackURL:         srv.URL,
				LastDeliveredStatus: models.StatusMined,
				RetryCount:          2,
				NextRetryAt:         &past,
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 5, InitialBackoffMs: 1},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, &fakeLeaser{grant: true},
	)

	// Drive a single reaper pass directly — tryReap acquires the lease,
	// reapOnce lists ready rows and re-fires deliver for each.
	svc.tryReap(t.Context())

	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly 1 retry POST, got %d", got)
	}
	// Verify the submission's retry state was cleared by the CAS.
	got, _ := st.GetSubmissionsByTxID(t.Context(), txA)
	if len(got) != 1 {
		t.Fatalf("expected 1 submission, got %d", len(got))
	}
	if got[0].RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0 (cleared by CAS)", got[0].RetryCount)
	}
	if got[0].NextRetryAt != nil {
		t.Errorf("NextRetryAt = %v, want nil (cleared by CAS)", got[0].NextRetryAt)
	}
}

// TestReaper_LeaseDenied_SkipsTick asserts a replica that loses the lease
// race performs no work — the canonical N-1 idle case in production.
func TestReaper_LeaseDenied_SkipsTick(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	past := time.Now().Add(-1 * time.Second)
	st := &fakeStore{
		subs: map[string][]*models.Submission{
			txA: {{
				SubmissionID:        "sub-retry",
				TxID:                txA,
				CallbackURL:         srv.URL,
				LastDeliveredStatus: models.StatusMined,
				RetryCount:          2,
				NextRetryAt:         &past,
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 5, InitialBackoffMs: 1},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, &fakeLeaser{grant: false},
	)

	svc.tryReap(t.Context())

	if got := hits.Load(); got != 0 {
		t.Fatalf("expected 0 POSTs from non-leader tick, got %d", got)
	}
}
