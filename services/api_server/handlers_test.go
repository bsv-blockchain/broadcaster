package api_server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/validator"
)

// mockStore implements store.Store for testing callback handlers.
//
// InsertStump records calls under a composite "blockHash:subtreeIndex" key so
// tests can verify the full round-trip of a STUMP payload (including hex
// decoding of models.HexBytes). A mutex protects the stumps map because the
// end-to-end STUMP test fires deliveries concurrently to mirror
// merkle-service's 64-worker delivery pool.
// Embeds store.Store so a method added to the interface does not break this
// fake at compile time; anything not explicitly implemented below panics if a
// test actually calls it. Matches propagation/sse/watchdog's fakes.
type mockStore struct {
	store.Store

	// datahubEndpoints backs ListDatahubEndpoints, which /health reads to
	// report each endpoint's advertised policy; datahubCalls counts the reads
	// so the TTL cache can be asserted, and datahubErr forces the failure path.
	datahubEndpoints []store.DatahubEndpoint
	datahubCalls     int
	datahubErr       error

	mu                  sync.Mutex
	updateStatusCalls   []*models.TransactionStatus
	stumps              map[string]*models.Stump
	insertStumpErr      error
	insertedSubmissions []*models.Submission
	// updateStatusErr, if non-nil, is returned from UpdateStatus and the call
	// is NOT recorded — this models the F-033 (#91) "no phantom row" guard
	// where a backend that rejects the call must not have written anything.
	updateStatusErr error
	// insertStumpFn, if set, runs before the default record step and may
	// return an error to simulate per-key failures (Aerospike RECORD_TOO_BIG,
	// DEVICE_OVERLOAD, HOT_KEY, etc.). Returning non-nil skips the record.
	insertStumpFn func(stump *models.Stump) error
	// batchUpdateReturningCalls captures the slices passed to
	// BatchUpdateStatusReturning so tests can assert the batch+bulk callback
	// refactor calls the store exactly once per inbound callback.
	batchUpdateReturningCalls [][]*models.TransactionStatus
	batchUpdateReturningErr   error
	// batchUpdatePrevFunc lets a test inject previous-row data per-txid for
	// transition-age assertions. Returning nil mirrors the not-found path.
	batchUpdatePrevFunc func(txid string) *models.TransactionStatus
	// getOrInsertFn lets tests drive the dedup CAS path: when non-nil it
	// is consulted on each GetOrInsertStatus call and its return value is
	// used directly. Default behavior (nil hook) is the legacy "always
	// fresh insert" stub: (nil, false, nil).
	getOrInsertFn func(status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	// tipHeight is served by GetActiveTipBlockHeight (the /health
	// blockHeight source); tipCalls counts reads so tests can assert the
	// handler's TTL cache actually coalesces probes.
	tipHeight uint64
	tipCalls  int
	// subsByTxID feeds GetSubmissionsByTxID and getStatusFn feeds GetStatus
	// for the GET /tx callback-state tests.
	subsByTxID  map[string][]*models.Submission
	getStatusFn func(txid string) *models.TransactionStatus
	// peerPolicies feeds ListPeerPolicies for the GET /policy tests;
	// listPeerPoliciesErr injects a store failure.
	peerPolicies        []store.PeerPolicy
	listPeerPoliciesErr error
}

func (m *mockStore) UpdateStatus(_ context.Context, status *models.TransactionStatus) error {
	if m.updateStatusErr != nil {
		return m.updateStatusErr
	}
	m.updateStatusCalls = append(m.updateStatusCalls, status)
	return nil
}

func (m *mockStore) GetOrInsertStatus(_ context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	if m.getOrInsertFn != nil {
		return m.getOrInsertFn(status)
	}
	return nil, false, nil
}

func (m *mockStore) BatchGetOrInsertStatus(context.Context, []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return nil, nil
}

func (m *mockStore) BatchUpdateStatus(context.Context, []*models.TransactionStatus) error {
	return nil
}

// BatchUpdateStatusReturning records calls and returns the previous statuses
// per input. Default prev for each known txid is a {Status: RECEIVED}
// snapshot — this both satisfies the handler's nil-prev = unknown-txid
// branch (so the batch path treats every txid as known by default) and
// keeps existing assertions on updateStatusCalls working unchanged
// (each input status is appended to updateStatusCalls as if the legacy
// per-row path had run). Tests that need a different prev shape override
// via batchUpdatePrevFunc.
func (m *mockStore) BatchUpdateStatusReturning(_ context.Context, statuses []*models.TransactionStatus) ([]store.StatusUpdate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchUpdateReturningCalls = append(m.batchUpdateReturningCalls, append([]*models.TransactionStatus(nil), statuses...))
	if m.batchUpdateReturningErr != nil {
		return nil, m.batchUpdateReturningErr
	}
	out := make([]store.StatusUpdate, len(statuses))
	// updateStatusErr == ErrNotFound models the production "unknown txid"
	// guard (F-033 / #91): the legacy backend silently skipped the write
	// AND did not record the call. The batch path mirrors that — an absent
	// result for every input, and no append to updateStatusCalls.
	if errors.Is(m.updateStatusErr, store.ErrNotFound) {
		return out, nil
	}
	for i, st := range statuses {
		// Mirror the legacy contract: record the update as if UpdateStatus
		// had been called per-row, so existing assertions on
		// updateStatusCalls keep working without rewriting every test.
		m.updateStatusCalls = append(m.updateStatusCalls, st)
		var prev *models.TransactionStatus
		if m.batchUpdatePrevFunc != nil {
			prev = m.batchUpdatePrevFunc(st.TxID)
		} else {
			// Default prev: RECEIVED with a fresh timestamp so the
			// handler's transition-age observation has a non-zero value.
			prev = &models.TransactionStatus{
				TxID:      st.TxID,
				Status:    models.StatusReceived,
				Timestamp: time.Now(),
			}
		}
		if prev != nil {
			out[i] = store.StatusUpdate{Prev: prev}
		}
	}
	return out, nil
}

func (m *mockStore) GetStatus(_ context.Context, txid string) (*models.TransactionStatus, error) {
	if m.getStatusFn != nil {
		return m.getStatusFn(txid), nil
	}
	return nil, nil
}

func (m *mockStore) EnrichMerklePath(context.Context, *models.TransactionStatus) {}

func (m *mockStore) GetStatusesSince(context.Context, time.Time) ([]*models.TransactionStatus, error) {
	return nil, nil
}

func (m *mockStore) IterateStatusesSince(context.Context, time.Time, func(*models.TransactionStatus) error) error {
	return nil
}

func (m *mockStore) CensusStatusesSince(context.Context, time.Time, time.Time, []models.Status) (map[models.Status]store.StatusCensus, error) {
	return map[models.Status]store.StatusCensus{}, nil
}

func (m *mockStore) SetStatusByBlockHash(context.Context, string, models.Status) ([]string, error) {
	return nil, nil
}
func (m *mockStore) InsertBUMP(context.Context, string, uint64, []byte) error { return nil }
func (m *mockStore) GetBUMP(context.Context, string) (uint64, []byte, error)  { return 0, nil, nil }
func (m *mockStore) SetMinedByTxIDs(context.Context, string, uint64, []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	return nil, nil, nil
}

func (m *mockStore) MarkMerkleRegisteredByTxIDs(context.Context, []string, time.Time) error {
	return nil
}

func (m *mockStore) GetTxIDsByBlockHash(context.Context, string) ([]string, error) {
	return nil, nil
}
func (m *mockStore) DeleteBUMPByBlockHash(context.Context, string) error { return nil }
func (m *mockStore) MarkBlockReconciled(context.Context, string, time.Time, time.Time) (bool, error) {
	return true, nil
}

func (m *mockStore) ListOrphanedBlocksToReconcile(context.Context, int) ([]*models.BlockProcessingStatus, error) {
	return nil, nil
}

func (m *mockStore) InsertSubmission(_ context.Context, sub *models.Submission) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertedSubmissions = append(m.insertedSubmissions, sub)
	return nil
}

func (m *mockStore) GetSubmissionsByTxID(_ context.Context, txid string) ([]*models.Submission, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subsByTxID[txid], nil
}

func (m *mockStore) GetSubmissionsByToken(context.Context, string) ([]*models.Submission, error) {
	return nil, nil
}

func (m *mockStore) IterateStatusesByToken(context.Context, string, time.Time, []models.Status, func(*models.TransactionStatus) error) error {
	return nil
}

func (m *mockStore) TokensForTxIDs(context.Context, []string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (m *mockStore) UpdateDeliveryStatus(context.Context, string, models.Status, int, *time.Time) error {
	return nil
}

func (m *mockStore) RecordDeliveryAttempt(context.Context, string, time.Time, string) error {
	return nil
}

func (m *mockStore) UpdateDeliveryStatusCAS(context.Context, string, models.Status, models.Status) (bool, error) {
	return true, nil
}

func (m *mockStore) ListSubmissionsReadyForRetry(context.Context, time.Time, int) ([]*models.Submission, error) {
	return nil, nil
}

func (m *mockStore) InsertStump(_ context.Context, stump *models.Stump) error {
	if m.insertStumpErr != nil {
		return m.insertStumpErr
	}
	if m.insertStumpFn != nil {
		if err := m.insertStumpFn(stump); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stumps == nil {
		m.stumps = make(map[string]*models.Stump)
	}
	// Copy the payload so later caller mutation can't race with assertions.
	dataCopy := append([]byte(nil), stump.StumpData...)
	m.stumps[fmt.Sprintf("%s:%d", stump.BlockHash, stump.SubtreeIndex)] = &models.Stump{
		BlockHash:    stump.BlockHash,
		SubtreeIndex: stump.SubtreeIndex,
		StumpData:    dataCopy,
	}
	return nil
}

func (m *mockStore) GetStumpsByBlockHash(context.Context, string) ([]*models.Stump, error) {
	return nil, nil
}
func (m *mockStore) DeleteStumpsByBlockHash(context.Context, string) error { return nil }
func (m *mockStore) BumpRetryCount(context.Context, string) (int, error)   { return 0, nil }
func (m *mockStore) SetPendingRetryFields(context.Context, string, []byte, time.Time, string) error {
	return nil
}

func (m *mockStore) GetReadyRetries(context.Context, time.Time, int) ([]*store.PendingRetry, error) {
	return nil, nil
}

func (m *mockStore) ClearRetryState(context.Context, string, models.Status, string) error {
	return nil
}
func (m *mockStore) EnsureIndexes(context.Context) error { return nil }
func (m *mockStore) UpsertDatahubEndpoint(context.Context, store.DatahubEndpoint) error {
	return nil
}

func (m *mockStore) ListDatahubEndpoints(context.Context, string) ([]store.DatahubEndpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datahubCalls++
	if m.datahubErr != nil {
		return nil, m.datahubErr
	}
	return m.datahubEndpoints, nil
}

func (m *mockStore) UpsertPeerPolicy(context.Context, store.PeerPolicy) error {
	return nil
}

func (m *mockStore) ListPeerPolicies(context.Context, string) ([]store.PeerPolicy, error) {
	if m.listPeerPoliciesErr != nil {
		return nil, m.listPeerPoliciesErr
	}
	return m.peerPolicies, nil
}

func (m *mockStore) UpsertBlockHeaderSeen(context.Context, string, uint64, time.Time) error {
	return nil
}

func (m *mockStore) MarkBlockProcessed(context.Context, string, uint64, time.Time) error {
	return nil
}

func (m *mockStore) MarkBlockBUMPBuilt(context.Context, string, uint64, time.Time) error {
	return nil
}

func (m *mockStore) MarkBlocksOrphaned(context.Context, []string, time.Time) (int, error) {
	return 0, nil
}
func (m *mockStore) MarkBlocksParked(context.Context, []string) error { return nil }

func (m *mockStore) GetBlockProcessingStatus(context.Context, string) (*models.BlockProcessingStatus, error) {
	return nil, store.ErrNotFound
}

func (m *mockStore) ListBlockProcessingStatus(context.Context, uint64, int) ([]*models.BlockProcessingStatus, error) {
	return nil, nil
}

func (m *mockStore) GetActiveTipBlockHeight(context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tipCalls++
	return m.tipHeight, nil
}

func (m *mockStore) ListStaleBlockProcessingStatus(context.Context, time.Time, uint64, int) ([]*models.BlockProcessingStatus, error) {
	return nil, nil
}

func (m *mockStore) Close() error { return nil }

func makeMinimalTx() []byte {
	tx := sdkTx.NewTransaction()
	return tx.Bytes()
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

// testCallbackToken is the bearer secret every callback test installs into the
// Server.cfg so handleCallback's mandatory auth check passes. Real deployments
// supply a high-entropy value via config.callback_token; tests just need a
// stable non-empty string.
const testCallbackToken = "test-callback-token"

func setupServerWithStore(broker *kafka.RecordingBroker, ms *mockStore) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       producer,
		store:          ms,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

func setupServer(broker *kafka.RecordingBroker) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       producer,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

// drainSubmissions flushes any queued submission rows into the store. Tests
// don't run the real recorder pool, so this stand-in performs the same
// InsertSubmission call the workers would.
func drainSubmissions(t *testing.T, srv *Server) {
	t.Helper()
	for {
		select {
		case rec := <-srv.submissionCh:
			if err := srv.store.InsertSubmission(context.Background(), rec.sub); err != nil {
				t.Logf("drainSubmissions: insert failed: %v", err)
			}
		default:
			return
		}
	}
}

// authedCallbackRequest builds a callback POST with the canonical bearer
// header so the mandatory auth check inside handleCallback accepts it. Tests
// that exercise the auth check itself construct their own requests instead.
func authedCallbackRequest(t *testing.T, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testCallbackToken)
	return req
}

// totalMessages returns the combined count of single-message Sends and
// batched entries — matching the old Sarama mock's flat-message semantics.
func totalMessages(broker *kafka.RecordingBroker) int {
	broker.Lock()
	defer broker.Unlock()
	return len(broker.Sends) + func() int {
		n := 0
		for _, b := range broker.Batches {
			n += len(b)
		}
		return n
	}()
}

func TestHandleSubmitTransactions_BatchPublish(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	// Concatenate 3 minimal transactions
	txBytes := makeMinimalTx()
	body := bytes.Repeat(txBytes, 3)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["submitted"].(float64)) != 3 {
		t.Errorf("expected submitted=3, got %v", resp["submitted"])
	}

	if broker.BatchCalls != 1 {
		t.Errorf("expected 1 batch call, got %d", broker.BatchCalls)
	}
	if got := broker.MessageCount(); got != 3 {
		t.Errorf("expected 3 messages, got %d", got)
	}
}

func TestHandleSubmitTransactions_ParseFailure_NoPublish(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	// Valid tx followed by garbage
	body := append(makeMinimalTx(), 0xff, 0xfe, 0xfd)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	if broker.BatchCalls != 0 {
		t.Errorf("expected 0 batch calls on parse failure, got %d", broker.BatchCalls)
	}
}

func TestHandleSubmitTransactions_100Txs_SingleBatchCall(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	// Concatenate 100 minimal transactions
	txBytes := makeMinimalTx()
	body := bytes.Repeat(txBytes, 100)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["submitted"].(float64)) != 100 {
		t.Errorf("expected submitted=100, got %v", resp["submitted"])
	}

	if broker.BatchCalls != 1 {
		t.Errorf("expected exactly 1 batch call for 100 txs, got %d", broker.BatchCalls)
	}
	if got := broker.MessageCount(); got != 100 {
		t.Errorf("expected 100 messages in batch, got %d", got)
	}
}

func TestHandleSubmitTransactions_KafkaFailure_Returns500(t *testing.T) {
	broker := &kafka.RecordingBroker{BatchErr: errors.New("broker down")}
	_, router := setupServer(broker)

	body := makeMinimalTx()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleSubmitTransaction_FreshSubmit_ReturnsARCResponse pins the ARC
// contract for a brand-new submission: HTTP 202 with a body carrying the txid,
// an integer status (202), and txStatus RECEIVED. ARC clients deserialise
// status as an integer and need the txid to track the tx (issue #209).
func TestHandleSubmitTransaction_FreshSubmit_ReturnsARCResponse(t *testing.T) {
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	ms := &mockStore{
		getOrInsertFn: func(_ *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			// Fresh insert: no existing row, inserted=true.
			return nil, true, nil
		},
	}
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		txTracker:      store.NewTxTracker(),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%q)", err, w.Body.String())
	}
	if resp["txid"] != wantTxID {
		t.Errorf("expected txid=%s, got %v", wantTxID, resp["txid"])
	}
	if resp["status"] != float64(http.StatusAccepted) {
		t.Errorf("expected status=202 (integer), got %v", resp["status"])
	}
	if resp["txStatus"] != string(models.StatusReceived) {
		t.Errorf("expected txStatus=RECEIVED, got %v", resp["txStatus"])
	}
	// The legacy string-status shape ("submitted" / "state") must be gone.
	if _, ok := resp["state"]; ok {
		t.Errorf("unexpected legacy field %q in response: %v", "state", resp)
	}

	// A fresh submission must be published to Kafka exactly once.
	if got := totalMessages(broker); got != 1 {
		t.Errorf("fresh submit must publish to Kafka once, got %d messages", got)
	}
}

// TestHandleSubmitTransaction_DedupBranch_PopulatesTxTracker pins the
// invariant that an idempotent re-submit (dedup CAS reports the txid
// already exists) still registers the txid with the in-process TxTracker
// using the persisted status. Without this, a client retrying after a
// process restart would leave the tx invisible to bump-builder's
// tracked-only filter and silently drop subsequent MINED/IMMUTABLE
// transitions.
func TestHandleSubmitTransaction_DedupBranch_PopulatesTxTracker(t *testing.T) {
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	const persistedStatus = models.StatusAcceptedByNetwork
	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			// Simulate the dedup-loser branch: return the existing row
			// with its already-persisted status and inserted=false.
			return &models.TransactionStatus{
				TxID:   in.TxID,
				Status: persistedStatus,
			}, false, nil
		},
	}
	tracker := store.NewTxTracker()
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	// ARC contract: integer status (decodes to float64), txid present, and the
	// existing lifecycle state echoed back in txStatus (issue #209).
	if resp["status"] != float64(http.StatusAccepted) {
		t.Errorf("expected status=202 (integer), got %v", resp["status"])
	}
	if resp["txid"] != wantTxID {
		t.Errorf("expected txid=%s, got %v", wantTxID, resp["txid"])
	}
	if resp["txStatus"] != string(persistedStatus) {
		t.Errorf("expected txStatus=%s, got %v", persistedStatus, resp["txStatus"])
	}

	gotStatus, ok := tracker.GetStatus(wantTxID)
	if !ok {
		t.Fatalf("tracker missing dedup'd txid %s — bug would let bump-builder skip MINED transitions after restart", wantTxID)
	}
	if gotStatus != persistedStatus {
		t.Errorf("tracker status = %q, want %q (persisted, not RECEIVED) — a downgrade would mis-represent state", gotStatus, persistedStatus)
	}

	// Dedup means we must NOT re-publish to Kafka — assert no broadcast
	// occurred to lock in the idempotency contract.
	if totalMessages(broker) != 0 {
		t.Errorf("dedup branch should not publish to Kafka, got %d messages", totalMessages(broker))
	}
}

// TestHandleSubmitTransactions_DedupBranch_PopulatesTxTracker is the
// batch-path mirror of the single-submit dedup test. A batch with one
// duplicate must add that duplicate to TxTracker with the persisted
// status while still publishing the non-duplicate txs.
func TestHandleSubmitTransactions_DedupBranch_PopulatesTxTracker(t *testing.T) {
	// Two distinct minimal txs so we can route one through the dedup
	// branch and one through the fresh-insert branch. Encoding two
	// identical minimal txs back-to-back would parse as two but the
	// store path keys by txid so we'd just see one dedup hit twice;
	// distinct txs make the assertion unambiguous.
	rawA := makeMinimalTx()
	parsedA, _, err := sdkTx.NewTransactionFromStream(rawA)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}
	txidA := parsedA.TxID().String()

	txB := sdkTx.NewTransaction()
	// Inject a single dummy locking-script output so wire bytes differ
	// from the empty minimal tx — drives a different txid.
	txB.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      0,
		LockingScript: &script.Script{},
	})
	rawB := txB.Bytes()
	parsedB, _, err := sdkTx.NewTransactionFromStream(rawB)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}
	txidB := parsedB.TxID().String()
	if txidA == txidB {
		t.Fatalf("expected distinct txids, got identical %s — adjust txB to ensure divergence", txidA)
	}

	const persistedStatus = models.StatusSeenMultipleNodes
	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if in.TxID == txidA {
				// A is the duplicate.
				return &models.TransactionStatus{
					TxID:   in.TxID,
					Status: persistedStatus,
				}, false, nil
			}
			// B is the fresh insert (legacy stub semantics).
			return nil, true, nil
		},
	}
	tracker := store.NewTxTracker()
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	body := append(append([]byte(nil), rawA...), rawB...)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	// Duplicate (A) must land in the tracker with the persisted status.
	gotA, okA := tracker.GetStatus(txidA)
	if !okA {
		t.Fatalf("tracker missing dedup'd batch txid %s — bug would let bump-builder skip MINED after restart", txidA)
	}
	if gotA != persistedStatus {
		t.Errorf("tracker[A].Status = %q, want %q (persisted, not RECEIVED)", gotA, persistedStatus)
	}

	// Fresh insert (B) must also be in the tracker (existing behavior;
	// guards against regressing the non-dedup path).
	gotB, okB := tracker.GetStatus(txidB)
	if !okB {
		t.Fatalf("tracker missing fresh-insert txid %s", txidB)
	}
	if gotB != models.StatusReceived {
		t.Errorf("tracker[B].Status = %q, want RECEIVED", gotB)
	}

	// Only the non-duplicate (B) is republished — duplicates must not
	// re-enter the propagation topic.
	if got := totalMessages(broker); got != 1 {
		t.Errorf("expected 1 published message (only B), got %d", got)
	}
}

// TestHandleSubmitTransaction_ResubmitRejected_RepublishesToKafka pins the
// resubmission contract: when a client POSTs a tx whose existing store row
// is REJECTED, the handler must NOT echo back the rejection — it must
// republish to Kafka so the propagator re-runs the broadcast pipeline.
// The status row is left as REJECTED until the new broadcast produces a
// verdict; the lattice already allows REJECTED → ACCEPTED / SEEN_* so the
// eventual terminal write will land cleanly.
func TestHandleSubmitTransaction_ResubmitRejected_RepublishesToKafka(t *testing.T) {
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return &models.TransactionStatus{
				TxID:   in.TxID,
				Status: models.StatusRejected,
			}, false, nil
		},
	}
	tracker := store.NewTxTracker()
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%q)", err, w.Body.String())
	}
	// Fresh-submit shape: integer status 202, txid present, txStatus RECEIVED
	// (the rejected resubmit re-enters the accept path) — issue #209.
	if got := resp["status"]; got != float64(http.StatusAccepted) {
		t.Errorf("expected status=202 (integer, fresh-submit shape), got %v", got)
	}
	if resp["txid"] != wantTxID {
		t.Errorf("expected txid=%s, got %v", wantTxID, resp["txid"])
	}
	if resp["txStatus"] != string(models.StatusReceived) {
		t.Errorf("expected txStatus=RECEIVED, got %v", resp["txStatus"])
	}

	// Tracker must still know about the txid (so bump-builder's tracked-only
	// filtering keeps working after a future MINED transition).
	if _, ok := tracker.GetStatus(wantTxID); !ok {
		t.Errorf("tracker missing resubmitted txid %s", wantTxID)
	}

	// Resubmit must reach Kafka — otherwise the propagator never sees the
	// new attempt and the row stays REJECTED forever.
	if got := totalMessages(broker); got != 1 {
		t.Errorf("REJECTED resubmit must publish to Kafka, got %d messages", got)
	}
}

// TestHandleSubmitTransaction_StrandedReceived_RepublishesToKafka is the
// issue #83 hole: intake wrote RECEIVED then Kafka Send failed. The client
// retries; the old duplicate branch returned 202 without re-publishing.
func TestHandleSubmitTransaction_StrandedReceived_RepublishesToKafka(t *testing.T) {
	rawTx := makeMinimalTx()
	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return &models.TransactionStatus{
				TxID:   in.TxID,
				Status: models.StatusReceived,
			}, false, nil
		},
	}
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		txTracker:      store.NewTxTracker(),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 1 {
		t.Errorf("stranded RECEIVED retry must publish to Kafka, got %d", got)
	}
}

func TestShouldRepublishExisting(t *testing.T) {
	if !shouldRepublishExisting(models.StatusReceived) || !shouldRepublishExisting(models.StatusRejected) {
		t.Fatal("RECEIVED and REJECTED must republish")
	}
	if shouldRepublishExisting(models.StatusSeenOnNetwork) || shouldRepublishExisting(models.StatusMined) {
		t.Fatal("SEEN/MINED must not republish")
	}
}

// TestHandleSubmitTransactions_BatchResubmitRejected_RepublishesToKafka is
// the batch-path mirror: a batch containing one REJECTED resubmit and one
// fresh-insert must publish both. The REJECTED resubmit is NOT counted as
// a duplicate.
func TestHandleSubmitTransactions_BatchResubmitRejected_RepublishesToKafka(t *testing.T) {
	rawA := makeMinimalTx()
	parsedA, _, err := sdkTx.NewTransactionFromStream(rawA)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}
	txidA := parsedA.TxID().String()

	txB := sdkTx.NewTransaction()
	txB.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      0,
		LockingScript: &script.Script{},
	})
	rawB := txB.Bytes()
	parsedB, _, err := sdkTx.NewTransactionFromStream(rawB)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}
	txidB := parsedB.TxID().String()
	if txidA == txidB {
		t.Fatalf("expected distinct txids, got identical %s", txidA)
	}

	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if in.TxID == txidA {
				return &models.TransactionStatus{
					TxID:   in.TxID,
					Status: models.StatusRejected,
				}, false, nil
			}
			return nil, true, nil
		},
	}
	tracker := store.NewTxTracker()
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	body := append(append([]byte(nil), rawA...), rawB...)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	// Both txs must reach Kafka: A as a REJECTED resubmit, B as a fresh
	// insert.
	if got := totalMessages(broker); got != 2 {
		t.Errorf("expected 2 published messages (A resubmit + B fresh), got %d", got)
	}
}

// TestHandleSubmitTransaction_ValidationFailure_RecordsSubmissionAndPublishes
// pins the intake-rejection contract: when a tx fails validation, the
// handler must (1) persist a REJECTED row, (2) queue an InsertSubmission
// so SSE/webhook can resolve the callback token to a txid, and (3)
// publish a REJECTED event so live subscribers see the terminal
// outcome. Without all three, an intake-rejected tx leaves clients
// silent — no callback, no SSE event — until they manually re-query.
func TestHandleSubmitTransaction_ValidationFailure_RecordsSubmissionAndPublishes(t *testing.T) {
	rawTx := makeMinimalTx() // empty tx → ErrNoInputsOrOutputs from ValidatePolicy
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing minimal tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	val := validator.NewValidator(nil)
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		publisher:      pub,
		validator:      val,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	const callbackToken = "client-cb-token"
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", callbackToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	// Persistence: persistRejectedAtIntake's GetOrInsertStatus stub
	// returns (nil, false, nil), so the code falls through to
	// UpdateStatus — captured here.
	ms.mu.Lock()
	calls := append([]*models.TransactionStatus(nil), ms.updateStatusCalls...)
	ms.mu.Unlock()
	var sawReject bool
	for _, c := range calls {
		if c.TxID == wantTxID && c.Status == models.StatusRejected {
			sawReject = true
			break
		}
	}
	if !sawReject {
		t.Errorf("expected a REJECTED row for txid %s; UpdateStatus calls=%v", wantTxID, calls)
	}

	// Submission queue: drain into the store so we can assert that
	// recordSubmission queued a row with the right txid + token.
	drainSubmissions(t, srv)
	ms.mu.Lock()
	subs := append([]*models.Submission(nil), ms.insertedSubmissions...)
	ms.mu.Unlock()
	if len(subs) != 1 {
		t.Fatalf("expected exactly 1 InsertSubmission for intake-rejected tx with callback token, got %d", len(subs))
	}
	if subs[0].TxID != wantTxID {
		t.Errorf("submission TxID = %q, want %q", subs[0].TxID, wantTxID)
	}
	if subs[0].CallbackToken != callbackToken {
		t.Errorf("submission CallbackToken = %q, want %q", subs[0].CallbackToken, callbackToken)
	}

	// Publish: REJECTED event must go to events.Publisher so SSE / live
	// subscribers see the terminal outcome.
	pub.mu.Lock()
	publishes := append([]*models.TransactionStatus(nil), pub.publishes...)
	pub.mu.Unlock()
	if len(publishes) != 1 {
		t.Fatalf("expected exactly 1 Publish call, got %d", len(publishes))
	}
	if publishes[0].TxID != wantTxID {
		t.Errorf("publish TxID = %q, want %q", publishes[0].TxID, wantTxID)
	}
	if publishes[0].Status != models.StatusRejected {
		t.Errorf("publish Status = %q, want %q", publishes[0].Status, models.StatusRejected)
	}
	if publishes[0].ExtraInfo == "" {
		t.Errorf("publish ExtraInfo is empty; expected the validator reason to be carried for subscriber context")
	}
}

// TestHandleSubmitTransaction_ValidationFailure_DoesNotClobberSeen is the
// issue #251 regression: a resubmit of a tx already at SEEN_* must not
// persist REJECTED or publish a REJECTED event when intake validation fails
// (typically because the same tx already spent its inputs on-chain).
func TestHandleSubmitTransaction_ValidationFailure_DoesNotClobberSeen(t *testing.T) {
	rawTx := makeMinimalTx()

	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return &models.TransactionStatus{
				TxID:   in.TxID,
				Status: models.StatusSeenMultipleNodes,
			}, false, nil
		},
	}
	pub := &recordingCallbackPub{}
	val := validator.NewValidator(nil)
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		publisher:      pub,
		validator:      val,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	ms.mu.Lock()
	updates := append([]*models.TransactionStatus(nil), ms.updateStatusCalls...)
	ms.mu.Unlock()
	if len(updates) != 0 {
		t.Errorf("UpdateStatus must not clobber SEEN_MULTIPLE_NODES, got %v", updates)
	}

	pub.mu.Lock()
	publishes := append([]*models.TransactionStatus(nil), pub.publishes...)
	pub.mu.Unlock()
	if len(publishes) != 0 {
		t.Errorf("must not publish REJECTED over a SEEN row, got %v", publishes)
	}

	if totalMessages(broker) != 0 {
		t.Errorf("must not publish to Kafka after an intake rejection, got %d", totalMessages(broker))
	}
}

// TestHandleSubmitTransaction_ValidationFailure_NoCallback_StillPublishes
// guards the "publish symmetrically with the success path" choice: even
// when the request had no callback URL or token, the REJECTED event is
// still broadcast so a token-less SSE listener or future bulk
// subscriber can observe the rejection. Without this assertion, a
// later refactor could quietly gate the publish on hasSubscription()
// and silently break observers on the broader channel.
func TestHandleSubmitTransaction_ValidationFailure_NoCallback_StillPublishes(t *testing.T) {
	rawTx := makeMinimalTx()

	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	val := validator.NewValidator(nil)
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		publisher:      pub,
		validator:      val,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	// Intentionally no X-CallbackUrl / X-CallbackToken — opts.hasSubscription() == false.
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	// recordSubmission no-ops without a subscription — confirm that
	// stays true so we don't silently start inserting empty rows.
	drainSubmissions(t, srv)
	ms.mu.Lock()
	subs := len(ms.insertedSubmissions)
	ms.mu.Unlock()
	if subs != 0 {
		t.Errorf("expected 0 InsertSubmission calls with no callback opts, got %d", subs)
	}

	// Publish still fires — this is the symmetry guard.
	pub.mu.Lock()
	publishes := len(pub.publishes)
	pub.mu.Unlock()
	if publishes != 1 {
		t.Errorf("expected 1 Publish call even without callback opts, got %d — REJECTED must broadcast for token-less / bulk subscribers", publishes)
	}
}

// TestHandleSubmitTransaction_ValidationFailure_NilPublisher_NoPanic
// guards struct-literal test setups (which leave publisher unset) from
// regressing into a nil dereference inside rejectAtIntake.
func TestHandleSubmitTransaction_ValidationFailure_NilPublisher_NoPanic(t *testing.T) {
	rawTx := makeMinimalTx()
	ms := &mockStore{}
	val := validator.NewValidator(nil)
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		publisher:      nil, // explicit
		validator:      val,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "tok")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req) // must not panic

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleSubmitTransactions_ValidationFailure_RecordsSubmissionAndPublishes
// is the batch-path mirror of the single-submit intake-rejection test.
// First (and only) tx in the batch fails validation; the helper must
// fire for that txid even though the batch is aborted before the dedup
// loop ever runs.
func TestHandleSubmitTransactions_ValidationFailure_RecordsSubmissionAndPublishes(t *testing.T) {
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing minimal tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	val := validator.NewValidator(nil)
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		publisher:      pub,
		validator:      val,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	const callbackToken = "client-cb-token"
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", callbackToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	drainSubmissions(t, srv)

	// REJECTED row written for the failing tx.
	ms.mu.Lock()
	calls := append([]*models.TransactionStatus(nil), ms.updateStatusCalls...)
	subs := append([]*models.Submission(nil), ms.insertedSubmissions...)
	ms.mu.Unlock()
	var sawReject bool
	for _, c := range calls {
		if c.TxID == wantTxID && c.Status == models.StatusRejected {
			sawReject = true
			break
		}
	}
	if !sawReject {
		t.Errorf("expected REJECTED row for batch txid %s; UpdateStatus calls=%v", wantTxID, calls)
	}

	if len(subs) != 1 || subs[0].TxID != wantTxID || subs[0].CallbackToken != callbackToken {
		t.Errorf("expected 1 InsertSubmission for batch txid %s with token %q; got %d: %+v", wantTxID, callbackToken, len(subs), subs)
	}

	pub.mu.Lock()
	publishes := append([]*models.TransactionStatus(nil), pub.publishes...)
	pub.mu.Unlock()
	if len(publishes) != 1 || publishes[0].TxID != wantTxID || publishes[0].Status != models.StatusRejected {
		t.Errorf("expected 1 REJECTED publish for batch txid %s; got %d: %+v", wantTxID, len(publishes), publishes)
	}

	// The batch must abort before publishing anything to TopicPropagation.
	if got := totalMessages(broker); got != 0 {
		t.Errorf("validation failure must abort the batch with no Kafka publish; got %d messages", got)
	}
}

// recordingCallbackPub captures publishes from the seen-callback path so
// tests can assert: (a) exactly one PublishBulk call per inbound callback,
// (b) zero per-tx Publish calls, (c) the bulk template carries every
// successful txid. Coarse but enough for the regression we care about —
// the perf-critical fan-out path lives in the subscriber side which has
// its own coverage.
type recordingCallbackPub struct {
	mu            sync.Mutex
	publishes     []*models.TransactionStatus
	bulkPublishes []*models.TransactionStatus
}

func (p *recordingCallbackPub) Publish(_ context.Context, status *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := *status
	p.publishes = append(p.publishes, &cp)
	return nil
}

func (p *recordingCallbackPub) PublishBulk(_ context.Context, template *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := *template
	cp.TxIDs = append([]string(nil), template.TxIDs...)
	p.bulkPublishes = append(p.bulkPublishes, &cp)
	return nil
}

func (p *recordingCallbackPub) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("not used")
}

func (p *recordingCallbackPub) Close() error { return nil }

// TestHandleSeenOnNetwork_BulkPath_OnePublishPerCallback is the regression
// guard for the perf optimization in plan "RECEIVED → SEEN_ON_NETWORK
// Latency": one inbound callback with N txids must produce exactly ONE
// PublishBulk event and ZERO per-tx Publish calls. The old code did N
// synchronous Kafka sends per callback; under 100 TPS with batches of 50
// that was ~50× the per-callback fan-out work.
func TestHandleSeenOnNetwork_BulkPath_OnePublishPerCallback(t *testing.T) {
	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		publisher:      pub,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	const n = 50
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("tx-%02d", i)
	}
	payload := models.CallbackMessage{Type: models.CallbackSeenOnNetwork, TxIDs: txids}
	req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.publishes) != 0 {
		t.Errorf("expected ZERO per-tx Publish calls, got %d", len(pub.publishes))
	}
	if len(pub.bulkPublishes) != 1 {
		t.Fatalf("expected exactly 1 PublishBulk call, got %d", len(pub.bulkPublishes))
	}
	bulk := pub.bulkPublishes[0]
	if bulk.Status != models.StatusSeenOnNetwork {
		t.Errorf("bulk template status = %q, want SEEN_ON_NETWORK", bulk.Status)
	}
	if len(bulk.TxIDs) != n {
		t.Errorf("bulk template TxIDs length = %d, want %d", len(bulk.TxIDs), n)
	}
	// The store also saw exactly one BatchUpdateStatusReturning call covering
	// every txid in input order.
	if got := len(ms.batchUpdateReturningCalls); got != 1 {
		t.Fatalf("expected 1 BatchUpdateStatusReturning call, got %d", got)
	}
	if got := len(ms.batchUpdateReturningCalls[0]); got != n {
		t.Errorf("expected batch of %d statuses, got %d", n, got)
	}
}

// TestApplySeenCallback_TrackerPrefilter_SkipsStaleTxids pins the optimization
// where the callback handler consults the in-memory TxTracker before hitting
// the store. At sustained 100 TPS ~91% of SEEN_ON_NETWORK callback txids end
// up lattice-skipped at the Pebble layer (merkle-service re-fires after the
// tx already moved past SEEN_ON_NETWORK). Filtering those out before the
// BatchUpdateStatusReturning call removes ~1100 wasted Pebble reads/sec from
// the hot path. Tracker is always ≤ store, so the prefilter cannot drop a
// transition that the store would have applied.
func TestApplySeenCallback_TrackerPrefilter_SkipsStaleTxids(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	tracker := store.NewTxTracker()

	const (
		nStale   = 50 // tracker says SEEN_MULTIPLE_NODES — SEEN_ON_NETWORK callback is a no-op
		nFresh   = 10 // tracker says ACCEPTED_BY_NETWORK — callback should apply
		nUnknown = 5  // not in tracker — must pass through to the store (authoritative)
	)
	// Tracker stores txids as chainhash.Hash, so each test id needs to be a
	// valid 64-char hex string — otherwise Add silently no-ops and the
	// prefilter sees an empty tracker.
	mkTxID := func(prefix byte, i int) string {
		return fmt.Sprintf("%02x%062x", prefix, i)
	}
	stale := make([]string, nStale)
	for i := range stale {
		stale[i] = mkTxID(0x01, i)
		tracker.Add(stale[i], models.StatusSeenMultipleNodes)
	}
	fresh := make([]string, nFresh)
	for i := range fresh {
		fresh[i] = mkTxID(0x02, i)
		tracker.Add(fresh[i], models.StatusAcceptedByNetwork)
	}
	unknown := make([]string, nUnknown)
	for i := range unknown {
		unknown[i] = mkTxID(0x03, i)
	}
	allTxIDs := append(append(append([]string(nil), stale...), fresh...), unknown...)

	// Inject a prev row for every kept txid so the post-batch loop fires
	// the publish path for the 10 fresh txids; default (RECEIVED) is fine.
	ms.batchUpdatePrevFunc = func(txid string) *models.TransactionStatus {
		return &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusAcceptedByNetwork,
			Timestamp: time.Now().Add(-1 * time.Second),
		}
	}

	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		publisher:      pub,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	stalePrev := testutil.ToFloat64(metrics.CallbackStaleTotal.WithLabelValues("SEEN_ON_NETWORK", string(models.StatusSeenMultipleNodes)))

	payload := models.CallbackMessage{Type: models.CallbackSeenOnNetwork, TxIDs: allTxIDs}
	req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	if got := len(ms.batchUpdateReturningCalls); got != 1 {
		t.Fatalf("expected 1 BatchUpdateStatusReturning call, got %d", got)
	}
	if got, want := len(ms.batchUpdateReturningCalls[0]), nFresh+nUnknown; got != want {
		t.Errorf("expected store to receive %d statuses (fresh+unknown), got %d — prefilter not active", want, got)
	}

	// Confirm the stale-callback counter advanced by exactly nStale for the
	// SEEN_MULTIPLE_NODES prev_status label.
	staleNow := testutil.ToFloat64(metrics.CallbackStaleTotal.WithLabelValues("SEEN_ON_NETWORK", string(models.StatusSeenMultipleNodes)))
	if got, want := staleNow-stalePrev, float64(nStale); got != want {
		t.Errorf("CallbackStaleTotal{prev=SEEN_MULTIPLE_NODES} delta = %v, want %v", got, want)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 1 {
		t.Fatalf("expected 1 PublishBulk, got %d", len(pub.bulkPublishes))
	}
	if got, want := len(pub.bulkPublishes[0].TxIDs), nFresh+nUnknown; got != want {
		t.Errorf("bulk publish carried %d txids, want %d", got, want)
	}
}

func TestHandleCallback_SeenMultipleNodes_UpdatesStatus(t *testing.T) {
	ms := &mockStore{}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)

	payload := models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{"tx1", "tx2"},
	}
	body := mustMarshalJSON(t, payload)

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if len(ms.updateStatusCalls) != 2 {
		t.Fatalf("expected 2 UpdateStatus calls, got %d", len(ms.updateStatusCalls))
	}
	for i, call := range ms.updateStatusCalls {
		if call.Status != models.StatusSeenMultipleNodes {
			t.Errorf("call %d: expected status %s, got %s", i, models.StatusSeenMultipleNodes, call.Status)
		}
	}
	if ms.updateStatusCalls[0].TxID != "tx1" {
		t.Errorf("expected first txid=tx1, got %s", ms.updateStatusCalls[0].TxID)
	}
	if ms.updateStatusCalls[1].TxID != "tx2" {
		t.Errorf("expected second txid=tx2, got %s", ms.updateStatusCalls[1].TxID)
	}
}

// TestHandleCallback_UnknownTxid_NoPhantomRow is the regression for F-033
// (#91). When merkle-service POSTs a SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES
// callback for a txid we never recorded, the store layer rejects the update
// with store.ErrNotFound and the handler must:
//
//   - log a Warn (operators want to see attempts to update unknown txids)
//   - skip publishStatus / txTracker updates (no observers should learn
//     about a tx that doesn't exist in our records)
//   - still return 200 OK so merkle-service stops retrying — the txid is
//     definitively unknown, not transiently unavailable.
//
// The contract here is intentionally NOT 404: merkle-service treats 4xx as a
// permanent reject already, but a callback can carry a batch of txids and a
// single unknown one shouldn't fail the whole batch.
func TestHandleCallback_UnknownTxid_NoPhantomRow(t *testing.T) {
	cases := []models.CallbackType{
		models.CallbackSeenOnNetwork,
		models.CallbackSeenMultipleNodes,
	}
	for _, cbType := range cases {
		t.Run(string(cbType), func(t *testing.T) {
			ms := &mockStore{updateStatusErr: store.ErrNotFound}
			_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)
			unknownBefore := testutil.ToFloat64(metrics.CallbackUnknownTxIDTotal.WithLabelValues(string(cbType)))

			payload := models.CallbackMessage{
				Type:  cbType,
				TxIDs: []string{"unknown-tx-1", "unknown-tx-2"},
			}
			body := mustMarshalJSON(t, payload)

			req := authedCallbackRequest(t, body)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 (callbacks for unknown txids are silently dropped), got %d: %s",
					w.Code, w.Body.String())
			}
			// The store rejected the call — mockStore must not have recorded
			// any UpdateStatus payload (otherwise the production backend would
			// have written a phantom row).
			if len(ms.updateStatusCalls) != 0 {
				t.Fatalf("expected 0 recorded UpdateStatus calls when store returns ErrNotFound, got %d",
					len(ms.updateStatusCalls))
			}
			if got := testutil.ToFloat64(metrics.CallbackUnknownTxIDTotal.WithLabelValues(string(cbType))) - unknownBefore; got != 2 {
				t.Fatalf("CallbackUnknownTxIDTotal delta = %v, want 2 for the absent txids", got)
			}
		})
	}
}

func TestHandleCallback_Stump_StorageError_Returns500(t *testing.T) {
	// When STUMP storage fails, we MUST return 5xx so merkle-service retries.
	// Swallowing the error with a 200 breaks the bump_builder's invariant that
	// every STUMP in a BLOCK_PROCESSED block is durably stored in Aerospike.
	broker := &kafka.RecordingBroker{}
	ms := &mockStore{insertStumpErr: errors.New("SERVER_MEM_ERROR")}
	_, router := setupServerWithStore(broker, ms)

	payload := models.CallbackMessage{
		Type:      models.CallbackStump,
		BlockHash: "abc123",
		Stump:     validStumpBytes(0, 0x01),
	}
	body := mustMarshalJSON(t, payload)

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 0 {
		t.Errorf("expected 0 Kafka messages after storage failure, got %d", got)
	}
}

// A STUMP that does not parse is refused with 400 and never stored: rows are
// content-addressed, so a stored bad copy would sit beside every good resend
// and fail each build of the block.
func TestHandleCallback_Stump_Malformed_Returns400(t *testing.T) {
	var stored int
	ms := &mockStore{insertStumpFn: func(*models.Stump) error { stored++; return nil }}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)

	for _, stump := range [][]byte{{0x01, 0x02}, make([]byte, 64)} {
		payload := models.CallbackMessage{Type: models.CallbackStump, BlockHash: "abc123", Stump: stump}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, authedCallbackRequest(t, mustMarshalJSON(t, payload)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("stump %x: expected 400, got %d: %s", stump, w.Code, w.Body.String())
		}
	}
	if stored != 0 {
		t.Fatalf("a malformed STUMP must not be stored, InsertStump calls = %d", stored)
	}
}

func TestHandleCallback_SeenMultipleNodes_EmptyTxIDs(t *testing.T) {
	ms := &mockStore{}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)

	payload := models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{},
	}
	body := mustMarshalJSON(t, payload)

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if len(ms.updateStatusCalls) != 0 {
		t.Errorf("expected 0 UpdateStatus calls, got %d", len(ms.updateStatusCalls))
	}
}

// TestHandleCallback_FullBlockFlow_20Subtrees simulates the production delivery
// pattern that merkle-service executes for a 20,000-tx block split across 20
// subtrees:
//
//  1. Twenty STUMP callbacks (one per subtreeIndex 0..19) POSTed to
//     /api/v1/merkle-service/callback, each carrying a realistic ~8 KB payload
//     hex-encoded via models.HexBytes. merkle-service fires these in parallel
//     from a 64-worker delivery pool (merkle-service/internal/callback/delivery.go),
//     so we dispatch them concurrently here.
//  2. A single BLOCK_PROCESSED callback with just the block hash, which
//     merkle-service's stumpGate only releases AFTER every STUMP has returned
//     2xx (merkle-service/internal/callback/delivery.go stumpGate.Wait).
//
// The test asserts end-to-end correctness of the code path that production is
// returning 500s from:
//
//   - every STUMP returns 200
//   - all 20 STUMPs land in the store with the correct composite key and
//     byte-identical payload (hex round-trip through models.HexBytes)
//   - Kafka is not touched for STUMPs (they go to the store only)
//   - BLOCK_PROCESSED produces exactly one Kafka message on
//     bsv-broadcaster.block_processed, keyed by block hash, with the full
//     CallbackMessage JSON as the value
//   - retry semantics: a duplicated STUMP delivery still returns 200 and
//     overwrites cleanly, because merkle-service retries on any non-2xx
func TestHandleCallback_FullBlockFlow_20Subtrees(t *testing.T) {
	const (
		numSubtrees = 20
		stumpSize   = 8 * 1024 // ~8 KB — realistic for a subtree covering ~1000 txs (BRC-0074 BUMP format)
	)
	blockHash := "000000000000000000001234567890abcdef1234567890abcdef1234567890ab"

	// Deterministic per-subtree payload so byte-level assertions are stable.
	stumpPayloads := make([][]byte, numSubtrees)
	for i := range stumpPayloads {
		buf := validStumpBytes(stumpSize, byte(i))
		for j := minimalStumpLen; j < len(buf); j++ {
			buf[j] = byte((i*31 + j) & 0xFF)
		}
		stumpPayloads[i] = buf
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithStore(broker, ms)

	// Phase 1: fire all 20 STUMPs in parallel.
	var wg sync.WaitGroup
	errCh := make(chan error, numSubtrees)
	for i := 0; i < numSubtrees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := models.CallbackMessage{
				Type:         models.CallbackStump,
				BlockHash:    blockHash,
				SubtreeIndex: i,
				Stump:        stumpPayloads[i],
			}
			body, err := json.Marshal(payload)
			if err != nil {
				errCh <- fmt.Errorf("marshal subtree %d: %w", i, err)
				return
			}
			req := authedCallbackRequest(t, body)
			// Match merkle-service's delivery headers exactly.
			req.Header.Set("X-Idempotency-Key", fmt.Sprintf("%s:%d:STUMP", blockHash, i))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errCh <- fmt.Errorf("STUMP subtree %d: expected 200, got %d: %s", i, w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	// All 20 STUMPs must be in the store, bit-identical to what was sent.
	ms.mu.Lock()
	stored := len(ms.stumps)
	ms.mu.Unlock()
	if stored != numSubtrees {
		t.Fatalf("expected %d STUMPs stored, got %d", numSubtrees, stored)
	}
	for i := 0; i < numSubtrees; i++ {
		key := fmt.Sprintf("%s:%d", blockHash, i)
		ms.mu.Lock()
		stump, ok := ms.stumps[key]
		ms.mu.Unlock()
		if !ok {
			t.Errorf("missing stump for subtree %d (key=%q)", i, key)
			continue
		}
		if stump.BlockHash != blockHash {
			t.Errorf("subtree %d: blockHash = %q, want %q", i, stump.BlockHash, blockHash)
		}
		if stump.SubtreeIndex != i {
			t.Errorf("subtree %d: SubtreeIndex = %d, want %d", i, stump.SubtreeIndex, i)
		}
		if !bytes.Equal(stump.StumpData, stumpPayloads[i]) {
			t.Errorf("subtree %d: stump bytes differ after hex round-trip (got %d bytes, want %d)",
				i, len(stump.StumpData), len(stumpPayloads[i]))
		}
	}

	// STUMPs must not produce Kafka traffic — the bump_builder consumes
	// bsv-broadcaster.block_processed only, and STUMPs are writes to the store.
	if got := totalMessages(broker); got != 0 {
		t.Fatalf("expected 0 Kafka messages after STUMP phase, got %d", got)
	}

	// Phase 2: BLOCK_PROCESSED. Carries only the block hash; merkle-service
	// does not resend the stump bytes here.
	blockMsg := models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: blockHash,
	}
	body, err := json.Marshal(blockMsg)
	if err != nil {
		t.Fatalf("marshal BLOCK_PROCESSED: %v", err)
	}
	req := authedCallbackRequest(t, body)
	req.Header.Set("X-Idempotency-Key", blockHash+":BLOCK_PROCESSED")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("BLOCK_PROCESSED: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Exactly one Kafka message on bsv-broadcaster.block_processed, keyed by block
	// hash, with the full CallbackMessage JSON as the value — this is what
	// the bump_builder consumer expects (services/bump_builder/builder.go).
	if got := totalMessages(broker); got != 1 {
		t.Fatalf("expected 1 Kafka message after BLOCK_PROCESSED, got %d", got)
	}
	broker.Lock()
	if len(broker.Sends) != 1 {
		broker.Unlock()
		t.Fatalf("expected 1 Send call, got %d", len(broker.Sends))
	}
	sent := broker.Sends[0]
	broker.Unlock()
	if sent.Topic != kafka.TopicBlockProcessed {
		t.Errorf("Kafka topic = %q, want %q", sent.Topic, kafka.TopicBlockProcessed)
	}
	if sent.Key != blockHash {
		t.Errorf("Kafka key = %q, want %q", sent.Key, blockHash)
	}
	var decoded models.CallbackMessage
	if err := json.Unmarshal(sent.Value, &decoded); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}
	if decoded.Type != models.CallbackBlockProcessed {
		t.Errorf("kafka value Type = %q, want %q", decoded.Type, models.CallbackBlockProcessed)
	}
	if decoded.BlockHash != blockHash {
		t.Errorf("kafka value BlockHash = %q, want %q", decoded.BlockHash, blockHash)
	}

	// Phase 3: idempotency. merkle-service retries on any non-2xx with
	// linear backoff, so a second delivery of subtree 0 must still return
	// 200. Our store is upsert-on-(blockHash,subtreeIndex) so the count
	// stays at 20.
	retryPayload := models.CallbackMessage{
		Type:         models.CallbackStump,
		BlockHash:    blockHash,
		SubtreeIndex: 0,
		Stump:        stumpPayloads[0],
	}
	retryBody := mustMarshalJSON(t, retryPayload)
	retryReq := authedCallbackRequest(t, retryBody)
	retryW := httptest.NewRecorder()
	router.ServeHTTP(retryW, retryReq)
	if retryW.Code != http.StatusOK {
		t.Fatalf("duplicate STUMP: expected 200, got %d: %s", retryW.Code, retryW.Body.String())
	}
	ms.mu.Lock()
	finalCount := len(ms.stumps)
	ms.mu.Unlock()
	if finalCount != numSubtrees {
		t.Errorf("expected stump count to stay at %d after duplicate, got %d", numSubtrees, finalCount)
	}
}

// TestHandleCallback_FullBlockFlow_PartialStumpFailure reproduces the
// production failure surface: during delivery of 20 STUMPs, one subtree's
// Put() fails at the store layer (simulating Aerospike's RECORD_TOO_BIG when
// a busy subtree's BUMP proof exceeds the namespace's write-block-size, or a
// transient DEVICE_OVERLOAD / HOT_KEY on a single composite key) while the
// other 19 succeed.
//
// The test locks down the observable behavior that the bump_builder and
// merkle-service both depend on:
//
//   - the failing STUMP responds 500 so merkle-service's retry loop
//     re-queues it (merkle-service/internal/callback/delivery.go dispatch →
//     retry path)
//   - the succeeding STUMPs respond 200 and land in the store
//   - NO BLOCK_PROCESSED-like Kafka publish happens during the STUMP phase,
//     so bump_builder never sees a block with missing STUMPs
//   - sending BLOCK_PROCESSED while one STUMP is still missing DOES still
//     publish to Kafka — BSV Broadcaster does not validate STUMP completeness here.
//     That is intentional: merkle-service's stumpGate is what gates
//     BLOCK_PROCESSED on upstream 2xx, and if the retry exhausts it falls
//     into merkle-service's DLQ rather than calling BLOCK_PROCESSED.
//     This assertion documents where responsibility sits.
func TestHandleCallback_FullBlockFlow_PartialStumpFailure(t *testing.T) {
	const (
		numSubtrees    = 20
		stumpSize      = 8 * 1024
		failingSubtree = 7
	)
	blockHash := "000000000000000000001234567890abcdef1234567890abcdef1234567890ab"

	stumpPayloads := make([][]byte, numSubtrees)
	for i := range stumpPayloads {
		buf := validStumpBytes(stumpSize, byte(i))
		for j := minimalStumpLen; j < len(buf); j++ {
			buf[j] = byte((i*31 + j) & 0xFF)
		}
		stumpPayloads[i] = buf
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{
		insertStumpFn: func(stump *models.Stump) error {
			if stump.SubtreeIndex == failingSubtree {
				// Shape matches an Aerospike-style error string so a log
				// consumer correlating with store-layer errors can find it.
				return errors.New("Put failed: ResultCode: SERVER_MEM_ERROR")
			}
			return nil
		},
	}
	_, router := setupServerWithStore(broker, ms)

	type result struct {
		idx    int
		status int
	}
	resCh := make(chan result, numSubtrees)

	var wg sync.WaitGroup
	for i := 0; i < numSubtrees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := models.CallbackMessage{
				Type:         models.CallbackStump,
				BlockHash:    blockHash,
				SubtreeIndex: i,
				Stump:        stumpPayloads[i],
			}
			body := mustMarshalJSON(t, payload)
			req := authedCallbackRequest(t, body)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			resCh <- result{idx: i, status: w.Code}
		}()
	}
	wg.Wait()
	close(resCh)

	statuses := make(map[int]int, numSubtrees)
	for r := range resCh {
		statuses[r.idx] = r.status
	}
	for i := 0; i < numSubtrees; i++ {
		want := http.StatusOK
		if i == failingSubtree {
			want = http.StatusInternalServerError
		}
		if statuses[i] != want {
			t.Errorf("subtree %d: status = %d, want %d", i, statuses[i], want)
		}
	}

	ms.mu.Lock()
	stored := len(ms.stumps)
	_, failingStored := ms.stumps[fmt.Sprintf("%s:%d", blockHash, failingSubtree)]
	ms.mu.Unlock()
	if stored != numSubtrees-1 {
		t.Errorf("expected %d STUMPs in store after partial failure, got %d", numSubtrees-1, stored)
	}
	if failingStored {
		t.Errorf("failing subtree %d must not be in the store", failingSubtree)
	}

	// Kafka must be untouched during STUMP phase, even with a mid-flight 500.
	if got := totalMessages(broker); got != 0 {
		t.Fatalf("expected 0 Kafka messages during STUMP phase, got %d", got)
	}

	// BLOCK_PROCESSED is still accepted and published. BSV Broadcaster does not check
	// STUMP completeness — that contract lives in merkle-service's stumpGate.
	// The bump_builder handles missing STUMPs downstream via its grace window
	// + GetStumpsByBlockHash retrieval, so this call must not be rejected.
	blockMsg := models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: blockHash,
	}
	body := mustMarshalJSON(t, blockMsg)
	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("BLOCK_PROCESSED: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 1 {
		t.Errorf("expected 1 Kafka message after BLOCK_PROCESSED, got %d", got)
	}
}

// makeRealTx returns a transaction with one funded input and one output so
// that tx.EF() produces a different byte sequence than tx.Bytes(). Without
// real source data, EF() returns ErrEmptyPreviousTx and the EF/legacy hashes
// would collide — defeating the purpose of the EF-vs-legacy regression tests.
func makeRealTx(t *testing.T) *sdkTx.Transaction {
	t.Helper()
	tx := sdkTx.NewTransaction()
	if err := tx.AddInputFrom(
		"0000000000000000000000000000000000000000000000000000000000000001",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		1000,
		nil,
	); err != nil {
		t.Fatalf("AddInputFrom: %v", err)
	}
	opReturn, err := script.NewFromHex("6a")
	if err != nil {
		t.Fatalf("script.NewFromHex: %v", err)
	}
	tx.AddOutput(&sdkTx.TransactionOutput{Satoshis: 900, LockingScript: opReturn})
	return tx
}

// TestHandleSubmitTransaction_TxID_IsCanonical verifies /tx records the
// canonical Bitcoin txid (tx.TxID()) — not a hash of the wire bytes — for
// every accepted content type and for both legacy and Extended Format
// submissions. Regression test for the EF / canonical txid mismatch that
// caused submissions.txid to never match transactions.txid, which broke SSE
// status fan-out for any client posting EF.
func TestHandleSubmitTransaction_TxID_IsCanonical(t *testing.T) {
	tx := makeRealTx(t)
	legacy := tx.Bytes()
	ef, err := tx.EF()
	if err != nil {
		t.Fatalf("tx.EF: %v", err)
	}
	canonical := tx.TxID().String()

	if bytes.Equal(legacy, ef) {
		t.Fatalf("EF and legacy bytes are identical — test would be trivial")
	}

	cases := []struct {
		name        string
		contentType string
		body        []byte
	}{
		{"octet-stream legacy", "application/octet-stream", legacy},
		{"octet-stream EF", "application/octet-stream", ef},
		{"text/plain hex legacy", "text/plain", []byte(hex.EncodeToString(legacy))},
		{"text/plain hex EF", "text/plain", []byte(hex.EncodeToString(ef))},
		{"json legacy", "application/json", mustMarshalJSON(t, map[string]string{"rawTx": hex.EncodeToString(legacy)})},
		{"json EF", "application/json", mustMarshalJSON(t, map[string]string{"rawTx": hex.EncodeToString(ef)})},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			broker := &kafka.RecordingBroker{}
			ms := &mockStore{}
			srv, router := setupServerWithStore(broker, ms)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(c.body))
			req.Header.Set("Content-Type", c.contentType)
			req.Header.Set("X-CallbackToken", "test-token")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			drainSubmissions(t, srv)

			if w.Code != http.StatusAccepted {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}

			if len(broker.Sends) != 1 {
				t.Fatalf("expected 1 Send, got %d", len(broker.Sends))
			}
			if broker.Sends[0].Key != canonical {
				t.Errorf("kafka key: want %s, got %s", canonical, broker.Sends[0].Key)
			}

			if len(ms.insertedSubmissions) != 1 {
				t.Fatalf("expected 1 submission, got %d", len(ms.insertedSubmissions))
			}
			if ms.insertedSubmissions[0].TxID != canonical {
				t.Errorf("submission txid: want %s, got %s", canonical, ms.insertedSubmissions[0].TxID)
			}
		})
	}
}

// TestHandleSubmitTransactions_TxID_IsCanonical is the bulk-endpoint
// counterpart. Mixing legacy and EF in a single batch also confirms the
// parser advances bytesUsed correctly across format changes.
func TestHandleSubmitTransactions_TxID_IsCanonical(t *testing.T) {
	txA := makeRealTx(t)
	txB := makeRealTx(t)
	txB.Outputs[0].Satoshis = 800 // make B distinct so canonicals differ

	legacyA := txA.Bytes()
	efB, err := txB.EF()
	if err != nil {
		t.Fatalf("EF: %v", err)
	}
	canonA := txA.TxID().String()
	canonB := txB.TxID().String()
	if canonA == canonB {
		t.Fatalf("test setup: txA and txB hashed equal")
	}

	body := append([]byte{}, legacyA...)
	body = append(body, efB...)

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	srv, router := setupServerWithStore(broker, ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "test-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	drainSubmissions(t, srv)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	if len(broker.Batches) != 1 || len(broker.Batches[0]) != 2 {
		t.Fatalf("expected 1 batch of 2, got Batches=%v", broker.Batches)
	}
	if got := broker.Batches[0][0].Key; got != canonA {
		t.Errorf("batch[0]: want %s, got %s", canonA, got)
	}
	if got := broker.Batches[0][1].Key; got != canonB {
		t.Errorf("batch[1]: want %s, got %s", canonB, got)
	}

	if len(ms.insertedSubmissions) != 2 {
		t.Fatalf("expected 2 submissions, got %d", len(ms.insertedSubmissions))
	}
	got := []string{ms.insertedSubmissions[0].TxID, ms.insertedSubmissions[1].TxID}
	want := []string{canonA, canonB}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("submissions: want %v, got %v", want, got)
	}
}

// TestHandleCallback_RejectsUnauthenticated locks down the F-018 fix: the
// /api/v1/merkle-service/callback receiver MUST refuse any request missing
// or presenting the wrong bearer token, and MUST refuse all requests when
// the configured token is empty (fail-closed). Pre-fix, the handler skipped
// the entire bearer check when CallbackToken == "", letting any unauthenticated
// caller submit forged Merkle status updates.
//
// This test exercises the runtime check directly. The "config rejects empty
// token when Merkle is enabled" half of the fix is covered in
// config/config_test.go (TestValidate_RequiresCallbackTokenWhenMerkleEnabled).
func TestHandleCallback_RejectsUnauthenticated(t *testing.T) {
	payload := mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{"tx1"},
	})

	cases := []struct {
		name    string
		token   string // configured CallbackToken on the Server.
		header  string // Authorization header sent on the request, "" = none.
		wantOK  bool
		wantErr int
	}{
		{
			name:    "no auth header is rejected",
			token:   testCallbackToken,
			header:  "",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			name:    "wrong bearer is rejected",
			token:   testCallbackToken,
			header:  "Bearer not-the-real-token",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			name:    "non-bearer scheme is rejected",
			token:   testCallbackToken,
			header:  "Basic " + testCallbackToken,
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			// Defense-in-depth. Config validation now refuses to start with an
			// empty CallbackToken when Merkle is enabled, but if a misconfigured
			// process somehow reaches the handler with cfg.CallbackToken == ""
			// every request — including one presenting an empty bearer — must
			// still be refused.
			name:    "empty configured token rejects all callers",
			token:   "",
			header:  "Bearer ",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			// Same scenario as above with no auth header — must also be 401.
			name:    "empty configured token rejects unauthenticated caller",
			token:   "",
			header:  "",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			name:   "correct bearer is accepted",
			token:  testCallbackToken,
			header: "Bearer " + testCallbackToken,
			wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{}
			gin.SetMode(gin.TestMode)
			producer := kafka.NewProducer(&kafka.RecordingBroker{})
			srv := &Server{
				cfg:      &config.Config{CallbackToken: tc.token},
				logger:   zap.NewNop(),
				producer: producer,
				store:    ms,
			}
			router := gin.New()
			srv.registerRoutes(router)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if tc.wantOK {
				if w.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
				}
				if len(ms.updateStatusCalls) != 1 {
					t.Errorf("expected store update on accepted callback, got %d", len(ms.updateStatusCalls))
				}
				return
			}
			if w.Code != tc.wantErr {
				t.Fatalf("expected %d, got %d: %s", tc.wantErr, w.Code, w.Body.String())
			}
			// Rejected requests must not reach the dispatch path.
			if len(ms.updateStatusCalls) != 0 {
				t.Errorf("rejected callback must not write to the store, got %d UpdateStatus calls", len(ms.updateStatusCalls))
			}
		})
	}
}

// setupServerWithCallbackLimit returns a Server / router pair with the
// callback body cap explicitly configured. Used by the F-019 body-cap tests
// so they can run against a small, predictable limit instead of the 16 MiB
// production default.
func setupServerWithCallbackLimit(broker *kafka.RecordingBroker, ms *mockStore, maxBytes int64) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg: &config.Config{
			CallbackToken: testCallbackToken,
			Callback:      config.CallbackConfig{MaxBodyBytes: maxBytes},
		},
		logger:   zap.NewNop(),
		producer: producer,
		store:    ms,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

// TestHandleCallback_BodySizeLimit_UnderLimitSucceeds verifies that a
// callback whose serialized JSON sits comfortably under the configured cap
// is processed normally — guarding against an overly aggressive limit
// rejecting legitimate STUMP deliveries. Locks down half of the F-019 fix.
func TestHandleCallback_BodySizeLimit_UnderLimitSucceeds(t *testing.T) {
	const limit = 64 * 1024 // 64 KiB
	ms := &mockStore{}
	_, router := setupServerWithCallbackLimit(&kafka.RecordingBroker{}, ms, limit)

	// Build a STUMP payload sized so that the resulting JSON body — which
	// hex-encodes the bytes via models.HexBytes — is just under the cap. Hex
	// roughly doubles the byte count, so half the limit (less envelope
	// overhead) leaves headroom for the JSON wrapper.
	stumpBytes := validStumpBytes(limit/2-1024, 0x01)
	payload := models.CallbackMessage{
		Type:      models.CallbackStump,
		BlockHash: "0000000000000000000000000000000000000000000000000000000000000001",
		Stump:     stumpBytes,
	}
	body := mustMarshalJSON(t, payload)
	if int64(len(body)) >= limit {
		t.Fatalf("test setup: body %d bytes is not under limit %d", len(body), limit)
	}

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	ms.mu.Lock()
	stored := len(ms.stumps)
	ms.mu.Unlock()
	if stored != 1 {
		t.Errorf("expected 1 stump stored on under-limit body, got %d", stored)
	}
}

// TestHandleCallback_BodySizeLimit_OverLimitReturns413 verifies the F-019
// fix: an oversize callback POST is rejected with 413 Payload Too Large
// (not 400) before any decoding allocates the full body. Locks down the
// other half of the fix.
func TestHandleCallback_BodySizeLimit_OverLimitReturns413(t *testing.T) {
	const limit = 64 * 1024 // 64 KiB
	ms := &mockStore{}
	_, router := setupServerWithCallbackLimit(&kafka.RecordingBroker{}, ms, limit)

	// Construct a body whose raw byte length exceeds the cap. We embed it in
	// a CallbackSeenOnNetwork payload (oversize TxIDs list) so the structure
	// is still valid JSON — what we're testing is the size check, not parse
	// behavior on garbage input.
	huge := strings.Repeat("a", int(limit)+1024)
	body := mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{huge},
	})
	if int64(len(body)) <= limit {
		t.Fatalf("test setup: body %d bytes is not over limit %d", len(body), limit)
	}

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", w.Code, w.Body.String())
	}

	// Oversize bodies must short-circuit before the dispatch path runs.
	if len(ms.updateStatusCalls) != 0 {
		t.Errorf("oversize callback must not write to the store, got %d UpdateStatus calls", len(ms.updateStatusCalls))
	}
}

// TestHandleCallback_BodySizeLimit_HugeStumpRejected covers the original
// F-019 scenario directly: a STUMP callback whose embedded payload is much
// larger than the configured cap returns 413 instead of allocating the
// entire body into memory.
func TestHandleCallback_BodySizeLimit_HugeStumpRejected(t *testing.T) {
	const limit = 32 * 1024 // 32 KiB
	ms := &mockStore{}
	_, router := setupServerWithCallbackLimit(&kafka.RecordingBroker{}, ms, limit)

	// 4 MiB STUMP — vastly larger than the cap, mimicking the unbounded
	// payload that motivated F-019.
	stumpBytes := make([]byte, 4*1024*1024)
	for i := range stumpBytes {
		stumpBytes[i] = byte(i & 0xFF)
	}
	body := mustMarshalJSON(t, models.CallbackMessage{
		Type:      models.CallbackStump,
		BlockHash: "0000000000000000000000000000000000000000000000000000000000000002",
		Stump:     stumpBytes,
	})

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", w.Code, w.Body.String())
	}
	ms.mu.Lock()
	stored := len(ms.stumps)
	ms.mu.Unlock()
	if stored != 0 {
		t.Errorf("oversize STUMP must not be persisted, got %d stored", stored)
	}
}

// TestHandleCallback_BodySizeLimit_DefaultsToConfigConstant confirms that
// leaving Callback.MaxBodyBytes unset (or zero/negative) falls back to the
// package default, so a misconfiguration can never disable the cap entirely.
func TestHandleCallback_BodySizeLimit_DefaultsToConfigConstant(t *testing.T) {
	srv := &Server{cfg: &config.Config{}}
	if got := srv.callbackMaxBodyBytes(); got != config.DefaultCallbackMaxBodyBytes {
		t.Errorf("zero value: got %d, want %d", got, config.DefaultCallbackMaxBodyBytes)
	}

	srv.cfg.Callback.MaxBodyBytes = -1
	if got := srv.callbackMaxBodyBytes(); got != config.DefaultCallbackMaxBodyBytes {
		t.Errorf("negative value: got %d, want %d", got, config.DefaultCallbackMaxBodyBytes)
	}

	srv.cfg.Callback.MaxBodyBytes = 1 << 10
	if got := srv.callbackMaxBodyBytes(); got != 1<<10 {
		t.Errorf("explicit value: got %d, want %d", got, 1<<10)
	}
}

// TestHandleCallback_BlockProcessed_ForwardsEnrichmentFields pins issue #195's
// wire contract: a BLOCK_PROCESSED carrying merkleRoot + subtreeCount +
// subtreeHashes + coinbaseBump is forwarded to the bsv-broadcaster.block_processed Kafka
// topic with all four fields intact, so the bump_builder consumer can build a
// datahub-independent compound BUMP. Guards against accidental field stripping
// in the producer/serialization path.
func TestHandleCallback_BlockProcessed_ForwardsEnrichmentFields(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithStore(broker, ms)

	const (
		blockHash  = "00000000c57cfda6619dd099aa447a28353cfaca71942770273f5818778f64f7"
		merkleRoot = "215b459eb39fb46220c591d89dffa89def1cf03f327765e6009b949aed741414"
		subtree0   = "1111111111111111111111111111111111111111111111111111111111111111"
		subtree1   = "2222222222222222222222222222222222222222222222222222222222222222"
	)
	in := models.CallbackMessage{
		Type:          models.CallbackBlockProcessed,
		BlockHash:     blockHash,
		MerkleRoot:    merkleRoot,
		SubtreeCount:  2,
		SubtreeHashes: []string{subtree0, subtree1},
		CoinbaseBUMP:  models.HexBytes{0xde, 0xad, 0xbe, 0xef},
	}
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, authedCallbackRequest(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	broker.Lock()
	defer broker.Unlock()
	if len(broker.Sends) != 1 {
		t.Fatalf("expected 1 Send on bsv-broadcaster.block_processed, got %d", len(broker.Sends))
	}
	sent := broker.Sends[0]
	if sent.Topic != kafka.TopicBlockProcessed {
		t.Errorf("topic = %q, want %q", sent.Topic, kafka.TopicBlockProcessed)
	}

	var decoded models.CallbackMessage
	if err := json.Unmarshal(sent.Value, &decoded); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}
	if decoded.MerkleRoot != merkleRoot {
		t.Errorf("MerkleRoot = %q, want %q", decoded.MerkleRoot, merkleRoot)
	}
	if decoded.SubtreeCount != 2 {
		t.Errorf("SubtreeCount = %d, want 2", decoded.SubtreeCount)
	}
	if len(decoded.SubtreeHashes) != 2 || decoded.SubtreeHashes[0] != subtree0 || decoded.SubtreeHashes[1] != subtree1 {
		t.Errorf("SubtreeHashes = %v, want [%s %s]", decoded.SubtreeHashes, subtree0, subtree1)
	}
	if string(decoded.CoinbaseBUMP) != string([]byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("CoinbaseBUMP = %x, want deadbeef", decoded.CoinbaseBUMP)
	}
}

// counterVal reads a labeled counter for delta assertions.
func counterVal(route, result string) float64 {
	return testutil.ToFloat64(metrics.APITxsSubmittedTotal.WithLabelValues(route, result))
}

// TestSubmitCounters_BatchCountsPerTransaction is the regression for the
// submissions-metric bug: the HTTP request histogram counts one sample per
// request, so a 3-tx /txs batch read as ONE submission on dashboards.
// bsv_broadcaster_api_txs_submitted_total must count per transaction.
func TestSubmitCounters_BatchCountsPerTransaction(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker) // no store: every parsed tx counts as new

	before := counterVal("/txs", "new")

	body := bytes.Repeat(makeMinimalTx(), 3)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if got := counterVal("/txs", "new") - before; got != 3 {
		t.Errorf("txs_submitted{/txs,new} delta = %v, want 3 (one per transaction, not per request)", got)
	}
}

// TestSubmitCounters_SingleDedupResults pins the result label on the single
// submit route: a fresh insert counts as new; an idempotent resubmit counts
// as duplicate (and is not double-counted as new).
func TestSubmitCounters_SingleDedupResults(t *testing.T) {
	broker := &kafka.RecordingBroker{}

	// Fresh insert.
	msNew := &mockStore{getOrInsertFn: func(st *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		return st, true, nil
	}}
	_, router := setupServerWithStore(broker, msNew)
	beforeNew, beforeDup := counterVal("/tx", "new"), counterVal("/tx", "duplicate")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(makeMinimalTx()))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if got := counterVal("/tx", "new") - beforeNew; got != 1 {
		t.Errorf("txs_submitted{/tx,new} delta = %v, want 1", got)
	}

	// Idempotent duplicate.
	existing := &models.TransactionStatus{Status: models.StatusSeenOnNetwork}
	msDup := &mockStore{getOrInsertFn: func(_ *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		return existing, false, nil
	}}
	_, router2 := setupServerWithStore(broker, msDup)
	beforeNew = counterVal("/tx", "new")

	req2 := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(makeMinimalTx()))
	req2.Header.Set("Content-Type", "application/octet-stream")
	w2 := httptest.NewRecorder()
	router2.ServeHTTP(w2, req2)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("duplicate resubmit: expected 202, got %d: %s", w2.Code, w2.Body.String())
	}
	if got := counterVal("/tx", "duplicate") - beforeDup; got != 1 {
		t.Errorf("txs_submitted{/tx,duplicate} delta = %v, want 1", got)
	}
	if got := counterVal("/tx", "new") - beforeNew; got != 0 {
		t.Errorf("duplicate resubmit must not count as new (delta %v)", got)
	}
}

// minimalStumpLen is the size of the smallest parseable BRC-74 STUMP: block
// height, tree height, leaf count, offset and flags (one byte each) plus one
// 32-byte hash.
const minimalStumpLen = 37

// validStumpBytes returns a STUMP the callback handler accepts, size bytes
// long (at least minimalStumpLen): a one-leaf merkle path whose hash is seed
// repeated, padded with zeros. The parser ignores trailing bytes, which lets
// size-sensitive tests keep their payload sizes.
func validStumpBytes(size int, seed byte) []byte {
	if size < minimalStumpLen {
		size = minimalStumpLen
	}
	buf := make([]byte, size)
	copy(buf, []byte{0x01, 0x01, 0x01, 0x00, 0x02})
	for i := 5; i < minimalStumpLen; i++ {
		buf[i] = seed
	}
	return buf
}

// LookupPartitionKeys satisfies the intake keying lookup; the generic mock
// knows no parents, so every submission keys by its own txid.
func (m *mockStore) LookupPartitionKeys(_ context.Context, _ []string) (map[string]store.ParentKey, error) {
	return nil, nil
}
