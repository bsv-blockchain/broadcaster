package api_server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// TestHandleSubmitTransaction_PublishesReceivedWithTokenHint pins the fix
// for issue 6d of the hosted-service report: a token-scoped SSE subscriber
// saw ACCEPTED_BY_NETWORK as the first frame because intake never published
// RECEIVED. The event must be published once the Kafka message is queued and
// must carry the request's callback token as a hint, since the submission
// row the SSE pod resolves tokens from is written asynchronously.
func TestHandleSubmitTransaction_PublishesReceivedWithTokenHint(t *testing.T) {
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	ms := &mockStore{
		getOrInsertFn: func(_ *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return nil, true, nil
		},
	}
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		publisher:      pub,
		txTracker:      store.NewTxTracker(),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "sse-session-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.publishes) != 1 {
		t.Fatalf("expected exactly one published event, got %d (%+v)", len(pub.publishes), pub.publishes)
	}
	got := pub.publishes[0]
	if got.TxID != wantTxID || got.Status != models.StatusReceived {
		t.Errorf("published %s/%s, want %s/RECEIVED", got.TxID, got.Status, wantTxID)
	}
	if got.CallbackTokenHint != "sse-session-1" {
		t.Errorf("CallbackTokenHint = %q, want the request's X-CallbackToken", got.CallbackTokenHint)
	}
	if got.Timestamp.IsZero() {
		t.Errorf("RECEIVED event has no timestamp")
	}
}

// TestPublishReceived_BulkForBatches: a POST /txs fans one bulk event with
// every published txid, the same way the propagator reports verdicts.
func TestPublishReceived_BulkForBatches(t *testing.T) {
	pub := &recordingCallbackPub{}
	srv := &Server{logger: zap.NewNop(), publisher: pub}
	srv.publishReceived(context.Background(), []string{"aa", "bb", "cc"}, submitOptions{CallbackToken: "tok"})

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 1 {
		t.Fatalf("expected one bulk publish and no single publishes, got %d/%d", len(pub.publishes), len(pub.bulkPublishes))
	}
	b := pub.bulkPublishes[0]
	if b.Status != models.StatusReceived || len(b.TxIDs) != 3 || b.TxID != "" || b.CallbackTokenHint != "tok" {
		t.Errorf("bulk RECEIVED event malformed: %+v", b)
	}

	// Nil publisher and empty lists are no-ops, never panics.
	(&Server{logger: zap.NewNop()}).publishReceived(context.Background(), []string{"aa"}, submitOptions{})
	srv.publishReceived(context.Background(), nil, submitOptions{})
}
