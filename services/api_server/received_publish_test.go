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
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// TestHandleSubmitTransaction_PublishesReceivedWithTokenHint pins the fix
// for issue 6d of the hosted-service report: a token-scoped SSE subscriber
// saw ACCEPTED_BY_NETWORK as the first frame because intake never published
// RECEIVED. The event must be published before the Kafka message is queued and
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

// orderingPub records, for every RECEIVED event, how many propagation
// messages the broker had already accepted when it was published. Any value
// above zero means a verdict could have overtaken RECEIVED on a live stream.
type orderingPub struct {
	recordingCallbackPub
	broker      *kafka.RecordingBroker
	sendsBefore []int
}

func (p *orderingPub) sent() int {
	p.broker.Lock()
	defer p.broker.Unlock()
	n := p.broker.BatchCalls
	for _, s := range p.broker.Sends {
		if s.Topic == kafka.TopicPropagation {
			n++
		}
	}
	return n
}

func (p *orderingPub) Publish(ctx context.Context, st *models.TransactionStatus) error {
	p.sendsBefore = append(p.sendsBefore, p.sent())
	return p.recordingCallbackPub.Publish(ctx, st)
}

func (p *orderingPub) PublishBulk(ctx context.Context, st *models.TransactionStatus) error {
	p.sendsBefore = append(p.sendsBefore, p.sent())
	return p.recordingCallbackPub.PublishBulk(ctx, st)
}

// rejectedTxIDs makes GetOrInsertStatus report an existing REJECTED row for
// the listed txids and insert everything else.
func rejectedTxIDs(txids ...string) *mockStore {
	return &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			for _, id := range txids {
				if in.TxID == id {
					return &models.TransactionStatus{TxID: id, Status: models.StatusRejected}, false, nil
				}
			}
			return nil, true, nil
		},
	}
}

func newReceivedServer(ms *mockStore) (*orderingPub, *kafka.RecordingBroker, *gin.Engine) {
	broker := &kafka.RecordingBroker{}
	pub := &orderingPub{broker: broker}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		publisher:      pub,
		txTracker:      store.NewTxTracker(),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	return pub, broker, router
}

func postSubmit(t *testing.T, router *gin.Engine, path string, body []byte) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "sse-session-1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

// twoDistinctTxs returns two raw txs with different txids.
func twoDistinctTxs(t *testing.T) (rawA, rawB []byte, txidA, txidB string) {
	t.Helper()
	rawA = makeMinimalTx()
	txB := sdkTx.NewTransaction()
	txB.AddOutput(&sdkTx.TransactionOutput{Satoshis: 0, LockingScript: &script.Script{}})
	rawB = txB.Bytes()
	for _, r := range []struct {
		raw []byte
		out *string
	}{{rawA, &txidA}, {rawB, &txidB}} {
		tx, _, err := sdkTx.NewTransactionFromStream(r.raw)
		if err != nil {
			t.Fatalf("parsing test tx: %v", err)
		}
		*r.out = tx.TxID().String()
	}
	if txidA == txidB {
		t.Fatalf("expected distinct txids, got %s twice", txidA)
	}
	return rawA, rawB, txidA, txidB
}

// TestHandleSubmitTransaction_ReceivedPrecedesPropagation: RECEIVED must be
// published before the propagation message is queued. Published after it,
// the propagator's ACCEPTED_BY_NETWORK or REJECTED can reach a live stream
// first and be followed by a stale RECEIVED.
func TestHandleSubmitTransaction_ReceivedPrecedesPropagation(t *testing.T) {
	pub, broker, router := newReceivedServer(rejectedTxIDs())
	postSubmit(t, router, "/tx", makeMinimalTx())

	if len(pub.publishes) != 1 || pub.publishes[0].Status != models.StatusReceived {
		t.Fatalf("expected one RECEIVED publish, got %+v", pub.publishes)
	}
	if pub.sendsBefore[0] != 0 {
		t.Errorf("RECEIVED published after %d propagation message(s); must precede them", pub.sendsBefore[0])
	}
	if len(broker.Sends) != 1 || broker.Sends[0].Topic != kafka.TopicPropagation {
		t.Errorf("expected the propagation message to be sent, got %+v", broker.Sends)
	}
}

// TestHandleSubmitTransaction_RejectedResubmitPublishesNoReceived: a REJECTED
// resubmit is re-broadcast but its row stays REJECTED, so a repeat rejection
// is suppressed as a no-op transition. A RECEIVED frame would then never be
// corrected; the resubmit must not publish one.
func TestHandleSubmitTransaction_RejectedResubmitPublishesNoReceived(t *testing.T) {
	raw := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(raw)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	pub, broker, router := newReceivedServer(rejectedTxIDs(parsed.TxID().String()))
	postSubmit(t, router, "/tx", raw)

	if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 0 {
		t.Errorf("REJECTED resubmit published events: single=%+v bulk=%+v", pub.publishes, pub.bulkPublishes)
	}
	if len(broker.Sends) != 1 {
		t.Errorf("REJECTED resubmit must still re-queue propagation, got %d sends", len(broker.Sends))
	}
}

// TestHandleSubmitTransactions_PublishesBulkReceived: POST /txs fans one bulk
// RECEIVED event naming every submitted txid, carrying the request's callback
// token as a hint, before the propagation batch is queued.
func TestHandleSubmitTransactions_PublishesBulkReceived(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	pub, broker, router := newReceivedServer(rejectedTxIDs())
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 1 {
		t.Fatalf("expected one bulk publish and no single publishes, got %d/%d", len(pub.publishes), len(pub.bulkPublishes))
	}
	b := pub.bulkPublishes[0]
	if b.Status != models.StatusReceived || b.TxID != "" || b.Timestamp.IsZero() {
		t.Errorf("bulk RECEIVED event malformed: %+v", b)
	}
	if len(b.TxIDs) != 2 || b.TxIDs[0] != txidA || b.TxIDs[1] != txidB {
		t.Errorf("bulk RECEIVED txids = %v, want [%s %s]", b.TxIDs, txidA, txidB)
	}
	if b.CallbackTokenHint != "sse-session-1" {
		t.Errorf("CallbackTokenHint = %q, want the request's X-CallbackToken", b.CallbackTokenHint)
	}
	if pub.sendsBefore[0] != 0 {
		t.Errorf("RECEIVED published after %d propagation batch(es); must precede them", pub.sendsBefore[0])
	}
	if broker.BatchCalls != 1 {
		t.Errorf("expected one propagation batch, got %d", broker.BatchCalls)
	}
}

// TestHandleSubmitTransactions_RejectedResubmitExcludedFromReceived: in a
// batch mixing a REJECTED resubmit with a new tx, both are re-queued but only
// the new tx gets RECEIVED.
func TestHandleSubmitTransactions_RejectedResubmitExcludedFromReceived(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	pub, broker, router := newReceivedServer(rejectedTxIDs(txidA))
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	if len(pub.bulkPublishes) != 0 || len(pub.publishes) != 1 {
		t.Fatalf("expected one single RECEIVED publish, got single=%d bulk=%d", len(pub.publishes), len(pub.bulkPublishes))
	}
	if got := pub.publishes[0]; got.TxID != txidB || got.Status != models.StatusReceived {
		t.Errorf("published %s/%s, want %s/RECEIVED", got.TxID, got.Status, txidB)
	}
	if pub.sendsBefore[0] != 0 {
		t.Errorf("RECEIVED published after %d propagation batch(es); must precede them", pub.sendsBefore[0])
	}
	if len(broker.Batches) != 1 || len(broker.Batches[0]) != 2 {
		t.Errorf("expected both txs re-queued in one batch, got %+v", broker.Batches)
	}
}

// TestHandleSubmitTransactions_AllRejectedResubmitsPublishNoReceived: a
// batch of only REJECTED resubmits publishes no RECEIVED at all.
func TestHandleSubmitTransactions_AllRejectedResubmitsPublishNoReceived(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	pub, broker, router := newReceivedServer(rejectedTxIDs(txidA, txidB))
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 0 {
		t.Errorf("REJECTED resubmits published events: single=%+v bulk=%+v", pub.publishes, pub.bulkPublishes)
	}
	if broker.BatchCalls != 1 {
		t.Errorf("REJECTED resubmits must still re-queue propagation, got %d batches", broker.BatchCalls)
	}
}

// TestPublishReceived_NoopWithoutPublisherOrTxIDs: nil publisher and empty
// lists are no-ops, never panics.
func TestPublishReceived_NoopWithoutPublisherOrTxIDs(t *testing.T) {
	(&Server{logger: zap.NewNop()}).publishReceived(context.Background(), []string{"aa"}, submitOptions{})
	pub := &recordingCallbackPub{}
	(&Server{logger: zap.NewNop(), publisher: pub}).publishReceived(context.Background(), nil, submitOptions{})
	if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 0 {
		t.Errorf("empty txid list published events")
	}
}
