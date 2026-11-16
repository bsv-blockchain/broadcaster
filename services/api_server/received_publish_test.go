package api_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
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
	return newReceivedServerWith(ms, nil)
}

// newReceivedServerWith is newReceivedServer with a hook to adjust the
// server before its routes are registered.
func newReceivedServerWith(ms *mockStore, adjust func(*Server)) (*orderingPub, *kafka.RecordingBroker, *gin.Engine) {
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
	if adjust != nil {
		adjust(srv)
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
	(&Server{logger: zap.NewNop()}).publishReceived(context.Background(), []string{"aa"}, time.Now(), submitOptions{})
	pub := &recordingCallbackPub{}
	(&Server{logger: zap.NewNop(), publisher: pub}).publishReceived(context.Background(), nil, time.Now(), submitOptions{})
	if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 0 {
		t.Errorf("empty txid list published events")
	}
}

// TestPublishReceived_ChunksLargeBatches: a bulk RECEIVED is split so no
// event outgrows the Kafka producer's message limit, every txid goes out
// exactly once, and each event carries the rows' insert time and the hint.
func TestPublishReceived_ChunksLargeBatches(t *testing.T) {
	txids := make([]string, 2*maxTxIDsPerReceivedEvent+1)
	for i := range txids {
		txids[i] = fmt.Sprintf("%064x", i)
	}
	at := time.Unix(1_700_000_000, 123)
	pub := &recordingCallbackPub{}
	(&Server{logger: zap.NewNop(), publisher: pub}).publishReceived(context.Background(), txids, at, submitOptions{CallbackToken: "tok"})

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 3 {
		t.Fatalf("bulk events = %d, want 3", len(pub.bulkPublishes))
	}
	seen := 0
	for _, ev := range pub.bulkPublishes {
		if len(ev.TxIDs) > maxTxIDsPerReceivedEvent {
			t.Errorf("event carries %d txids, cap %d", len(ev.TxIDs), maxTxIDsPerReceivedEvent)
		}
		if !ev.Timestamp.Equal(at) || ev.CallbackTokenHint != "tok" {
			t.Errorf("event timestamp %v hint %q, want %v and tok", ev.Timestamp, ev.CallbackTokenHint, at)
		}
		for _, id := range ev.TxIDs {
			if id != txids[seen] {
				t.Fatalf("txid %d = %s, want %s", seen, id, txids[seen])
			}
			seen++
		}
	}
	if seen != len(txids) {
		t.Fatalf("published %d txids, want %d", seen, len(txids))
	}
}

// staleReceivedSnapshot reproduces the concurrent-duplicate window: this
// request's CAS observes the RECEIVED row an earlier attempt inserted, and by
// the time it resumes that attempt's REJECTED verdict is already on the
// stream (publishing it through pub records it ahead of anything this request
// publishes). Txids not listed are inserted.
func staleReceivedSnapshot(pub *orderingPub, txids ...string) *mockStore {
	return &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			for _, id := range txids {
				if in.TxID == id {
					_ = pub.recordingCallbackPub.Publish(context.Background(), &models.TransactionStatus{TxID: id, Status: models.StatusRejected})
					return &models.TransactionStatus{TxID: id, Status: models.StatusReceived}, false, nil
				}
			}
			return nil, true, nil
		},
	}
}

// assertNoReceivedAfterRejected fails when any RECEIVED event (single or
// bulk) names a txid whose REJECTED was already published.
func assertNoReceivedAfterRejected(t *testing.T, pub *orderingPub) {
	t.Helper()
	rejected := map[string]bool{}
	for _, st := range pub.publishes {
		switch {
		case st.Status == models.StatusRejected:
			rejected[st.TxID] = true
		case st.Status == models.StatusReceived && rejected[st.TxID]:
			t.Errorf("RECEIVED for %s published after its REJECTED", st.TxID)
		}
	}
	for _, b := range pub.bulkPublishes {
		for _, id := range b.TxIDs {
			if b.Status == models.StatusReceived && rejected[id] {
				t.Errorf("bulk RECEIVED names %s after its REJECTED", id)
			}
		}
	}
}

// TestHandleSubmitTransaction_ConcurrentDuplicatePublishesNoReceived: a
// duplicate whose CAS saw the earlier attempt's RECEIVED row re-queues the tx
// but must not publish RECEIVED — the earlier attempt's verdict may already be
// on the stream, and a repeat rejection would never correct the stale frame.
func TestHandleSubmitTransaction_ConcurrentDuplicatePublishesNoReceived(t *testing.T) {
	raw := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(raw)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	ms := &mockStore{}
	pub, broker, router := newReceivedServer(ms)
	*ms = *staleReceivedSnapshot(pub, parsed.TxID().String())
	postSubmit(t, router, "/tx", raw)

	assertNoReceivedAfterRejected(t, pub)
	if len(pub.publishes) != 1 || len(pub.bulkPublishes) != 0 {
		t.Errorf("expected only the earlier attempt's REJECTED, got single=%+v bulk=%+v", pub.publishes, pub.bulkPublishes)
	}
	if len(broker.Sends) != 1 {
		t.Errorf("stranded RECEIVED retry must still re-queue propagation, got %d sends", len(broker.Sends))
	}
}

// TestHandleSubmitTransactions_ConcurrentDuplicateExcludedFromReceived: the
// same window in a batch — the duplicate is re-queued with the new tx, and
// only the tx this request inserted gets RECEIVED.
func TestHandleSubmitTransactions_ConcurrentDuplicateExcludedFromReceived(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	ms := &mockStore{}
	pub, broker, router := newReceivedServer(ms)
	*ms = *staleReceivedSnapshot(pub, txidA)
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	assertNoReceivedAfterRejected(t, pub)
	var received []string
	for _, st := range pub.publishes {
		if st.Status == models.StatusReceived {
			received = append(received, st.TxID)
		}
	}
	if len(pub.bulkPublishes) != 0 || len(received) != 1 || received[0] != txidB {
		t.Errorf("expected RECEIVED for %s only, got single=%v bulk=%+v", txidB, received, pub.bulkPublishes)
	}
	if len(broker.Batches) != 1 || len(broker.Batches[0]) != 2 {
		t.Errorf("expected both txs re-queued in one batch, got %+v", broker.Batches)
	}
}

// TestHandleSubmitTransactions_AllConcurrentDuplicatesPublishNoReceived: a
// batch made only of duplicates that lost the CAS publishes no RECEIVED.
func TestHandleSubmitTransactions_AllConcurrentDuplicatesPublishNoReceived(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	ms := &mockStore{}
	pub, broker, router := newReceivedServer(ms)
	*ms = *staleReceivedSnapshot(pub, txidA, txidB)
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	assertNoReceivedAfterRejected(t, pub)
	if len(pub.bulkPublishes) != 0 || len(pub.publishes) != 2 {
		t.Errorf("expected only the two earlier REJECTEDs, got single=%+v bulk=%+v", pub.publishes, pub.bulkPublishes)
	}
	if broker.BatchCalls != 1 {
		t.Errorf("duplicates must still re-queue propagation, got %d batches", broker.BatchCalls)
	}
}

// TestHandleSubmit_DedupErrorSheds503: when the CAS errors the row may not
// exist, and the propagation message carries the txid only (#329), so the
// consumer would have no raw tx to load. Both endpoints answer 503 with
// Retry-After and publish nothing: no Kafka message, no status event.
func TestHandleSubmit_DedupErrorSheds503(t *testing.T) {
	failing := func() *mockStore {
		return &mockStore{
			getOrInsertFn: func(*models.TransactionStatus) (*models.TransactionStatus, bool, error) {
				return nil, false, errors.New("store unavailable")
			},
		}
	}
	rawA, rawB, _, _ := twoDistinctTxs(t)
	for _, tc := range []struct {
		path string
		body []byte
	}{
		{"/tx", rawA},
		{"/txs", append(append([]byte(nil), rawA...), rawB...)},
	} {
		t.Run(tc.path, func(t *testing.T) {
			pub, broker, router := newReceivedServer(failing())
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tc.path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
			}
			if w.Header().Get("Retry-After") == "" {
				t.Error("503 without Retry-After")
			}
			if len(pub.publishes) != 0 || len(pub.bulkPublishes) != 0 {
				t.Errorf("CAS error published events: single=%+v bulk=%+v", pub.publishes, pub.bulkPublishes)
			}
			if n := len(broker.Sends) + broker.BatchCalls; n != 0 {
				t.Errorf("CAS error queued propagation: sends=%d batches=%d", len(broker.Sends), broker.BatchCalls)
			}
		})
	}
}

// TestHandleSubmitTransactions_ReceivedCarriesInsertedRowTimestamp: the
// RECEIVED carries the inserted row's timestamp, so a Last-Event-ID
// reconnect replays the batch's rows instead of skipping them, and a CAS
// loser's row does not contribute one.
func TestHandleSubmitTransactions_ReceivedCarriesInsertedRowTimestamp(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	var mu sync.Mutex
	rows := map[string]time.Time{}
	ms := &mockStore{getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		mu.Lock()
		rows[in.TxID] = in.Timestamp
		mu.Unlock()
		if in.TxID == txidA {
			return &models.TransactionStatus{TxID: txidA, Status: models.StatusRejected}, false, nil
		}
		return nil, true, nil
	}}
	pub, _, router := newReceivedServer(ms)
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	if rows[txidB].IsZero() {
		t.Fatalf("row %s written without a timestamp", txidB)
	}
	if len(pub.publishes) != 1 || pub.publishes[0].TxID != txidB {
		t.Fatalf("expected one RECEIVED for the inserted %s, got %+v", txidB, pub.publishes)
	}
	if got := pub.publishes[0].Timestamp; !got.Equal(rows[txidB]) {
		t.Errorf("RECEIVED timestamp %v, want the inserted row's %v", got, rows[txidB])
	}
}

// TestHandleSubmitTransactions_BulkReceivedSitsBelowEveryRow: a bulk RECEIVED's
// id must be below every batch row even at a millisecond-precision store, so a
// Last-Event-ID reconnect mid-burst replays the rows rather than skipping them.
func TestHandleSubmitTransactions_BulkReceivedSitsBelowEveryRow(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	var mu sync.Mutex
	rows := map[string]time.Time{}
	ms := &mockStore{getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		rows[in.TxID] = in.Timestamp
		return nil, true, nil
	}}
	pub, _, router := newReceivedServer(ms)
	postSubmit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...))

	if len(pub.bulkPublishes) != 1 {
		t.Fatalf("expected one bulk RECEIVED, got %d", len(pub.bulkPublishes))
	}
	at := pub.bulkPublishes[0].Timestamp
	for _, id := range []string{txidA, txidB} {
		if !at.Before(rows[id].Truncate(time.Millisecond)) {
			t.Errorf("event at %v is not below row %s stored at ms precision (%v)", at, id, rows[id].Truncate(time.Millisecond))
		}
	}
	if want := rows[txidA].Truncate(time.Millisecond).Add(-time.Nanosecond); !at.Equal(want) {
		t.Errorf("event at %v, want just below the earliest row: %v", at, want)
	}
}

func TestBulkReceivedAt(t *testing.T) {
	ts := time.Date(2026, 11, 3, 12, 0, 0, 123_456_789, time.UTC)
	if got := bulkReceivedAt(ts, 1); !got.Equal(ts) {
		t.Errorf("single: %v, want %v", got, ts)
	}
	if got, want := bulkReceivedAt(ts, 2), time.Date(2026, 11, 3, 12, 0, 0, 122_999_999, time.UTC); !got.Equal(want) {
		t.Errorf("bulk: %v, want %v", got, want)
	}
}

// TestHandleSubmitTransactions_PartialCASFailure_PublishesReceivedForInserted:
// when a /txs CAS fails after earlier rows were inserted, the batch sheds
// with 503 and nothing reaches Kafka, but the inserted rows still get their
// RECEIVED. The retry republishes them as CAS losers, which never publish
// RECEIVED (#391), so this is the only chance.
func TestHandleSubmitTransactions_PartialCASFailure_PublishesReceivedForInserted(t *testing.T) {
	rawA, rawB, txidA, _ := twoDistinctTxs(t)
	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if in.TxID == txidA {
				return in, true, nil
			}
			return nil, false, errors.New("store unavailable")
		},
	}
	pub, broker, router := newReceivedServer(ms)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(append(append([]byte(nil), rawA...), rawB...)))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
	if n := len(broker.Sends) + broker.BatchCalls; n != 0 {
		t.Errorf("shed batch queued propagation: sends=%d batches=%d", len(broker.Sends), broker.BatchCalls)
	}
	events := append(append([]*models.TransactionStatus(nil), pub.publishes...), pub.bulkPublishes...)
	var got []string
	for _, e := range events {
		got = append(got, e.TxID+":"+string(e.Status))
		for _, id := range e.TxIDs {
			got = append(got, id+":"+string(e.Status))
		}
	}
	want := txidA + ":" + string(models.StatusReceived)
	if !slices.Contains(got, want) || len(events) != 1 {
		t.Errorf("events = %v, want exactly one RECEIVED for the inserted %s", got, txidA)
	}
}

// Rows after a failed CAS are inserted too now that the batch CASes
// concurrently, so a shed must publish RECEIVED for every row this request
// inserted, not only the rows ahead of the failure.
func TestHandleSubmitTransactions_EarlyCASFailure_PublishesReceivedForLaterInserted(t *testing.T) {
	prev := store.BatchConcurrency()
	store.SetBatchConcurrency(2)
	t.Cleanup(func() { store.SetBatchConcurrency(prev) })

	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	// A fails only once B's CAS is in flight: the batch stops starting rows
	// after a failure, so this pins that B started and was inserted.
	bStarted := make(chan struct{})
	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if in.TxID == txidA {
				<-bStarted
				return nil, false, errors.New("store unavailable")
			}
			close(bStarted)
			return in, true, nil
		},
	}
	pub, broker, router := newReceivedServer(ms)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(append(append([]byte(nil), rawA...), rawB...)))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
	if n := len(broker.Sends) + broker.BatchCalls; n != 0 {
		t.Errorf("shed batch queued propagation: sends=%d batches=%d", len(broker.Sends), broker.BatchCalls)
	}
	events := append(append([]*models.TransactionStatus(nil), pub.publishes...), pub.bulkPublishes...)
	var got []string
	for _, e := range events {
		got = append(got, e.TxID+":"+string(e.Status))
		for _, id := range e.TxIDs {
			got = append(got, id+":"+string(e.Status))
		}
	}
	want := txidB + ":" + string(models.StatusReceived)
	if !slices.Contains(got, want) || len(events) != 1 {
		t.Errorf("events = %v, want exactly one RECEIVED for the inserted %s", got, txidB)
	}
}

// A /txs batch dedups in one store call, not one per row: on MongoDB that is
// one InsertMany for the whole batch instead of a majority-acknowledged
// round trip per BatchConcurrency rows.
func TestHandleSubmitTransactions_OneBatchCASPerRequest(t *testing.T) {
	rawA, rawB, _, _ := twoDistinctTxs(t)
	ms := &mockStore{}
	_, _, router := newReceivedServer(ms)
	if w := submit(t, router, "/txs", append(append([]byte(nil), rawA...), rawB...)); w.Code >= 300 {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	if n := ms.batchCASCalls.Load(); n != 1 {
		t.Errorf("BatchGetOrInsertStatus calls = %d, want 1 for the whole batch", n)
	}
}

// The batch CAS holds a pod-wide intake slot: with every slot taken, the
// request waits instead of opening another store connection, and gives up
// with 503 when the client does.
func TestHandleSubmitTransactions_BatchCASWaitsForIntakeSlot(t *testing.T) {
	rawA, rawB, _, _ := twoDistinctTxs(t)
	ms := &mockStore{}
	slots := semaphore.NewWeighted(1)
	if !slots.TryAcquire(1) {
		t.Fatal("could not take the only slot")
	}
	_, _, router := newReceivedServerWith(ms, func(s *Server) { s.intakeSlots = slots })
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/txs", bytes.NewReader(append(append([]byte(nil), rawA...), rawB...)))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
	if n := ms.batchCASCalls.Load(); n != 0 {
		t.Errorf("BatchGetOrInsertStatus calls = %d, want 0 while no slot is free", n)
	}
}

// A txid repeated in one body is CASed and published once. CASed twice
// concurrently, either copy could win, and the loser would take the
// republish path: a raw_tx rewrite of the row this request just wrote and a
// second propagation message.
func TestHandleSubmitTransactions_RepeatedTxidCASedOnce(t *testing.T) {
	rawA, _, txidA, _ := twoDistinctTxs(t)
	var calls atomic.Int32
	ms := &mockStore{getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		if calls.Add(1) > 1 {
			return &models.TransactionStatus{TxID: in.TxID, Status: models.StatusReceived}, false, nil
		}
		return in, true, nil
	}}
	_, broker, router := newReceivedServer(ms)
	w := submit(t, router, "/txs", append(append([]byte(nil), rawA...), rawA...))
	if w.Code >= 300 {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}

	if n := calls.Load(); n != 1 {
		t.Errorf("CAS calls = %d, want 1", n)
	}
	if len(ms.setRawTxCalls) != 0 {
		t.Errorf("SetRawTx called for %v; nothing was republished", slices.Collect(maps.Keys(ms.setRawTxCalls)))
	}
	envs := publishedEnvelopes(t, broker)
	if len(envs) != 1 || envs[0]["txid"] != txidA {
		t.Errorf("published %v, want one message for %s", envs, txidA)
	}
	var resp struct {
		Submitted  int `json:"submitted"`
		Duplicates int `json:"duplicates"`
		Total      int `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Submitted != 1 || resp.Duplicates != 1 || resp.Total != 2 {
		t.Errorf("response = %+v, want submitted 1, duplicates 1, total 2", resp)
	}
}

// ctxCheckingPub fails a publish whose context is already done, as the Kafka
// producer does.
type ctxCheckingPub struct{ *orderingPub }

func (p ctxCheckingPub) Publish(ctx context.Context, st *models.TransactionStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.orderingPub.Publish(ctx, st)
}

func (p ctxCheckingPub) PublishBulk(ctx context.Context, st *models.TransactionStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.orderingPub.PublishBulk(ctx, st)
}

// A client that gives up mid-batch cancels the request, and the batch sheds.
// The rows it inserted still get their RECEIVED: only the inserter publishes
// it (#391), so dropping it here would lose it for good.
func TestHandleSubmitTransactions_ShedAfterClientCancel_PublishesReceived(t *testing.T) {
	prev := store.BatchConcurrency()
	store.SetBatchConcurrency(1)
	t.Cleanup(func() { store.SetBatchConcurrency(prev) })

	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	ctx, cancel := context.WithCancel(t.Context())
	ms := &mockStore{getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		if in.TxID == txidB {
			cancel() // the client times out while B's write is in flight
			return nil, false, context.Canceled
		}
		return in, true, nil
	}}
	var pub ctxCheckingPub
	_, _, router := newReceivedServerWith(ms, func(s *Server) {
		pub = ctxCheckingPub{s.publisher.(*orderingPub)}
		s.publisher = pub
	})
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/txs", bytes.NewReader(append(append([]byte(nil), rawA...), rawB...)))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
	if len(pub.publishes) != 1 || pub.publishes[0].TxID != txidA || pub.publishes[0].Status != models.StatusReceived {
		t.Errorf("publishes = %+v, want one RECEIVED for the inserted %s", pub.publishes, txidA)
	}
}
