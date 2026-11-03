package sse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
)

// e2eHarness wires the SSE handler against the real KafkaPublisher
// backed by the in-memory Kafka broker. Unlike fakePublisher this
// exercises the same publish→Kafka→subscribe→fanOut path that runs in
// production — catches regressions in Subscribe-group plumbing or
// message ordering that a fake would miss.
type e2eHarness struct {
	broker    kafka.Broker
	publisher *events.KafkaPublisher
	store     *sseStoreStub
	httpSrv   *httptest.Server
}

func (h *e2eHarness) Close() {
	h.httpSrv.Close()
	_ = h.broker.Close()
}

func newE2EHarness(t *testing.T) *e2eHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)

	broker := kafka.NewMemoryBroker(64)
	publisher := events.NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop(), 0)
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}

	svc := &Service{
		cfg:       &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger:    zap.NewNop(),
		store:     st,
		publisher: publisher,
	}

	mgr, err := newManager(t.Context(), publisher, st, zap.NewNop())
	if err != nil {
		_ = broker.Close()
		t.Fatalf("newManager: %v", err)
	}
	svc.manager = mgr

	router := gin.New()
	router.GET("/events", svc.handleEvents)
	httpSrv := httptest.NewServer(router)

	return &e2eHarness{
		broker:    broker,
		publisher: publisher,
		store:     st,
		httpSrv:   httpSrv,
	}
}

// publishAndPersist sets up the test state expected by this SSE e2e
// harness: it writes the status row to the stub store first, then
// publishes the status onto the events stream.
func (h *e2eHarness) publishAndPersist(t *testing.T, status *models.TransactionStatus) {
	t.Helper()
	h.store.setStatus(status.TxID, status)
	if err := h.publisher.Publish(t.Context(), status); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// waitForSubscriberReady gives the KafkaPublisher's consumer goroutine
// a beat to wire up its claim before the test publishes.
func waitForSubscriberReady() { time.Sleep(100 * time.Millisecond) }

func TestSSE_E2E_LiveDelivery(t *testing.T) {
	h := newE2EHarness(t)
	defer h.Close()
	waitForSubscriberReady()

	const txid = "tx-live-1"
	const token = "tok-live"
	h.store.subsByToken[token] = []*models.Submission{{TxID: txid, CallbackToken: token}}

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	time.Sleep(100 * time.Millisecond)

	// Intake's RECEIVED carries the submitting request's token as its hint.
	h.publishAndPersist(t, &models.TransactionStatus{
		TxID:              txid,
		Status:            models.StatusReceived,
		Timestamp:         time.Now().UTC(),
		CallbackTokenHint: token,
	})

	frame, err := readNextSSEFrame(resp.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"`+txid+`"`) ||
		!strings.Contains(frame, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("unexpected frame: %q", frame)
	}
}

func TestSSE_E2E_RaceCatchup(t *testing.T) {
	h := newE2EHarness(t)
	defer h.Close()
	waitForSubscriberReady()

	const txid = "tx-race-1"
	const token = "tok-race"
	h.store.subsByToken[token] = []*models.Submission{{TxID: txid, CallbackToken: token}}

	h.publishAndPersist(t, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusReceived,
		Timestamp: time.Now().UTC(),
	})
	time.Sleep(100 * time.Millisecond)

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("read frame (race catchup): %v", err)
	}
	if !strings.Contains(frame, `"txid":"`+txid+`"`) ||
		!strings.Contains(frame, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("expected initial-state replay of RECEIVED, got %q", frame)
	}
}

func TestSSE_E2E_ReconnectWithLastEventID(t *testing.T) {
	h := newE2EHarness(t)
	defer h.Close()
	waitForSubscriberReady()

	const txid = "tx-reconnect-1"
	const token = "tok-reconnect"
	h.store.subsByToken[token] = []*models.Submission{{TxID: txid, CallbackToken: token}}

	receivedAt := time.Now().UTC().Truncate(time.Microsecond)
	h.publishAndPersist(t, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusReceived,
		Timestamp: receivedAt,
	})

	req1, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("first GET /events: %v", err)
	}
	frame1, err := readNextSSEFrame(resp1.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	_ = resp1.Body.Close()
	if !strings.Contains(frame1, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("first frame should be RECEIVED, got %q", frame1)
	}

	seenAt := receivedAt.Add(time.Second)
	h.store.statusByTx[txid] = &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: seenAt,
	}

	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	req2.Header.Set("Last-Event-ID", fmt.Sprintf("%d", receivedAt.UnixNano()))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second GET /events: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	frame2, err := readNextSSEFrame(resp2.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if !strings.Contains(frame2, `"txStatus":"`+string(models.StatusSeenOnNetwork)+`"`) {
		t.Fatalf("second frame should be SEEN_ON_NETWORK, got %q", frame2)
	}
	if strings.Contains(frame2, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("RECEIVED must not replay on reconnect: %q", frame2)
	}
}
