package api_server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// callbackSlotServer builds a router whose callback writes share the given
// number of pod-wide callback slots.
func callbackSlotServer(ms *mockStore, slots int) (*Server, *recordingCallbackPub, *gin.Engine) {
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:               &config.Config{CallbackToken: testCallbackToken},
		logger:            zap.NewNop(),
		producer:          kafka.NewProducer(&kafka.RecordingBroker{}),
		store:             ms,
		publisher:         pub,
		submissionCh:      make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop:    make(chan struct{}),
		callbackSlots:     semaphore.NewWeighted(int64(slots)),
		callbackSlotCount: slots,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, pub, router
}

func seenCallback(ctx context.Context, t *testing.T, router *gin.Engine, n int) *httptest.ResponseRecorder {
	t.Helper()
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("tx-%02d", i)
	}
	body := mustMarshalJSON(t, models.CallbackMessage{Type: models.CallbackSeenOnNetwork, TxIDs: txids})
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testCallbackToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// A callback writes its rows in chunks no larger than its share of the
// pod-wide slots, so it never has more writes in flight than it holds
// slots, and every row is still updated and published in order.
func TestSeenCallback_WritesInChunksOfItsSlots(t *testing.T) {
	prev := store.BatchConcurrency()
	store.SetBatchConcurrency(8)
	t.Cleanup(func() { store.SetBatchConcurrency(prev) })

	ms := &mockStore{}
	_, pub, router := callbackSlotServer(ms, 3)
	if w := seenCallback(t.Context(), t, router, 10); w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	sizes := make([]int, 0, len(ms.batchUpdateReturningCalls))
	var order []string
	for _, call := range ms.batchUpdateReturningCalls {
		sizes = append(sizes, len(call))
		for _, st := range call {
			order = append(order, st.TxID)
		}
	}
	if fmt.Sprint(sizes) != "[3 3 3 1]" {
		t.Errorf("store call sizes = %v, want [3 3 3 1]", sizes)
	}
	for i, id := range order {
		if want := fmt.Sprintf("tx-%02d", i); id != want {
			t.Fatalf("row %d written as %s, want %s (input order)", i, id, want)
		}
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 1 || len(pub.bulkPublishes[0].TxIDs) != 10 {
		t.Errorf("bulk publishes = %+v, want one covering all 10 txids", pub.bulkPublishes)
	}
}

// With every callback slot taken, a callback waits rather than opening more
// store connections, and fails retriably (5xx, so merkle-service redelivers)
// when its request ends first, having written nothing.
func TestSeenCallback_WaitsForCallbackSlots(t *testing.T) {
	ms := &mockStore{}
	srv, _, router := callbackSlotServer(ms, 2)
	if !srv.callbackSlots.TryAcquire(2) {
		t.Fatal("could not take the slots")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	w := seenCallback(ctx, t, router, 5)

	if w.Code < 500 {
		t.Fatalf("status = %d, want a retriable 5xx; body=%s", w.Code, w.Body.String())
	}
	if n := len(ms.batchUpdateReturningCalls); n != 0 {
		t.Errorf("store calls = %d, want 0 while no slot is free", n)
	}
}
