package webhook

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
)

// TestDeliverStripsCallbackTokenHint: the event-only hint (carried by intake's
// REJECTED) is an SSE subscriber's token and must never be posted to a
// webhook receiver.
func TestDeliverStripsCallbackTokenHint(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body = string(raw)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{subs: map[string][]*models.Submission{
		txA: {{SubmissionID: "sub-1", TxID: txA, CallbackURL: srv.URL, CallbackToken: "tok-A", FullStatusUpdates: true}},
	}}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil,
	)
	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID: txA, Status: models.StatusRejected, Timestamp: time.Now(), CallbackTokenHint: "sse-secret-token",
	})

	if body == "" {
		t.Fatal("no webhook delivered")
	}
	if strings.Contains(body, "sse-secret-token") || strings.Contains(body, "callbackTokenHint") {
		t.Errorf("webhook body leaks the SSE token hint: %s", body)
	}
	if !strings.Contains(body, `"txStatus":"REJECTED"`) {
		t.Errorf("webhook body lost the status: %s", body)
	}
}

// TestHandleUpdate_ReceivedIsNeverPosted: RECEIVED is intake's SSE-facing
// event. Even a FullStatusUpdates receiver must not get it — posting it would
// race ACCEPTED_BY_NETWORK for the first delivery claim — and the event must
// not cost a submission lookup. The ACCEPTED_BY_NETWORK after it still posts.
func TestHandleUpdate_ReceivedIsNeverPosted(t *testing.T) {
	var statuses []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		statuses = append(statuses, string(raw))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{subs: map[string][]*models.Submission{
		txA: {{SubmissionID: "sub-1", TxID: txA, CallbackURL: srv.URL, FullStatusUpdates: true}},
	}}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st, nil,
	)
	svc.handleUpdate(t.Context(), &models.TransactionStatus{TxID: txA, Status: models.StatusReceived, Timestamp: time.Now()})
	svc.handleUpdate(t.Context(), &models.TransactionStatus{TxIDs: []string{txA}, Status: models.StatusReceived, Timestamp: time.Now()})
	if len(statuses) != 0 {
		t.Fatalf("RECEIVED posted: %v", statuses)
	}
	svc.handleUpdate(t.Context(), &models.TransactionStatus{TxID: txA, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()})
	if len(statuses) != 1 || !strings.Contains(statuses[0], `"txStatus":"ACCEPTED_BY_NETWORK"`) {
		t.Fatalf("posts = %v, want one ACCEPTED_BY_NETWORK", statuses)
	}
}
