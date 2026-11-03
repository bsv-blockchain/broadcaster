package webhook

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
)

// TestDeliverStripsCallbackTokenHint: the event-only hint is an SSE
// subscriber's token and must never be posted to a webhook receiver.
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
		TxID: txA, Status: models.StatusReceived, Timestamp: time.Now(), CallbackTokenHint: "sse-secret-token",
	})

	if body == "" {
		t.Fatal("no webhook delivered")
	}
	if strings.Contains(body, "sse-secret-token") || strings.Contains(body, "callbackTokenHint") {
		t.Errorf("webhook body leaks the SSE token hint: %s", body)
	}
	if !strings.Contains(body, `"txStatus":"RECEIVED"`) {
		t.Errorf("webhook body lost the status: %s", body)
	}
}

// A RECEIVED event can trail the transaction's verdict on the events topic
// (the inserting request paused between its CAS and its publish while a
// concurrent duplicate's attempt was rejected). Posted then, it would leave a
// FullStatusUpdates receiver at an in-flight state after its verdict, and a
// repeated rejection is a non-transition that publishes nothing to correct it.
func TestHandleUpdate_StaleReceivedIsWithheld(t *testing.T) {
	cases := []struct {
		name      string
		row       *models.TransactionStatus
		statusErr error
		wantPost  bool
	}{
		{name: "row still RECEIVED", row: &models.TransactionStatus{TxID: txA, Status: models.StatusReceived}, wantPost: true},
		{name: "no row", wantPost: true},
		{name: "row REJECTED", row: &models.TransactionStatus{TxID: txA, Status: models.StatusRejected}},
		{name: "row ACCEPTED_BY_NETWORK", row: &models.TransactionStatus{TxID: txA, Status: models.StatusAcceptedByNetwork}},
		{name: "lookup error", statusErr: errors.New("store down")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				posts.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			st := &fakeStore{
				subs: map[string][]*models.Submission{
					txA: {{SubmissionID: "sub-1", TxID: txA, CallbackURL: srv.URL, FullStatusUpdates: true}},
				},
				rows:      map[string]*models.TransactionStatus{},
				statusErr: tc.statusErr,
			}
			if tc.row != nil {
				st.rows[txA] = tc.row
			}
			svc := New(
				config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1},
				config.CallbackConfig{AllowPrivateIPs: true},
				zap.NewNop(), recordingPub{}, st, nil,
			)
			svc.handleUpdate(t.Context(), &models.TransactionStatus{TxID: txA, Status: models.StatusReceived, Timestamp: time.Now()})

			if got := posts.Load() == 1; got != tc.wantPost {
				t.Fatalf("RECEIVED posted = %v (%d posts), want %v", got, posts.Load(), tc.wantPost)
			}
		})
	}
}
