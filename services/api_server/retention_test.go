package api_server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
)

// retentionStore is a mockStore that enforces retention, recording what
// the handlers stamp and extend.
type retentionStore struct {
	*mockStore

	mu             sync.Mutex
	inserted       []*models.TransactionStatus
	extended       map[string][]*time.Time
	existing       bool       // GetOrInsertStatus reports an existing row (a resubmit)
	existingExpiry *time.Time // that row's expiry; nil = kept forever
	ensured        int        // EnsureRetentionIndexes calls
}

func (r *retentionStore) EnsureRetentionIndexes(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensured++
	return nil
}

func (r *retentionStore) GetOrInsertStatus(_ context.Context, st *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.existing {
		return &models.TransactionStatus{TxID: st.TxID, Status: models.StatusAcceptedByNetwork, ExpiresAt: r.existingExpiry}, false, nil
	}
	r.inserted = append(r.inserted, st)
	return st, true, nil
}

func (r *retentionStore) ExtendTransactionExpiry(_ context.Context, txid string, until *time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extended[txid] = append(r.extended[txid], until)
	return nil
}

func retentionConfig() *config.Config {
	return &config.Config{
		CallbackToken: testCallbackToken,
		RateLimit: config.RateLimitConfig{
			Anonymous: config.TierConfig{RetentionDays: 30},
			Tiers: map[string]config.TierConfig{
				config.TierFree: {RetentionDays: 90},
				"enterprise":    {RetentionDays: 730},
				"archive":       {}, // keeps forever
			},
		},
	}
}

// submitAs posts a tx to /tx as a caller on tier (anonymous when tier is
// config.TierAnonymous) and returns the txid.
func submitAs(t *testing.T, st any, cfg *config.Config, tier string) (string, *Server) {
	t.Helper()
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            cfg,
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	switch s := st.(type) {
	case *retentionStore:
		srv.store = s
	case *mockStore:
		srv.store = s
	}
	srv.initRetention(t.Context())
	router := gin.New()
	if tier != config.TierAnonymous {
		router.Use(func(c *gin.Context) {
			apikey.WithPrincipal(c, apikey.Principal{KeyID: "key_1", Tier: tier, ClientIP: "203.0.113.1"})
		})
	}
	srv.registerRoutes(router)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "cb-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code >= 300 && w.Code != http.StatusAccepted {
		t.Fatalf("submit: %d %s", w.Code, w.Body.String())
	}
	return parsed.TxID().String(), srv
}

func requireExpiry(t *testing.T, what string, got *time.Time, want time.Duration) {
	t.Helper()
	if want == 0 {
		if got != nil {
			t.Fatalf("%s: expires_at = %v, want none (kept forever)", what, *got)
		}
		return
	}
	if got == nil {
		t.Fatalf("%s: no expires_at, want about now+%v", what, want)
	}
	if d := time.Until(*got); d < want-time.Minute || d > want+time.Minute {
		t.Fatalf("%s: expires in %v, want about %v", what, d, want)
	}
}

// TestRetention_StampsTierExpiryOnInsert pins that the transaction row and
// the submission row a request writes carry its tier's retention.
func TestRetention_StampsTierExpiryOnInsert(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		tier string
		want time.Duration
	}{
		{config.TierAnonymous, 30 * day},
		{config.TierFree, 90 * day},
		{"enterprise", 730 * day},
		{"archive", 0},
		{"unconfigured", 90 * day}, // falls back to free, like the rate limiter
	} {
		t.Run(tc.tier, func(t *testing.T) {
			rs := &retentionStore{mockStore: &mockStore{}, extended: map[string][]*time.Time{}}
			_, srv := submitAs(t, rs, retentionConfig(), tc.tier)
			if len(rs.inserted) != 1 {
				t.Fatalf("inserted %d rows, want 1", len(rs.inserted))
			}
			if rs.ensured != 1 {
				t.Fatalf("EnsureRetentionIndexes called %d times at boot, want 1", rs.ensured)
			}
			requireExpiry(t, "tx row", rs.inserted[0].ExpiresAt, tc.want)
			select {
			case rec := <-srv.submissionCh:
				requireExpiry(t, "submission row", rec.sub.ExpiresAt, tc.want)
			default:
				t.Fatal("no submission recorded")
			}
		})
	}
}

// TestRetention_ResubmitExtendsExpiry pins that a resubmit asks the store
// to extend the existing row only when that buys meaningfully longer
// retention: a later tier (or keep-forever) extends; a same-tier resubmit
// (clients polling by resubmitting) and a row already kept forever do not
// cost a write.
func TestRetention_ResubmitExtendsExpiry(t *testing.T) {
	day := 24 * time.Hour
	in := func(d time.Duration) *time.Time { t := time.Now().Add(d); return &t }
	for _, tc := range []struct {
		name     string
		tier     string
		existing *time.Time
		extend   bool
		want     time.Duration
	}{
		{"longer tier extends", "enterprise", in(30 * day), true, 730 * day},
		{"keep-forever tier removes the expiry", "archive", in(30 * day), true, 0},
		{"same tier a few hours later is skipped", config.TierFree, in(90*day - 3*time.Hour), false, 0},
		{"shorter tier is skipped", config.TierFree, in(730 * day), false, 0},
		{"a row kept forever is skipped", "enterprise", nil, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := &retentionStore{
				mockStore: &mockStore{}, extended: map[string][]*time.Time{},
				existing: true, existingExpiry: tc.existing,
			}
			txid, _ := submitAs(t, rs, retentionConfig(), tc.tier)
			calls := rs.extended[txid]
			if !tc.extend {
				if len(calls) != 0 {
					t.Fatalf("ExtendTransactionExpiry called %d times, want none", len(calls))
				}
				return
			}
			if len(calls) != 1 {
				t.Fatalf("ExtendTransactionExpiry called %d times, want 1", len(calls))
			}
			requireExpiry(t, "extension", calls[0], tc.want)
		})
	}
}

// TestRetention_OffWithoutConfigOrSupport pins that nothing expires unless
// a tier configures retention and the store can enforce it.
func TestRetention_OffWithoutConfigOrSupport(t *testing.T) {
	t.Run("no tier configures retention", func(t *testing.T) {
		rs := &retentionStore{mockStore: &mockStore{}, extended: map[string][]*time.Time{}}
		cfg := retentionConfig()
		cfg.RateLimit.Anonymous.RetentionDays = 0
		cfg.RateLimit.Tiers = map[string]config.TierConfig{config.TierFree: {}}
		_, srv := submitAs(t, rs, cfg, config.TierAnonymous)
		if srv.retention != nil || rs.ensured != 0 {
			t.Fatal("retention enabled (or indexes built) with no tier configuring it")
		}
		requireExpiry(t, "tx row", rs.inserted[0].ExpiresAt, 0)
	})
	t.Run("store does not enforce retention", func(t *testing.T) {
		ms := &mockStore{getOrInsertFn: func(st *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if st.ExpiresAt != nil {
				t.Errorf("row stamped with expires_at on a store without retention")
			}
			return st, true, nil
		}}
		_, srv := submitAs(t, ms, retentionConfig(), "enterprise")
		if srv.retention != nil {
			t.Fatal("retention enabled on a store that cannot enforce it")
		}
	})
}
