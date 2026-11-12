package api_server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/validator"
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
	extendErr      error      // ExtendTransactionExpiry fails with this
	ensured        int        // EnsureRetentionIndexes calls
}

func newRetentionStore() *retentionStore {
	return &retentionStore{mockStore: &mockStore{}, extended: map[string][]*time.Time{}}
}

func (r *retentionStore) EnsureRetentionIndexes(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensured++
	return nil
}

func (r *retentionStore) ensuredCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensured
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
	if r.extendErr != nil {
		return r.extendErr
	}
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
				"starter":       {RetentionDays: 7},
				"enterprise":    {RetentionDays: 730},
				"archive":       {}, // keeps forever
			},
		},
	}
}

// retentionServer builds a server over st whose callers are on tier
// (anonymous when tier is config.TierAnonymous). validate adds the intake
// validator, which rejects makeMinimalTx's empty transaction.
func retentionServer(t *testing.T, st any, cfg *config.Config, tier string, validate bool) (*Server, *gin.Engine) {
	t.Helper()
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
	if validate {
		srv.validator = validator.NewValidator(nil)
	}
	srv.initRetention(t.Context())
	router := gin.New()
	if tier != config.TierAnonymous {
		router.Use(func(c *gin.Context) {
			apikey.WithPrincipal(c, apikey.Principal{KeyID: "key_1", Tier: tier, ClientIP: "203.0.113.1"})
		})
	}
	srv.registerRoutes(router)
	return srv, router
}

// post submits makeMinimalTx to path with a callback token and returns the
// response and the txid.
func post(t *testing.T, router *gin.Engine, path string) (*httptest.ResponseRecorder, string) {
	t.Helper()
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "cb-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w, parsed.TxID().String()
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

func requireSubmissionExpiry(t *testing.T, srv *Server, want time.Duration) {
	t.Helper()
	select {
	case rec := <-srv.submissionCh:
		requireExpiry(t, "submission row", rec.sub.ExpiresAt, want)
	default:
		t.Fatal("no submission recorded")
	}
}

// TestRetention_StampsTierExpiryOnInsert pins that the transaction row and
// the submission row a request writes carry its tier's retention, on every
// path that creates them: POST /tx, POST /txs and an intake rejection.
func TestRetention_StampsTierExpiryOnInsert(t *testing.T) {
	day := 24 * time.Hour
	tiers := []struct {
		tier string
		want time.Duration
	}{
		{config.TierAnonymous, 30 * day},
		{config.TierFree, 90 * day},
		{"enterprise", 730 * day},
		{"archive", 0},
		{"unconfigured", 90 * day}, // falls back to free, like the rate limiter
	}
	paths := []struct {
		name     string
		path     string
		validate bool
		code     int
	}{
		{"POST /tx", "/tx", false, http.StatusOK},
		{"POST /txs", "/txs", false, http.StatusOK},
		{"intake rejection", "/tx", true, http.StatusBadRequest},
	}
	for _, p := range paths {
		for _, tc := range tiers {
			t.Run(p.name+"/"+tc.tier, func(t *testing.T) {
				rs := newRetentionStore()
				srv, router := retentionServer(t, rs, retentionConfig(), tc.tier, p.validate)
				w, _ := post(t, router, p.path)
				if p.code == http.StatusBadRequest {
					require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				} else {
					require.Less(t, w.Code, 300, w.Body.String())
				}
				require.Len(t, rs.inserted, 1)
				requireExpiry(t, "tx row", rs.inserted[0].ExpiresAt, tc.want)
				requireSubmissionExpiry(t, srv, tc.want)
				require.Eventually(t, func() bool { return rs.ensuredCalls() == 1 }, time.Second, time.Millisecond,
					"retention indexes are provisioned once at boot")
			})
		}
	}
}

// TestRetention_ResubmitExtendsExpiry pins that a resubmit asks the store
// to extend the existing row only when that buys meaningfully longer
// retention: a later tier (or keep-forever) extends, and so does a
// same-tier resubmit once the slack (a thirtieth of the retention, at most
// a day) has passed; a resubmit inside the slack, a shorter tier and a row
// already kept forever do not cost a write.
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
		{"same 90-day tier within a day is skipped", config.TierFree, in(90*day - 3*time.Hour), false, 0},
		{"same 90-day tier after a day extends", config.TierFree, in(90*day - 25*time.Hour), true, 90 * day},
		{"same 7-day tier within its slack is skipped", "starter", in(7*day - 5*time.Hour), false, 0},
		{"same 7-day tier past its slack extends", "starter", in(7*day - 6*time.Hour), true, 7 * day},
		{"shorter tier is skipped", config.TierFree, in(730 * day), false, 0},
		{"a row kept forever is skipped", "enterprise", nil, false, 0},
	} {
		for _, path := range []string{"/tx", "/txs"} {
			t.Run(tc.name+path, func(t *testing.T) {
				rs := newRetentionStore()
				rs.existing, rs.existingExpiry = true, tc.existing
				_, router := retentionServer(t, rs, retentionConfig(), tc.tier, false)
				w, txid := post(t, router, path)
				require.Less(t, w.Code, 300, w.Body.String())
				calls := rs.extended[txid]
				if !tc.extend {
					require.Empty(t, calls, "ExtendTransactionExpiry must not be called")
					return
				}
				require.Len(t, calls, 1)
				requireExpiry(t, "extension", calls[0], tc.want)
			})
		}
	}
}

// TestRetention_FailedExtensionIs503 pins that a resubmit whose extension
// fails is not acknowledged: the client's retry repeats the extend instead
// of leaving the row to expire before the caller's retention.
func TestRetention_FailedExtensionIs503(t *testing.T) {
	for _, path := range []string{"/tx", "/txs"} {
		t.Run(path, func(t *testing.T) {
			rs := newRetentionStore()
			rs.existing = true
			rs.existingExpiry = func() *time.Time { t := time.Now().Add(7 * 24 * time.Hour); return &t }()
			rs.extendErr = errors.New("primary stepped down")
			_, router := retentionServer(t, rs, retentionConfig(), "enterprise", false)
			w, _ := post(t, router, path)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			require.NotEmpty(t, w.Header().Get("Retry-After"))
		})
	}
}

// TestRetention_OffWithoutConfigOrSupport pins that nothing expires unless
// a tier configures retention and the store can enforce it.
func TestRetention_OffWithoutConfigOrSupport(t *testing.T) {
	t.Run("no tier configures retention", func(t *testing.T) {
		rs := newRetentionStore()
		cfg := retentionConfig()
		cfg.RateLimit.Anonymous.RetentionDays = 0
		cfg.RateLimit.Tiers = map[string]config.TierConfig{config.TierFree: {}}
		srv, router := retentionServer(t, rs, cfg, config.TierAnonymous, false)
		post(t, router, "/tx")
		require.Nil(t, srv.retention, "retention enabled with no tier configuring it")
		require.Zero(t, rs.ensuredCalls(), "indexes built with no tier configuring retention")
		requireExpiry(t, "tx row", rs.inserted[0].ExpiresAt, 0)
	})
	t.Run("store does not enforce retention", func(t *testing.T) {
		ms := &mockStore{getOrInsertFn: func(st *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			if st.ExpiresAt != nil {
				t.Errorf("row stamped with expires_at on a store without retention")
			}
			return st, true, nil
		}}
		srv, router := retentionServer(t, ms, retentionConfig(), "enterprise", false)
		post(t, router, "/tx")
		require.Nil(t, srv.retention, "retention enabled on a store that cannot enforce it")
	})
}
