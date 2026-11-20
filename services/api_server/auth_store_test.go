package api_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/services/ratelimit"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
)

// txOnlyStore hides every method beyond store.Store, so the main store
// implements neither APIKeyStore nor QuotaStore: any key or quota call that
// reached it would make initAccessControl fail, not silently succeed.
type txOnlyStore struct{ store.Store }

// countingQuotaStore records every ReserveQuota call so the test can prove
// quota is charged against the injected auth store.
type countingQuotaStore struct {
	*pebble.Store

	reserves int
}

func (c *countingQuotaStore) ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (int64, int64, error) {
	c.reserves++
	return c.Store.ReserveQuota(ctx, keyID, windowStart, want, limit)
}

func newPebble(t *testing.T) *pebble.Store {
	t.Helper()
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func authStoreTestConfig() *config.Config {
	return &config.Config{
		Auth: config.AuthConfig{Enabled: true, Store: config.AuthStoreConfig{
			Mongo: config.Mongo{URI: "mongodb://auth.invalid:27017", Database: "bsv_broadcaster_auth"},
		}},
		Admin: config.AdminConfig{Enabled: true, Token: "admin-secret"},
		RateLimit: config.RateLimitConfig{
			Enabled: true, LeaseFraction: 1, ShrinkThreshold: 0.8,
			KeyCacheTTLMs: 1, KeyCacheStaleMaxMs: 1000, ExpectedReplicas: 1,
			Costs:     config.RateLimitCosts{SubmitTx: 10, Read: 1},
			Anonymous: config.TierConfig{UnitsPerMinute: 2, MaxInFlight: 5, MaxBatch: 1},
			Tiers: map[string]config.TierConfig{
				config.TierFree: {UnitsPerMinute: 2, MaxInFlight: 5, MaxBatch: 1},
			},
		},
	}
}

// TestAuthStoreServesKeysAndQuota proves that with an auth store set, the
// admin key endpoints, key identification and quota all use it and never
// the main store: a key created through the admin API lands in the auth
// store, authenticates on the public API, and its quota is reserved there.
func TestAuthStoreServesKeysAndQuota(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mainStore := newPebble(t)
	authStore := &countingQuotaStore{Store: newPebble(t)}

	cfg := authStoreTestConfig()
	srv := &Server{cfg: cfg, logger: zap.NewNop(), store: txOnlyStore{mainStore}}
	srv.SetAuthStore(authStore)
	require.NoError(t, srv.initAccessControl(), "auth/admin/ratelimit must resolve to the auth store, not the tx-only main store")
	fixed := time.Date(2026, 10, 22, 12, 0, 30, 0, time.UTC)
	srv.limiter = ratelimit.New(cfg.RateLimit, authStore, zap.NewNop(), ratelimit.WithClock(func() time.Time { return fixed }))

	admin := srv.newAdminRouter()
	w := adminDo(t, admin, http.MethodPost, "/admin/v1/keys", "admin-secret", `{"name":"shared","tier":"free"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

	inAuth, err := authStore.ListAPIKeys(t.Context())
	require.NoError(t, err)
	require.Len(t, inAuth, 1, "admin-created key must be written to the auth store")
	require.Equal(t, created.ID, inAuth[0].ID)
	inMain, err := mainStore.ListAPIKeys(t.Context())
	require.NoError(t, err)
	require.Empty(t, inMain, "the main store must not see the key")

	api := gin.New()
	api.GET("/limited", srv.guarded("v1/test", 1, func(c *gin.Context) { c.Status(http.StatusNoContent) })...)
	call := func() *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/limited", nil)
		req.Header.Set("Authorization", "Bearer "+created.Key)
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		return rec
	}
	require.Equal(t, http.StatusNoContent, call().Code)
	require.Equal(t, http.StatusNoContent, call().Code)
	w = call()
	require.Equal(t, http.StatusTooManyRequests, w.Code, "free tier is 2/min")
	require.Equal(t, "quota", w.Header().Get(ratelimit.HeaderReason))
	require.Positive(t, authStore.reserves, "quota must be reserved against the auth store")
}

// TestAuthStoreConfiguredButNotWiredFailsClosed: auth.store set in config
// with no store injected must refuse to start rather than quietly serve a
// per-network key set from the main store.
func TestAuthStoreConfiguredButNotWiredFailsClosed(t *testing.T) {
	srv := &Server{cfg: authStoreTestConfig(), logger: zap.NewNop(), store: newPebble(t)}
	err := srv.initAccessControl()
	require.Error(t, err)
	require.Contains(t, err.Error(), "auth.store.mongodb")
}
