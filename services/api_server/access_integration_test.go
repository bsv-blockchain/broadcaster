package api_server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/services/ratelimit"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
)

// TestAccessControlEndToEnd drives a real Pebble store through the admin
// router and the public guard chain: create a key, exhaust the free tier,
// upgrade it, revoke it.
func TestAccessControlEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{
		Auth:  config.AuthConfig{Enabled: true},
		Admin: config.AdminConfig{Enabled: true, Token: "admin-secret"},
		RateLimit: config.RateLimitConfig{
			Enabled: true, LeaseFraction: 0.1, ShrinkThreshold: 0.8,
			KeyCacheTTLMs: 1, KeyCacheStaleMaxMs: 1000, ExpectedReplicas: 1,
			Costs:     config.RateLimitCosts{SubmitTx: 10, Read: 1},
			Anonymous: config.TierConfig{UnitsPerMinute: 2, MaxInFlight: 5, MaxBatch: 1},
			Tiers: map[string]config.TierConfig{
				config.TierFree: {UnitsPerMinute: 3, MaxInFlight: 5, MaxBatch: 1},
				"enterprise":    {UnitsPerMinute: 0, MaxInFlight: 5, MaxBatch: 100},
			},
		},
	}
	srv := &Server{cfg: cfg, logger: zap.NewNop(), store: st}
	require.NoError(t, srv.initAccessControl())
	fixed := time.Date(2026, 10, 21, 12, 0, 30, 0, time.UTC)
	srv.limiter = ratelimit.New(cfg.RateLimit, st, zap.NewNop(), ratelimit.WithClock(func() time.Time { return fixed }))

	api := gin.New()
	api.GET("/limited", srv.guarded(1, func(c *gin.Context) { c.Status(http.StatusNoContent) })...)
	admin := srv.newAdminRouter()

	call := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/limited", nil)
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, req)
		return w
	}

	w := adminDo(t, admin, http.MethodPost, "/admin/v1/keys", "admin-secret", `{"name":"e2e","tier":"free"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

	for i := range 3 {
		w = call(created.Key)
		require.Equal(t, http.StatusNoContent, w.Code, "request %d", i+1)
		require.Equal(t, `"free";q=3;w=60`, w.Header().Get("RateLimit-Policy"))
	}
	w = call(created.Key)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "30", w.Header().Get("Retry-After"))
	require.Equal(t, "quota", w.Header().Get(ratelimit.HeaderReason))

	require.Equal(t, http.StatusUnauthorized, call("bsvb_garbage").Code)
	alt := "a"
	if strings.HasSuffix(created.Key, "a") {
		alt = "b"
	}
	tampered := created.Key[:len(created.Key)-1] + alt
	require.Equal(t, http.StatusUnauthorized, call(tampered).Code, "a one-character change fails the checksum")

	require.Equal(t, http.StatusNoContent, call("").Code, "anonymous 1/2")
	require.Equal(t, http.StatusNoContent, call("").Code, "anonymous 2/2")
	require.Equal(t, http.StatusTooManyRequests, call("").Code, "anonymous exhausted")

	require.Equal(t, http.StatusOK,
		adminDo(t, admin, http.MethodPatch, "/admin/v1/keys/"+created.ID, "admin-secret", `{"tier":"enterprise"}`).Code)
	time.Sleep(5 * time.Millisecond) // key_cache_ttl_ms is 1
	w = call(created.Key)
	require.Equal(t, http.StatusNoContent, w.Code, "enterprise has no quota")
	require.Empty(t, w.Header().Get("RateLimit-Policy"))

	require.Equal(t, http.StatusOK,
		adminDo(t, admin, http.MethodDelete, "/admin/v1/keys/"+created.ID, "admin-secret", "").Code)
	time.Sleep(5 * time.Millisecond)
	require.Equal(t, http.StatusUnauthorized, call(created.Key).Code, "revoked within the cache TTL")
}

// TestIntrospectionProviderEndToEnd drives the public guard chain with a
// bsv_ provider configured: provider keys resolve through introspection,
// an unbounded provider tier is never limited while anonymous callers are,
// and two keys sharing a limit_id share one quota.
func TestIntrospectionProviderEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	unlimited := providerKey(t)
	planA, planB := providerKey(t), providerKey(t)
	answers := map[string]string{
		apikey.Hash(unlimited): `{"valid":true,"key_id":"k-unlimited","tier":"standard","expires_at":null}`,
		apikey.Hash(planA):     `{"valid":true,"key_id":"k-a","limit_id":"lim_plan","tier":"free"}`,
		apikey.Hash(planB):     `{"valid":true,"key_id":"k-b","limit_id":"lim_plan","tier":"free"}`,
	}
	calls := 0
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body struct {
			KeyHash string `json:"key_hash"`
		}
		if r.Header.Get("Authorization") != "Bearer svc-token" || json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		answer, ok := answers[body.KeyHash]
		if !ok {
			answer = `{"valid":false}`
		}
		_, _ = w.Write([]byte(answer))
	}))
	t.Cleanup(provider.Close)

	cfg := &config.Config{
		Auth: config.AuthConfig{Enabled: true, Introspection: map[string]config.IntrospectionProvider{
			"woc": {KeyPrefix: "bsv_", URL: provider.URL, Token: "svc-token"},
		}},
		RateLimit: config.RateLimitConfig{
			Enabled: true, LeaseFraction: 0.1, ShrinkThreshold: 0.8,
			KeyCacheTTLMs: 30000, KeyCacheStaleMaxMs: 60000, ExpectedReplicas: 1,
			Costs:     config.RateLimitCosts{SubmitTx: 10, Read: 1},
			Anonymous: config.TierConfig{UnitsPerMinute: 2, MaxInFlight: 5, MaxBatch: 1},
			Tiers: map[string]config.TierConfig{
				config.TierFree: {UnitsPerMinute: 3, MaxInFlight: 5, MaxBatch: 1},
				"standard":      {},
			},
		},
	}
	srv := &Server{cfg: cfg, logger: zap.NewNop(), store: st}
	require.NoError(t, srv.initAccessControl())
	fixed := time.Date(2026, 10, 30, 12, 0, 30, 0, time.UTC)
	srv.limiter = ratelimit.New(cfg.RateLimit, st, zap.NewNop(), ratelimit.WithClock(func() time.Time { return fixed }))

	api := gin.New()
	api.GET("/limited", srv.guarded(1, func(c *gin.Context) { c.Status(http.StatusNoContent) })...)
	call := func(key string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/limited", nil)
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, req)
		return w.Code
	}

	require.Equal(t, http.StatusNoContent, call(""))
	require.Equal(t, http.StatusNoContent, call(""))
	require.Equal(t, http.StatusTooManyRequests, call(""), "anonymous is limited")

	for i := range 200 {
		require.Equal(t, http.StatusNoContent, call(unlimited), "request %d", i)
	}
	require.Equal(t, 1, calls, "the answer is cached")

	require.Equal(t, http.StatusUnauthorized, call(providerKey(t)), "a key the provider doesn't know")

	// free is 3 units/minute per allowance, and planA and planB share one.
	require.Equal(t, http.StatusNoContent, call(planA))
	require.Equal(t, http.StatusNoContent, call(planB))
	require.Equal(t, http.StatusNoContent, call(planA))
	require.Equal(t, http.StatusTooManyRequests, call(planB), "keys on one limit_id share its quota")
}

// providerKey returns a well-formed key under the bsv_ provider prefix.
func providerKey(t *testing.T) string {
	t.Helper()
	k, err := apikey.Generate()
	require.NoError(t, err)
	return "bsv_" + strings.TrimPrefix(k, apikey.Prefix)
}
