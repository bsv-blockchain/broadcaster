package api_server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
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

// TestUsageReportedToProviderEndToEnd drives the guard chain with a
// provider that both verifies keys and collects usage: the provider's keys
// are reported under the provider's own key id, by route name and status
// class, with the units charged (0 for a 429); local and anonymous callers
// are never reported.
func TestUsageReportedToProviderEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	providerK := providerKey(t)
	type row struct {
		KeyID       string `json:"key_id"`
		Endpoint    string `json:"endpoint"`
		StatusClass string `json:"status_class"`
		DateUTC     string `json:"date_utc"`
		Count       int64  `json:"count"`
		Units       int64  `json:"units"`
	}
	var mu sync.Mutex
	var reported []row
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer svc-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/introspect":
			var body struct {
				KeyHash string `json:"key_hash"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.KeyHash == apikey.Hash(providerK) {
				_, _ = w.Write([]byte(`{"valid":true,"key_id":"k-123","limit_id":"plan-1","tier":"basic"}`))
				return
			}
			_, _ = w.Write([]byte(`{"valid":false}`))
		case "/usage":
			var b struct {
				Entries []row `json:"entries"`
			}
			if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
				t.Errorf("undecodable usage batch: %v", err)
			}
			mu.Lock()
			reported = append(reported, b.Entries...)
			mu.Unlock()
			_, _ = w.Write([]byte(`{"accepted":1,"dropped":0}`))
		}
	}))
	t.Cleanup(provider.Close)

	cfg := &config.Config{
		Auth: config.AuthConfig{Enabled: true, Introspection: map[string]config.IntrospectionProvider{
			"acme": {
				KeyPrefix: "acme_", URL: provider.URL + "/introspect", UsageURL: provider.URL + "/usage",
				Token: "svc-token", Tiers: map[string]string{"basic": config.TierFree},
			},
		}},
		RateLimit: config.RateLimitConfig{
			Enabled: true, LeaseFraction: 0.1, ShrinkThreshold: 0.8,
			KeyCacheTTLMs: 30000, KeyCacheStaleMaxMs: 60000, ExpectedReplicas: 1,
			Costs:     config.RateLimitCosts{SubmitTx: 10, Read: 1},
			Anonymous: config.TierConfig{UnitsPerMinute: 100, MaxInFlight: 5, MaxBatch: 1},
			Tiers:     map[string]config.TierConfig{config.TierFree: {UnitsPerMinute: 3, MaxInFlight: 5, MaxBatch: 1}},
		},
		Usage: config.UsageConfig{
			Enabled: true, FlushIntervalMs: 3_600_000, MaxBuckets: 1000, MaxPendingEntries: 1000, ShutdownFlushTimeoutMs: 5000,
		},
	}
	srv := &Server{cfg: cfg, logger: zap.NewNop(), store: st}
	require.NoError(t, srv.initAccessControl())
	srv.initUsage(t.Context())
	fixed := time.Date(2026, 11, 12, 12, 0, 30, 0, time.UTC)
	srv.limiter = ratelimit.New(cfg.RateLimit, st, zap.NewNop(), ratelimit.WithClock(func() time.Time { return fixed }))

	api := gin.New()
	api.GET("/thing", srv.guarded("v1/thing", 1, func(c *gin.Context) { c.Status(http.StatusOK) })...)
	call := func(key string) int {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/thing", nil)
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, req)
		return w.Code
	}

	for range 3 {
		require.Equal(t, http.StatusOK, call(providerK))
	}
	require.Equal(t, http.StatusTooManyRequests, call(providerK), "free is 3 units/minute")
	require.Equal(t, http.StatusOK, call(""), "anonymous: not reported")
	require.Equal(t, http.StatusUnauthorized, call(providerKey(t)), "unknown key: not reported")

	srv.stopUsage() // final flush, as on shutdown

	mu.Lock()
	defer mu.Unlock()
	today := time.Now().UTC().Format(time.DateOnly)
	require.ElementsMatch(t, []row{
		{KeyID: "k-123", Endpoint: "v1/thing", StatusClass: "2xx", DateUTC: today, Count: 3, Units: 3},
		{KeyID: "k-123", Endpoint: "v1/thing", StatusClass: "429", DateUTC: today, Count: 1, Units: 0},
	}, reported)
}
