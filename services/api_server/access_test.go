package api_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/services/ratelimit"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
)

// newIdentifyOnlyServer builds a Server with auth enabled and rate limiting
// disabled, over a real (empty) Pebble store: every presented key is
// unknown, so every request with an Authorization header fails
// identification. That isolates the apikey invalid-key throttle this test
// is pinning from any quota/concurrency behavior in services/ratelimit.
func newIdentifyOnlyServer(t *testing.T) *Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	srv := &Server{
		cfg: &config.Config{
			Auth: config.AuthConfig{Enabled: true},
			RateLimit: config.RateLimitConfig{
				Enabled:            false,
				KeyCacheTTLMs:      60_000,
				KeyCacheStaleMaxMs: 60_000,
			},
		},
		logger: zap.NewNop(),
		store:  st,
	}
	require.NoError(t, srv.initAccessControl())
	require.NotNil(t, srv.identify, "auth.enabled must build the identify middleware")
	require.Nil(t, srv.limiter, "ratelimit.enabled is false; no limiter should be built")
	return srv
}

// TestGuardedSharesIdentifyThrottleAcrossRoutes pins the fix for the bug
// where guarded() called apikey.Middleware(s.keyResolver) fresh on every
// call, giving every guarded route its own independent invalid-key
// throttle budget (N routes = N× the intended budget for one attacking
// IP). initAccessControl now builds the identify middleware exactly once,
// and guarded() reuses it, so the throttle budget is shared across every
// route it guards.
//
// This is proven, not just asserted, by splitting 60 failing attempts
// across two different routes and then sending one more to a THIRD route:
// if each route still had its own 60/min budget, that 61st attempt
// (route-c's first ever) would be its own route's first failure and pass.
// Because the budget is shared, it is the 61st failure overall and trips
// the per-IP throttle.
func TestGuardedSharesIdentifyThrottleAcrossRoutes(t *testing.T) {
	srv := newIdentifyOnlyServer(t)

	ok := func(c *gin.Context) { c.Status(http.StatusNoContent) }
	router := gin.New()
	router.GET("/route-a", srv.guarded("v1/test", 1, ok)...)
	router.GET("/route-b", srv.guarded("v1/test", 1, ok)...)
	router.GET("/route-c", srv.guarded("v1/test", 1, ok)...)

	call := func(path string) *httptest.ResponseRecorder {
		// httptest.NewRequest defaults RemoteAddr to the same value
		// ("192.0.2.1:1234") on every call, so every request below shares
		// one client IP for the throttle's purposes.
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer bsvb_garbage")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	// authFailureLimitPerMinute/Burst are both 60 (see
	// services/apikey/authfailure.go); split the budget across two routes.
	for i := range 30 {
		w := call("/route-a")
		require.Equal(t, http.StatusUnauthorized, w.Code, "route-a attempt %d", i+1)
	}
	for i := range 30 {
		w := call("/route-b")
		require.Equal(t, http.StatusUnauthorized, w.Code, "route-b attempt %d", i+1)
	}

	// The 61st failing attempt overall, on a route that has never been hit
	// before: it must still trip the shared throttle.
	w := call("/route-c")
	require.Equal(t, http.StatusTooManyRequests, w.Code, "61st failure overall must trip the shared throttle")
	require.Equal(t, "auth_failures", w.Header().Get(ratelimit.HeaderReason))
}

// TestApplyTrustedPlatform pins which header (if any) c.ClientIP() honors,
// across the three cases access.go's applyTrustedPlatform has to get
// right: nothing trusted by default, X-Forwarded-For trusted only from a
// configured proxy CIDR, and CF-Connecting-IP trusted only under
// cloudflare (independent of trusted_proxies).
func TestApplyTrustedPlatform(t *testing.T) {
	newServer := func(cfg config.API) *Server {
		return &Server{cfg: &config.Config{APIServer: cfg}, logger: zap.NewNop()}
	}
	clientIP := func(router *gin.Engine, remoteAddr string, headers map[string]string) string {
		var got string
		router.GET("/ip", func(c *gin.Context) { got = c.ClientIP() })
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ip", nil)
		req.RemoteAddr = remoteAddr
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		router.ServeHTTP(httptest.NewRecorder(), req)
		return got
	}

	t.Run("XFF ignored by default", func(t *testing.T) {
		srv := newServer(config.API{})
		router := gin.New()
		srv.applyTrustedPlatform(router)
		ip := clientIP(router, "192.0.2.1:1234", map[string]string{"X-Forwarded-For": "203.0.113.99"})
		require.Equal(t, "192.0.2.1", ip, "an untrusted peer's X-Forwarded-For must never be honored")
	})

	t.Run("XFF honored from a trusted proxy CIDR", func(t *testing.T) {
		srv := newServer(config.API{TrustedProxies: []string{"192.0.2.0/24"}})
		router := gin.New()
		srv.applyTrustedPlatform(router)
		ip := clientIP(router, "192.0.2.1:1234", map[string]string{"X-Forwarded-For": "203.0.113.99"})
		require.Equal(t, "203.0.113.99", ip, "a peer inside the trusted CIDR may set X-Forwarded-For")
	})

	t.Run("XFF from outside the trusted CIDR is still ignored", func(t *testing.T) {
		srv := newServer(config.API{TrustedProxies: []string{"192.0.2.0/24"}})
		router := gin.New()
		srv.applyTrustedPlatform(router)
		ip := clientIP(router, "198.51.100.1:1234", map[string]string{"X-Forwarded-For": "203.0.113.99"})
		require.Equal(t, "198.51.100.1", ip, "a peer outside the trusted CIDR must not be able to set the client IP")
	})

	t.Run("CF-Connecting-IP honored only under cloudflare", func(t *testing.T) {
		srv := newServer(config.API{TrustedPlatform: config.TrustedPlatformCloudflare})
		router := gin.New()
		srv.applyTrustedPlatform(router)
		// No trusted_proxies at all: cloudflare's header is honored purely
		// from TrustedPlatform, independent of the proxy CIDR list — a
		// request reaching the pod directly through a Cloudflare Tunnel has
		// no TCP-level proxy hop to list there.
		ip := clientIP(router, "192.0.2.1:1234", map[string]string{"CF-Connecting-IP": "203.0.113.50"})
		require.Equal(t, "203.0.113.50", ip)
	})

	t.Run("CF-Connecting-IP ignored without cloudflare", func(t *testing.T) {
		srv := newServer(config.API{})
		router := gin.New()
		srv.applyTrustedPlatform(router)
		ip := clientIP(router, "192.0.2.1:1234", map[string]string{"CF-Connecting-IP": "203.0.113.50"})
		require.Equal(t, "192.0.2.1", ip)
	})
}
