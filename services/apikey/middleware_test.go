package apikey

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

// serve sends one request through h, using httptest's default remote
// address for every call so a test exercises a single, consistent client
// IP unless it calls serveFrom directly.
func serve(t *testing.T, h gin.HandlerFunc, auth string) (*httptest.ResponseRecorder, Principal) {
	t.Helper()
	return serveFrom(t, h, auth, "")
}

// serveFrom is serve with an explicit remote address, for tests that need
// to distinguish clients for the per-IP auth-failure throttle.
func serveFrom(t *testing.T, h gin.HandlerFunc, auth, remoteAddr string) (*httptest.ResponseRecorder, Principal) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	var got Principal
	e := gin.New()
	e.GET("/x", h, func(c *gin.Context) {
		got = PrincipalFrom(c)
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	w := httptest.NewRecorder()
	e.ServeHTTP(w, req)
	return w, got
}

func TestMiddleware(t *testing.T) {
	r, lk, _, raw := newTestResolver(t)
	h := Middleware(r)

	w, p := serve(t, h, "")
	require.Equal(t, http.StatusNoContent, w.Code)
	require.True(t, p.Anonymous())
	require.Equal(t, config.TierAnonymous, p.Tier)
	require.Equal(t, "ip:"+p.ClientIP, p.Subject())

	w, p = serve(t, h, "Bearer "+raw)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, "key_1", p.KeyID)
	require.Equal(t, "pro", p.Tier)
	require.Equal(t, "key:key_1", p.Subject())

	w, _ = serve(t, h, "bearer "+raw)
	require.Equal(t, http.StatusNoContent, w.Code, "the scheme is case-insensitive")

	for _, bad := range []string{"Basic abc", "Bearer ", "Bearer bsvb_nope"} {
		w, _ = serve(t, h, bad)
		require.Equal(t, http.StatusUnauthorized, w.Code, bad)
		require.NotEmpty(t, w.Header().Get("WWW-Authenticate"), bad)
	}

	other, err := Generate()
	require.NoError(t, err)
	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	w, _ = serve(t, h, "Bearer "+other)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, "5", w.Header().Get("Retry-After"))
}

// TestMiddlewareMultipleAuthorizationHeaders checks that more than one
// Authorization header is rejected outright, rather than silently picking
// one and ignoring the rest -- including when the first value is empty,
// which GetHeader alone cannot distinguish from "no header".
func TestMiddlewareMultipleAuthorizationHeaders(t *testing.T) {
	r, _, _, raw := newTestResolver(t)
	h := Middleware(r)

	cases := []struct {
		name string
		vals []string
	}{
		{"two valid-looking values", []string{"Bearer " + raw, "Bearer " + raw}},
		{"empty value followed by a real one", []string{"", "Bearer " + raw}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var got Principal
			e := gin.New()
			e.GET("/x", h, func(c *gin.Context) {
				got = PrincipalFrom(c)
				c.Status(http.StatusNoContent)
			})
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
			for _, v := range tc.vals {
				req.Header.Add("Authorization", v)
			}
			w := httptest.NewRecorder()
			e.ServeHTTP(w, req)

			require.Equal(t, http.StatusUnauthorized, w.Code)
			require.True(t, got.Anonymous(), "the aborted request never reaches the handler with a principal set")
		})
	}
}

// TestMiddlewareKeyOutcomes covers the key states the happy-path test in
// TestMiddleware doesn't: well-formed-but-unknown, revoked and expired keys
// must all come back 401, same as a malformed one.
func TestMiddlewareKeyOutcomes(t *testing.T) {
	r, lk, clk, _ := newTestResolver(t)
	h := Middleware(r)

	unknown, err := Generate()
	require.NoError(t, err)
	w, _ := serve(t, h, "Bearer "+unknown)
	require.Equal(t, http.StatusUnauthorized, w.Code, "well-formed but unknown")

	revoked, err := Generate()
	require.NoError(t, err)
	lk.set(func(f *fakeLookup) {
		f.keys[Hash(revoked)] = &store.APIKey{ID: "key_revoked", Tier: "pro", Status: store.APIKeyRevoked}
	})
	w, _ = serve(t, h, "Bearer "+revoked)
	require.Equal(t, http.StatusUnauthorized, w.Code, "revoked")

	expired, err := Generate()
	require.NoError(t, err)
	past := clk.t.Add(-time.Second)
	lk.set(func(f *fakeLookup) {
		f.keys[Hash(expired)] = &store.APIKey{ID: "key_expired", Tier: "pro", Status: store.APIKeyActive, ExpiresAt: &past}
	})
	w, _ = serve(t, h, "Bearer "+expired)
	require.Equal(t, http.StatusUnauthorized, w.Code, "expired")
}

// TestMiddlewareAuthFailureThrottle checks the per-client-IP invalid-key
// throttle: it trips after its burst is exhausted, independently per IP.
func TestMiddlewareAuthFailureThrottle(t *testing.T) {
	r, _, _, _ := newTestResolver(t)
	h := newMiddleware(r, newAuthFailureLimiter(10, 3, 3)) // burst 3: the 4th bad attempt is throttled

	const ipA = "203.0.113.9:1111"
	const ipB = "203.0.113.10:2222"

	for i := range 3 {
		w, _ := serveFrom(t, h, "Bearer bsvb_nope", ipA)
		require.Equal(t, http.StatusUnauthorized, w.Code, "attempt %d from ipA", i)
	}
	w, _ := serveFrom(t, h, "Bearer bsvb_nope", ipA)
	require.Equal(t, http.StatusTooManyRequests, w.Code, "the 4th bad attempt from ipA is throttled")
	require.Equal(t, "auth_failures", w.Header().Get(HeaderRateLimitReason))
	require.NotEmpty(t, w.Header().Get("Retry-After"))

	w, _ = serveFrom(t, h, "Bearer bsvb_nope", ipB)
	require.Equal(t, http.StatusUnauthorized, w.Code, "a different IP is unaffected by ipA's throttle")
}

// TestMiddlewareCachedKeyBypassesThrottle checks that a caller presenting a
// key already proven good -- fresh in the resolver's cache -- still
// succeeds even from an IP that has exhausted its invalid-key throttle: one
// bad-faith client sharing an IP with a legitimate one must not lock the
// legitimate one out.
func TestMiddlewareCachedKeyBypassesThrottle(t *testing.T) {
	r, _, _, raw := newTestResolver(t)
	h := newMiddleware(r, newAuthFailureLimiter(10, 3, 3)) // burst 3

	const ip = "203.0.113.55:4444"

	// Warm the cache with a successful resolve; a valid key never consumes
	// a throttle token, so the bucket is still full afterward.
	w, p := serveFrom(t, h, "Bearer "+raw, ip)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, "key_1", p.KeyID)

	// Exhaust the IP's throttle with bad attempts.
	for i := range 3 {
		bad, _ := serveFrom(t, h, "Bearer bsvb_nope", ip)
		require.Equal(t, http.StatusUnauthorized, bad.Code, "attempt %d", i)
	}
	w, _ = serveFrom(t, h, "Bearer bsvb_nope", ip)
	require.Equal(t, http.StatusTooManyRequests, w.Code, "the IP is now throttled")

	// The same IP, presenting its already-proven-good key, must still
	// succeed.
	w, p = serveFrom(t, h, "Bearer "+raw, ip)
	require.Equal(t, http.StatusNoContent, w.Code, "a valid cached key bypasses the IP's own throttle")
	require.Equal(t, "key_1", p.KeyID)
}

// TestMiddlewareResolvesOnceAndReturns499OnDisconnectDuringIdentify covers
// the narrow race the fast path's two-step check/resolve leaves open: by
// the time Resolve is actually called, the entry it just saw as fresh
// (cachedUsable) has gone stale, and -- in this test -- the caller's own
// context is already done. Two things must both hold:
//   - the response is 499 (the caller is gone), not 503 (nobody told the
//     store was down, because it wasn't asked);
//   - Resolve is called at most once. If the fast path's failed attempt
//     fell through to a second Resolve call (the bug), this exact scenario
//     would hit resolveStale's ctxDone branch twice for one request, double
//     -counting bsv_broadcaster_apikey_cache_total{result="canceled"} -- so that
//     counter incrementing by exactly 1, not 2, is the regression check.
func TestMiddlewareResolvesOnceAndReturns499OnDisconnectDuringIdentify(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)

	// Warm the positive cache with one successful, synchronous resolve.
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, 1, lk.n())

	// Block any further store lookup. If Resolve were (wrongly) called
	// twice, singleflight would join the same in-flight attempt rather than
	// issuing a second store call -- so this also guarantees both attempts,
	// if there were two, observe the exact same (blocked, then
	// context-canceled) outcome, rather than racing a real store response.
	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate })
	t.Cleanup(func() { close(gate) })

	// A stateful clock: cachedUsable's read (the first) sees the original
	// fetch time, so the entry still looks fresh and the fast path is
	// taken. Every read after that -- Resolve's own -- is just past ttl
	// (30s, per newTestResolver) but within staleMax, so Resolve takes the
	// resolveStale path instead of a plain cache hit.
	fresh := clk.t
	stale := fresh.Add(31 * time.Second)
	reads := 0
	r.now = func() time.Time {
		reads++
		if reads == 1 {
			return fresh
		}
		return stale
	}

	before := testutil.ToFloat64(metrics.APIKeyCacheTotal.WithLabelValues("canceled"))

	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.GET("/x", Middleware(r), func(c *gin.Context) { c.Status(http.StatusNoContent) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	require.Equal(t, StatusClientClosedRequest, rec.Code)
	require.InDelta(t, before+1, testutil.ToFloat64(metrics.APIKeyCacheTotal.WithLabelValues("canceled")), 0.0001,
		"Resolve must be called at most once per request; a second call would double-count this")
}

// TestLogFields checks the three states LogFields can observe: no
// principal set at all, an anonymous principal, and a keyed one.
func TestLogFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	require.Nil(t, LogFields(c), "no principal set")

	WithPrincipal(c, Principal{Tier: config.TierAnonymous, ClientIP: "203.0.113.1"})
	require.Nil(t, LogFields(c), "anonymous principal")

	WithPrincipal(c, Principal{KeyID: "key_1", Tier: "pro", ClientIP: "203.0.113.1"})
	require.Equal(t, []zap.Field{zap.String("key_id", "key_1"), zap.String("tier", "pro")}, LogFields(c))

	WithPrincipal(c, Principal{KeyID: "key_1", QuotaID: "lim_1", Tier: "standard", ClientIP: "203.0.113.1"})
	require.Equal(t, []zap.Field{zap.String("key_id", "key_1"), zap.String("tier", "standard"), zap.String("limit_id", "lim_1")}, LogFields(c))
}

// TestPrincipalFromWithoutMiddleware checks the documented fallback: a
// request that never passed through Middleware is anonymous, not a panic
// or a zero-value Principal with the wrong tier.
func TestPrincipalFromWithoutMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)

	p := PrincipalFrom(c)
	require.True(t, p.Anonymous())
	require.Equal(t, config.TierAnonymous, p.Tier)
}
