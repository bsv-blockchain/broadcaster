package ratelimit

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
	"github.com/bsv-blockchain/broadcaster/services/apikey"
)

func testConfig() config.RateLimitConfig {
	return config.RateLimitConfig{
		Enabled: true, LeaseFraction: 0.1, ShrinkThreshold: 0.8, ExpectedReplicas: 1,
		Costs:     config.RateLimitCosts{SubmitTx: 10, Read: 1},
		Anonymous: config.TierConfig{UnitsPerMinute: 2, MaxInFlight: 5, MaxBatch: 1},
		Tiers: map[string]config.TierConfig{
			config.TierFree: {UnitsPerMinute: 25, MaxInFlight: 1, MaxBatch: 3},
			"enterprise":    {UnitsPerMinute: 0, MaxInFlight: 5, MaxBatch: 100},
		},
	}
}

// engine mounts route with the limiter, standing in for the identify
// middleware with a fixed principal.
func engine(rl *RateLimiter, p apikey.Principal, units int64, h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.GET("/x", func(c *gin.Context) { apikey.WithPrincipal(c, p); c.Next() }, rl.Middleware(units), h)
	return e
}

func do(t *testing.T, e *gin.Engine) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	return w
}

func ok(c *gin.Context) { c.Status(http.StatusNoContent) }

var freeKey = apikey.Principal{KeyID: "key_f", Tier: config.TierFree, ClientIP: "10.0.0.1"}

func TestQuotaHeadersAnd429(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop(), WithClock(func() time.Time { return noon }))
	e := engine(rl, freeKey, 10, ok)

	w := do(t, e)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, `"free";q=25;w=60`, w.Header().Get("RateLimit-Policy"))
	require.Equal(t, `"free";r=15;t=45`, w.Header().Get("RateLimit"))
	require.Equal(t, "25", w.Header().Get("X-RateLimit-Limit"))
	require.Equal(t, "15", w.Header().Get("X-RateLimit-Remaining"))
	require.Equal(t, "45", w.Header().Get("X-RateLimit-Reset"))

	require.Equal(t, http.StatusNoContent, do(t, e).Code)
	w = do(t, e)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "45", w.Header().Get("Retry-After"))
	require.Equal(t, "quota", w.Header().Get(HeaderReason))
	require.Contains(t, w.Body.String(), `"tier":"free"`)
}

func TestUnboundedTierHasNoQuota(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop())
	e := engine(rl, apikey.Principal{KeyID: "key_e", Tier: "enterprise"}, 1_000_000, ok)
	for range 5 {
		w := do(t, e)
		require.Equal(t, http.StatusNoContent, w.Code)
		require.Empty(t, w.Header().Get("RateLimit-Policy"))
	}
}

func TestAnonymousAndUnknownTier(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop(), WithClock(func() time.Time { return noon }))
	anon := engine(rl, apikey.Principal{Tier: config.TierAnonymous, ClientIP: "1.2.3.4"}, 1, ok)
	require.Equal(t, http.StatusNoContent, do(t, anon).Code)
	require.Equal(t, http.StatusNoContent, do(t, anon).Code)
	require.Equal(t, http.StatusTooManyRequests, do(t, anon).Code)

	stray := engine(rl, apikey.Principal{KeyID: "key_s", Tier: "gold"}, 10, ok)
	w := do(t, stray)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, `"free";q=25;w=60`, w.Header().Get("RateLimit-Policy"), "an unconfigured tier falls back to free")
}

// TestAnonymousBucketSharedWithinIPv6Slash64 pins that an IPv6 client
// rotating addresses within its own /64 must not get fresh anonymous quota
// on every rotation. Principal.Subject() normalizes via
// apikey.NormalizeClientIP, so two different addresses in the same /64 must
// land in the same anonymous bucket.
func TestAnonymousBucketSharedWithinIPv6Slash64(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop(), WithClock(func() time.Time { return noon }))
	first := engine(rl, apikey.Principal{Tier: config.TierAnonymous, ClientIP: "2001:db8:1234:5678::1"}, 1, ok)
	second := engine(rl, apikey.Principal{Tier: config.TierAnonymous, ClientIP: "2001:db8:1234:5678::2"}, 1, ok)

	// anonymous units_per_minute is 2 (testConfig): the first two requests,
	// from two DIFFERENT addresses in the same /64, must exhaust the shared
	// bucket, and a third (from either address) must be denied.
	require.Equal(t, http.StatusNoContent, do(t, first).Code)
	require.Equal(t, http.StatusNoContent, do(t, second).Code)
	require.Equal(t, http.StatusTooManyRequests, do(t, first).Code,
		"two addresses in the same /64 must share the anonymous bucket")
}

func TestConcurrencyCap(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop())
	entered, release := make(chan struct{}), make(chan struct{})
	e := engine(rl, freeKey, 1, func(c *gin.Context) {
		entered <- struct{}{}
		<-release
		c.Status(http.StatusNoContent)
	})
	done := make(chan int)
	go func() { done <- do(t, e).Code }()
	<-entered
	w := do(t, e)
	require.Equal(t, http.StatusTooManyRequests, w.Code, "free max_in_flight is 1")
	require.Equal(t, "concurrency", w.Header().Get(HeaderReason))
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	close(release)
	require.Equal(t, http.StatusNoContent, <-done)
	go func() { <-entered }()
	require.Equal(t, http.StatusNoContent, do(t, e).Code, "the slot is released after the handler returns")
}

func TestFailOpen(t *testing.T) {
	qs := newFakeQuota()
	qs.err = errors.New("store down")
	rl := New(testConfig(), qs, zap.NewNop(), WithClock(func() time.Time { return noon }))
	e := engine(rl, freeKey, 10, ok)
	before := testutil.ToFloat64(metrics.RateLimitStoreFailOpenTotal)
	require.Equal(t, http.StatusNoContent, do(t, e).Code)
	require.Equal(t, http.StatusNoContent, do(t, e).Code)
	require.Equal(t, http.StatusTooManyRequests, do(t, e).Code, "the local bucket still enforces the tier limit")
	require.InDelta(t, before+3, testutil.ToFloat64(metrics.RateLimitStoreFailOpenTotal), 0.0001)
}

func TestChargeTxsAndCheckBatch(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop(), WithClock(func() time.Time { return noon }))
	n := 0
	e := engine(rl, freeKey, 1, func(c *gin.Context) {
		if !CheckBatch(c, n) || !ChargeTxs(c, n) {
			return
		}
		c.Status(http.StatusNoContent)
	})

	n = 4
	require.Equal(t, http.StatusRequestEntityTooLarge, do(t, e).Code, "free max_batch is 3")
	n = 3
	before := testutil.ToFloat64(metrics.RateLimitDecisionsTotal.WithLabelValues(config.TierFree, "unpayable"))
	w := do(t, e)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, "3 transactions cost 30 units, which can never fit in the free tier's 25/min, so it's a 413, not a 429")
	require.Contains(t, w.Body.String(), "request costs 30 units, above the free tier's 25 per minute")
	require.Contains(t, w.Body.String(), `"tier":"free"`)
	require.InDelta(t, before+1, testutil.ToFloat64(metrics.RateLimitDecisionsTotal.WithLabelValues(config.TierFree, "unpayable")), 0.0001,
		"the unpayable 413 must be metered, same as every other rate-limit decision")
	n = 2
	require.Equal(t, http.StatusNoContent, do(t, e).Code, "the denied batch consumed nothing beyond its base cost")

	require.True(t, ChargeTxs(&gin.Context{}, 100), "no limiter on the route: always allowed")
	require.True(t, CheckBatch(&gin.Context{}, 100))
}

// TestRejectBatchBytesTooLarge covers both shapes: a tier-derived cap
// (max_batch > 0) gets CheckBatch's own response shape, metered the same
// way as a count-based batch_too_large rejection; the absence of a
// tier-derived cap -- no limiter guarding the route at all, an unbounded
// tier (max_batch: 0), or a max_batch large enough that the flat ceiling
// was the tighter cap -- gets the plain, unmetered message.
func TestRejectBatchBytesTooLarge(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set(ctxTier, Tier{Name: "free", MaxBatch: 3})
	before := testutil.ToFloat64(metrics.RateLimitDecisionsTotal.WithLabelValues("free", "batch_too_large"))

	RejectBatchBytesTooLarge(c, true)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	require.Contains(t, w.Body.String(), "request body exceeds the free tier's batch byte limit")
	require.Contains(t, w.Body.String(), `"tier":"free"`)
	require.Contains(t, w.Body.String(), `"maxBatch":3`)
	require.InDelta(t, before+1, testutil.ToFloat64(metrics.RateLimitDecisionsTotal.WithLabelValues("free", "batch_too_large")), 0.0001,
		"must be metered identically to a count-based batch_too_large rejection")

	for _, tc := range []struct {
		name       string
		set        func(*gin.Context)
		tierCapped bool
	}{
		{"unbounded tier", func(c *gin.Context) { c.Set(ctxTier, Tier{Name: "enterprise", MaxBatch: 0}) }, false},
		{"no limiter on the route", func(*gin.Context) {}, false},
		{"flat ceiling tighter than the tier cap", func(c *gin.Context) { c.Set(ctxTier, Tier{Name: "high", MaxBatch: 1000}) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			tc.set(c)

			RejectBatchBytesTooLarge(c, tc.tierCapped)

			require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
			require.JSONEq(t, `{"error":"request body too large"}`, w.Body.String())
		})
	}
}

// TestConcurrencyCapReleasedAfterPanic locks in that the in-flight slot is
// released even when the handler panics: gin's deferred release runs during
// panic unwinding, before gin.Recovery() (mounted ahead of the limiter)
// recovers it. If the slot leaked, the second request would be rejected for
// concurrency (429) instead of reaching the handler and panicking (500).
func TestConcurrencyCapReleasedAfterPanic(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop())
	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.Use(gin.Recovery())
	e.GET("/x", func(c *gin.Context) { apikey.WithPrincipal(c, freeKey); c.Next() }, rl.Middleware(1),
		func(_ *gin.Context) { panic("boom") })

	require.Equal(t, http.StatusInternalServerError, do(t, e).Code)
	require.Equal(t, http.StatusInternalServerError, do(t, e).Code,
		"free max_in_flight is 1: a leaked slot would turn this into a 429 instead of reaching (and panicking in) the handler again")
}

// TestClientDisconnectDoesNotCountAsFailOpen checks that a keyed charge
// erroring because the caller already hung up — not because the store is
// down — is aborted silently: no fail-open count, no local bucket spent.
func TestClientDisconnectDoesNotCountAsFailOpen(t *testing.T) {
	qs := newFakeQuota()
	rl := New(testConfig(), cancelAwareQuota{qs}, zap.NewNop(), WithClock(func() time.Time { return noon }))
	e := engine(rl, freeKey, 10, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/x", nil)

	before := testutil.ToFloat64(metrics.RateLimitStoreFailOpenTotal)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, req)
	require.Equal(t, 499, w.Code, "a client disconnect is reported as 499, not a bare abort that reads back as 200")
	require.InDelta(t, before, testutil.ToFloat64(metrics.RateLimitStoreFailOpenTotal), 0.0001,
		"a client disconnect must not be counted as a store failure")
}

// TestKeysSharingALimitShareQuotaAndConcurrency pins that keys drawing from
// one allowance (a provider's limit_id) are accounted together: a customer
// must not multiply their plan by creating more keys.
func TestKeysSharingALimitShareQuotaAndConcurrency(t *testing.T) {
	rl := New(testConfig(), newFakeQuota(), zap.NewNop(), WithClock(func() time.Time { return noon }))
	a := engine(rl, apikey.Principal{KeyID: "key_a", QuotaID: "lim_1", Tier: config.TierFree}, 10, ok)
	b := engine(rl, apikey.Principal{KeyID: "key_b", QuotaID: "lim_1", Tier: config.TierFree}, 10, ok)
	c := engine(rl, apikey.Principal{KeyID: "key_c", QuotaID: "lim_2", Tier: config.TierFree}, 10, ok)

	// free is 25 units/minute: two 10-unit requests fit, the third does not.
	require.Equal(t, http.StatusNoContent, do(t, a).Code)
	require.Equal(t, http.StatusNoContent, do(t, b).Code)
	require.Equal(t, http.StatusTooManyRequests, do(t, a).Code, "key_b spent key_a's shared allowance")
	require.Equal(t, http.StatusNoContent, do(t, c).Code, "another limit has its own allowance")

	require.Equal(t, "key:lim_1", apikey.Principal{KeyID: "key_a", QuotaID: "lim_1"}.Subject(),
		"in-flight and fail-open buckets are per limit too")
	require.Equal(t, "key:key_a", apikey.Principal{KeyID: "key_a"}.Subject())
}

// TestChargedReportsWhatWasGranted pins ratelimit.Charged, which usage
// reporting bills from: a granted charge (plus ChargeTxs), the nominal cost
// on an unbounded tier, and nothing for a refused request or a client that
// hung up before the quota was granted.
func TestChargedReportsWhatWasGranted(t *testing.T) {
	charged := func(rl *RateLimiter, p apikey.Principal, units int64, txs int, req *http.Request) (int64, bool) {
		var got int64
		var limited bool
		gin.SetMode(gin.TestMode)
		e := gin.New()
		e.GET("/x", func(c *gin.Context) { apikey.WithPrincipal(c, p); c.Next() },
			func(c *gin.Context) {
				c.Next()
				got, limited = Charged(c)
			},
			rl.Middleware(units),
			func(c *gin.Context) {
				if txs > 0 && !ChargeTxs(c, txs) {
					return
				}
				c.Status(http.StatusNoContent)
			})
		e.ServeHTTP(httptest.NewRecorder(), req)
		return got, limited
	}

	rl := New(testConfig(), newFakeQuota(), zap.NewNop(), WithClock(func() time.Time { return noon }))
	got, limited := charged(rl, freeKey, 1, 0, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	require.True(t, limited)
	require.EqualValues(t, 1, got, "a granted charge")

	got, _ = charged(rl, apikey.Principal{KeyID: "key_e", Tier: "enterprise"}, 1, 3, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	require.EqualValues(t, 1+3*10, got, "an unbounded tier is charged its nominal cost, ChargeTxs included")

	got, _ = charged(rl, freeKey, 100, 0, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)) // free is 25/minute: unpayable
	require.Zero(t, got, "a refused request was charged nothing")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	hungUp := New(testConfig(), cancelAwareQuota{newFakeQuota()}, zap.NewNop(), WithClock(func() time.Time { return noon }))
	got, limited = charged(hungUp, freeKey, 10, 0, httptest.NewRequestWithContext(cancelled, http.MethodGet, "/x", nil))
	require.True(t, limited)
	require.Zero(t, got, "a client that hung up before the quota was granted was charged nothing")

	e := gin.New()
	var noLimiter bool
	e.GET("/x", func(c *gin.Context) { _, noLimiter = Charged(c) })
	e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	require.False(t, noLimiter, "no limiter on the route")
}
