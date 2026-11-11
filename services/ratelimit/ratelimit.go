package ratelimit

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/store"
)

// HeaderReason says why a 429 was returned: quota | concurrency |
// auth_failures. Aliases apikey's exported constant so both packages use
// the same header name under one definition — ratelimit already imports
// apikey, and apikey cannot import back (no cycle either way).
const HeaderReason = apikey.HeaderRateLimitReason

const (
	ctxLimiter          = "ratelimit.limiter"
	ctxTier             = "ratelimit.tier"
	localBucketCapacity = 100_000
	warnInterval        = 10 * time.Second
	// StatusClientClosedRequest aliases apikey's exported constant so both
	// packages use the same status code under one definition -- ratelimit
	// already imports apikey, and apikey cannot import back (no cycle
	// either way). Exported so api_server's requestLogger can log it at the
	// same level as the identify/rate-limit 401/429s.
	StatusClientClosedRequest = apikey.StatusClientClosedRequest
)

// RateLimiter applies the configured tiers to identified principals.
type RateLimiter struct {
	cfg      config.RateLimitConfig
	logger   *zap.Logger
	now      func() time.Time
	keyed    *keyedLimiter
	anon     *localLimiter
	failopen *localLimiter
	inflight *inflight

	warnMu   sync.Mutex
	warnedAt map[string]time.Time // throttles each distinct log message independently
}

// Option configures a RateLimiter.
type Option func(*RateLimiter)

// WithClock replaces time.Now, for tests.
func WithClock(now func() time.Time) Option {
	return func(rl *RateLimiter) {
		rl.now = now
		rl.keyed.now = now
	}
}

// New builds a RateLimiter. qs is the shared quota store keyed tiers lease from.
func New(cfg config.RateLimitConfig, qs store.QuotaStore, logger *zap.Logger, opts ...Option) *RateLimiter {
	rl := &RateLimiter{
		cfg:      cfg,
		logger:   logger.Named("ratelimit"),
		now:      time.Now,
		keyed:    newKeyedLimiter(qs, cfg.LeaseFraction, cfg.ShrinkThreshold, time.Now),
		anon:     newLocalLimiter(localBucketCapacity),
		failopen: newLocalLimiter(localBucketCapacity),
		inflight: newInflight(),
		warnedAt: map[string]time.Time{},
	}
	for _, o := range opts {
		o(rl)
	}
	tiers := make([]string, 0, 1+len(cfg.Tiers))
	tiers = append(tiers, config.TierAnonymous)
	for name := range cfg.Tiers {
		tiers = append(tiers, name)
	}
	metrics.PreRegisterRateLimit(tiers)
	return rl
}

func tierFrom(name string, t config.TierConfig) Tier {
	return Tier{Name: name, UnitsPerMinute: t.UnitsPerMinute, MaxInFlight: t.MaxInFlight, MaxBatch: t.MaxBatch}
}

func (rl *RateLimiter) tierFor(p apikey.Principal) Tier {
	if p.Anonymous() {
		return tierFrom(config.TierAnonymous, rl.cfg.Anonymous)
	}
	if t, ok := rl.cfg.Tiers[p.Tier]; ok {
		return tierFrom(p.Tier, t)
	}
	rl.throttledLog(zapcore.ErrorLevel, "api key references an unconfigured tier; applying free",
		zap.String("key_id", p.KeyID), zap.String("tier", p.Tier))
	return tierFrom(config.TierFree, rl.cfg.Tiers[config.TierFree])
}

func (rl *RateLimiter) perReplica(maxInFlight int) int {
	return int(math.Ceil(float64(maxInFlight) / float64(max(rl.cfg.ExpectedReplicas, 1))))
}

// Middleware enforces the caller's tier on a route that costs units: first
// the concurrency cap (held until the handler returns), then the quota.
func (rl *RateLimiter) Middleware(units int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		p := apikey.PrincipalFrom(c)
		tier := rl.tierFor(p)
		c.Set(ctxLimiter, rl)
		c.Set(ctxTier, tier)
		if tier.MaxInFlight > 0 {
			subject := p.Subject()
			if !rl.inflight.acquire(subject, rl.perReplica(tier.MaxInFlight)) {
				rl.reject(c, tier, "concurrency", time.Second)
				return
			}
			defer rl.inflight.release(subject)
		}
		if !rl.charge(c, p, tier, units) {
			return
		}
		c.Next()
	}
}

// ChargeTxs charges n × costs.submit_tx against the caller's quota, all or
// nothing. It returns false after writing a 429, and true when allowed or
// when no limiter guards the route.
func ChargeTxs(c *gin.Context, n int) bool {
	lv, ok := c.Get(ctxLimiter)
	if !ok {
		return true
	}
	rl, ok := lv.(*RateLimiter)
	if !ok {
		return true
	}
	tier, ok := tierFromContext(c)
	if !ok {
		return true
	}
	return rl.charge(c, apikey.PrincipalFrom(c), tier, int64(n)*rl.cfg.Costs.SubmitTx)
}

// CheckBatch rejects a batch of n transactions above the tier's max_batch
// with a 413. It returns true when the batch fits or no limiter guards the route.
func CheckBatch(c *gin.Context, n int) bool {
	tier, ok := tierFromContext(c)
	if !ok {
		return true
	}
	if tier.MaxBatch == 0 || n <= tier.MaxBatch {
		return true
	}
	metrics.RateLimitDecisionsTotal.WithLabelValues(tier.Name, "batch_too_large").Inc()
	c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
		"error":    fmt.Sprintf("batch of %d transactions exceeds the %s tier limit of %d", n, tier.Name, tier.MaxBatch),
		"tier":     tier.Name,
		"maxBatch": tier.MaxBatch,
	})
	return false
}

// MaxBatch returns the caller's tier batch cap: 0 means unlimited, either
// because the tier itself has no cap or because no limiter guards the
// route at all. Handlers that parse a batch incrementally (e.g. /txs) use
// this to stop parsing as soon as the cap is exceeded, instead of waiting
// for the whole body to be read before CheckBatch can reject it.
func MaxBatch(c *gin.Context) int {
	tier, ok := tierFromContext(c)
	if !ok {
		return 0
	}
	return tier.MaxBatch
}

// RejectBatchBytesTooLarge responds 413 for a request body that overran
// its byte cap before any transaction could even be parsed out of it --
// http.MaxBytesReader's error on the initial read, not CheckBatch's own
// per-transaction-count rejection. tierCapped reports whether the cap in
// play was derived from the caller's tier (see /txs's per-tier body-size
// cap in handleSubmitTransactions) -- a large enough max_batch leaves the
// flat ceiling as the tighter cap, so MaxBatch > 0 alone doesn't say. When
// it was, the response mirrors CheckBatch's own shape
// and is metered the same way, so an operator reading
// bsv_broadcaster_ratelimit_decisions_total can't tell the two rejections apart —
// they're the same tier limit, just caught at a different point. Without a
// tier-derived cap in play (the request hit the flat maxBatchBytes ceiling
// instead), there's no tier limit to report, so the response is the plain,
// unmetered message.
func RejectBatchBytesTooLarge(c *gin.Context, tierCapped bool) {
	tier, ok := tierFromContext(c)
	if !tierCapped || !ok || tier.MaxBatch == 0 {
		c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body too large"})
		return
	}
	metrics.RateLimitDecisionsTotal.WithLabelValues(tier.Name, "batch_too_large").Inc()
	c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
		"error":    fmt.Sprintf("request body exceeds the %s tier's batch byte limit", tier.Name),
		"tier":     tier.Name,
		"maxBatch": tier.MaxBatch,
	})
}

// tierFromContext reads back the Tier Middleware stashed on c. ok is false
// when no limiter guards this route at all (ctxTier was never set).
func tierFromContext(c *gin.Context) (Tier, bool) {
	tv, ok := c.Get(ctxTier)
	if !ok {
		return Tier{}, false
	}
	tier, ok := tv.(Tier)
	return tier, ok
}

func (rl *RateLimiter) charge(c *gin.Context, p apikey.Principal, tier Tier, units int64) bool {
	if units <= 0 || tier.UnitsPerMinute == 0 {
		metrics.RateLimitDecisionsTotal.WithLabelValues(tier.Name, "allowed").Inc()
		return true
	}
	if units > tier.UnitsPerMinute {
		// A 429 here would never resolve itself: the request can never fit
		// in a full, empty window either. 413 says so plainly instead of
		// inviting a retry loop.
		rl.rejectUnpayable(c, tier, units)
		return false
	}
	d, ok := rl.takeDecision(c, p, tier, units)
	if !ok {
		return false // the client is gone: already aborted, nothing left to report
	}
	setQuotaHeaders(c, tier, d)
	if !d.Allowed {
		rl.reject(c, tier, "quota", d.Reset)
		return false
	}
	metrics.RateLimitDecisionsTotal.WithLabelValues(tier.Name, "allowed").Inc()
	return true
}

// takeDecision resolves one Decision for the caller's tier: the anonymous
// per-IP bucket, or the shared keyed quota falling open to a local bucket
// when the store errors. ok is false only when the caller's own context was
// done when the keyed charge errored — a client disconnect, not a store
// outage — and charge must abort without counting a fail-open or reporting
// anything to a reader who is no longer there.
func (rl *RateLimiter) takeDecision(c *gin.Context, p apikey.Principal, tier Tier, units int64) (Decision, bool) {
	if p.Anonymous() {
		return rl.anon.take(p.Subject(), tier.UnitsPerMinute, units, rl.now()), true
	}
	d, err := rl.keyed.take(c.Request.Context(), p.QuotaKey(), tier.UnitsPerMinute, units)
	if err == nil {
		return d, true
	}
	if c.Request.Context().Err() != nil {
		// The client is gone: 499, not a bare Abort that would otherwise
		// log and meter this as a 200.
		c.AbortWithStatus(StatusClientClosedRequest)
		return Decision{}, false
	}
	metrics.RateLimitStoreFailOpenTotal.Inc()
	rl.warn("quota store unavailable; failing open to the local bucket", zap.String("key_id", p.KeyID), zap.Error(err))
	return rl.failopen.take(p.Subject(), tier.UnitsPerMinute, units, rl.now()), true
}

// rejectUnpayable answers a charge that can never succeed under this tier,
// regardless of how empty the window is — a batch sized (or mis-configured)
// above the tier's entire per-minute budget.
func (rl *RateLimiter) rejectUnpayable(c *gin.Context, tier Tier, units int64) {
	metrics.RateLimitDecisionsTotal.WithLabelValues(tier.Name, "unpayable").Inc()
	c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
		"error": fmt.Sprintf("request costs %d units, above the %s tier's %d per minute", units, tier.Name, tier.UnitsPerMinute),
		"tier":  tier.Name,
	})
}

func (rl *RateLimiter) reject(c *gin.Context, tier Tier, reason string, retryAfter time.Duration) {
	metrics.RateLimitDecisionsTotal.WithLabelValues(tier.Name, reason).Inc()
	secs := max(ceilSeconds(retryAfter), 1)
	c.Header("Retry-After", strconv.Itoa(secs))
	c.Header(HeaderReason, reason)
	c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
		"error":             "rate limit exceeded",
		"reason":            reason,
		"tier":              tier.Name,
		"retryAfterSeconds": secs,
	})
}

// setQuotaHeaders writes the IETF draft-ietf-httpapi-ratelimit-headers-11
// fields and their X-RateLimit-* equivalents.
func setQuotaHeaders(c *gin.Context, tier Tier, d Decision) {
	reset := ceilSeconds(d.Reset)
	h := c.Writer.Header()
	h.Set("RateLimit-Policy", fmt.Sprintf("%q;q=%d;w=60", tier.Name, tier.UnitsPerMinute))
	h.Set("RateLimit", fmt.Sprintf("%q;r=%d;t=%d", tier.Name, d.Remaining, reset))
	h.Set("X-RateLimit-Limit", strconv.FormatInt(tier.UnitsPerMinute, 10))
	h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	h.Set("X-RateLimit-Reset", strconv.Itoa(reset))
}

func ceilSeconds(d time.Duration) int { return int(math.Ceil(d.Seconds())) }

// warn logs msg at Warn level, at most once per warnInterval.
func (rl *RateLimiter) warn(msg string, fields ...zap.Field) {
	rl.throttledLog(zapcore.WarnLevel, msg, fields...)
}

// throttledLog logs at most once per warnInterval per distinct msg: these
// fire per request while a condition lasts, and messages are throttled
// independently so one noisy condition can't suppress another's first log.
func (rl *RateLimiter) throttledLog(level zapcore.Level, msg string, fields ...zap.Field) {
	rl.warnMu.Lock()
	if last, ok := rl.warnedAt[msg]; ok && time.Since(last) < warnInterval {
		rl.warnMu.Unlock()
		return
	}
	rl.warnedAt[msg] = time.Now()
	rl.warnMu.Unlock()
	rl.logger.Log(level, msg, fields...)
}
