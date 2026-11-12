package apikey

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

// tierNone labels decisions made before any tier is known: unauthorized
// callers and callers throttled for repeated invalid-key attempts.
const tierNone = "none"

// HeaderRateLimitReason names the response header that says why a 429 was
// returned: "auth_failures" from this package's invalid-key throttle, or
// "quota"/"concurrency" from services/ratelimit, which aliases this
// constant as HeaderReason (ratelimit imports apikey, so the shared
// definition has to live here to avoid a cycle).
const HeaderRateLimitReason = "X-RateLimit-Reason"

// StatusClientClosedRequest is nginx's convention for "the client hung up
// before we responded." net/http has no constant for it. Defined here (not
// in services/ratelimit, which uses it too) so both packages share one
// definition -- ratelimit imports apikey, so the constant has to live on
// this side to avoid a cycle. See the Resolve-error handling below, and
// ratelimit.takeDecision for the other caller of it.
const StatusClientClosedRequest = 499

// Middleware identifies the caller. No Authorization header (or an
// Authorization header present but empty -- net/http's Header.Get
// canonicalizes both to "", so the two cannot be and need not be told
// apart) makes it anonymous. More than one Authorization header, or a
// header that is not a valid, usable key, gets a 401 and never falls back
// to anonymous. A client IP that has racked up too many invalid attempts
// gets a 429 before its key is even looked at -- unless the presented key is
// already a fresh cache hit, in which case it is let through regardless, so
// one bad-faith client cannot lock out another caller sharing its IP. A key
// that cannot be verified because the store is down gets a 503.
func Middleware(r *Resolver) gin.HandlerFunc {
	return newMiddleware(r, newAuthFailureLimiter(authFailureBucketSize, authFailureLimitPerMinute, authFailureBurst))
}

// newMiddleware is Middleware with an injectable failure limiter, so tests
// can use a throttle small enough to trip in a handful of requests instead
// of sixty.
func newMiddleware(r *Resolver, failures *authFailureLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Read every Authorization header value up front: GetHeader only
		// ever returns the first, so checking len() after calling it would
		// miss a second header riding behind an empty (or no) first value.
		vals := c.Request.Header.Values("Authorization")
		if len(vals) == 0 || (len(vals) == 1 && vals[0] == "") {
			// No header, or a header present but empty -- net/http
			// canonicalizes both to the same thing, so we cannot (and need
			// not) tell them apart: either way the caller is anonymous.
			WithPrincipal(c, Principal{Tier: config.TierAnonymous, ClientIP: c.ClientIP()})
			c.Next()
			return
		}

		ip := c.ClientIP()

		// A single header presenting a key already proven good -- fresh in
		// the resolver's own cache -- is resolved without ever touching the
		// throttle: validating it costs nothing beyond a cache lookup, so
		// another client's bad attempts sharing this IP must never block a
		// caller who already has a working key. Resolve is called at most
		// once per request regardless of which path below is taken: if this
		// attempt fails (the cache entry expired or was invalidated between
		// the two checks here -- a narrow race), its own err is what the
		// normal, throttled path below reacts to, never a second Resolve
		// call.
		var (
			fastPathErr   error
			fastPathTried bool
		)
		if len(vals) == 1 {
			if raw, ok := bearerToken(vals[0]); ok && r.cachedUsable(raw) {
				k, err := r.Resolve(c.Request.Context(), raw)
				if err == nil {
					authenticated(c, ip, k)
					return
				}
				fastPathErr, fastPathTried = err, true
			}
		}

		now := time.Now()
		if tokens, retryAfter := failures.check(ip, now); tokens < 1 {
			tooManyAuthFailures(c, retryAfter)
			return
		}
		if len(vals) > 1 {
			// Never pick one of several Authorization headers and silently
			// ignore the rest.
			unauthorized(c, failures, ip, now)
			return
		}
		raw, ok := bearerToken(vals[0])
		if !ok {
			unauthorized(c, failures, ip, now)
			return
		}

		var err error
		var k *store.APIKey
		if fastPathTried {
			err = fastPathErr
		} else {
			k, err = r.Resolve(c.Request.Context(), raw)
		}
		switch {
		case err == nil:
			authenticated(c, ip, k)
		case errors.Is(err, ErrUnavailable):
			if c.Request.Context().Err() != nil {
				// The client is gone, not the store: 499, not a 503 that
				// nobody is left to retry on.
				c.AbortWithStatus(StatusClientClosedRequest)
				return
			}
			c.Header("Retry-After", "5")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "api key verification temporarily unavailable"})
		default:
			unauthorized(c, failures, ip, now)
		}
	}
}

// authenticated records the resolved key as the request's principal and
// lets the request proceed.
func authenticated(c *gin.Context, ip string, k *store.APIKey) {
	WithPrincipal(c, Principal{
		KeyID: k.ID, QuotaID: k.QuotaID(), Tier: k.Tier, ClientIP: ip,
		Provider: k.Provider, ProviderKeyID: k.ProviderKeyID,
	})
	attrs := []attribute.KeyValue{attribute.String("key_id", k.ID), attribute.String("tier", k.Tier)}
	if k.LimitID != "" {
		attrs = append(attrs, attribute.String("limit_id", k.LimitID))
	}
	trace.SpanFromContext(c.Request.Context()).SetAttributes(attrs...)
	c.Next()
}

// unauthorized responds 401 and counts the attempt against ip's
// invalid-key throttle. Only this path (malformed, unknown, revoked or
// expired keys, and malformed presentation such as multiple Authorization
// headers) consumes a token; valid keys and 503s never do.
func unauthorized(c *gin.Context, failures *authFailureLimiter, ip string, now time.Time) {
	failures.fail(ip, now)
	metrics.RateLimitDecisionsTotal.WithLabelValues(tierNone, "unauthorized").Inc()
	c.Header("WWW-Authenticate", `Bearer realm="api", error="invalid_token"`)
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
}

// tooManyAuthFailures responds 429 for a client IP that has exhausted its
// invalid-key budget, before its current attempt is even looked at.
func tooManyAuthFailures(c *gin.Context, retryAfter time.Duration) {
	metrics.RateLimitDecisionsTotal.WithLabelValues(tierNone, "auth_failures").Inc()
	c.Header("Retry-After", strconv.Itoa(retrySeconds(retryAfter)))
	c.Header(HeaderRateLimitReason, "auth_failures")
	c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "too many invalid api key attempts"})
}

// retrySeconds rounds d up to a whole number of seconds, floored at 1.
func retrySeconds(d time.Duration) int {
	s := int(math.Ceil(d.Seconds()))
	if s < 1 {
		return 1
	}
	return s
}

// bearerToken parses a single, non-empty Authorization header value. ok is
// false for any scheme other than "bearer" (case-insensitively) or an empty
// token.
func bearerToken(h string) (token string, ok bool) {
	const scheme = "bearer "
	if len(h) <= len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return "", false
	}
	token = strings.TrimSpace(h[len(scheme):])
	return token, token != ""
}
