package ratelimit

import (
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/time/rate"
)

// localLimiter is a per-replica token bucket per subject, refilling at
// perMinute/60 units a second and holding up to burst units. It backs the
// anonymous tier (keyed by client IP, so spraying IPs costs the shared store
// nothing) and the fail-open path for keyed tiers. Nothing is shared across
// replicas, so a subject spread over N replicas can get up to N × perMinute.
type localLimiter struct {
	mu      sync.Mutex
	buckets *lru.Cache[string, *rate.Limiter]
}

func newLocalLimiter(size int) *localLimiter {
	c, _ := lru.New[string, *rate.Limiter](size) // errors only for size <= 0
	return &localLimiter{buckets: c}
}

// take spends cost units from subject's bucket, creating it — or resizing
// it, when perMinute or burst no longer match what it was built with — at
// full burst. That makes a tier change (and a subject's very first request)
// optimistic: the subject gets a full fresh allowance rather than a
// carry-over of whatever was left under the old limit. The same applies to
// the fail-open bucket: the first request after the shared store goes down
// finds a full bucket, not an empty one.
func (l *localLimiter) take(subject string, perMinute, burst, cost int64, now time.Time) Decision {
	l.mu.Lock()
	lim, ok := l.buckets.Get(subject)
	every := rate.Limit(float64(perMinute) / 60)
	if !ok || lim.Burst() != int(burst) || lim.Limit() != every {
		lim = rate.NewLimiter(every, int(burst))
		l.buckets.Add(subject, lim)
	}
	l.mu.Unlock()

	allowed := lim.AllowN(now, int(cost))
	tokens := lim.TokensAt(now)
	d := Decision{Allowed: allowed, Remaining: max(int64(math.Floor(tokens)), 0)}
	// Denied: time until this cost fits again. Allowed: time until full.
	missing := float64(cost) - tokens
	if allowed {
		missing = float64(burst) - tokens
	}
	if missing > 0 {
		d.Reset = time.Duration(missing / (float64(perMinute) / 60) * float64(time.Second))
	}
	return d
}
