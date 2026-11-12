package usage

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/services/ratelimit"
)

const ctxTxCount = "usage.tx_count"

// SetTxCount records how many transactions a batch request was charged
// for beyond its base cost (POST /txs, once the batch is parsed and
// accepted by the limiter).
func SetTxCount(c *gin.Context, n int) { c.Set(ctxTxCount, n) }

func txCount(c *gin.Context) int64 {
	if v, ok := c.Get(ctxTxCount); ok {
		if n, ok := v.(int); ok {
			return int64(n)
		}
	}
	return 0
}

// Recorder builds the per-route middleware that counts requests into an
// Aggregator. Only keys from a provider that has a usage sink are counted:
// usage nobody receives is not worth holding.
type Recorder struct {
	agg        *Aggregator
	providers  map[string]bool
	submitCost int64
	now        func() time.Time
}

// NewRecorder counts requests by keys of the given providers. submitCost is
// costs.submit_tx, charged per transaction of a batch on top of a route's
// base cost.
func NewRecorder(agg *Aggregator, providers []string, submitCost int64) *Recorder {
	p := make(map[string]bool, len(providers))
	for _, name := range providers {
		p[name] = true
	}
	return &Recorder{agg: agg, providers: p, submitCost: submitCost, now: time.Now}
}

// Middleware counts requests to the route named endpoint, whose base cost
// is baseUnits. Install it outermost, ahead of identification and the rate
// limiter, so it sees their outcome: it records after the rest of the chain
// has run (or panicked: a panicking request is counted as 5xx, then the
// panic continues to the router's recovery), and only for a caller whose
// key resolved.
//
// Units are what the request was charged. With a rate limiter that is what
// the limiter granted (ratelimit.Charged): nothing for a request it
// refused or whose client hung up before the charge. Without one it is the
// nominal cost: baseUnits plus submitCost per transaction of an accepted
// batch. A 429 is always 0.
func (r *Recorder) Middleware(endpoint string, baseUnits int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if v := recover(); v != nil {
				r.record(c, endpoint, baseUnits, http.StatusInternalServerError)
				panic(v)
			}
		}()
		c.Next()
		r.record(c, endpoint, baseUnits, c.Writer.Status())
	}
}

func (r *Recorder) record(c *gin.Context, endpoint string, baseUnits int64, status int) {
	p := apikey.PrincipalFrom(c)
	if p.Provider == "" || !r.providers[p.Provider] {
		return
	}
	class := Classify(status)
	var units int64
	if status != http.StatusTooManyRequests {
		if charged, limited := ratelimit.Charged(c); limited {
			units = charged
		} else {
			units = baseUnits + txCount(c)*r.submitCost
		}
	}
	r.agg.Add(Bucket{
		Source:   p.Provider,
		KeyID:    p.ProviderKeyID,
		Endpoint: endpoint,
		Class:    class,
		Day:      DayOf(r.now()),
	}, units)
	metrics.UsageRequestsTotal.WithLabelValues(p.Provider, endpoint, class).Inc()
}
