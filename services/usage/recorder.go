package usage

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
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
// has run, and only for a caller whose key resolved.
//
// Units are what the request was charged: the base cost, plus submitCost
// per transaction of an accepted batch, and 0 for a 429 (the limiter
// granted nothing worth billing).
func (r *Recorder) Middleware(endpoint string, baseUnits int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		p := apikey.PrincipalFrom(c)
		if p.Provider == "" || !r.providers[p.Provider] {
			return
		}
		status := c.Writer.Status()
		class := Classify(status)
		var units int64
		if status != http.StatusTooManyRequests {
			units = baseUnits + txCount(c)*r.submitCost
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
}
