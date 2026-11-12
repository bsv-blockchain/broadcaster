// Package usage meters API usage per key and reports it to the
// delegated-auth provider that issued the key (auth.introspection.<name>.
// usage_url). Requests are counted per (key, route, status class, UTC day)
// in memory on each replica and pushed in batches; counts add up at the
// receiver, so replicas need no coordination. See docs/api-keys.md.
package usage

import (
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/bsv-blockchain/broadcaster/metrics"
)

// Status classes reported for a request.
const (
	Class2xx = "2xx"
	Class4xx = "4xx"
	Class5xx = "5xx"
	Class429 = "429"
)

// Classify maps an HTTP status to its reported class. 429 is its own class
// so a receiver can tell a customer hitting its limit from other client
// errors.
func Classify(status int) string {
	switch {
	case status == http.StatusTooManyRequests:
		return Class429
	case status >= http.StatusInternalServerError:
		return Class5xx
	case status >= http.StatusBadRequest:
		return Class4xx
	default:
		return Class2xx
	}
}

// endpointRe is the route-name shape reported as "endpoint": lowercase
// segments of letters, digits, "-" and "_", one to three of them. A name is
// a stable route name, never a path: it can't carry an id.
var endpointRe = regexp.MustCompile(`^[a-z0-9_-]+(/[a-z0-9_-]+){0,2}$`)

// ValidEndpoint reports whether name may be reported as an endpoint: the
// shape above and at most 64 characters.
func ValidEndpoint(name string) bool { return len(name) <= 64 && endpointRe.MatchString(name) }

// Bucket is one aggregation cell. Source names the provider the key belongs
// to and KeyID is that provider's own key id.
type Bucket struct {
	Source   string
	KeyID    string
	Endpoint string
	Class    string
	Day      string // UTC YYYY-MM-DD the request finished
}

// Totals is what a Bucket accumulated.
type Totals struct {
	Count int64
	Units int64
}

// Entry is one reported row.
type Entry struct {
	Bucket
	Totals
}

// DayOf is the reported day of a request finishing at t.
func DayOf(t time.Time) string { return t.UTC().Format(time.DateOnly) }

// Aggregator accumulates Totals per Bucket for one replica. It is bounded:
// once it holds maxBuckets distinct buckets, a request for a new bucket is
// dropped (and counted) and an early flush is signaled; existing buckets
// keep counting.
type Aggregator struct {
	maxBuckets int
	full       chan struct{} // signaled (non-blocking) when the map fills

	mu      sync.Mutex
	buckets map[Bucket]*Totals
}

// NewAggregator builds an Aggregator holding at most maxBuckets buckets.
func NewAggregator(maxBuckets int) *Aggregator {
	return &Aggregator{
		maxBuckets: maxBuckets,
		full:       make(chan struct{}, 1),
		buckets:    make(map[Bucket]*Totals),
	}
}

// Add counts one request with the units it was charged.
func (a *Aggregator) Add(b Bucket, units int64) {
	a.mu.Lock()
	t, ok := a.buckets[b]
	if !ok {
		if len(a.buckets) >= a.maxBuckets {
			a.mu.Unlock()
			metrics.UsageEntriesDroppedTotal.WithLabelValues(b.Source, "overflow").Inc()
			a.signalFull()
			return
		}
		t = &Totals{}
		a.buckets[b] = t
		metrics.UsageBuckets.Set(float64(len(a.buckets)))
	}
	t.Count++
	t.Units += units
	a.mu.Unlock()
}

func (a *Aggregator) signalFull() {
	select {
	case a.full <- struct{}{}:
	default:
	}
}

// Full is signaled when the aggregator reaches its bucket limit.
func (a *Aggregator) Full() <-chan struct{} { return a.full }

// Drain returns every accumulated entry and starts afresh.
func (a *Aggregator) Drain() []Entry {
	a.mu.Lock()
	old := a.buckets
	a.buckets = make(map[Bucket]*Totals, len(old))
	a.mu.Unlock()
	metrics.UsageBuckets.Set(0)
	out := make([]Entry, 0, len(old))
	for b, t := range old {
		out = append(out, Entry{Bucket: b, Totals: *t})
	}
	return out
}
