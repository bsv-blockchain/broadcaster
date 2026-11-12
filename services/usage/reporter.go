package usage

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Sink receives one provider's usage.
type Sink interface {
	Name() string
	Send(ctx context.Context, entries []Entry)
	DropPending(reason string)
}

// Reporter drains the Aggregator into the sinks every interval (±10%
// jitter, so replicas don't flush in step), early when the aggregator
// fills up, and one last time on Close.
type Reporter struct {
	agg      *Aggregator
	sinks    map[string]Sink
	interval time.Duration
	logger   *zap.Logger

	flushMu   sync.Mutex // one flush at a time: sinks are single-threaded
	stop      chan struct{}
	done      chan struct{}
	started   bool
	closeOnce sync.Once
}

// NewReporter builds a Reporter; sinks are keyed by provider name.
func NewReporter(agg *Aggregator, sinks []Sink, interval time.Duration, logger *zap.Logger) *Reporter {
	m := make(map[string]Sink, len(sinks))
	for _, s := range sinks {
		m[s.Name()] = s
	}
	return &Reporter{
		agg: agg, sinks: m, interval: interval, logger: logger.Named("usage"),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// Start runs the flush loop until Close.
func (r *Reporter) Start(ctx context.Context) {
	r.started = true
	go func() {
		defer close(r.done)
		timer := time.NewTimer(r.jittered())
		defer timer.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ctx.Done():
				return
			case <-timer.C:
				r.Flush(ctx)
				timer.Reset(r.jittered())
			case <-r.agg.Full():
				r.Flush(ctx)
			}
		}
	}()
}

func (r *Reporter) jittered() time.Duration {
	j := float64(r.interval) * (0.9 + 0.2*rand.Float64()) //nolint:gosec // scheduling jitter, not security
	return time.Duration(j)
}

// Flush drains the aggregator and hands each provider its entries. A sink
// with nothing new still gets a call, so its pending queue is retried.
func (r *Reporter) Flush(ctx context.Context) {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	by := make(map[string][]Entry, len(r.sinks))
	for _, e := range r.agg.Drain() {
		by[e.Source] = append(by[e.Source], e)
	}
	for name, s := range r.sinks {
		s.Send(ctx, by[name])
	}
}

// Close stops the loop, then flushes once more within ctx (call it after
// the HTTP server has drained, so the last requests are counted). Whatever
// is still undelivered is dropped and counted. Safe to call more than once
// (the api-server's Stop can run twice); only the first call does anything.
func (r *Reporter) Close(ctx context.Context) {
	r.closeOnce.Do(func() {
		close(r.stop)
		if r.started {
			<-r.done
		}
		r.Flush(ctx)
		r.flushMu.Lock()
		defer r.flushMu.Unlock()
		for _, s := range r.sinks {
			s.DropPending("shutdown")
		}
	})
}
