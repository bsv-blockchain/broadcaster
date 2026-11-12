package usage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeSink records what each Send received.
type fakeSink struct {
	name string

	mu      sync.Mutex
	sends   [][]Entry
	dropped string
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Send(_ context.Context, e []Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, e)
}

func (f *fakeSink) DropPending(reason string) { f.mu.Lock(); f.dropped = reason; f.mu.Unlock() }

func (f *fakeSink) all() []Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Entry
	for _, s := range f.sends {
		out = append(out, s...)
	}
	return out
}

func TestReporterRoutesEachProviderItsOwnEntries(t *testing.T) {
	agg := NewAggregator(100)
	acme, other := &fakeSink{name: testSource}, &fakeSink{name: "other"}
	r := NewReporter(agg, []Sink{acme, other}, time.Hour, zap.NewNop())
	agg.Add(Bucket{Source: testSource, KeyID: "a", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}, 10)
	agg.Add(Bucket{Source: "other", KeyID: "o", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}, 10)

	r.Flush(context.Background())
	require.Len(t, acme.all(), 1)
	require.Equal(t, "a", acme.all()[0].KeyID)
	require.Len(t, other.all(), 1)
	require.Equal(t, "o", other.all()[0].KeyID)

	r.Flush(context.Background())
	acme.mu.Lock()
	require.Len(t, acme.sends, 2, "a sink is called even with nothing new, so its pending queue is retried")
	acme.mu.Unlock()
}

// TestReporterCloseFlushesTheRest pins that requests counted before Close
// are reported by the final flush, and that anything still undelivered is
// dropped as "shutdown".
func TestReporterCloseFlushesTheRest(t *testing.T) {
	agg := NewAggregator(100)
	acme := &fakeSink{name: testSource}
	r := NewReporter(agg, []Sink{acme}, time.Hour, zap.NewNop())
	r.Start(context.Background())
	agg.Add(Bucket{Source: testSource, KeyID: "a", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}, 10)
	r.Close(context.Background())
	require.Len(t, acme.all(), 1)
	require.Equal(t, "shutdown", acme.dropped)
	require.NotPanics(t, func() { r.Close(context.Background()) }, "Stop can run twice")
	require.Len(t, acme.all(), 1, "a second Close does nothing")
}

func TestReporterFlushesEarlyWhenFull(t *testing.T) {
	agg := NewAggregator(1)
	acme := &fakeSink{name: testSource}
	r := NewReporter(agg, []Sink{acme}, time.Hour, zap.NewNop())
	r.Start(context.Background())
	defer r.Close(context.Background())
	agg.Add(Bucket{Source: testSource, KeyID: "a", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}, 1)
	agg.Add(Bucket{Source: testSource, KeyID: "b", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}, 1) // full
	require.Eventually(t, func() bool { return len(acme.all()) == 1 }, time.Second, 5*time.Millisecond)
}
