package usage

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const testToken = "test-service-token"

// fakeReceiver records every usage call and answers with the next scripted
// response (status, Retry-After, body), repeating the last one.
type fakeReceiver struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	script   []response
	calls    []call
	recorded map[string]int64 // key_id|endpoint|class|day -> count, for 200s only
}

type response struct {
	status     int
	retryAfter string
	body       string
	hang       time.Duration // sleep before answering (timeouts after the write)
}

type call struct {
	auth, idem string
	entries    []wireEntry
}

func newReceiver(t *testing.T, script ...response) *fakeReceiver {
	t.Helper()
	f := &fakeReceiver{t: t, script: script, recorded: map[string]int64{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var b wireBatch
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Errorf("undecodable usage batch: %v", err)
		}
		f.mu.Lock()
		resp := f.script[min(len(f.calls), len(f.script)-1)]
		f.calls = append(f.calls, call{auth: r.Header.Get("Authorization"), idem: r.Header.Get("Idempotency-Key"), entries: b.Entries})
		if resp.status == http.StatusOK {
			for _, e := range b.Entries {
				f.recorded[e.KeyID+"|"+e.Endpoint+"|"+e.StatusClass+"|"+e.DateUTC] += e.Count
			}
		}
		f.mu.Unlock()
		if resp.hang > 0 {
			time.Sleep(resp.hang)
		}
		if resp.retryAfter != "" {
			w.Header().Set("Retry-After", resp.retryAfter)
		}
		w.WriteHeader(resp.status)
		body := resp.body
		if body == "" && resp.status == http.StatusOK {
			body = fmt.Sprintf(`{"accepted":%d,"dropped":0}`, len(b.Entries))
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeReceiver) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newSink(t *testing.T, url string, timeout time.Duration, maxPending int) (*HTTPSink, *clock) {
	t.Helper()
	client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s := NewHTTPSink(testSource, url, testToken, "ttn", client, maxPending, zap.NewNop())
	clk := &clock{t: time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)}
	s.now = clk.now
	return s, clk
}

func entries(n int, key string) []Entry {
	out := make([]Entry, n)
	for i := range out {
		out[i] = Entry{
			Bucket: Bucket{Source: testSource, KeyID: fmt.Sprintf("%s-%d", key, i), Endpoint: testEndpoint, Class: Class2xx, Day: "2026-11-12"},
			Totals: Totals{Count: 3, Units: 30},
		}
	}
	return out
}

// TestHTTPSinkRequestShape pins the protocol: bearer token, idempotency key,
// and the entry fields, with the provider's own key id and the request's day.
func TestHTTPSinkRequestShape(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusOK})
	s, _ := newSink(t, f.srv.URL, time.Second, 1000)
	s.Send(context.Background(), []Entry{{
		Bucket: Bucket{Source: testSource, KeyID: "c1566bac", Endpoint: "v1/txs", Class: Class429, Day: "2026-11-11"},
		Totals: Totals{Count: 2, Units: 0},
	}})
	require.Len(t, f.calls, 1)
	require.Equal(t, "Bearer "+testToken, f.calls[0].auth)
	require.NotEmpty(t, f.calls[0].idem)
	require.Equal(t, []wireEntry{{KeyID: "c1566bac", Network: "ttn", Endpoint: "v1/txs", StatusClass: "429", DateUTC: "2026-11-11", Count: 2, Units: 0}}, f.calls[0].entries)
	require.Zero(t, s.Pending())
}

func TestHTTPSinkChunks(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusOK})
	s, _ := newSink(t, f.srv.URL, time.Second, 10_000)
	s.Send(context.Background(), entries(2500, "k"))
	require.Len(t, f.calls, 3)
	for _, c := range f.calls {
		require.LessOrEqual(t, len(c.entries), maxChunkEntries)
		body, err := json.Marshal(wireBatch{Entries: c.entries})
		require.NoError(t, err)
		require.LessOrEqual(t, len(body), maxChunkBytes)
	}
	require.Len(t, f.recorded, 2500)
	require.NotEqual(t, f.calls[0].idem, f.calls[1].idem)
}

// TestHTTPSinkRetriesWhatWasNotRecorded pins that a 503 keeps the chunk
// intact (same idempotency key) and resends it only after Retry-After, and
// that totals add up exactly once across the retry.
func TestHTTPSinkRetriesWhatWasNotRecorded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first response
		pause time.Duration
	}{
		{"503 with Retry-After", response{status: http.StatusServiceUnavailable, retryAfter: "60", body: `{"retry":true}`}, 60 * time.Second},
		{"503 without Retry-After", response{status: http.StatusServiceUnavailable}, pauseRetry},
		{"429", response{status: http.StatusTooManyRequests, retryAfter: "7"}, 7 * time.Second},
		{"401", response{status: http.StatusUnauthorized}, pauseUnauthorized},
		{"403", response{status: http.StatusForbidden}, pauseUnauthorized},
		{"404 (usage not enabled yet)", response{status: http.StatusNotFound}, pauseUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReceiver(t, tc.first, response{status: http.StatusOK})
			s, clk := newSink(t, f.srv.URL, time.Second, 1000)
			s.Send(context.Background(), entries(2, "k"))
			require.Equal(t, 1, f.callCount())
			require.Equal(t, 2, s.Pending(), "kept for a retry")

			clk.advance(tc.pause - time.Second)
			s.Send(context.Background(), nil)
			require.Equal(t, 1, f.callCount(), "not resent before the pause ends")

			clk.advance(time.Second)
			s.Send(context.Background(), entries(1, "new"))
			require.Equal(t, 3, f.callCount(), "the kept chunk first, then the new one")
			require.Equal(t, f.calls[0].idem, f.calls[1].idem, "a retried chunk keeps its idempotency key")
			require.Equal(t, f.calls[0].entries, f.calls[1].entries, "a retried chunk is resent intact")
			require.Zero(t, s.Pending())
			require.Equal(t, map[string]int64{
				"k-0|v1/tx|2xx|2026-11-12": 3, "k-1|v1/tx|2xx|2026-11-12": 3, "new-0|v1/tx|2xx|2026-11-12": 3,
			}, f.recorded, "each bucket recorded exactly once")
		})
	}
}

// TestHTTPSinkNeverResendsWhatMayHaveBeenRecorded pins at-most-once: a 502,
// another 5xx, or a timeout after the request was written drops the chunk.
func TestHTTPSinkNeverResendsWhatMayHaveBeenRecorded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first response
	}{
		{"502", response{status: http.StatusBadGateway, body: `{"retry":false}`}},
		{"500", response{status: http.StatusInternalServerError}},
		{"timeout after the write", response{status: http.StatusOK, hang: 300 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReceiver(t, tc.first, response{status: http.StatusOK})
			s, clk := newSink(t, f.srv.URL, 100*time.Millisecond, 1000)
			s.Send(context.Background(), entries(2, "k"))
			require.Zero(t, s.Pending(), "dropped, not kept")
			clk.advance(time.Hour)
			s.Send(context.Background(), nil)
			require.Equal(t, 1, f.callCount(), "never resent")
		})
	}
}

func TestHTTPSinkConnectionFailureBeforeWriteIsRetried(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusOK})
	url := f.srv.URL
	f.srv.Close() // nothing listening: the request is never written
	s, clk := newSink(t, url, time.Second, 1000)
	s.Send(context.Background(), entries(2, "k"))
	require.Equal(t, 2, s.Pending())

	g := newReceiver(t, response{status: http.StatusOK})
	s.url = g.srv.URL
	clk.advance(pauseNotSent)
	s.Send(context.Background(), nil)
	require.Zero(t, s.Pending())
	require.Len(t, g.recorded, 2)
}

func TestHTTPSinkBadRequestDropsOnlyThatChunk(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusBadRequest, body: `{"error":"bad"}`}, response{status: http.StatusOK})
	s, _ := newSink(t, f.srv.URL, time.Second, 10_000)
	s.Send(context.Background(), entries(1500, "k")) // two chunks
	require.Equal(t, 2, f.callCount(), "the next chunk still goes")
	require.Zero(t, s.Pending())
	require.Len(t, f.recorded, 500)
}

func TestHTTPSinkNotImplementedDisables(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusNotImplemented})
	s, clk := newSink(t, f.srv.URL, time.Second, 1000)
	s.Send(context.Background(), entries(1, "k"))
	clk.advance(time.Hour)
	s.Send(context.Background(), entries(1, "k"))
	require.Equal(t, 1, f.callCount(), "a receiver that collects nothing is not called again")
	require.Zero(t, s.Pending())
}

func TestHTTPSinkPendingIsBounded(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusServiceUnavailable})
	s, _ := newSink(t, f.srv.URL, time.Second, 1500)
	s.Send(context.Background(), entries(1000, "old"))
	s.Send(context.Background(), entries(1000, "new")) // paused: queued behind
	require.Equal(t, 1000, s.Pending(), "the oldest chunk was dropped to stay under the bound")
	require.Equal(t, "new-0", s.pending[0].entries[0].KeyID)
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	require.Equal(t, 60*time.Second, retryAfter("60", now))
	require.Equal(t, pauseMaxRetry, retryAfter("99999999999", now))
	require.Equal(t, 30*time.Second, retryAfter("Thu, 12 Nov 2026 12:00:30 GMT", now))
	require.Equal(t, pauseRetry, retryAfter("", now))
	require.Equal(t, pauseRetry, retryAfter("soon", now))
}

// TestHTTPSinkDeliversBeforeBoundingTheQueue pins that max_pending_entries
// only bounds what is left undelivered: a healthy receiver gets a flush
// larger than the bound.
func TestHTTPSinkDeliversBeforeBoundingTheQueue(t *testing.T) {
	f := newReceiver(t, response{status: http.StatusOK})
	s, _ := newSink(t, f.srv.URL, time.Second, 100)
	s.Send(context.Background(), entries(101, "k"))
	require.Len(t, f.recorded, 101)
	require.Zero(t, s.Pending())
}

// TestHTTPSinkIgnoresImplausibleCounts pins that a 200 whose counts don't
// add up (negative, or more than were sent) is treated as all accepted
// instead of panicking a counter.
func TestHTTPSinkIgnoresImplausibleCounts(t *testing.T) {
	for _, body := range []string{`{"accepted":-1,"dropped":0}`, `{"accepted":0,"dropped":-5}`, `{"accepted":99,"dropped":0}`} {
		f := newReceiver(t, response{status: http.StatusOK, body: body})
		s, _ := newSink(t, f.srv.URL, time.Second, 100)
		require.NotPanics(t, func() { s.Send(context.Background(), entries(2, "k")) }, body)
		require.Zero(t, s.Pending(), body)
	}
}

func TestNotSent(t *testing.T) {
	require.True(t, notSent(&url.Error{Op: "Post", Err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}}))
	require.True(t, notSent(&url.Error{Op: "Post", Err: &net.DNSError{Err: "no such host"}}))
	require.True(t, notSent(&url.Error{Op: "Post", Err: x509.UnknownAuthorityError{}}))
	require.False(t, notSent(&url.Error{Op: "Post", Err: &net.OpError{Op: "read", Err: errors.New("connection reset")}}), "a reset may follow the write")
	require.False(t, notSent(&url.Error{Op: "Post", Err: context.DeadlineExceeded}), "a timeout may follow the write")
	require.False(t, notSent(io.ErrUnexpectedEOF))
}
