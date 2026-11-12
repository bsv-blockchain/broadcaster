package usage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/metrics"
)

const (
	maxChunkEntries = 1000
	maxChunkBytes   = 1 << 20
	maxRespBody     = 64 << 10

	pauseRetry        = 60 * time.Second // 503/429 without Retry-After
	pauseMaxRetry     = 5 * time.Minute  // cap on a receiver's Retry-After
	pauseUnauthorized = 5 * time.Minute  // 401/403: needs a config fix
	pauseNotSent      = 30 * time.Second // connection failed before the request was written
	pauseUncertain    = 30 * time.Second // 502, other 5xx, timeout after writing
	logEvery          = 10 * time.Second
)

// Flush results for metrics.UsageFlushTotal.
const (
	resultOK           = "ok"
	resultRetry        = "retry"
	resultUnauthorized = "unauthorized"
	resultUncertain    = "uncertain"
	resultRejected     = "rejected"
	resultDisabled     = "disabled"
)

// wireEntry is one row of the usage protocol.
type wireEntry struct {
	KeyID       string `json:"key_id"`
	Endpoint    string `json:"endpoint"`
	StatusClass string `json:"status_class"`
	DateUTC     string `json:"date_utc"`
	Count       int64  `json:"count"`
	Units       int64  `json:"units"`
}

type wireBatch struct {
	Entries []wireEntry `json:"entries"`
}

type wireResult struct {
	Accepted int64 `json:"accepted"`
	Dropped  int64 `json:"dropped"`
}

// chunk is one request's worth of entries, kept intact (with its
// idempotency key) while it waits for a retry.
type chunk struct {
	key     string
	entries []wireEntry
	body    []byte
}

// HTTPSink delivers a provider's usage to its usage endpoint:
//
//	POST <url>
//	Authorization: Bearer <service token>
//	Idempotency-Key: <stable per chunk>
//	{"entries": [{"key_id","endpoint","status_class","date_utc","count","units"}]}
//
// at most 1000 entries and 1 MiB per call. Counts add up at the receiver,
// so delivery is at most once: a chunk the receiver may already have
// recorded (502, another 5xx, a timeout once the request was written) is
// dropped and counted, never resent. A chunk it certainly did not record
// (503, 429, 401/403, a connection that failed before the request was
// written) is kept, intact, in a bounded pending queue and retried oldest
// first on a later flush. 501 disables the sink until restart.
//
// Send is not safe for concurrent use; the Reporter calls it from one
// goroutine.
type HTTPSink struct {
	name       string
	url        string
	token      string
	client     *http.Client
	logger     *zap.Logger
	maxPending int
	now        func() time.Time

	prefix      string
	seq         uint64
	pending     []chunk
	pendingRows int
	pausedUntil time.Time
	disabled    bool
	loggedAt    map[string]time.Time
}

// NewHTTPSink builds the sink for provider name. client must not follow
// redirects (the token travels with every call).
func NewHTTPSink(name, url, token string, client *http.Client, maxPending int, logger *zap.Logger) *HTTPSink {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return &HTTPSink{
		name: name, url: url, token: token, client: client, maxPending: maxPending,
		logger:   logger.Named("usage").With(zap.String("provider", name)),
		now:      time.Now,
		prefix:   hex.EncodeToString(b[:]),
		loggedAt: map[string]time.Time{},
	}
}

// Name is the provider this sink reports for.
func (s *HTTPSink) Name() string { return s.name }

// Pending is how many entries are waiting for a retry.
func (s *HTTPSink) Pending() int { return s.pendingRows }

// Send queues entries behind any pending chunks and delivers as many as the
// receiver's state allows. Entries must all belong to this sink's provider.
func (s *HTTPSink) Send(ctx context.Context, entries []Entry) {
	if s.disabled {
		s.drop(len(entries), resultDisabled)
		return
	}
	s.enqueue(entries)
	for len(s.pending) > 0 && ctx.Err() == nil && !s.now().Before(s.pausedUntil) && !s.disabled {
		c := s.pending[0]
		if !s.deliver(ctx, c) {
			break
		}
	}
	metrics.UsagePendingEntries.WithLabelValues(s.name).Set(float64(s.pendingRows))
}

// DropPending discards everything still queued (shutdown).
func (s *HTTPSink) DropPending(reason string) {
	s.drop(s.pendingRows, reason)
	s.pending, s.pendingRows = nil, 0
	metrics.UsagePendingEntries.WithLabelValues(s.name).Set(0)
}

// enqueue chunks entries onto the pending queue, then trims the oldest
// chunks past maxPending.
func (s *HTTPSink) enqueue(entries []Entry) {
	const envelope = len(`{"entries":[]}`)
	cur := make([]wireEntry, 0, min(len(entries), maxChunkEntries))
	size := envelope
	flush := func() {
		if len(cur) == 0 {
			return
		}
		body, err := json.Marshal(wireBatch{Entries: cur})
		if err != nil { // plain strings and ints: cannot happen
			s.drop(len(cur), "invalid")
			cur, size = cur[:0:0], envelope
			return
		}
		s.seq++
		s.pending = append(s.pending, chunk{key: fmt.Sprintf("%s-%s-%d", s.name, s.prefix, s.seq), entries: cur, body: body})
		s.pendingRows += len(cur)
		cur, size = make([]wireEntry, 0, maxChunkEntries), envelope
	}
	for _, e := range entries {
		w := wireEntry{KeyID: e.KeyID, Endpoint: e.Endpoint, StatusClass: e.Class, DateUTC: e.Day, Count: e.Count, Units: e.Units}
		row, err := json.Marshal(w)
		if err != nil { // plain strings and ints: cannot happen
			s.drop(1, "invalid")
			continue
		}
		if len(cur) == maxChunkEntries || size+len(row)+1 > maxChunkBytes {
			flush()
		}
		cur = append(cur, w)
		size += len(row) + 1
	}
	flush()
	for s.pendingRows > s.maxPending && len(s.pending) > 0 {
		s.drop(len(s.pending[0].entries), "overflow")
		s.pendingRows -= len(s.pending[0].entries)
		s.pending = s.pending[1:]
	}
}

// deliver sends the head chunk and applies the outcome. It reports whether
// the next chunk may be sent now.
func (s *HTTPSink) deliver(ctx context.Context, c chunk) bool {
	wrote := false
	trace := &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { wrote = true }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, s.url, bytes.NewReader(c.body))
	if err != nil {
		s.pop(resultRejected, "invalid")
		s.log("usage request could not be built", zap.Error(err))
		return true
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", c.key)

	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil && !wrote {
			return false // shutting down before sending: leave it queued
		}
		if !wrote {
			s.pause(pauseNotSent, resultRetry)
			s.log("usage report not sent; will retry", zap.Error(err))
			return false
		}
		s.pop(resultUncertain, "uncertain")
		s.pause(pauseUncertain, "")
		s.log("usage report outcome unknown; dropped rather than risk counting twice", zap.Error(err))
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxRespBody))

	switch code := resp.StatusCode; code {
	case http.StatusOK:
		var r wireResult
		if err := json.Unmarshal(raw, &r); err != nil {
			r.Accepted = int64(len(c.entries)) // recorded; the body is informational
		}
		s.pending = s.pending[1:]
		s.pendingRows -= len(c.entries)
		metrics.UsageFlushTotal.WithLabelValues(s.name, resultOK).Inc()
		metrics.UsageEntriesSentTotal.WithLabelValues(s.name).Add(float64(r.Accepted))
		if r.Dropped > 0 {
			metrics.UsageEntriesDroppedTotal.WithLabelValues(s.name, "rejected").Add(float64(r.Dropped))
			s.log("usage receiver dropped entries as invalid", zap.Int64("dropped", r.Dropped))
		}
		return true
	case http.StatusServiceUnavailable, http.StatusTooManyRequests:
		s.pause(retryAfter(resp.Header.Get("Retry-After"), s.now()), resultRetry)
		return false
	case http.StatusUnauthorized, http.StatusForbidden:
		s.pause(pauseUnauthorized, resultUnauthorized)
		s.log("usage receiver rejected the service token; check auth.introspection token", zap.Int("http_status", code))
		return false
	case http.StatusNotImplemented:
		s.disabled = true
		metrics.UsageFlushTotal.WithLabelValues(s.name, resultDisabled).Inc()
		s.DropPending(resultDisabled)
		s.logger.Warn("usage receiver does not collect usage (501); usage reporting disabled until restart")
		return false
	case http.StatusBadRequest:
		s.pop(resultRejected, "invalid")
		s.log("usage receiver rejected a batch as malformed", zap.ByteString("body", raw))
		return true
	default:
		// 502 and any other status: the receiver may have recorded it.
		s.pop(resultUncertain, "uncertain")
		s.pause(pauseUncertain, "")
		s.log("usage report outcome unknown; dropped rather than risk counting twice", zap.Int("http_status", code))
		return false
	}
}

// pop drops the head chunk with result and drop reason.
func (s *HTTPSink) pop(result, reason string) {
	c := s.pending[0]
	s.pending = s.pending[1:]
	s.pendingRows -= len(c.entries)
	metrics.UsageFlushTotal.WithLabelValues(s.name, result).Inc()
	s.drop(len(c.entries), reason)
}

func (s *HTTPSink) pause(d time.Duration, result string) {
	if t := s.now().Add(d); t.After(s.pausedUntil) {
		s.pausedUntil = t
	}
	if result != "" {
		metrics.UsageFlushTotal.WithLabelValues(s.name, result).Inc()
	}
}

func (s *HTTPSink) drop(n int, reason string) {
	if n > 0 {
		metrics.UsageEntriesDroppedTotal.WithLabelValues(s.name, reason).Add(float64(n))
	}
}

// log logs msg at Error at most once per logEvery per message.
func (s *HTTPSink) log(msg string, fields ...zap.Field) {
	now := s.now()
	if last, ok := s.loggedAt[msg]; ok && now.Sub(last) < logEvery {
		return
	}
	s.loggedAt[msg] = now
	s.logger.Error(msg, fields...)
}

// retryAfter parses Retry-After (seconds or an HTTP date), capped at
// pauseMaxRetry; pauseRetry when absent or unusable.
func retryAfter(h string, now time.Time) time.Duration {
	var d time.Duration
	if secs, err := strconv.ParseInt(h, 10, 64); err == nil {
		if secs >= int64(pauseMaxRetry/time.Second) {
			return pauseMaxRetry
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(h); err == nil {
		d = t.Sub(now)
	}
	if d <= 0 {
		return pauseRetry
	}
	return min(d, pauseMaxRetry)
}
