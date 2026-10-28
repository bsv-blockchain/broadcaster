package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// Client is a single connected SSE consumer. The handler goroutine
// drains `ch` and writes frames to the wire; the Manager pushes onto
// `ch` from its fan-out goroutine. token narrows the stream to txids
// associated with that callback token; empty means unfiltered.
//
// ctx is derived from the manager's parent context so we can broadcast
// "client is gone" to fan-out without racing the consumer-side close.
// The consumer (handleEvents) calls cancel() via Manager.unregister on
// disconnect, which causes any concurrent fan-out send to fall through
// the ctx.Done() arm of its select rather than blocking or panicking.
// The channel is intentionally NOT closed: senders always race the
// close otherwise (F-020).
type Client struct {
	ID    int64
	Token string
	Ch    chan *models.TransactionStatus
	// Ctx and Cancel intentionally live on the struct: this is the
	// per-client cancel signal that fan-out selects on (F-020). The
	// standard "don't store contexts" guidance doesn't apply to
	// long-lived cancellation handles owned by a registry entry.
	Ctx    context.Context    //nolint:containedctx // see comment above
	Cancel context.CancelFunc //nolint:containedctx // paired with Ctx above

	// The fields below are the mid-stream catchup delivery state. They are
	// raced between the manager's fan-out goroutine (recordDrop) and the
	// per-connection writer goroutine (handleEvents / midstreamCatchup),
	// hence atomics.

	// lastDelivered is the Timestamp.UnixNano() of the last frame (live or
	// catchup) the writer goroutine successfully wrote to this client. It is
	// the resume watermark for capped mid-stream catchup rounds. Written only
	// by the writer goroutine.
	lastDelivered atomic.Int64
	// dropped is set by fanOut's drop arm when this client's channel
	// overflowed; the writer goroutine clears it and runs a store-backed
	// catchup. recordDrop writes droppedFloor BEFORE setting this flag, so a
	// writer that observes the flag also observes the floor.
	dropped atomic.Bool
	// droppedFloor is the minimum Timestamp.UnixNano() among events dropped
	// since the writer last consumed it (math.MaxInt64 = none). It bounds the
	// mid-stream catchup replay window from below.
	droppedFloor atomic.Int64
	// dropsSinceLog and lastDropLog implement the rate-limited aggregate
	// drop Warn (at most one line per dropLogInterval per client) — a big
	// block's burst used to emit one Warn PER dropped event.
	dropsSinceLog atomic.Int64
	lastDropLog   atomic.Int64

	// drainRetryMS is this connection's personal reconnect delay, set by
	// Manager.Drain just before it cancels Ctx and read once by the writer
	// goroutine on its way out. Zero means "no hint" (either not a drain, or
	// hints are disabled). Per-client rather than shared because a single
	// shared delay relocates a reconnect herd instead of dispersing it.
	drainRetryMS atomic.Int64
}

// Manager owns the per-pod registry of SSE clients listening on
// /events. It calls events.Publisher.Subscribe ONCE at startup and fans
// every update out to every registered client. New clients register at
// /events connect time; deregister on disconnect.
//
// The subscription is live-only (it starts at the topic head): anything a
// client missed while disconnected is served from the store by sendCatchup
// via Last-Event-ID, never by Kafka replay.
//
// Token-based filtering happens in the fan-out path, but the store is asked
// once per EVENT, not once per (event, client): tokenIndex resolves a txid's
// subscribed token set — batched for bulk events, LRU-cached for live ones —
// and each client is then matched against that set in memory. Nothing on this
// path may materialize a token's full submission list (see #237/#238).
type Manager struct {
	publisher events.Publisher
	store     store.Store
	logger    *zap.Logger

	// tokens resolves txid → subscribed callback tokens for fan-out. Owned
	// by the fan-out goroutine (the index is itself lock-guarded).
	tokens *tokenIndex

	// fanoutEWMA is an exponentially-weighted mean of per-event fan-out
	// duration in nanoseconds, and fanoutEvents the lifetime event count.
	// Both are written and read ONLY on the single fan-out goroutine (fanOut
	// → deliver → recordDrop), so they need no synchronization; they exist to
	// put the dispatcher's own cost into the drop warning, which used to name
	// only the client.
	fanoutEWMA   float64
	fanoutEvents int64

	// parentCtx roots every per-client context, so canceling it cancels every
	// registered client's fan-out path at once.
	//
	// It is derived from the service context via context.WithoutCancel, so it
	// inherits that context's VALUES (trace and logging context stay attached
	// to per-client work) while dropping its cancellation, and is cancelled
	// only by Drain. If it inherited cancellation too, a shutdown would cancel
	// every client the instant SIGTERM landed — before the drain had failed
	// readiness, waited for the endpoint to be withdrawn, or handed out
	// reconnect hints. The drain would be a no-op and every stream would still
	// be severed simultaneously. The manager's own run goroutine keeps using
	// the service context directly, because it SHOULD stop immediately.
	parentCtx    context.Context //nolint:containedctx // long-lived registry root
	parentCancel context.CancelFunc

	nextClientID atomic.Int64

	// clientBufferSize is the capacity of each new client's send channel.
	// Wired from config.SSEConfig.ClientBufferSize by the Service at
	// startup; when unset (<= 0, e.g. tests that construct via newManager
	// without threading config) NewClient falls back to
	// defaultClientBuffer.
	clientBufferSize int

	// catchupOnDrop gates mid-stream store-backed catchup for token-scoped
	// clients whose channel overflowed. Wired from
	// config.SSEConfig.CatchupOnDrop by the Service at startup; newManager
	// initializes it to true so tests constructing the manager directly get
	// the production default.
	catchupOnDrop bool

	mu      sync.RWMutex
	clients map[int64]*Client
}

// defaultClientBuffer is the fallback capacity for a client's send channel
// when Manager.clientBufferSize is unset (<= 0). Mirrors
// config.DefaultSSEClientBuffer; kept local so newManager callers that don't
// thread config (tests) still get a burst-tolerant buffer instead of the old
// silently-dropping 64-slot channel.
const defaultClientBuffer = 8192

// newManager constructs the manager and starts a single subscriber
// goroutine that runs for the lifetime of ctx. Returns (nil, nil) only
// when publisher is nil — that signals "no fan-out wired" so the
// handler returns 503; it's not an error condition.
func newManager(ctx context.Context, publisher events.Publisher, st store.Store, logger *zap.Logger) (*Manager, error) {
	if publisher == nil {
		return nil, nil //nolint:nilnil // intentional: nil manager means "no fan-out wired"
	}
	named := logger.Named("sse")
	// clientRoot outlives ctx on purpose — see Manager.parentCtx.
	clientRoot, cancelClients := context.WithCancel(context.WithoutCancel(ctx))
	m := &Manager{
		publisher:     publisher,
		store:         st,
		logger:        named,
		tokens:        newTokenIndex(st, named),
		parentCtx:     clientRoot,
		parentCancel:  cancelClients,
		catchupOnDrop: true,
		clients:       make(map[int64]*Client),
	}
	ch, err := publisher.Subscribe(ctx, "sse")
	if err != nil {
		return nil, fmt.Errorf("subscribing to events publisher: %w", err)
	}
	go m.run(ctx, ch)
	return m, nil
}

// run drains the upstream subscription and fans every update out to
// clients. Exits when ctx is canceled or the upstream channel closes.
//
// Bulk events (status.TxIDs non-empty) are unfanned here: SSE clients
// expect a stream of per-tx events, so each entry in TxIDs is turned into
// its own TransactionStatus before fan-out. The bump-builder coalesces
// per-block MINED bursts into one bulk event so the webhook service's
// bounded work queue can't saturate — the unfan cost lives downstream.
func (m *Manager) run(ctx context.Context, in <-chan *models.TransactionStatus) {
	for {
		select {
		case <-ctx.Done():
			return
		case status, ok := <-in:
			if !ok {
				return
			}
			if status == nil {
				continue
			}
			if len(status.TxIDs) == 0 {
				m.fanOut(ctx, status)
				continue
			}
			m.fanOutBulk(ctx, status)
		}
	}
}

// snapshot copies the client registry. Callers send AFTER releasing the lock
// (F-020) — see deliver for the concurrency contract that makes that safe.
func (m *Manager) snapshot() []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	clients := make([]*Client, 0, len(m.clients))
	for _, c := range m.clients {
		clients = append(clients, c)
	}
	return clients
}

// tokenScoped reports whether any snapshotted client filters by token. When
// none does, fan-out skips token resolution entirely — a firehose-only pod
// never touches the store.
func tokenScoped(clients []*Client) bool {
	for _, c := range clients {
		if c.Token != "" {
			return true
		}
	}
	return false
}

// fanOut delivers ONE per-tx status update. The txid's subscribed token set
// is resolved once (cache, else a single store lookup) and every client is
// then matched against it in memory — the store is never asked per client.
func (m *Manager) fanOut(ctx context.Context, status *models.TransactionStatus) {
	start := time.Now()
	clients := m.snapshot()
	if len(clients) == 0 {
		return // nobody listening — skip token resolution and enrichment
	}
	// The hint is consumed here and never travels further: it is another
	// subscriber's token, and the frame builder would not serialize it anyway.
	hint := status.CallbackTokenHint
	status.CallbackTokenHint = ""
	var tokens tokenSet
	if tokenScoped(clients) {
		tokens = m.tokens.lookup(ctx, status.TxID).withHint(hint)
	}
	m.deliver(ctx, clients, status, tokens)
	m.observeFanout("single", start)
}

// fanOutBulk unfans a bulk event (a block's MINED burst, or a reorg revert)
// into per-tx frames.
//
// The whole txid list is resolved in ONE batched store call before the unfan
// loop. Previously each synthetic per-tx status ran a full fan-out pass with
// its own per-client store probe, so a 570k-transaction block cost 570k
// probes per connected token client on a serial goroutine — the reason
// block bursts pushed emit→applied p95 to 21.6 s.
//
// The client registry is snapshotted once for the whole bulk event rather
// than per txid: a bulk event unfans in milliseconds, and a client that
// connects mid-burst is served the same window by its connect-time catchup.
func (m *Manager) fanOutBulk(ctx context.Context, status *models.TransactionStatus) {
	start := time.Now()
	clients := m.snapshot()
	if len(clients) == 0 {
		return
	}
	hint := status.CallbackTokenHint
	status.CallbackTokenHint = ""
	var resolved map[string]tokenSet
	if tokenScoped(clients) {
		resolved = m.tokens.resolveBatch(ctx, status.TxIDs)
	}
	for _, txid := range status.TxIDs {
		perTx := *status
		perTx.TxID = txid
		perTx.TxIDs = nil
		m.deliver(ctx, clients, &perTx, resolved[txid].withHint(hint))
	}
	m.observeFanout("bulk", start)
}

// deliver pushes one status to every client subscribed to it. tokens is the
// txid's already-resolved subscriber set; a client with a non-empty Token
// receives the frame only if the set contains it. Sends are non-blocking —
// an overflowing client drops the event and recovers via mid-stream catchup
// or a Last-Event-ID reconnect.
//
// Concurrency contract (F-020): the caller snapshotted the client list under
// RLock and released the lock before we send. A client may unregister
// between the snapshot and the send. Each client owns a context that
// unregister cancels; the send selects on Ctx.Done() so a canceled-and-gone
// client takes the drop arm instead of blocking or panicking. The send
// channel is never closed by the manager — closing would race this exact
// send.
func (m *Manager) deliver(ctx context.Context, clients []*Client, status *models.TransactionStatus, tokens tokenSet) {
	enriched := false
	for _, c := range clients {
		if c.Ctx.Err() != nil {
			metrics.APISSEDroppedTotal.WithLabelValues("client_gone").Inc()
			continue
		}
		if c.Token != "" && !tokens.has(c.Token) {
			continue
		}
		// This client will receive the frame, so attach the merkle proof — once
		// per event (the status pointer is shared across the sends below) and
		// lazily after the token match, so we only pay for txids a connected
		// client actually wants. Safe on this single fan-out goroutine: the
		// mutation completes before any c.Ch <- status send establishes
		// happens-before with the client's read-only writeStatus. No-op for
		// non-mined statuses.
		if !enriched {
			m.enrichMerklePath(ctx, status)
			enriched = true
		}
		select {
		case c.Ch <- status:
		case <-c.Ctx.Done():
			metrics.APISSEDroppedTotal.WithLabelValues("client_gone").Inc()
		default:
			metrics.APISSEDroppedTotal.WithLabelValues("slow_client").Inc()
			m.recordDrop(c, status)
		}
	}
}

// fanoutEWMAAlpha weights the newest sample in the fan-out duration mean.
// 1/64 smooths per-event jitter while still tracking a sustained stall within
// a second or so of events.
const fanoutEWMAAlpha = 1.0 / 64.0

// observeFanout records one completed fan-out pass: the Prometheus histogram
// plus the goroutine-local mean the drop warning quotes. Runs on the single
// fan-out goroutine, which is what makes the unsynchronized fields safe.
func (m *Manager) observeFanout(kind string, start time.Time) {
	d := time.Since(start)
	metrics.SSEFanoutDuration.WithLabelValues(kind).Observe(d.Seconds())
	m.fanoutEvents++
	if m.fanoutEWMA == 0 {
		m.fanoutEWMA = float64(d)
		return
	}
	m.fanoutEWMA += fanoutEWMAAlpha * (float64(d) - m.fanoutEWMA)
}

// dropLogInterval rate-limits the aggregate slow-client drop Warn: at most
// one line per client per interval, carrying the count of drops folded into
// it. A ≥20k-tx block used to emit ~12-22k per-event Warn lines per slow
// client.
const dropLogInterval = time.Second

// recordDrop notes one fan-out drop on a slow client: it arms the mid-stream
// catchup state read by the client's writer goroutine and folds the event
// into the rate-limited aggregate Warn. Runs on the manager's single fan-out
// goroutine; the writer goroutine concurrently consumes the state, hence the
// atomics.
func (m *Manager) recordDrop(c *Client, status *models.TransactionStatus) {
	// Track the earliest dropped timestamp since the writer last consumed the
	// floor: it lower-bounds the catchup replay window. The floor MUST be
	// written before the dropped flag so a writer that observes the flag also
	// observes the floor (it consumes them in the opposite order).
	ts := status.Timestamp.UnixNano()
	for {
		cur := c.droppedFloor.Load()
		if ts >= cur || c.droppedFloor.CompareAndSwap(cur, ts) {
			break
		}
	}
	c.dropped.Store(true)

	c.dropsSinceLog.Add(1)
	now := time.Now().UnixNano()
	last := c.lastDropLog.Load()
	if now-last < int64(dropLogInterval) || !c.lastDropLog.CompareAndSwap(last, now) {
		return
	}
	// The message deliberately names the BUFFER, not the client. This line
	// previously read "dropped events for slow SSE client" and was emitted
	// 1.6M times during a benchmark whose consumer was idle at 6 of 32 cores:
	// the actual producer-side bottleneck was this manager's own fan-out. The
	// fan-out cost travels with the warning so the two sides can be told
	// apart at a glance — a fanout_avg near or above the inter-event interval
	// means the dispatcher is the limit, not the consumer.
	m.logger.Warn(
		"sse fan-out could not enqueue events: client send buffer full; will catch up mid-stream",
		zap.Int64("client_id", c.ID),
		zap.Int64("dropped", c.dropsSinceLog.Swap(0)),
		zap.Int("buffer_cap", cap(c.Ch)),
		zap.Duration("fanout_avg", time.Duration(m.fanoutEWMA)),
		zap.Int64("fanout_events", m.fanoutEvents),
		zap.Int("clients", m.clientCount()),
		zap.Bool("catchup_eligible", m.catchupOnDrop && c.Token != ""),
	)
}

// clientCount reports how many clients are registered.
func (m *Manager) clientCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.clients)
}

// Register adds a client to the registry.
func (m *Manager) Register(c *Client) {
	m.mu.Lock()
	m.clients[c.ID] = c
	n := len(m.clients)
	m.mu.Unlock()
	metrics.SSEConnectedClients.Set(float64(n))
}

// Unregister removes a client and signals any in-flight fan-out send to
// drop rather than push onto the channel. We deliberately do NOT close
// c.Ch: closing would race with concurrent fanOut sends (F-020). The
// consumer goroutine in handleEvents selects on its request ctx (which
// is what drives the unregister) so it exits without needing a
// channel-close signal; the unreferenced channel is reclaimed by the GC.
func (m *Manager) Unregister(id int64) {
	m.mu.Lock()
	c, ok := m.clients[id]
	if ok {
		delete(m.clients, id)
	}
	n := len(m.clients)
	m.mu.Unlock()
	metrics.SSEConnectedClients.Set(float64(n))
	if ok {
		c.Cancel()
	}
}

// Drain releases every registered client for a graceful shutdown and reports
// how many it released.
//
// Each client is given its OWN reconnect delay, drawn uniformly below
// retryMax, before its context is canceled. The per-client draw is the whole
// point: releasing N clients with one shared delay just moves the herd, it
// does not disperse it. The writer goroutine observes the cancellation, emits
// a shutdown frame carrying that delay as an SSE `retry:` directive, and
// returns — so the stream ends with a clean FIN instead of the RST that a
// process exit produces.
//
// retryMax <= 0 skips the hint entirely and clients fall back to their own
// backoff, which is still correct, just less well spread.
func (m *Manager) Drain(retryMax time.Duration) int {
	clients := m.snapshot()
	for _, c := range clients {
		if retryMax > 0 {
			//nolint:gosec // reconnect spreading is a load-shaping device, not a security primitive
			c.drainRetryMS.Store(1 + rand.Int64N(retryMax.Milliseconds()))
		}
		c.Cancel()
	}
	// Catch anything that connected between the snapshot and here. Those get
	// no reconnect hint, which is the right trade: they arrived after readiness
	// had already failed, so there should be none.
	m.parentCancel()
	return len(clients)
}

// NewClient assembles a client with a fresh id, buffered channel, and
// per-client cancel handle. The client context is derived from the
// manager's parent ctx so a manager shutdown propagates to every client.
func (m *Manager) NewClient(token string) *Client {
	parent := m.parentCtx
	if parent == nil {
		parent = context.Background()
	}
	// cancel is stored on Client and invoked by Manager.Unregister — its
	// lifetime is the client connection, not this function.
	ctx, cancel := context.WithCancel(parent)
	size := m.clientBufferSize
	if size <= 0 {
		size = defaultClientBuffer
	}
	c := &Client{
		ID:     m.nextClientID.Add(1),
		Token:  token,
		Ch:     make(chan *models.TransactionStatus, size),
		Ctx:    ctx,
		Cancel: cancel,
	}
	c.droppedFloor.Store(math.MaxInt64)
	return c
}

// handleEvents serves GET /events?callbackToken=<token>. Streams
// transaction status updates as Server-Sent Events. Optional
// Last-Event-ID request header triggers a catchup pass that emits any
// updates that occurred after the supplied nanosecond timestamp.
func (s *Service) handleEvents(c *gin.Context) {
	if s.manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "events stream not enabled"})
		return
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "streaming unsupported"})
		return
	}

	token := c.Query("callbackToken")
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	writer := &sseWriter{w: c.Writer, f: flusher}
	ctx := c.Request.Context()

	client := s.manager.NewClient(token)
	s.manager.Register(client)
	defer s.manager.Unregister(client.ID)

	// outcome records why this connection ended. The rate of this counter is
	// the reconnect rate and the label is the reason — a storm of write_error
	// is a completely different problem from a storm of drain, and the two
	// used to be indistinguishable.
	outcome := "client_closed"
	defer func() { metrics.SSEConnectionsTotal.WithLabelValues(outcome).Inc() }()

	if token != "" && !s.connectCatchup(ctx, writer, token, c.GetHeader("Last-Event-ID"), client) {
		outcome = "write_error"
		return // connection is gone; nothing left to serve
	}

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-client.Ctx.Done():
			// Manager-initiated release (Drain). Unlike the request-ctx arm
			// above, the connection is still writable here, so say goodbye
			// with a staggered reconnect hint rather than dropping the socket.
			outcome = "drain"
			writeShutdown(writer, client)
			return
		case status := <-client.Ch:
			if status != nil {
				if err := writeStatusBuffered(writer, status); err != nil {
					outcome = "write_error"
					return
				}
				client.lastDelivered.Store(status.Timestamp.UnixNano())
			}
			// Coalesce the rest of this burst and flush once. A bulk-unfan of
			// one Kafka message lands ~50 frames here; without coalescing we'd
			// flush per frame.
			if _, err := drainLive(writer, client); err != nil {
				outcome = "write_error"
				return
			}
			if err := writer.flush(); err != nil {
				outcome = "write_error"
				return
			}
			if !s.midstreamCatchup(ctx, writer, client) {
				outcome = "write_error"
				return
			}
		case <-keepalive.C:
			if err := writer.write(": keepalive\n\n"); err != nil {
				outcome = "write_error"
				return
			}
			observeClientLag(client)
			// Also heal on the keepalive tick: a drop can land just after the
			// writer's post-drain check (the flag is set by the concurrent
			// fan-out goroutine), and if the stream then goes quiet no further
			// drain cycle would notice it.
			if !s.midstreamCatchup(ctx, writer, client) {
				outcome = "write_error"
				return
			}
		}
	}
}

// observeClientLag samples how far behind the present this connection's last
// delivered frame is.
//
// Sampled on the keepalive tick rather than per frame: the watermark is
// already maintained by the writer goroutine, so this costs one atomic load
// and one histogram observation per client per 15s. It is the signal that
// describes what a consumer actually experiences — drops and fan-out duration
// each describe one component in isolation, which is how 1.6M dropped events
// went unnoticed for a whole benchmark run.
//
// Skipped before the first delivery: a connection that has legitimately had
// nothing to receive would otherwise report its entire uptime as lag.
func observeClientLag(client *Client) {
	last := client.lastDelivered.Load()
	if last <= 0 {
		return
	}
	metrics.SSEClientLagSeconds.Observe(time.Since(time.Unix(0, last)).Seconds())
}

// connectCatchup runs the pre-stream replay for a token-scoped connection and
// reports whether the connection is still writable.
//
// The client is threaded through so lastDelivered is initialized from the
// replayed history — a later mid-stream catchup then resumes from a fresh
// watermark instead of replaying from zero. A malformed Last-Event-ID is
// treated as absent, which yields fresh-connect semantics (non-terminal
// statuses only) rather than an error.
//
// A store failure here leaves a writable connection that owes history it will
// never send, so it raises a gap rather than sliding silently into live
// frames. Truncation raises its own gap inside sendCatchup.
func (s *Service) connectCatchup(ctx context.Context, w *sseWriter, token, lastEventID string, client *Client) bool {
	var since time.Time
	if lastEventID != "" {
		if ns, err := strconv.ParseInt(lastEventID, 10, 64); err == nil {
			since = time.Unix(0, ns)
		}
	}
	res := s.sendCatchup(ctx, w, token, since, client, false, catchupMaxFrames)
	if res.writeFailed {
		return false
	}
	if res.err == nil {
		return true
	}
	s.logger.Warn("sse connect catchup failed; client notified of gap",
		zap.Int64("client_id", client.ID),
		zap.Int("frames", res.frames),
		zap.Error(res.err))
	return writeGap(w, gapReasonReplayUnavailable, since.UnixNano(), 0) == nil
}

// catchupMaxFrames bounds a single catchup replay. A well-behaved token
// has a small pending set (fresh connect) or a short disconnect window
// (Last-Event-ID), so the cap only bites pathological histories — where
// truncating beats the alternative this cap replaced: replaying millions
// of frames used to OOM the whole pod, killing every connected client.
const catchupMaxFrames = 10_000

// catchupBlockBudget bounds how many DISTINCT blocks a single catchup replay
// will enrich with merkle paths. Enrichment parses (and caches) a block's
// compound BUMP; within one replay the LRU keeps recent blocks hot, but a
// pathological reconnect window spanning many distinct blocks would otherwise
// re-parse a large BUMP per block. Capping distinct blocks bounds that CPU to
// at most catchupBlockBudget parses per replay, independent of frame count —
// the memory ceiling is already the store's bump-cache size. Over-budget MINED
// frames still ship with blockHash/blockHeight; the proof stays recoverable via
// GET /tx/:id.
const catchupBlockBudget = 64

// midstreamRoundFrames bounds ONE mid-stream catchup round for a client.
//
// This is the fix for a self-sustaining drop/catchup loop. A mid-stream round
// runs on the connection's writer goroutine, which is the same goroutine that
// drains client.Ch — so for the whole round nothing consumes the live
// channel. With the shared catchupMaxFrames (10,000) budget and the default
// 8,192-slot client buffer, a single capped round was GUARANTEED to overflow
// the buffer if fan-out was producing at all, which re-armed the drop flag
// and started another round. A production stream logged 34 rounds and 570
// drop lines in ten minutes while the load generator was stopped, alternating
// "catchup round frames:11818 capped:true" with "dropped:3919".
//
// Half the buffer leaves headroom for what fan-out enqueues during the round;
// the caller then drains the live channel before the next round, so replay
// and live delivery interleave instead of starving each other.
func midstreamRoundFrames(c *Client) int {
	budget := cap(c.Ch) / 2
	if budget < minMidstreamRoundFrames {
		budget = minMidstreamRoundFrames
	}
	return min(budget, catchupMaxFrames)
}

// minMidstreamRoundFrames keeps rounds from degenerating into per-frame
// queries when a client is configured with a tiny buffer: a round must make
// enough progress to outpace the store query it costs.
const minMidstreamRoundFrames = 256

// errCatchupCapped stops the catchup iteration at the pass's frame budget.
// Internal sentinel, never surfaced to the client.
var errCatchupCapped = errors.New("sse catchup frame cap reached")

// midstreamCatchupSlack widens the mid-stream replay window below the
// earliest dropped event's timestamp. Bulk MINED events carry the store's
// timestamp_at verbatim (see setMinedAndPublish), so for the dominant burst
// source the floor is exact; other publish paths (propagation, api-server)
// stamp events with a publish-time time.Now() taken shortly AFTER the store
// row's timestamp_at, and the store filter is strictly-after — the slack
// covers that skew. Cost of the rewind is only duplicate frames, which the
// catchup contract already allows.
const midstreamCatchupSlack = 2 * time.Second

// catchupResult reports one sendCatchup pass.
type catchupResult struct {
	// frames is how many status frames were written.
	frames int
	// lastTS is the Timestamp.UnixNano() of the last frame written (0 when
	// frames == 0).
	lastTS int64
	// capped is true when the pass stopped at the frame cap with more
	// matching history remaining beyond lastTS.
	capped bool
	// err is any non-cap error: a write error (connection dead) or a store
	// iteration error.
	err error
	// writeFailed distinguishes the two err sources. A store failure leaves a
	// writable connection that has silently missed history and must be told so
	// with a gap frame; a write failure means there is nobody left to tell.
	writeFailed bool
}

// sendCatchup replays persisted statuses for txids registered under the
// supplied token. When `since` is non-zero, every status with a timestamp
// strictly after `since` is emitted (the Last-Event-ID reconnect contract —
// the window is naturally bounded by the disconnect duration). When `since`
// is zero (fresh connect), only NON-TERMINAL statuses are replayed: that's
// the actionable pending state a client needs on connect; terminal history
// is queryable via GET /tx/:id, and replaying it wholesale is what used to
// OOM this service (a token with 2.2M submissions × a GetStatus per txid,
// each MINED row parsing its block's compound BUMP, inside a 512Mi limit).
//
// The store streams a projection (no raw tx, no merkle-path enrichment) in
// ascending timestamp order, so replayed event ids stay monotonic for
// Last-Event-ID resume. Merkle-path enrichment is layered on HERE (not inside
// the iterator, which must stay a pure projection per its contract): terminal
// frames on a Last-Event-ID reconnect are enriched best-effort, bounded by
// catchupBlockBudget distinct blocks. Fresh connects replay only non-terminal
// statuses, so the enrichment branch never fires there.
//
// client, when non-nil, has its lastDelivered watermark advanced per written
// frame so mid-stream catchup can resume from where any earlier pass ended.
//
// The frame cap is ALWAYS soft at a timestamp boundary, on both the
// connect-time and the mid-stream path: once the cap is reached the pass keeps
// writing while the timestamp stays EQUAL to the last written frame's. The
// store's since-filter is strictly-after and timestamp_at is only
// microsecond-resolution, so a timestamp cursor cannot resume mid-way through
// an equal-timestamp run (SetMinedByTxIDs stamps a whole block's rows with one
// timestamp_at) — stopping inside one strands the remainder FOREVER. That was
// a silent, permanent loss on the Last-Event-ID reconnect path: the pass
// stopped mid-tie, reported lastTS as the tied timestamp, and the client
// resumed strictly after it. Draining the tie bounds the overshoot by the
// largest same-timestamp batch (frames are streamed, never accumulated, so the
// cost is time and not memory) and guarantees a `capped` pass always ends on a
// boundary the next round can resume from.
//
// midstream=true only changes how a capped pass is REPORTED: the live loop
// paginates by design, so it is not logged as a truncation and does not raise
// a gap.
//
// maxFrames is the pass's frame budget: catchupMaxFrames for connect-time
// replay, and the smaller midstreamRoundFrames(client) for live rounds, which
// must not outrun the client's send buffer while the writer goroutine is busy
// replaying.
func (s *Service) sendCatchup(ctx context.Context, w *sseWriter, token string, since time.Time, client *Client, midstream bool, maxFrames int) catchupResult {
	var only []models.Status
	if since.IsZero() {
		only = models.NonTerminalStatuses()
	}
	var res catchupResult
	enrichedBlocks := make(map[string]struct{})
	err := s.store.IterateStatusesByToken(ctx, token, since, only, func(status *models.TransactionStatus) error {
		ts := status.Timestamp.UnixNano()
		if res.frames >= maxFrames && ts != res.lastTS {
			res.capped = true
			return errCatchupCapped
		}
		// Reaching here past the cap means a same-timestamp tie with the frame
		// that hit it — finish the tie (see the function comment).
		if status.Status == models.StatusMined || status.Status == models.StatusImmutable {
			_, seen := enrichedBlocks[status.BlockHash]
			if status.BlockHash != "" && (seen || len(enrichedBlocks) < catchupBlockBudget) {
				enrichedBlocks[status.BlockHash] = struct{}{}
				s.store.EnrichMerklePath(ctx, status)
				if len(status.MerklePath) == 0 {
					metrics.MinedPushWithoutMerklePathTotal.WithLabelValues("sse").Inc()
				}
			}
		}
		if err := writeStatus(w, status); err != nil {
			res.writeFailed = true
			return err
		}
		res.frames++
		res.lastTS = ts
		if client != nil {
			client.lastDelivered.Store(ts)
		}
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, errCatchupCapped):
		if midstream {
			// Expected multi-round pagination for the live loop, not a
			// truncation — the caller resumes from lastTS.
			break
		}
		s.logger.Warn("sse catchup truncated at frame cap",
			zap.Int("cap", maxFrames),
			zap.Bool("fresh_connect", since.IsZero()))
		// Tell the CLIENT, not just the log. Everything after res.lastTS that
		// matched this token is history it will never be pushed; without this
		// frame it has no way to know that and would carry on as if whole.
		if gapErr := writeGap(w, gapReasonCatchupTruncated, res.lastTS, int64(res.frames)); gapErr != nil {
			res.err = gapErr
			res.writeFailed = true
		}
	default:
		res.err = err
	}
	return res
}

// declareFirehoseGap handles a drop on a tokenless (firehose) connection.
//
// No bounded store query can reconstruct an unfiltered stream, so there is
// nothing to replay — but the drop can still be declared. Consumes the drop
// flag (so the gap is raised once per episode rather than on every keepalive
// tick) and writes one gap frame carrying the earliest dropped timestamp as
// the cursor the client can still trust. Returns false when the connection is
// no longer writable.
func declareFirehoseGap(w *sseWriter, client *Client) bool {
	if !client.dropped.Swap(false) {
		return true
	}
	from := int64(0)
	if floor := client.droppedFloor.Swap(math.MaxInt64); floor != math.MaxInt64 {
		from = floor
	}
	return writeGap(w, gapReasonFirehoseDrop, from, 0) == nil
}

// midstreamCatchup heals a live token-scoped connection after fanOut dropped
// events on channel overflow: it replays the missed window from the store
// (the same engine as Last-Event-ID reconnect catchup) without forcing a
// reconnect. Returns false when the connection is no longer writable and the
// handler must exit.
//
// Replay window: from min(earliest dropped timestamp - slack, resume point of
// a previous capped round). Frames the client already holds may be replayed
// again — duplicates across the drop/catchup boundary are the documented
// catchup semantics (clients dedupe by txid+status). Rounds loop while more
// drops land or a capped round leaves history unreplayed; each capped round
// ends on a timestamp boundary strictly above the previous one, so the loop
// always progresses and terminates once it overtakes the live stream. Under
// sustained overload the connection degrades to store-paced replay, which is
// exactly the at-least-once contract.
//
// Tokenless (firehose) clients keep the historical drop-only behavior — the
// catchup source is store.IterateStatusesByToken, which is token-scoped;
// there is no bounded store query that reconstructs the unfiltered stream.
// They keep the drop metric and the aggregate Warn.
//
// Rounds are rate-matched to the connection rather than run back to back:
// each round is bounded by midstreamRoundFrames (a fraction of the client's
// send buffer) and the live channel is drained between rounds. Replaying
// occupies this same goroutine, so an unbounded round guaranteed the buffer
// it was healing overflowed again mid-replay — the drop→catchup→drop loop
// that kept a stream in permanent recovery.
func (s *Service) midstreamCatchup(ctx context.Context, w *sseWriter, client *Client) bool {
	if client.Token == "" {
		return declareFirehoseGap(w, client)
	}
	if !s.manager.catchupOnDrop {
		return true
	}
	roundFrames := midstreamRoundFrames(client)
	resume := int64(-1) // lastTS of a capped round; -1 = no round pending
	for client.dropped.Load() || resume >= 0 {
		// Consume the flag BEFORE the floor: recordDrop writes the floor
		// before setting the flag, so this order can at worst yield one
		// spurious extra round, never a missed floor.
		client.dropped.Store(false)
		floor := client.droppedFloor.Swap(math.MaxInt64)

		sinceNS := int64(math.MaxInt64)
		if floor != math.MaxInt64 {
			sinceNS = floor - int64(midstreamCatchupSlack)
		}
		if resume >= 0 && resume < sinceNS {
			sinceNS = resume
		}
		if sinceNS == math.MaxInt64 {
			// Spurious flag: its floor was consumed (and covered) by the
			// previous round, and no capped round is pending.
			break
		}

		metrics.SSEMidstreamCatchupsTotal.Inc()
		res := s.sendCatchup(ctx, w, client.Token, time.Unix(0, sinceNS), client, true, roundFrames)
		metrics.SSEMidstreamCatchupFramesTotal.Add(float64(res.frames))
		if errors.Is(res.err, store.ErrReplayUnavailable) {
			// The backend refused this replay (a token too large to serve
			// without an index over it). That is a bounded, declared
			// degradation, not a dead connection: tell the client to reconcile
			// and keep the LIVE stream running, which is still useful.
			s.logger.Warn("sse mid-stream catchup unavailable; client notified of gap",
				zap.Int64("client_id", client.ID),
				zap.Error(res.err))
			return writeGap(w, gapReasonReplayUnavailable, sinceNS, 0) == nil
		}
		if res.err != nil {
			// Either the connection is dead (write error) or the store
			// iteration failed mid-window. Both ways the safe move is to end
			// the stream: the client reconnects with Last-Event-ID and the
			// reconnect catchup re-covers the window.
			s.logger.Warn("sse mid-stream catchup aborted",
				zap.Int64("client_id", client.ID),
				zap.Int("frames", res.frames),
				zap.Error(res.err))
			return false
		}
		// Rate-match: hand the live channel back the goroutine before the next
		// round. Without this the replay loop starves the very buffer it is
		// healing and re-arms its own drop flag. Live frames are newer than
		// what the round just replayed, so ids move forward again here —
		// non-monotonic ids across a drop/catchup boundary are already part of
		// the catchup contract.
		live, err := drainLive(w, client)
		if err != nil {
			return false
		}
		if err := w.flush(); err != nil {
			return false
		}
		s.logger.Info("sse mid-stream catchup round",
			zap.Int64("client_id", client.ID),
			zap.Int("frames", res.frames),
			zap.Int("round_cap", roundFrames),
			zap.Int("live_frames", live),
			zap.Bool("capped", res.capped),
			zap.Time("since", time.Unix(0, sinceNS)))
		if res.capped {
			resume = res.lastTS
		} else {
			resume = -1
		}
	}
	return true
}

// drainLive writes every frame already buffered on the client's send channel
// WITHOUT flushing, returning how many it wrote. The default arm exits once
// the channel is empty, so it only ever writes what is ready and cannot spin.
// Callers flush once afterwards.
func drainLive(w *sseWriter, client *Client) (int, error) {
	written := 0
	for {
		select {
		case status := <-client.Ch:
			if status == nil {
				continue
			}
			if err := writeStatusBuffered(w, status); err != nil {
				return written, err
			}
			client.lastDelivered.Store(status.Timestamp.UnixNano())
			written++
		default:
			return written, nil
		}
	}
}

// enrichMerklePath attaches the merkle proof to a mined/immutable status before
// it is framed to clients. No-op for other statuses, when there is no store, or
// when the block's BUMP isn't retrievable — best-effort, so it never blocks a
// frame. Records a metric when a mined frame still lacks a proof so operators
// can spot BUMP-availability gaps.
func (m *Manager) enrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	if m.store == nil {
		return
	}
	m.store.EnrichMerklePath(ctx, status)
	if (status.Status == models.StatusMined || status.Status == models.StatusImmutable) && len(status.MerklePath) == 0 {
		metrics.MinedPushWithoutMerklePathTotal.WithLabelValues("sse").Inc()
	}
}

// buildStatusFrame renders one status as an SSE frame. Event id is the
// timestamp in nanoseconds so clients can use it as Last-Event-ID on
// reconnect.
//
// The payload timestamp is RFC3339Nano, not RFC3339: the frame id already
// carries nanoseconds, so a second-resolution payload left any consumer
// measuring latency from `data` with up to ±1 s of error against the same
// event's own id. RFC3339Nano is a strict superset of the previous output —
// it elides trailing zeros, so a whole-second timestamp still renders
// byte-identically — and fractional seconds are optional in RFC 3339, so
// conformant parsers (Go's time.RFC3339, JavaScript's Date, Python's
// datetime.fromisoformat) accept both spellings unchanged.
func buildStatusFrame(status *models.TransactionStatus) (string, error) {
	data, err := json.Marshal(statusPayload{
		TxID:         status.TxID,
		TxStatus:     string(status.Status),
		Timestamp:    status.Timestamp.UTC().Format(time.RFC3339Nano),
		BlockHash:    status.BlockHash,
		BlockHeight:  status.BlockHeight,
		MerklePath:   status.MerklePath,
		StatusCode:   status.StatusCode,
		ExtraInfo:    status.ExtraInfo,
		CompetingTxs: status.CompetingTxs,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("id: %d\nevent: status\ndata: %s\n\n", status.Timestamp.UnixNano(), data), nil
}

// Gap reasons. These are the `reason` field on a gap frame and the label on
// metrics.SSEGapEventsTotal — keep the two in step.
const (
	// gapReasonCatchupTruncated: a connect-time replay stopped at the frame cap
	// with matching history still beyond the cursor.
	gapReasonCatchupTruncated = "catchup_truncated"
	// gapReasonFirehoseDrop: a tokenless client's frame was dropped on channel
	// overflow and no token-scoped replay can recover it.
	gapReasonFirehoseDrop = "firehose_drop"
	// gapReasonReplayUnavailable: the store could not serve the replay, so the
	// connection proceeded to live frames without the history it owed.
	gapReasonReplayUnavailable = "replay_unavailable"
)

// gapPayload is the JSON shape inside a `data:` field on an `event: gap`
// frame. It is the server admitting, in-band, that the stream it just handed
// this client is discontiguous.
//
// Before this existed, the two ways a client silently lost history — a
// connect-time replay stopped at the frame cap, and a tokenless client's
// dropped frame — produced only a server-side Warn and a counter. The client
// streamed on believing it was whole. `from` is the last cursor the client can
// trust; everything after it, up to the live stream, may be missing.
//
// Unknown event types are ignored by every conformant SSE client (including
// go-bsv-broadcaster-toolbox, which skips any `event:` other than "status"), so this
// frame is purely additive and safe to emit at any protocol version.
type gapPayload struct {
	// Reason is machine-readable and matches the SSEGapEventsTotal label.
	Reason string `json:"reason"`
	// From is the nanosecond cursor the gap starts after — the last id the
	// client was handed. Empty when no cursor is known (a firehose client with
	// no delivered frame yet).
	From string `json:"from,omitempty"`
	// Count is a best-effort magnitude of what was skipped, when known.
	Count int64 `json:"count,omitempty"`
	// Action tells the client what to do. Always "reconcile": re-fetch the
	// affected transactions over REST.
	Action string `json:"action"`
}

// writeGap emits an `event: gap` frame and flushes it. Errors are returned so
// callers on the writer goroutine can treat a failed write as a dead
// connection, matching writeStatus.
func writeGap(w *sseWriter, reason string, from, count int64) error {
	p := gapPayload{Reason: reason, Count: count, Action: "reconcile"}
	if from > 0 {
		p.From = strconv.FormatInt(from, 10)
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	metrics.SSEGapEventsTotal.WithLabelValues(reason).Inc()
	return w.write(fmt.Sprintf("event: gap\ndata: %s\n\n", data))
}

// writeShutdown emits the farewell frame for a client released by
// Manager.Drain: a shutdown event naming the reason, preceded by an SSE
// `retry:` directive carrying this connection's own reconnect delay when one
// was assigned.
//
// The shutdown event is emitted UNCONDITIONALLY. It is what distinguishes a
// deliberate release from a connection that broke, and a client that can tell
// those apart does not have to treat a rolling restart as an error — so
// withholding it when reconnect hints happen to be disabled would remove the
// more important of the two signals to save a line of output.
//
// The `retry:` directive is the only in-band channel a server has for saying
// WHEN to come back. Without it a rolling restart releases every client at
// once and they all re-dial on their own identical backoff schedule, landing
// together on a pod that is still cold; a client that receives no hint simply
// falls back to its own backoff, which is correct but less well spread.
//
// Errors are ignored: the connection is closing either way.
func writeShutdown(w *sseWriter, client *Client) {
	var frame strings.Builder
	if retryMS := client.drainRetryMS.Load(); retryMS > 0 {
		fmt.Fprintf(&frame, "retry: %d\n", retryMS)
		fmt.Fprintf(&frame, "event: shutdown\ndata: {\"reason\":\"draining\",\"reconnectAfterMs\":%d}\n\n", retryMS)
	} else {
		frame.WriteString("event: shutdown\ndata: {\"reason\":\"draining\"}\n\n")
	}
	_ = w.write(frame.String())
}

// writeStatus emits one status frame and flushes it. Used for single frames
// (keepalive/catchup) where there is no burst to coalesce.
func writeStatus(w *sseWriter, status *models.TransactionStatus) error {
	frame, err := buildStatusFrame(status)
	if err != nil {
		return err
	}
	return w.write(frame)
}

// writeStatusBuffered emits one status frame WITHOUT flushing. The live
// fan-out path writes a whole burst this way and then calls w.flush() once, so
// a bulk-unfan (~50 frames from one Kafka message) drains with a single flush
// instead of one flush per frame.
func writeStatusBuffered(w *sseWriter, status *models.TransactionStatus) error {
	frame, err := buildStatusFrame(status)
	if err != nil {
		return err
	}
	return w.writeNoFlush(frame)
}

// statusPayload is the JSON shape inside a `data:` field. txid/txStatus/timestamp
// are the always-present legacy fields; blockHash/blockHeight/merklePath are
// added for mined/immutable frames (omitempty keeps every other frame at the
// original three-field shape). merklePath is a BUMP, hex-encoded like GET /tx.
// status/extraInfo mirror the same fields on GET /tx: on REJECTED frames
// extraInfo carries the verbatim Teranode rejection line (e.g. "UTXO_SPENT
// (70): … utxo already spent by tx …") and status the ARC code when one maps
// (466 conflict, 467 generic, 476 non-final) — wallets branching on the
// reject reason no longer need a follow-up GET /tx. Reorg-correction frames
// (issue #279) reuse extraInfo for the reorg_reanchor/reorg_unmined markers.
type statusPayload struct {
	TxID        string          `json:"txid"`
	TxStatus    string          `json:"txStatus"`
	Timestamp   string          `json:"timestamp"`
	BlockHash   string          `json:"blockHash,omitempty"`
	BlockHeight uint64          `json:"blockHeight,omitempty"`
	MerklePath  models.HexBytes `json:"merklePath,omitempty"`
	StatusCode  int             `json:"status,omitempty"`
	ExtraInfo   string          `json:"extraInfo,omitempty"`
	// CompetingTxs names the transaction(s) that own the outpoints a
	// double-spend REJECTED frame tried to spend, as on GET /tx.
	CompetingTxs []string `json:"competingTxs,omitempty"`
}
