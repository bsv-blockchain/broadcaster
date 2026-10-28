// Package webhook delivers transaction status updates to client-supplied
// callback URLs. The service subscribes to events.Publisher, looks up the
// submission(s) registered for each updated txid, applies the
// FullStatusUpdates filter, and POSTs the status JSON to the registered URL.
// Failed deliveries are retried with exponential backoff; retry state is
// persisted via store.UpdateDeliveryStatus so a process restart never loses
// pending callbacks.
package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/callbackurl"
	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/version"
)

// webhookUserAgent identifies BSV Broadcaster's callback POSTs. The "bsv-broadcaster-webhook/"
// prefix is a documented, stable string receivers can allowlist at their
// edge — Go's default Go-http-client UA is a common bot-rule target and got
// every callback to one production receiver silently 403'd (issue #249).
var webhookUserAgent = "bsv-broadcaster-webhook/" + version.Version

// defaultMaxConcurrentDeliveries is the fallback pool size when
// WebhookConfig.MaxConcurrentDeliveries is unset or non-positive.
const defaultMaxConcurrentDeliveries = 32

// workQueueDepth is the bounded capacity of the channel that hands
// statuses from the upstream-channel reader to the delivery workers.
// Sized larger than MaxConcurrentDeliveries so a brief HTTP stall
// doesn't immediately back-pressure the reader. Drops past this depth
// land in the WebhookPoolSaturatedTotal counter — the signal that the
// pool needs to grow.
const workQueueDepth = 1024

// Service is the entry point for webhook delivery. Wire it up in main.go for
// every process that should ship callbacks (typically the api-server, but
// could be a dedicated mode if operators want to scale delivery separately).
type Service struct {
	services.ReadyHook

	cfg       config.WebhookConfig
	logger    *zap.Logger
	publisher events.Publisher
	store     store.Store
	client    *http.Client

	// leaser, holderID, reaperInterval and leaseTTL govern the webhook
	// retry sweep — see reaper.go. leaser may be nil in tests and single-
	// process deployments; the sweep is then skipped and event-driven
	// delivery is the only retry path.
	leaser         store.Leaser
	holderID       string
	reaperInterval time.Duration
	leaseTTL       time.Duration

	mu      sync.Mutex
	running bool
}

// New constructs a Service. publisher must be non-nil; store provides
// submission lookup and retry persistence. leaser may be nil — in that case
// the retry sweep is disabled and event-driven delivery is the only retry
// path (acceptable for tests and single-process deployments). The HTTP
// client is constructed here so each Service has its own pool — keeps tests
// hermetic.
//
// callbackCfg threads the SSRF guard through to the dialer: when
// AllowPrivateIPs is false (the default), the underlying transport
// refuses to connect to loopback / link-local / RFC1918 / metadata IPs
// even if a host previously survived registration-time validation
// (catches DNS rebinding). See finding F-017 / issue #75.
func New(cfg config.WebhookConfig, callbackCfg config.CallbackConfig, logger *zap.Logger, publisher events.Publisher, st store.Store, leaser store.Leaser) *Service {
	timeout := time.Duration(cfg.HTTPTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	reaperInterval := time.Duration(cfg.ReaperIntervalMs) * time.Millisecond
	if reaperInterval <= 0 {
		reaperInterval = 30 * time.Second
	}
	leaseTTL := time.Duration(cfg.LeaseTTLMs) * time.Millisecond
	if leaseTTL <= 0 {
		leaseTTL = 3 * reaperInterval
	}
	return &Service{
		cfg:            cfg,
		logger:         logger.Named("webhook"),
		publisher:      publisher,
		store:          st,
		client:         newCallbackClient(timeout, callbackCfg.AllowPrivateIPs),
		leaser:         leaser,
		holderID:       newHolderID(),
		reaperInterval: reaperInterval,
		leaseTTL:       leaseTTL,
	}
}

// newHolderID returns a lease-holder identifier stable for this process's
// lifetime: "<hostname>-<8-hex-chars>". The random suffix disambiguates
// restarts — if an old expired-but-not-yet-purged record still names the
// previous incarnation by hostname alone, the new process will see it as a
// foreign holder and wait for TTL rather than believe it already owns the
// lease. Mirrors propagation.newHolderID; kept separate to avoid a
// cross-package import for a five-line helper.
func newHolderID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return host + "-" + hex.EncodeToString(buf[:])
}

// newCallbackClient builds an http.Client whose dialer enforces the SSRF
// guard. The Dialer.Control hook fires after DNS resolution but before
// connect(), so a hostname that resolves to a banned IP fails fast with
// an error from the callbackurl package — the request never leaves the
// machine. Pulled out of New so tests can construct an equivalent client
// without instantiating the whole Service.
//
// The SSRF-guarding *http.Transport is wrapped by otelhttp so outbound spans
// and traceparent propagation are added on top — the guard remains the inner
// RoundTripper and still runs on every dial, unaffected by the wrapping.
func newCallbackClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		Control:   callbackurl.DialControl(allowPrivate),
	}
	guardedTransport := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       90 * time.Second,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: otelhttp.NewTransport(guardedTransport),
	}
}

// Name satisfies the service interface used by main.go.
func (s *Service) Name() string { return "webhook" }

// Start subscribes to the event stream and spins up a goroutine that
// dispatches each update to the configured CallbackURL(s). Blocks until ctx
// is canceled.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("webhook service already running")
	}
	s.running = true
	s.mu.Unlock()

	ch, err := s.publisher.Subscribe(ctx, "webhook")
	if err != nil {
		return fmt.Errorf("subscribing to events publisher: %w", err)
	}

	workers := s.cfg.MaxConcurrentDeliveries
	if workers <= 0 {
		workers = defaultMaxConcurrentDeliveries
	}

	s.logger.Info(
		"webhook service started",
		zap.Int("max_retries", s.cfg.MaxRetries),
		zap.Int("http_timeout_ms", s.cfg.HTTPTimeoutMs),
		zap.Int("max_concurrent_deliveries", workers),
	)

	// Bounded delivery worker pool. handleUpdate does a synchronous
	// store.GetSubmissionsByTxID plus up to N synchronous HTTP POSTs;
	// running it on the channel-reader goroutine would let a single slow
	// callback stall the entire upstream subscription and trigger drops
	// at the publisher (bsv_broadcaster_events_subscriber_dropped_total).
	work := make(chan *models.TransactionStatus, workQueueDepth)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for status := range work {
				s.handleUpdate(ctx, status)
			}
		}()
	}
	// Retry sweep — re-fires deliveries whose POST failed and whose
	// NextRetryAt has elapsed. Without this, the CAS-at-claim-time advance
	// of LastDeliveredStatus would make a single-attempt failure permanent.
	// Skipped when no leaser is wired (tests / single-process).
	var reaperWG sync.WaitGroup
	if s.leaser != nil {
		reaperWG.Add(1)
		go func() {
			defer reaperWG.Done()
			s.runReaper(ctx)
		}()
	}
	// Subscribe succeeded and the delivery pool is running. The select
	// below blocks until shutdown, so this is the startup-success point.
	s.SignalReady()
	defer func() {
		close(work)
		wg.Wait()
		reaperWG.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case status, ok := <-ch:
			if !ok {
				return nil
			}
			if status == nil {
				continue
			}
			select {
			case work <- status:
			default:
				// Pool saturated — every worker is blocked on a slow
				// callback and the work queue is full. Drop with metric;
				// recovery is store-backed (failed-POST retries via
				// NextRetryAt swept by the reaper, plus CAS dedup gating on
				// the next transition), NOT Kafka replay — the subscription
				// is an ephemeral random group with no durable offsets. We'd
				// rather drop here than back-pressure the upstream subscriber
				// channel (which would multiply the impact across this and
				// other subscribers in the same publisher).
				metrics.WebhookPoolSaturatedTotal.Inc()
				s.logger.Warn(
					"webhook delivery pool saturated, dropping status",
					logfields.TxID(status.TxID),
					logfields.Status(string(status.Status)),
				)
			}
		}
	}
}

// Stop is a no-op — the service shuts down via the ctx passed to Start.
// Defined to satisfy the lifecycle interface main.go uses.
func (s *Service) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	return nil
}

// handleUpdate dispatches a status update to every submission registered
// for the affected txid(s). For bulk events (status.TxIDs non-empty) it
// iterates the txids and dispatches each, so ONE queue entry covers an
// entire block's MINED fan-out without holding 14k slots in the work
// channel. The expensive bit — N store.GetSubmissionsByTxID calls — runs
// inside the worker, never blocking the channel reader.
func (s *Service) handleUpdate(ctx context.Context, status *models.TransactionStatus) {
	if len(status.TxIDs) == 0 {
		s.dispatchOne(ctx, status)
		return
	}
	for _, txid := range status.TxIDs {
		perTx := *status
		perTx.TxID = txid
		perTx.TxIDs = nil
		s.dispatchOne(ctx, &perTx)
	}
}

// dispatchOne handles a single-tx status update: look up registered
// submissions and deliver to those whose CallbackURL is set and whose
// shouldDeliver gating matches.
func (s *Service) dispatchOne(ctx context.Context, status *models.TransactionStatus) {
	subs, err := s.store.GetSubmissionsByTxID(ctx, status.TxID)
	if err != nil {
		s.logger.Warn(
			"submission lookup failed",
			logfields.TxID(status.TxID),
			zap.Error(err),
		)
		return
	}
	enriched := false
	for _, sub := range subs {
		if sub.CallbackURL == "" {
			continue // SSE-only subscription; no webhook to send
		}
		if !shouldDeliver(sub, status) {
			continue
		}
		// Attach the merkle proof to MINED/IMMUTABLE deliveries. Done lazily on
		// the first deliverable subscriber (not for SSE-only txids) and only
		// once — the status pointer is shared across every deliver() below, so
		// all subscribers of this txid get the same enriched body. Best-effort:
		// a no-op for non-mined statuses or when the BUMP isn't retrievable, so
		// it never blocks delivery. deliver() emits the metric when a MINED body
		// still lacks a path.
		if !enriched {
			s.store.EnrichMerklePath(ctx, status)
			enriched = true
		}
		s.deliver(ctx, sub, status)
	}
}

// shouldDeliver implements the FullStatusUpdates / dedup gating rules:
//   - When FullStatusUpdates is true, every distinct status transition is
//     delivered.
//   - When FullStatusUpdates is false (the default), only terminal statuses
//     (MINED, REJECTED, MINED_IN_STALE_BLOCK / IMMUTABLE) are delivered.
//   - Same status as LastDeliveredStatus is suppressed (idempotent dedup) —
//     EXCEPT a MINED event flagged as a reorg re-anchor (issue #279): it is
//     the one same-status transition that carries new information (the
//     block hash + merkle path changed), and suppressing it would leave the
//     receiver holding a proof anchored to an orphaned block.
func shouldDeliver(sub *models.Submission, status *models.TransactionStatus) bool {
	if sub.LastDeliveredStatus == status.Status {
		return status.Status == models.StatusMined && status.ExtraInfo == models.ExtraInfoReorgReanchor
	}
	if sub.FullStatusUpdates {
		return true
	}
	switch status.Status {
	case models.StatusMined, models.StatusRejected, models.StatusImmutable, models.StatusDoubleSpendAttempted:
		return true
	default:
		return false
	}
}

// deliver attempts a single POST and records the outcome.
//
// Cross-replica coordination uses store.UpdateDeliveryStatusCAS as a claim
// primitive: before POSTing, the caller advances LastDeliveredStatus from
// the value it observed (sub.LastDeliveredStatus) to the new value with a
// conditional UPDATE. With N api-server replicas all subscribed to the
// same Kafka topic (each via its own consumer group, so each pod sees
// every event), exactly one wins the CAS for each transition; the rest
// silently skip. Issue #166.
//
// On POST failure, recordFailure increments RetryCount and writes
// NextRetryAt. LastDeliveredStatus has already advanced as part of the
// claim, so an event re-arriving with the same status is now a dedup
// rather than a retry trigger. The lease-gated webhook reaper
// (see reaper.go) closes that gap: it sweeps submissions whose
// NextRetryAt has elapsed and re-runs deliver for them, so a transient
// 5xx no longer translates into permanent loss.
func (s *Service) deliver(ctx context.Context, sub *models.Submission, status *models.TransactionStatus) {
	logger := s.logger.With(
		logfields.TxID(status.TxID),
		logfields.CallbackURL(sub.CallbackURL),
		logfields.Status(string(status.Status)),
	)

	claimed, err := s.store.UpdateDeliveryStatusCAS(ctx, sub.SubmissionID, sub.LastDeliveredStatus, status.Status)
	if err != nil {
		logger.Warn("delivery cas failed", zap.Error(err))
		return
	}
	if !claimed {
		metrics.WebhookCASLostTotal.Inc()
		return
	}

	if (status.Status == models.StatusMined || status.Status == models.StatusImmutable) && len(status.MerklePath) == 0 {
		metrics.MinedPushWithoutMerklePathTotal.WithLabelValues("webhook").Inc()
	}

	// The event-only CallbackTokenHint must never reach a receiver: it is an
	// SSE subscriber's token, not part of the status.
	payload := *status
	payload.CallbackTokenHint = ""
	body, err := json.Marshal(&payload)
	if err != nil {
		logger.Warn("encoding status payload failed", zap.Error(err))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.CallbackURL, bytes.NewReader(body))
	if err != nil {
		logger.Warn("building callback request failed", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// A distinctive UA (instead of Go's default Go-http-client/…) lets
	// receivers allowlist BSV Broadcaster's callbacks explicitly — generic-Go-UA
	// bot/WAF rules silently 403'd every POST to one production receiver
	// while its origin saw nothing (issue #249).
	req.Header.Set("User-Agent", webhookUserAgent)
	if sub.CallbackToken != "" {
		req.Header.Set("Authorization", "Bearer "+sub.CallbackToken)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		s.recordAttempt(ctx, sub, logger, err.Error())
		s.recordFailure(ctx, sub, status, logger, err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		reason := fmt.Sprintf("status %d", resp.StatusCode)
		s.recordAttempt(ctx, sub, logger, reason)
		s.recordFailure(ctx, sub, status, logger, reason)
		return
	}

	s.recordAttempt(ctx, sub, logger, deliveryResultDelivered)

	// The CAS already advanced LastDeliveredStatus and cleared retry state,
	// and recordAttempt above stamped the attempt bookkeeping — recordFailure
	// remains the only retry-state writer. Info (not Debug): only subscribed
	// txs reach here, so volume is bounded by callback subscriptions, not raw
	// TPS — and this is the terminal-delivery lifecycle line for the
	// txid/callback_url/status search story. No telemetry.LoggerWith here:
	// deliver runs on the delivery worker pool from a Kafka-fed status
	// event, not an HTTP request, so no per-request span ctx is plumbed in.
	logger.Info("callback delivered")
}

// deliveryResultDelivered is the LastResult value recorded for a successful
// POST; failures record the transport error or "status <code>" instead. The
// exact string is part of the GET /tx `callbacks[].lastResult` surface.
const deliveryResultDelivered = "delivered"

// recordAttempt stamps the outcome of one POST attempt on the submission row
// (attempts+1, lastAttemptAt, lastResult) so a client can self-diagnose
// delivery problems from GET /tx?callbackToken=… — e.g. a WAF 403-ing every
// callback looks like "BSV Broadcaster never called" from the receiver's side (issue
// #249). Orthogonal to retry-state bookkeeping (CAS/recordFailure), and
// best-effort: a write failure never changes the delivery outcome.
func (s *Service) recordAttempt(ctx context.Context, sub *models.Submission, logger *zap.Logger, result string) {
	if err := s.store.RecordDeliveryAttempt(ctx, sub.SubmissionID, time.Now(), result); err != nil {
		logger.Warn("recording delivery attempt failed", zap.Error(err))
	}
}

// recordFailure increments the submission's retry counter and computes the
// next-retry timestamp using exponential backoff bounded by MaxBackoffMs.
// Once RetryCount exceeds MaxRetries the submission is marked terminal so
// the reaper / future retry sweeps skip it.
//
// IMPORTANT: must write status.Status (the post-CAS value) as the lastStatus
// argument, NOT sub.LastDeliveredStatus (the pre-CAS in-memory value).
// Writing the pre-CAS value here would amount to an unconditional rollback
// of the claim and could clobber a concurrent CAS-success on a later
// transition — see issue #166. The CAS is the authoritative state advance;
// recordFailure only updates retry bookkeeping.
func (s *Service) recordFailure(ctx context.Context, sub *models.Submission, status *models.TransactionStatus, logger *zap.Logger, reason string) {
	maxRetries := s.cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10
	}
	nextCount := sub.RetryCount + 1
	logger.Warn("callback delivery failed", zap.Int("retry_count", nextCount), zap.String("reason", reason))

	if nextCount >= maxRetries {
		// Stop retrying — record the attempt count but don't schedule another.
		if err := s.store.UpdateDeliveryStatus(ctx, sub.SubmissionID, status.Status, nextCount, nil); err != nil {
			logger.Warn("updating delivery status on final failure failed", zap.Error(err))
		}
		return
	}

	backoff := s.computeBackoff(nextCount)
	next := time.Now().Add(backoff)
	if err := s.store.UpdateDeliveryStatus(ctx, sub.SubmissionID, status.Status, nextCount, &next); err != nil {
		logger.Warn("scheduling retry failed", zap.Error(err))
	}
}

// computeBackoff returns the wait time for the n-th retry: initial * 2^(n-1),
// capped by MaxBackoffMs.
func (s *Service) computeBackoff(retryCount int) time.Duration {
	initial := time.Duration(s.cfg.InitialBackoffMs) * time.Millisecond
	if initial <= 0 {
		initial = 5 * time.Second
	}
	maxBackoff := time.Duration(s.cfg.MaxBackoffMs) * time.Millisecond
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Minute
	}
	d := initial
	for i := 1; i < retryCount; i++ {
		d *= 2
		if d >= maxBackoff {
			return maxBackoff
		}
	}
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
