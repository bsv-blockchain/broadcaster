package api_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/finality"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/services/httpmiddleware"
	"github.com/bsv-blockchain/broadcaster/services/ratelimit"
	"github.com/bsv-blockchain/broadcaster/services/usage"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/telemetry"
	"github.com/bsv-blockchain/broadcaster/teranode"
	"github.com/bsv-blockchain/broadcaster/validator"
)

const (
	// submissionRecorderBuffer caps the in-memory queue depth feeding the
	// async InsertSubmission workers. 4096 absorbs ~80s of 50 TPS without
	// dropping; sustained backpressure triggers drop+metric (best-effort
	// contract preserved).
	submissionRecorderBuffer = 4096
	// submissionRecorderWorkers is the worker count draining submissionCh.
	// 8 is comfortably above expected DB write concurrency on Pebble; the
	// real limiter is the store.BatchConcurrency knob.
	submissionRecorderWorkers = 8
)

type Server struct {
	services.ReadyHook

	cfg    *config.Config
	logger *zap.Logger
	// usageRecorder meters guarded routes and usageReporter delivers the
	// counts to providers; both nil unless usage.enabled (see initUsage).
	usageRecorder *usage.Recorder
	usageReporter *usage.Reporter
	// retention enforces per-tier data retention; nil when no tier
	// configures it or the store cannot (see initRetention).
	retention    store.RetentionExtender
	producer     *kafka.Producer
	publisher    events.Publisher // nil-safe; status updates flow to SSE via Kafka — the api-server itself publishes but does not subscribe
	store        store.Store
	txTracker    *store.TxTracker
	teranode     *teranode.Client      // used by /health for datahub URL inventory; nil in tests
	merkleClient merkleservice.Service // nil when no merkle_service endpoint is configured; gates POST /api/v1/blocks/:blockHash/reprocess
	// validator runs synchronous policy validation in the submit handler.
	// Nil-safe: tests that use struct-literal construction may leave it
	// unset, in which case the handler skips validation. Production
	// wiring through New requires it.
	validator *validator.Validator
	// finality runs the nLockTime/BIP113 pre-check in the submit handler,
	// rejecting provably non-final txs with an actionable reason instead of
	// letting teranode bounce them with a generic error (issue #245).
	// Nil-safe: nil (no chain source configured, or disabled) skips the gate.
	finality *finality.Checker
	server   *http.Server

	// keyResolver, identify and limiter are built by initAccessControl from
	// auth.* and ratelimit.*; nil when disabled (the default), in which case
	// guarded() adds no middleware. identify wraps keyResolver exactly once
	// (not once per route — see initAccessControl) so its per-client-IP
	// invalid-key throttle has one shared budget across every guarded route.
	// adminServer is the admin.* listener.
	keyResolver *apikey.Resolver
	// authStore, when set (auth.store.mongodb), is where API keys and quota
	// counters live instead of store. See SetAuthStore.
	authStore   AuthStore
	identify    gin.HandlerFunc
	limiter     *ratelimit.RateLimiter
	adminServer *http.Server

	// submissionCh decouples the InsertSubmission Pebble write from the HTTP
	// handler tail latency. recordSubmission enqueues onto it via a non-
	// blocking select; a worker pool drains and writes asynchronously. Drop on
	// full is acceptable because the underlying call is already best-effort
	// (errors are logged, not surfaced to the client).
	submissionCh   chan submissionRecord
	submissionStop chan struct{}
	stopOnce       sync.Once

	// tipMu/tipHeight/tipFetchedAt cache the store's active-tip height for
	// /health (see activeTipHeight): probes arrive every few seconds and
	// the underlying read is a store scan we don't want to pay per probe.
	tipMu        sync.Mutex
	tipHeight    uint64
	tipFetchedAt time.Time

	// policyMu/policyByURL/policyFetchedAt cache the per-endpoint advertised
	// policies /health reports alongside each datahub URL (see
	// endpointPolicies), on the same TTL and for the same reason as the tip
	// height above: one registry read per TTL rather than one per probe.
	policyMu        sync.Mutex
	policyByURL     map[string]*store.EndpointPolicy
	policyFetchedAt time.Time
}

// submissionRecord is the in-memory payload the async recorder consumes.
// Kept tiny — just enough to call store.InsertSubmission with the original
// values. The original request context is intentionally NOT propagated;
// recordSubmission is best-effort and shouldn't be canceled when the HTTP
// handler returns.
type submissionRecord struct {
	sub *models.Submission
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, publisher events.Publisher, st store.Store, tracker *store.TxTracker, tc *teranode.Client, mc merkleservice.Service, val *validator.Validator, fin *finality.Checker) *Server {
	// Export every series this service can emit at 0 from the first scrape —
	// a series born mid-burst is invisible to increase() until its second
	// sample, which undercounted submissions after every rollout.
	metrics.PreRegisterTxSubmissions()
	metrics.PreRegisterStatusTransitions(models.StatusSeenOnNetwork, models.StatusSeenMultipleNodes)
	return &Server{
		cfg:            cfg,
		logger:         logger.Named("api-server"),
		producer:       producer,
		publisher:      publisher,
		store:          st,
		txTracker:      tracker,
		teranode:       tc,
		merkleClient:   mc,
		validator:      val,
		finality:       fin,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
}

func (s *Server) Name() string { return "api-server" }

// newRouter assembles the gin engine and its middleware stack: inbound OTEL
// tracing (outermost), then panic recovery, then the request logger/metrics
// middleware, followed by every registered route. Factored out of Start so
// tests can exercise the full middleware stack (span creation, trace-id log
// correlation) through httptest without binding a real listener.
//
// otelgin must be OUTSIDE CustomRecovery: otelgin restores the pre-span
// request context in a defer, and deferred funcs run innermost-first during
// a panic unwind — were recovery outermost, the span would already be gone
// from c.Request.Context() by the time recoverPanic runs, and panic logs
// would lose their trace_id/span_id. With otelgin outermost the panic is
// recovered while the span is still live (fields attach) and otelgin's own
// post-processing then ends the span with the 500 recovery wrote, so panics
// show up on traces too. The cost — a panic inside otelgin itself has no
// recovery above it — is acceptable: otelgin is thin and widely deployed,
// and net/http still contains per-request panics at the server level.
//
// otelgin.Middleware is installed unconditionally so tracing can be turned
// on via env at runtime without a redeploy. It is NOT free when telemetry is
// disabled: on every non-filtered request otelgin still runs the propagator
// Extract, builds the full semconv attribute slice, copies the request
// context, and assembles the metric attributes (~15+ allocations) — it just
// never exports or touches the network (the no-op provider drops it all).
// That per-request overhead is negligible next to the JSON (de)serialisation
// each handler already does, so gating on Enabled isn't worth losing the
// zero-redeploy toggle. /health, /ready, and /metrics are filtered out —
// they're polled far more often than real traffic and carry no useful trace
// information.
func (s *Server) newRouter() *gin.Engine {
	router := gin.New()
	s.applyTrustedPlatform(router)
	router.Use(otelgin.Middleware(s.cfg.Telemetry.ServiceName, otelgin.WithGinFilter(func(c *gin.Context) bool {
		p := c.FullPath()
		return p != "/health" && p != "/ready" && p != "/metrics"
	})))
	router.Use(gin.CustomRecovery(s.recoverPanic))
	router.Use(s.requestLogger())

	s.registerRoutes(router)
	return router
}

func (s *Server) Start(ctx context.Context) error {
	gin.SetMode(gin.ReleaseMode)
	if err := s.initAccessControl(); err != nil { //nolint:contextcheck // false positive: contextcheck's call graph walks into apikey.Middleware's inner gin closures (newMiddleware -> authenticated), which use c.Request.Context() -- the correct per-request context -- not a ctx parameter for the linter to compare against; initAccessControl itself takes no ctx.
		return err
	}
	s.initRetention(ctx)
	s.initUsage(ctx)
	if s.cfg.Admin.Enabled {
		if err := s.startAdmin(ctx); err != nil {
			return err
		}
	} else {
		s.logger.Warn("POST /api/v1/blocks/:blockHash/reprocess is served unauthenticated on the public API; enable admin.* to move it behind the admin token")
	}
	router := s.newRouter() //nolint:contextcheck // false positive: requestLogger's telemetry.Fields(c.Request.Context()) already uses the correct per-request context; requestLogger is a gin middleware factory with no ctx parameter of its own for the linter to compare against.

	// Spin up the submission recorder pool. Workers exit on submissionStop
	// (Stop()) which is signaled before the HTTP server is closed. The
	// recorder is intentionally NOT bound to the request context — it's a
	// fire-and-forget best-effort DB write that must outlive the HTTP
	// handler that triggered it, so gosec G118 ("use request-scoped ctx")
	// does not apply here.
	var recorderWG sync.WaitGroup
	for i := 0; i < submissionRecorderWorkers; i++ {
		recorderWG.Add(1)
		go s.runSubmissionRecorder(ctx, &recorderWG)
	}

	// For each policy dimension the operator hasn't pinned, track what the
	// network will accept (from peer node_status observations) and push it into
	// the validator so intake enforces it (issue #212): the lowest mining fee,
	// and the most permissive size limits. Bound to ctx; no drain needed, so it
	// isn't part of recorderWG.
	if s.validator != nil && s.policyTracked() {
		s.logger.Info("starting dynamic intake policy tracking from network observations",
			zap.Bool("track_fee", !s.feePinned()),
			zap.Bool("track_max_tx_size", !s.maxTxSizePinned()),
			zap.Bool("track_max_script_size", !s.maxScriptSizePinned()),
			zap.Uint64("refresh_ms", s.observedFeeRefreshMs()),
			zap.Uint64("ttl_ms", s.observedFeeTTLMs()))
		go s.runPolicyRefresher(ctx)
	}

	addr := fmt.Sprintf("%s:%d", s.cfg.APIServer.Host, s.cfg.APIServer.Port)
	s.server = &http.Server{
		Addr:              addr,
		Handler:           httpmiddleware.WithCORS(router),
		ReadHeaderTimeout: 30 * time.Second,
	}

	s.logger.Info("API server listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		_ = s.Stop() //nolint:contextcheck // Stop uses context.Background() so the 15s drain outlives the parent ctx that just fired.
		recorderWG.Wait()
	}()

	// /ready on this listener is served only after the bind succeeds, which is
	// the same moment the process-wide health gate is signaled.
	if err := services.ListenAndServeReady(ctx, s.server, &s.ReadyHook); err != nil && !errors.Is(err, http.ErrServerClosed) {
		// The admin listener (if any) was already started above; nobody else
		// is going to stop it on this error path, since Stop() is only
		// invoked by the ctx-watcher goroutine above once ctx is canceled —
		// which never happens here, because Start is returning on its own.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if adminErr := s.stopAdmin(shutdownCtx); adminErr != nil { //nolint:contextcheck // deliberate: ctx is what just failed/is being torn down, not a parent to derive a shutdown deadline from; context.Background() mirrors Stop()'s own shutdown contexts below.
			s.logger.Warn("admin listener shutdown failed after public listener error", zap.Error(adminErr))
		}
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// runSubmissionRecorder drains submissionCh into store.InsertSubmission.
// parentCtx is the process/server lifetime context so a service shutdown
// also unwinds in-flight DB writes; per-call writes derive a short timeout
// child so a recorder write outlives the HTTP request that triggered it
// (handler returns before the row lands — that's the whole point of the
// decoupling).
func (s *Server) runSubmissionRecorder(parentCtx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-s.submissionStop:
			return
		case <-parentCtx.Done():
			return
		case rec, ok := <-s.submissionCh:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
			if err := s.store.InsertSubmission(ctx, rec.sub); err != nil {
				s.logger.Warn(
					"failed to insert submission (async)",
					logfields.TxID(rec.sub.TxID),
					zap.Error(err),
				)
			}
			cancel()
		}
	}
}

func (s *Server) requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics.APIRequestsInFlight.Inc()
		defer metrics.APIRequestsInFlight.Dec()

		start := time.Now()
		c.Next()
		status := c.Writer.Status()

		// Use the matched gin route pattern (not the resolved URL) so /tx/:txid
		// reports as one bucket regardless of which txid was requested. Falls
		// back to "unmatched" for routes Gin couldn't resolve.
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		metrics.APIRequestDuration.WithLabelValues(
			route,
			c.Request.Method,
			metrics.ObserveStatusClass(status),
		).Observe(time.Since(start).Seconds())

		// Request body size — caps cardinality by routing through the route
		// label rather than per-request. ContentLength is -1 if not set; clamp
		// to 0 in that case.
		if reqLen := c.Request.ContentLength; reqLen > 0 {
			metrics.APIRequestBytes.WithLabelValues(route).Observe(float64(reqLen))
		}

		fields := append([]zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			// status_code (HTTP), not "status": the canonical "status" field is
			// reserved for the transaction-status string (logfields.Status), so
			// an HTTP status int must use a distinct key to avoid a mixed-type
			// collision in the shared log-field namespace.
			zap.Int("status_code", status),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		}, telemetry.Fields(c.Request.Context())...)
		fields = append(fields, apikey.LogFields(c)...)
		switch {
		case status == http.StatusServiceUnavailable:
			// Transient unavailability (store down, merkle client not
			// configured, quota store unreachable, …) is an operational
			// condition this service reports on, not a bug in it — Warn,
			// not Error.
			s.logger.Warn("request", fields...)
		case status >= 500:
			s.logger.Error("request", fields...)
		case status == ratelimit.StatusClientClosedRequest,
			c.Writer.Header().Get("WWW-Authenticate") != "",
			c.Writer.Header().Get(apikey.HeaderRateLimitReason) != "":
			// Expected client behavior under the identify/rate-limit layers
			// specifically -- an invalid/missing key (WWW-Authenticate) or a
			// 429 from either of them (X-RateLimit-Reason) -- counted by
			// bsv_broadcaster_ratelimit_decisions_total; at warn level it would flood
			// sampled logs. 499 (client hung up mid-identify or mid-charge)
			// is the same kind of ordinary client behavior, not ours to warn
			// about. Gating on these response markers, not on status alone,
			// keeps an unrelated 401 (e.g. the merkle callback's own bearer
			// check in handleCallback) at Warn: it carries neither header, so
			// a misconfigured callback token is still reported loudly.
			s.logger.Debug("request", fields...)
		case status >= 400:
			s.logger.Warn("request", fields...)
		default:
			s.logger.Debug("request", fields...)
		}
	}
}

// recoverPanic is wired into gin.CustomRecovery so handler panics are logged
// through zap (structured) rather than gin's default stderr text writer.
// Because recovery sits inside otelgin (see newRouter), the request context
// still carries the live span here, so the panic line gets trace_id/span_id
// and the span is subsequently ended with the 500 written below.
func (s *Server) recoverPanic(c *gin.Context, recovered any) {
	fields := append([]zap.Field{
		zap.Any("panic", recovered),
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("client_ip", c.ClientIP()),
		zap.Stack("stack"),
	}, telemetry.Fields(c.Request.Context())...)
	s.logger.Error("panic in handler", fields...)
	c.AbortWithStatus(http.StatusInternalServerError)
}

func (s *Server) Stop() error {
	// Stop() is invoked by the Start ctx-watcher and may race with an explicit
	// supervisor caller; sync.Once is the only race-free way to ensure the
	// channel close happens exactly once.
	s.stopOnce.Do(func() {
		close(s.submissionStop)
	})

	// The public and admin listeners each get their own 15s shutdown budget
	// and drain concurrently, so the total wall time Stop can take is 15s,
	// not 30s — and a slow/stuck admin drain can never delay the public
	// listener's shutdown (or vice versa).
	var wg sync.WaitGroup
	var publicErr, adminErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		if s.server == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		s.logger.Info("shutting down API server")
		publicErr = s.server.Shutdown(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		adminErr = s.stopAdmin(ctx)
	}()

	wg.Wait()
	// After the listeners drained, so the last requests are counted.
	s.stopUsage()
	return errors.Join(publicErr, adminErr)
}
