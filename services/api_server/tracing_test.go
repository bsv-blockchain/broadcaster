package api_server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
)

// restoreOTELTracerProvider snapshots the global tracer provider and restores
// it when the test finishes, mirroring telemetry/telemetry_test.go's
// restoreOTELGlobals helper (unexported there, so replicated here scoped to
// just the tracer provider this test touches).
func restoreOTELTracerProvider(t *testing.T) {
	t.Helper()
	tp := otel.GetTracerProvider()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != tp {
			otel.SetTracerProvider(tp)
		}
	})
}

// newTestRouter builds a Server with an observed logger and wires it through
// newRouter — the same middleware assembly Start() uses (otelgin,
// CustomRecovery, requestLogger) — so tests exercise the real middleware
// stack without binding a socket.
func newTestRouter(logger *zap.Logger) *gin.Engine {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg: &config.Config{
			Telemetry: config.TelemetryConfig{ServiceName: "bsv-broadcaster-tracing-test"},
		},
		logger: logger,
	}
	return srv.newRouter()
}

// TestNewRouter_CreatesServerSpanWithRoutePattern asserts otelgin.Middleware
// is wired into the engine Start() builds: a request through a traced route
// produces exactly one server span named after the matched gin route
// pattern, and the request log line (emitted by requestLogger, which runs
// inside otelgin) carries the matching trace_id.
func TestNewRouter_CreatesServerSpanWithRoutePattern(t *testing.T) {
	restoreOTELTracerProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	core, logs := observer.New(zap.DebugLevel)
	router := newTestRouter(zap.New(core))
	// "/" is the docs site, which is deliberately untraced (site.go), so use
	// a route of the test's own.
	router.GET("/span-test/:id", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/span-test/42", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /span-test/42 status = %d, want 200", w.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans for GET /span-test/42, want 1: %+v", len(spans), spans)
	}
	span := spans[0]
	if want := "GET /span-test/:id"; span.Name != want {
		t.Errorf("span name = %q, want %q (route pattern as name)", span.Name, want)
	}
	if !span.SpanContext.IsValid() {
		t.Fatal("recorded span has an invalid span context")
	}

	// The request log line must carry the same trace_id so logs and traces
	// correlate in the downstream backend.
	var requestEntry *observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Message == "request" {
			entry := e
			requestEntry = &entry
			break
		}
	}
	if requestEntry == nil {
		t.Fatal("no \"request\" log line emitted")
	}
	ctxMap := requestEntry.ContextMap()
	wantTraceID := span.SpanContext.TraceID().String()
	if got := ctxMap["trace_id"]; got != wantTraceID {
		t.Errorf("request log trace_id = %v, want %q", got, wantTraceID)
	}
	if got := ctxMap["span_id"]; got != span.SpanContext.SpanID().String() {
		t.Errorf("request log span_id = %v, want %q", got, span.SpanContext.SpanID().String())
	}
}

// TestNewRouter_PanicLogCarriesTraceID pins the middleware ordering contract
// documented on newRouter: otelgin sits OUTSIDE CustomRecovery, so when a
// handler panics the span is still live in the request context when
// recoverPanic runs. Asserts (a) the "panic in handler" log line carries the
// trace_id/span_id of the exported span, (b) the client still gets a 500,
// and (c) a server span for the panicking route is exported and marked as an
// error (otelgin ends it after recovery wrote the 500).
func TestNewRouter_PanicLogCarriesTraceID(t *testing.T) {
	restoreOTELTracerProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	core, logs := observer.New(zap.DebugLevel)
	router := newTestRouter(zap.New(core))
	router.GET("/panic-test", func(_ *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic-test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("GET /panic-test status = %d, want 500", w.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans for GET /panic-test, want 1: %+v", len(spans), spans)
	}
	span := spans[0]
	if want := "GET /panic-test"; span.Name != want {
		t.Errorf("span name = %q, want %q", span.Name, want)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error (recovered panic returned 500)", span.Status.Code)
	}

	var panicEntry *observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Message == "panic in handler" {
			entry := e
			panicEntry = &entry
			break
		}
	}
	if panicEntry == nil {
		t.Fatal("no \"panic in handler\" log line emitted")
	}
	ctxMap := panicEntry.ContextMap()
	if got := ctxMap["trace_id"]; got != span.SpanContext.TraceID().String() {
		t.Errorf("panic log trace_id = %v, want %q (recovery must run inside the otelgin span)", got, span.SpanContext.TraceID().String())
	}
	if got := ctxMap["span_id"]; got != span.SpanContext.SpanID().String() {
		t.Errorf("panic log span_id = %v, want %q", got, span.SpanContext.SpanID().String())
	}
}

// TestNewRouter_HealthReadyMetricsAreNotTraced pins the otelgin filter: the
// three polling endpoints must never produce spans, however often infra
// probes them.
func TestNewRouter_HealthReadyMetricsAreNotTraced(t *testing.T) {
	restoreOTELTracerProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	router := newTestRouter(zap.NewNop())

	for _, path := range []string{"/health", "/ready", "/metrics"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
	}

	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("expected no spans for /health, /ready, /metrics, got %d: %+v", len(spans), spans)
	}
}

// TestRequestLogger_RestrictsDebugToIdentifyAndRateLimitResponses pins the
// requestLogger log-level split: a 401 carrying WWW-Authenticate (apikey's
// own) logs at Debug, while handleCallback's unrelated 401 (a misconfigured
// or absent callback token -- no WWW-Authenticate, no X-RateLimit-Reason)
// must still log at Warn. Before this split, every 401/429/499 logged at
// Debug regardless of origin, silently demoting the callback case too.
func TestRequestLogger_RestrictsDebugToIdentifyAndRateLimitResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	srv := &Server{
		cfg: &config.Config{
			Telemetry:     config.TelemetryConfig{ServiceName: "bsv-broadcaster-requestlogger-test"},
			CallbackToken: "cb-token",
			Auth:          config.AuthConfig{Enabled: true},
			RateLimit:     config.RateLimitConfig{KeyCacheTTLMs: 60_000, KeyCacheStaleMaxMs: 60_000},
		},
		logger:         logger,
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          st,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	require.NoError(t, srv.initAccessControl())
	router := srv.newRouter()

	// handleCallback's own bearer check: wrong/missing token. No
	// WWW-Authenticate, no X-RateLimit-Reason -- an ordinary 4xx.
	cbReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/merkle-service/callback", strings.NewReader(`{}`))
	cbReq.Header.Set("Content-Type", "application/json")
	cbW := httptest.NewRecorder()
	router.ServeHTTP(cbW, cbReq)
	require.Equal(t, http.StatusUnauthorized, cbW.Code)

	// apikey.Middleware's 401: malformed bearer token, carries
	// WWW-Authenticate.
	txReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", nil)
	txReq.Header.Set("Authorization", "Bearer garbage")
	txW := httptest.NewRecorder()
	router.ServeHTTP(txW, txReq)
	require.Equal(t, http.StatusUnauthorized, txW.Code)
	require.NotEmpty(t, txW.Header().Get("WWW-Authenticate"))

	entries := logs.FilterMessage("request").All()
	require.Len(t, entries, 2)

	levelFor := func(path string) zapcore.Level {
		t.Helper()
		for _, e := range entries {
			if e.ContextMap()["path"] == path {
				return e.Level
			}
		}
		t.Fatalf("no \"request\" log line for path %q", path)
		return 0
	}
	require.Equal(t, zapcore.WarnLevel, levelFor("/api/v1/merkle-service/callback"),
		"handleCallback's 401 must stay at Warn")
	require.Equal(t, zapcore.DebugLevel, levelFor("/tx"),
		"apikey's 401 (WWW-Authenticate) must log at Debug")
}
