package api_server

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/broadcaster/config"
)

// exportFS is a minimal static export with the shapes the real one has: pages
// as .html files, a page's RSC payloads in a directory beside it, hashed
// assets under _next/, and the machine-readable files.
func exportFS() fstest.MapFS {
	return fstest.MapFS{
		".gitkeep":                         {Data: nil},
		"index.html":                       {Data: []byte("<!doctype html><title>home</title>")},
		"docs.html":                        {Data: []byte("<!doctype html><title>docs</title>")},
		"404.html":                         {Data: []byte("<!doctype html><title>not found</title>")},
		"docs/__next.docs.__PAGE__.txt":    {Data: []byte("rsc")},
		"_next/static/chunks/app.js":       {Data: []byte("console.log(1)")},
		"llms.txt":                         {Data: []byte("# BSV Broadcaster")},
		"openapi.yaml":                     {Data: []byte("openapi: 3.0.3")},
		".well-known/webmcp.json":          {Data: []byte("{}")},
		"licenses/Geist-OFL.txt":           {Data: []byte("OFL")},
		"_not-found.html":                  {Data: []byte("<!doctype html><title>not found</title>")},
		"_not-found/__next._index.txt":     {Data: []byte("rsc")},
		"docs/nested/__next._index.txt":    {Data: []byte("rsc")},
		"images/bsv-mark.svg":              {Data: []byte("<svg/>")},
		"__next.__PAGE__.txt":              {Data: []byte("rsc")},
		"sitemap.xml":                      {Data: []byte("<urlset/>")},
		"robots.txt":                       {Data: []byte("User-Agent: *")},
		"llms-full.txt":                    {Data: []byte("# full")},
		"openapi.json":                     {Data: []byte("{}")},
		"favicon-32.png":                   {Data: []byte("png")},
		"images/hero.webp":                 {Data: []byte("webp")},
		"_next/static/media/font.woff2":    {Data: []byte("woff2")},
		"_next/static/css/app.css":         {Data: []byte("body{}")},
		"_next/build-id/_buildManifest.js": {Data: []byte("x")},
	}
}

func siteRouter(t *testing.T, site fstest.MapFS) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:    &config.Config{Telemetry: config.TelemetryConfig{ServiceName: "site-test"}},
		logger: zap.NewNop(),
		site:   site,
	}
	return srv.newRouter()
}

func get(router http.Handler, path string, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	for _, m := range mutate {
		m(req)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestSiteServesExportWithSecurityHeaders(t *testing.T) {
	router := siteRouter(t, exportFS())

	for path, title := range map[string]string{"/": "home", "/docs": "docs", "/docs/": "docs"} {
		w := get(router, path)
		require.Equal(t, http.StatusOK, w.Code, path)
		assert.Contains(t, w.Body.String(), "<title>"+title+"</title>", path)
		assert.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"), path)

		csp := w.Header().Get("Content-Security-Policy")
		assert.Contains(t, csp, "default-src 'self'", path)
		assert.Contains(t, csp, "frame-ancestors 'none'", path)
		assert.NotContains(t, csp, "upgrade-insecure-requests", "plain HTTP must not upgrade: "+path)
		assert.Empty(t, w.Header().Get("Strict-Transport-Security"), "no HSTS over plain HTTP: "+path)
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"), path)
		assert.Equal(t, "DENY", w.Header().Get("X-Frame-Options"), path)
		assert.NotEmpty(t, w.Header().Get("Referrer-Policy"), path)
		assert.NotEmpty(t, w.Header().Get("Permissions-Policy"), path)
	}
}

func TestSiteHTTPSAddsHSTSAndUpgrade(t *testing.T) {
	router := siteRouter(t, exportFS())

	for name, mutate := range map[string]func(*http.Request){
		"tls":             func(r *http.Request) { r.TLS = &tls.ConnectionState{} },
		"forwarded-proto": func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "https") },
	} {
		w := get(router, "/", mutate)
		require.Equal(t, http.StatusOK, w.Code, name)
		assert.Contains(t, w.Header().Get("Content-Security-Policy"), "upgrade-insecure-requests", name)
		assert.Equal(t, "max-age=63072000", w.Header().Get("Strict-Transport-Security"), name)
	}
}

func TestSiteContentTypesAndCaching(t *testing.T) {
	router := siteRouter(t, exportFS())

	for path, want := range map[string]string{
		"/llms.txt":                      "text/markdown; charset=utf-8",
		"/llms-full.txt":                 "text/markdown; charset=utf-8",
		"/openapi.yaml":                  "application/yaml; charset=utf-8",
		"/openapi.json":                  "application/json; charset=utf-8",
		"/.well-known/webmcp.json":       "application/json; charset=utf-8",
		"/robots.txt":                    "text/plain; charset=utf-8",
		"/sitemap.xml":                   "application/xml; charset=utf-8",
		"/_next/static/chunks/app.js":    "text/javascript; charset=utf-8",
		"/_next/static/media/font.woff2": "font/woff2",
		"/images/bsv-mark.svg":           "image/svg+xml",
		"/images/hero.webp":              "image/webp",
		"/docs/__next.docs.__PAGE__.txt": "text/plain; charset=utf-8",
		"/__next.__PAGE__.txt":           "text/plain; charset=utf-8",
	} {
		w := get(router, path)
		require.Equal(t, http.StatusOK, w.Code, path)
		assert.Equal(t, want, w.Header().Get("Content-Type"), path)
	}

	// Machine-readable files must be fetchable cross-origin; the API's CORS
	// wrapper (applied around the router in Start) provides that.
	assert.Equal(t, "public, max-age=31536000, immutable", get(router, "/_next/static/chunks/app.js").Header().Get("Cache-Control"))
	assert.Equal(t, "public, max-age=300", get(router, "/").Header().Get("Cache-Control"))
}

func TestSiteHead(t *testing.T) {
	router := siteRouter(t, exportFS())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/docs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestSiteMissingAssetServes404Page(t *testing.T) {
	router := siteRouter(t, exportFS())
	for _, path := range []string{"/_next/static/chunks/missing.js", "/_next/../index.html", "/docs/nope"} {
		w := get(router, path)
		assert.Equal(t, http.StatusNotFound, w.Code, path)
	}
	assert.Equal(t, http.StatusNotFound, get(router, "/404").Code)
	w := get(router, "/_next/static/chunks/missing.js")
	assert.Contains(t, w.Body.String(), "<title>not found</title>")
	assert.NotEmpty(t, w.Header().Get("Content-Security-Policy"))
}

// postOnlyAPIRoute is an API route with no GET handler: a GET to it must get
// the router's 404, not the site.
const postOnlyAPIRoute = "/tx"

// Unknown paths outside the site's entries are not swallowed by the site: they
// get the router's own 404, not HTML.
func TestSiteDoesNotShadowAPI404(t *testing.T) {
	router := siteRouter(t, exportFS())
	for _, path := range []string{"/nope", "/api/v1/nope", postOnlyAPIRoute} {
		w := get(router, path)
		assert.Equal(t, http.StatusNotFound, w.Code, path)
		assert.NotContains(t, w.Header().Get("Content-Type"), "text/html", path)
		assert.Empty(t, w.Header().Get("Content-Security-Policy"), path)
	}
	// API routes are untouched by the site's headers.
	w := get(router, "/ready")
	assert.Empty(t, w.Header().Get("Content-Security-Policy"))
}

func TestSiteFallsBackWithoutExport(t *testing.T) {
	router := siteRouter(t, fstest.MapFS{".gitkeep": {Data: nil}})

	w := get(router, "/")
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
	assert.Contains(t, w.Body.String(), "Chaintracks is a separate BSV Broadcaster listener")

	assert.Equal(t, http.StatusNotFound, get(router, "/docs").Code)
	assert.Equal(t, http.StatusNotFound, get(router, "/llms.txt").Code)
}

// An export entry that collides with an API route must fail loudly at
// registration, not shadow the route.
func TestSiteCollisionWithAPIRoutePanics(t *testing.T) {
	site := exportFS()
	site["health"] = &fstest.MapFile{Data: []byte("shadow")}
	assert.Panics(t, func() { siteRouter(t, site) })
}

// The real embedded export (or its placeholder) must register cleanly.
func TestSiteEmbeddedExportRegisters(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:    &config.Config{Telemetry: config.TelemetryConfig{ServiceName: "site-test"}},
		logger: zap.NewNop(),
	}
	router := srv.newRouter()
	assert.Equal(t, http.StatusOK, get(router, "/").Code)
}

// Site routes are page views, not API traffic: no otelgin spans (or metrics,
// which the same filter skips).
func TestSiteRoutesAreNotTraced(t *testing.T) {
	restoreOTELTracerProvider(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	router := siteRouter(t, exportFS())
	for _, path := range []string{"/", "/docs", "/llms.txt", "/_next/static/chunks/app.js", "/_next/static/chunks/missing.js", "/404"} {
		get(router, path)
	}
	assert.Empty(t, exporter.GetSpans())

	// The fallback page is a site route too.
	get(siteRouter(t, fstest.MapFS{}), "/")
	assert.Empty(t, exporter.GetSpans())

	// An unknown path is not a site route: still traced, as before.
	get(router, "/nope")
	assert.NotEmpty(t, exporter.GetSpans())
}

// A missing site asset is not an API fault: Debug, while an unknown API path
// keeps logging at Warn.
func TestSiteAsset404LogsAtDebug(t *testing.T) {
	gin.SetMode(gin.TestMode)
	core, logs := observer.New(zapcore.DebugLevel)
	srv := &Server{
		cfg:    &config.Config{Telemetry: config.TelemetryConfig{ServiceName: "site-test"}},
		logger: zap.New(core),
		site:   exportFS(),
	}
	router := srv.newRouter()

	require.Equal(t, http.StatusNotFound, get(router, "/_next/static/chunks/missing.js").Code)
	require.Equal(t, http.StatusNotFound, get(router, "/nope").Code)

	levels := map[string]zapcore.Level{}
	for _, e := range logs.FilterMessage("request").All() {
		levels[e.ContextMap()["path"].(string)] = e.Level
	}
	assert.Equal(t, zapcore.DebugLevel, levels["/_next/static/chunks/missing.js"])
	assert.Equal(t, zapcore.WarnLevel, levels["/nope"])
}
