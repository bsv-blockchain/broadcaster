package api_server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

// An export entry whose route would collide with an API route is skipped and
// reported, not registered: gin would panic on the conflict and crash-loop
// the api-server at startup. The API route keeps working.
func TestSiteCollisionWithAPIRouteIsSkipped(t *testing.T) {
	site := exportFS()
	site["health"] = &fstest.MapFile{Data: []byte("shadow")}
	site["policy.html"] = &fstest.MapFile{Data: []byte("<!doctype html><title>shadow</title>")}
	site["tx/index.txt"] = &fstest.MapFile{Data: []byte("shadow")}

	gin.SetMode(gin.TestMode)
	core, logs := observer.New(zapcore.ErrorLevel)
	srv := &Server{
		cfg:    &config.Config{Telemetry: config.TelemetryConfig{ServiceName: "site-test"}},
		logger: zap.New(core),
		site:   site,
	}
	var router *gin.Engine
	require.NotPanics(t, func() { router = srv.newRouter() })

	entries := logs.FilterMessage("docs site entries collide with API routes; not served").All()
	require.Len(t, entries, 1)
	assert.ElementsMatch(t, []any{"health", "policy.html", "tx"}, entries[0].ContextMap()["entries"])

	// The API keeps the paths: /policy is its own handler, not the export's page.
	assert.NotContains(t, get(router, "/health").Body.String(), "shadow")
	assert.NotContains(t, get(router, "/policy").Body.String(), "shadow")
	// The rest of the export is still served.
	assert.Equal(t, http.StatusOK, get(router, "/docs").Code)
}

// The export is built with originPlaceholder; every response names the origin
// the client reached instead, so each operator's canonical URLs, sitemap and
// llms.txt point at itself.
func TestSiteRewritesPlaceholderOrigin(t *testing.T) {
	site := exportFS()
	site["index.html"] = &fstest.MapFile{Data: []byte(`<link rel="canonical" href="` + originPlaceholder + `/">`)}
	site["sitemap.xml"] = &fstest.MapFile{Data: []byte(`<loc>` + originPlaceholder + `/docs</loc>`)}
	site["404.html"] = &fstest.MapFile{Data: []byte(`<a href="` + originPlaceholder + `/">home</a>`)}
	router := siteRouter(t, site)

	host := func(h string) func(*http.Request) { return func(r *http.Request) { r.Host = h } }
	https := func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "https") }

	w := get(router, "/", host("broadcast.example.org"), https)
	assert.Equal(t, `<link rel="canonical" href="https://broadcast.example.org/">`, w.Body.String())
	assert.Equal(t, strconv.Itoa(w.Body.Len()), w.Header().Get("Content-Length"))

	assert.Equal(t, `<loc>http://localhost:8080/docs</loc>`, get(router, "/sitemap.xml", host("localhost:8080")).Body.String())
	assert.Equal(t, `<loc>http://[::1]:8080/docs</loc>`, get(router, "/sitemap.xml", host("[::1]:8080")).Body.String())

	nf := get(router, "/docs/nope", host("op.example"))
	assert.Equal(t, http.StatusNotFound, nf.Code)
	assert.Equal(t, `<a href="http://op.example/">home</a>`, nf.Body.String())

	// A Host that is not a plain host[:port] is never written into a page.
	assert.Contains(t, get(router, "/", host(`x"><script>`)).Body.String(), originPlaceholder)
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

// flightRows parses a Flight payload as React's client does and returns each
// row's ID and body, failing the test when a length-prefixed row overruns.
func flightRows(t *testing.T, payload string) map[string]string {
	t.Helper()
	rows := map[string]string{}
	for i := 0; i < len(payload); {
		colon := strings.IndexByte(payload[i:], ':')
		require.GreaterOrEqual(t, colon, 0, "row header at %d", i)
		id, tag := payload[i:i+colon], i+colon+1
		if isFlightLengthTag(payload[tag]) {
			comma := tag + strings.IndexByte(payload[tag:], ',')
			n, err := strconv.ParseUint(payload[tag+1:comma], 16, 31)
			require.NoError(t, err)
			require.LessOrEqual(t, comma+1+int(n), len(payload), "row %s overruns", id)
			rows[id], i = payload[comma+1:comma+1+int(n)], comma+1+int(n)
			continue
		}
		end := strings.IndexByte(payload[tag:], '\n')
		require.GreaterOrEqual(t, end, 0, "row %s unterminated", id)
		rows[id], i = payload[tag:tag+end], tag+end+1
	}
	return rows
}

// Text rows in the RSC payloads are length-prefixed: rewriting the origin in
// one must rewrite its length, or the client loses every row after it and
// throws "Connection closed" (React #412) on load.
func TestSiteRewritesOriginInFlightTextRows(t *testing.T) {
	text := `{"@id":"` + originPlaceholder + `/#org","name":"Ünïcode ✓","url":"` + originPlaceholder + `/"}` + "\nsecond line"
	rsc := ":HL[\"/_next/static/chunks/app.css\",\"style\"]\n" +
		"0:[\"$\",\"link\",null,{\"href\":\"" + originPlaceholder + "/\"}]\n" +
		"3d:T" + strconv.FormatInt(int64(len(text)), 16) + "," + text +
		"21:[\"$\",\"script\",null,{\"dangerouslySetInnerHTML\":{\"__html\":\"$3d\"}}]\n" +
		"3e:A4,\x00\x01\x02\x03" +
		"3f:null\n"
	want := func(origin string) map[string]string {
		return map[string]string{
			"":   `HL["/_next/static/chunks/app.css","style"]`,
			"0":  `["$","link",null,{"href":"` + origin + `/"}]`,
			"3d": strings.ReplaceAll(text, originPlaceholder, origin),
			"21": `["$","script",null,{"dangerouslySetInnerHTML":{"__html":"$3d"}}]`,
			"3e": "\x00\x01\x02\x03",
			"3f": "null",
		}
	}
	require.Equal(t, want(originPlaceholder), flightRows(t, rsc))

	// The payload split over two push scripts, mid text row.
	push := func(s string) string {
		b, err := json.Marshal(s)
		require.NoError(t, err)
		return `<script>self.__next_f.push([1,` + string(b) + `])</script>`
	}
	split := strings.Index(rsc, "Ünïcode")
	html := `<link rel="canonical" href="` + originPlaceholder + `/"><script>(self.__next_f=self.__next_f||[]).push([0])</script>` +
		push(rsc[:split]) + `<p>` + originPlaceholder + `</p>` + push(rsc[split:]) + `</body>`

	site := exportFS()
	site["index.txt"] = &fstest.MapFile{Data: []byte(rsc)}
	site["index.html"] = &fstest.MapFile{Data: []byte(html)}
	router := siteRouter(t, site)

	for _, h := range []string{"localhost:8080", "broadcast.whatsonchain.com", "a-much-longer-host-name.example.org:8443"} {
		host := func(r *http.Request) { r.Host = h }
		origin := "http://" + h

		assert.Equal(t, want(origin), flightRows(t, get(router, "/index.txt", host).Body.String()), h)

		body := get(router, "/", host).Body.String()
		assert.NotContains(t, body, originPlaceholder, h)
		assert.Contains(t, body, `<link rel="canonical" href="`+origin+`/">`, h)
		assert.Contains(t, body, `<p>`+origin+`</p>`, h)
		var payload strings.Builder
		for _, m := range nextFlightPushRE.FindAllStringSubmatch(body, -1) {
			var args []json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(m[1]), &args))
			if string(args[0]) != "1" {
				continue
			}
			var s string
			require.NoError(t, json.Unmarshal(args[1], &s))
			payload.WriteString(s)
		}
		assert.Equal(t, want(origin), flightRows(t, payload.String()), h)
	}

	// Plain-text .txt files are not Flight payloads and are still rewritten.
	site["llms.txt"] = &fstest.MapFile{Data: []byte("# Broadcaster\n\n" + originPlaceholder + "/docs\n")}
	assert.Equal(t, "# Broadcaster\n\nhttp://op.example/docs\n",
		get(siteRouter(t, site), "/llms.txt", func(r *http.Request) { r.Host = "op.example" }).Body.String())
}
