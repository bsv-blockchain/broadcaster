package api_server

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// The docs site is a static export (web/out, embedded by package web). With an
// export present, RegisterSite serves it; without one ("go build" without a
// web build) it falls back to the built-in docs.html page at "/".
//
// Routes are registered per top-level entry of the export, not as a catch-all:
// a file gets an exact route ("/llms.txt"; "docs.html" as "/docs"), a directory
// a prefix route ("/_next/*filepath"). Unknown paths therefore still reach the
// router's own 404. The site is registered after the API, and an entry whose
// route would share its first path segment with an API route ("tx/" beside
// "/tx/:txid", a "health" file) is skipped and reported instead of registered:
// gin would panic on the conflict, and no Go test sees the real export.

// originPlaceholder is the origin the export is built with (defaultOrigin in
// web/lib/site.ts; keep the two in step). Every operator serves the export
// from its own API server, so each response replaces it with the origin the
// client reached: canonical URLs, the sitemap, robots.txt, llms.txt and
// JSON-LD then name the serving deployment, never a host baked in at build.
const originPlaceholder = "https://broadcaster.invalid"

// siteHostRE accepts a DNS name or IPv4 address, or a bracketed IPv6 address,
// with an optional port. The host is written into HTML, JSON and JavaScript,
// so anything else (quotes, angle brackets, spaces) leaves the placeholder.
var siteHostRE = regexp.MustCompile(`^(?:[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?|\[[0-9A-Fa-f:.]+\])(?::[0-9]{1,5})?$`)

// siteCSP is the Content-Security-Policy of every site response. Nothing is
// loaded from another origin: scripts, styles and fonts are self-hosted, and
// the status dot fetches the same origin's /health. 'unsafe-inline' is needed
// for the inline bootstrap scripts a static Next.js export emits (nonces would
// need a per-request render).
const siteCSP = "default-src 'self'; " +
	"script-src 'self' 'unsafe-inline'; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"font-src 'self' data:; " +
	"connect-src 'self'; " +
	"object-src 'none'; " +
	"base-uri 'self'; " +
	"form-action 'self'; " +
	"frame-ancestors 'none'"

// siteContentTypes overrides the type inferred from a file's extension.
var siteContentTypes = map[string]string{
	"llms.txt":      "text/markdown; charset=utf-8",
	"llms-full.txt": "text/markdown; charset=utf-8",
}

// siteExtTypes covers extensions the standard library's built-in table lacks
// or types it differently from what the site expects.
var siteExtTypes = map[string]string{
	".html":  "text/html; charset=utf-8",
	".txt":   "text/plain; charset=utf-8",
	".js":    "text/javascript; charset=utf-8",
	".css":   "text/css; charset=utf-8",
	".json":  "application/json; charset=utf-8",
	".yaml":  "application/yaml; charset=utf-8",
	".xml":   "application/xml; charset=utf-8",
	".svg":   "image/svg+xml",
	".png":   "image/png",
	".ico":   "image/x-icon",
	".woff2": "font/woff2",
	".woff":  "font/woff",
}

// RegisterSite registers the docs site from fsys (the root of a static export)
// on r. Exported so cmd/docs-preview serves the page exactly as the API server
// does, headers included. Register any other routes first: on a *gin.Engine, entries that would
// collide with them are skipped, as on the API server.
func RegisterSite(r gin.IRoutes, fsys fs.FS) error {
	var reserved map[string]bool
	if e, ok := r.(*gin.Engine); ok {
		reserved = firstSegments(e.Routes())
	}
	_, _, err := registerSite(r, fsys, reserved)
	return err
}

// registerSite registers the site and returns the route patterns it
// registered, which the API server keeps out of traces and Warn logs, and the
// export entries it skipped because their route would collide with an API
// route (one whose first path segment is in reserved).
func registerSite(r gin.IRoutes, fsys fs.FS, reserved map[string]bool) ([]string, []string, error) {
	var routes, skipped []string
	handle := func(route string, h gin.HandlerFunc) {
		handleGetHead(r, route, h)
		routes = append(routes, route)
	}

	if _, err := fs.Stat(fsys, "index.html"); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, nil, fmt.Errorf("docs site: %w", err)
		}
		handle("/", serveFallbackDocs)
		return routes, nil, nil
	}

	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("docs site: %w", err)
	}
	site := &siteHandler{fsys: fsys}
	for _, e := range entries {
		name := e.Name()
		switch {
		case name == ".gitkeep":
			continue
		case reserved[strings.TrimSuffix(name, ".html")]:
			skipped = append(skipped, name)
		case e.IsDir():
			handle("/"+name+"/*filepath", site.serveDir(name))
		case name == "index.html":
			handle("/", site.serveFile(name))
		case name == "404.html":
			handle("/404", site.notFound)
		case strings.HasSuffix(name, ".html"):
			handle("/"+strings.TrimSuffix(name, ".html"), site.serveFile(name))
		default:
			handle("/"+name, site.serveFile(name))
		}
	}
	return routes, skipped, nil
}

// firstSegments returns the first path segment of every route, the names a
// site entry must not take.
func firstSegments(routes gin.RoutesInfo) map[string]bool {
	segs := make(map[string]bool, len(routes))
	for _, rt := range routes {
		seg, _, _ := strings.Cut(strings.TrimPrefix(rt.Path, "/"), "/")
		segs[seg] = true
	}
	return segs
}

func handleGetHead(r gin.IRoutes, route string, h gin.HandlerFunc) {
	r.GET(route, h)
	r.HEAD(route, h)
}

// serveFallbackDocs serves the built-in docs page, for binaries built without
// the web export.
func serveFallbackDocs(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	_ = RenderDocs(c.Writer) // a failed write to the client has no one left to report to
}

type siteHandler struct {
	fsys fs.FS
}

func (s *siteHandler) serveFile(name string) gin.HandlerFunc {
	return func(c *gin.Context) { s.serve(c, name, http.StatusOK) }
}

// serveDir serves files under one top-level directory of the export. A path
// naming a directory serves its sibling page ("/docs/" serves docs.html) when
// there is one; anything else missing gets the site's 404 page.
func (s *siteHandler) serveDir(dir string) gin.HandlerFunc {
	return func(c *gin.Context) {
		name := path.Join(dir, path.Clean("/" + c.Param("filepath"))[1:])
		info, err := fs.Stat(s.fsys, name)
		switch {
		case err == nil && !info.IsDir():
			s.serve(c, name, http.StatusOK)
		case err == nil && fileExists(s.fsys, name+".html"):
			s.serve(c, name+".html", http.StatusOK)
		default:
			s.notFound(c)
		}
	}
}

func (s *siteHandler) notFound(c *gin.Context) {
	if fileExists(s.fsys, "404.html") {
		s.serve(c, "404.html", http.StatusNotFound)
		return
	}
	c.Status(http.StatusNotFound)
}

func (s *siteHandler) serve(c *gin.Context, name string, status int) {
	data, err := fs.ReadFile(s.fsys, name)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	if origin, ok := siteOrigin(c.Request); ok {
		data = rewriteOrigin(name, data, origin)
	}

	setSiteHeaders(c, name)
	if status != http.StatusOK {
		c.Status(status)
		if c.Request.Method != http.MethodHead {
			_, _ = c.Writer.Write(data)
		}
		return
	}
	// Zero modtime: embedded files have none, so no Last-Modified.
	http.ServeContent(c.Writer, c.Request, name, time.Time{}, bytes.NewReader(data))
}

// siteOrigin is the origin the client reached, for originPlaceholder: the
// scheme from isHTTPS, the host from the Host header. false when the Host is
// not a plain host[:port], so a malformed one is never written into a page.
func siteOrigin(r *http.Request) (string, bool) {
	if !siteHostRE.MatchString(r.Host) {
		return "", false
	}
	scheme := "http"
	if isHTTPS(r) {
		scheme = "https"
	}
	return scheme + "://" + r.Host, true
}

// setSiteHeaders sets the security headers, content type and caching of a
// site file. These replace what next.config.ts headers() would set on a
// Next.js server, which a static export ignores. CORS is already open on every
// route (httpmiddleware.WithCORS), which the machine-readable files rely on.
func setSiteHeaders(c *gin.Context, name string) {
	h := c.Writer.Header()
	https := isHTTPS(c.Request)

	csp := siteCSP
	if https {
		// Only over HTTPS: on plain-HTTP deployments and local previews it
		// would upgrade every same-origin request to an https:// that is not there.
		csp += "; upgrade-insecure-requests"
		// HSTS is meaningful only over HTTPS. Without includeSubDomains or
		// preload: operators serve this page from hosts whose sibling
		// subdomains this binary knows nothing about.
		h.Set("Strict-Transport-Security", "max-age=63072000")
	}
	h.Set("Content-Security-Policy", csp)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), browsing-topics=()")
	h.Set("Content-Type", siteContentType(name))

	if strings.HasPrefix(name, "_next/static/") {
		// Content-hashed build output: safe to cache forever.
		h.Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		h.Set("Cache-Control", "public, max-age=300")
	}
}

func siteContentType(name string) string {
	if t, ok := siteContentTypes[name]; ok {
		return t
	}
	if strings.HasPrefix(name, ".well-known/") {
		return "application/json; charset=utf-8"
	}
	ext := path.Ext(name)
	if t, ok := siteExtTypes[ext]; ok {
		return t
	}
	// Anything else the designer adds under public/ (.webp, .jpg, .mp4,
	// .webmanifest, ...) gets the standard type, with no Go change.
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}

// isHTTPS reports whether the client reached this server over HTTPS, directly
// or through a TLS-terminating proxy. X-Forwarded-Proto is not authenticated,
// but it only decides whether two stricter headers are added, so a forged
// value can do no more than tighten the response it is sent with.
func isHTTPS(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func fileExists(fsys fs.FS, name string) bool {
	info, err := fs.Stat(fsys, name)
	return err == nil && !info.IsDir()
}
