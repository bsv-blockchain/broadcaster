// Command docs-preview serves the BSV Broadcaster docs site at
// http://localhost:8080/ exactly as the API server does (same routes, same
// security headers), without standing up the full API server (Kafka, store,
// validator, ...).
//
// It serves the export embedded at build time (web/out), or the built-in docs
// page when the binary was built without one. SITE_DIR serves an export from
// disk instead, so a fresh `npm run build` shows without recompiling. /health
// answers with a stub so the page's status dot has something to poll.
//
// Usage:
//
//	make docs-preview                      # build the site, then run this
//	go run ./cmd/docs-preview
//	SITE_DIR=web/out go run ./cmd/docs-preview
//	PORT=9000 go run ./cmd/docs-preview
package main

import (
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bsv-blockchain/broadcaster/services/api_server"
	"github.com/bsv-blockchain/broadcaster/services/httpmiddleware"
	"github.com/bsv-blockchain/broadcaster/web"
)

func main() {
	port := 8080
	if p := os.Getenv("PORT"); p != "" {
		parsed, err := strconv.Atoi(p)
		if err != nil || parsed < 1 || parsed > 65535 {
			log.Fatal("invalid PORT: must be an integer between 1 and 65535")
		}
		port = parsed
	}
	addr := ":" + strconv.Itoa(port)

	site := web.FS()
	source := "embedded export"
	if dir := os.Getenv("SITE_DIR"); dir != "" {
		site = os.DirFS(dir)
		source = "export in SITE_DIR"
	}
	if _, err := fs.Stat(site, "index.html"); err != nil {
		source = "built-in docs page (no export found; run `make web` first)"
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	if err := api_server.RegisterSite(router, site); err != nil {
		log.Fatal(err)
	}
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"healthy": true, "version": "docs-preview", "status": "ok"})
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpmiddleware.WithCORS(router),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("BSV Broadcaster docs preview (%s) listening on http://localhost:%d/", source, port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
