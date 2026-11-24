// Command docs-routes writes the docs site's endpoint list,
// web/content/routes.json, from the API server's route docs
// (services/api_server/routes.go). Run it after changing a route's docs:
//
//	go run ./cmd/docs-routes
//	go run ./cmd/docs-routes -o path/to/routes.json
package main

import (
	"bytes"
	"flag"
	"log"
	"os"

	"github.com/bsv-blockchain/broadcaster/services/api_server"
)

func main() {
	out := flag.String("o", "web/content/routes.json", "output file")
	flag.Parse()

	var buf bytes.Buffer
	if err := api_server.WriteRoutesJSON(&buf); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, buf.Bytes(), 0o644); err != nil { //nolint:gosec // a source file in the repository, world-readable like the rest
		log.Fatal(err)
	}
	log.Printf("wrote %s", *out)
}
