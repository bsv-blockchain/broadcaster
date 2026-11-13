.PHONY: build test lint docker-up docker-down docker-build run web docs-preview docs-routes

GOARCH ?= $(shell go env GOARCH)

# Version stamped into the binary (exposed via GET /health, issue #208). Derived
# from the git tag; falls back to "dev" outside a tagged checkout.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# CGO is required: the transaction validator links the BSV BDK C++ consensus
# library (gobdk) via cgo, so a C/C++ toolchain (gcc/g++, libstdc++) must be
# present to build and test. Builds are native per arch — gobdk ships a
# prebuilt static archive per platform, so there is no cross-compilation.
build:
	CGO_ENABLED=1 go build ./...

test:
	CGO_ENABLED=1 go test ./...

lint:
	golangci-lint run

# Full microservices stack (see docker-compose.yaml). Runs the published
# image by default; BSV_BROADCASTER_IMAGE=bsv-broadcaster:local make docker-up runs a working
# tree build produced by `make docker-build`.
docker-up:
	podman-compose up -d

docker-down:
	podman-compose down

# Build a single-arch image for local use that matches the CI layout. The
# Dockerfile expects a binary at dist/linux-<arch>/bsv-broadcaster; this target produces
# that layout for the host's architecture and tags the image bsv-broadcaster:local.
# Requires a Linux host: CGO + gobdk's per-platform static archive cannot
# cross-compile to linux, so building the binary off-Linux would fail.
# Depends on `web` so the image embeds the docs site, as CI's does.
docker-build: web
	@if [ "$$(go env GOOS)" != "linux" ]; then \
		echo "docker-build requires a Linux host: CGO + gobdk cannot cross-compile a linux binary from $$(go env GOOS). Run on Linux or inside a Linux container."; \
		exit 1; \
	fi
	mkdir -p dist/linux-$(GOARCH)
	CGO_ENABLED=1 GOOS=linux GOARCH=$(GOARCH) go build -trimpath -ldflags="-s -w -X github.com/bsv-blockchain/broadcaster/version.Version=$(VERSION)" -o dist/linux-$(GOARCH)/bsv-broadcaster ./cmd/bsv-broadcaster
	docker build --platform=linux/$(GOARCH) -t bsv-broadcaster:local .

run:
	go run ./cmd/bsv-broadcaster

# Docs site (web/): a Next.js static export embedded into the binary by
# package web (web/embed.go). Without `make web`, binaries serve the built-in
# docs page instead. Requires Node.js 20+.
web:
	cd web && npm ci && npm run build

# Build the docs site, then serve it as the API server does at
# http://localhost:8080/ (PORT=... to change). The quickest end-to-end check of
# a design change.
docs-preview: web
	go run ./cmd/docs-preview

# Regenerate web/content/routes.json from services/api_server/routes.go.
docs-routes:
	go run ./cmd/docs-routes
