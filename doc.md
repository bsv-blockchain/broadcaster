# BSV Broadcaster

BSV Broadcaster is a transaction management service for Bitcoin SV. It registers transactions with a [merkle-service](https://github.com/bsv-blockchain/merkle-service), receives state callbacks as transactions move through the network, builds full BUMPs (Bitcoin Unified Merkle Paths) once blocks are processed, and broadcasts transactions to a fleet of [Teranode](https://github.com/bsv-blockchain/teranode) datahubs.

It is designed to scale to blocks containing millions of transactions, subtrees of thousands, and tens of thousands of in-flight registrations per block — but it also runs as a single self-contained binary with zero external dependencies for development.

## Architecture

BSV Broadcaster is a microservice binary: one Go binary, several services, selectable via `--mode`. Services communicate over Kafka and share a Store.

| Service        | Mode flag      | Responsibility                                                                                              |
| -------------- | -------------- | ----------------------------------------------------------------------------------------------------------- |
| `api_server`   | `api-server`   | HTTP surface: tx submit/get, merkle-service callbacks, embedded chaintracks routes, `/metrics`              |
| `tx_validator` | `tx-validator` | Parallel batch validation of incoming transactions; valid txs are forwarded to propagation                  |
| `propagation`  | `propagation`  | Broadcasts to all configured/discovered datahub URLs in parallel with per-endpoint circuit-breaking + reaper |
| `bump_builder` | `bump-builder` | On `BLOCK_PROCESSED`, reads STUMPs from the store and builds full BUMPs using the coinbase merkle path      |
| `p2p_client`   | `p2p-client`   | libp2p subscriber for the Teranode `node_status` topic; populates the shared datahub registry               |

Pass `--mode all` (the default) to run every service in one process.

```
                               ┌──────────────┐
        merkle-service ◀──────▶│  api_server  │──── /chaintracks/v1, v2
                               └──────┬───────┘
                                      │ Kafka: bsv-broadcaster.transaction
                                      ▼
                                ┌─────────────┐
                                │ tx_validator│
                                └──────┬──────┘
                                       │ Kafka: bsv-broadcaster.propagation
                                       ▼
        Teranode datahubs ◀──────┬─────────────┐
                                 │ propagation │─── circuit-breaker per endpoint
        merkle-service     ◀─────┴─────────────┘    reaper for retryables
                                       ▲
                                       │ Kafka: bsv-broadcaster.block_processed
                                ┌──────┴──────┐
                                │ bump_builder│─── reads STUMPs after grace window
                                └─────────────┘
```

### Topics

- `bsv-broadcaster.transaction` — submitted transactions awaiting validation
- `bsv-broadcaster.propagation` — validated transactions awaiting broadcast
- `bsv-broadcaster.block_processed` — fan-out trigger for BUMP construction
- Each topic has a `.dlq` companion for poison messages

### Stores

Pluggable via `store.backend`:

- `aerospike` (default for production) — batched ops everywhere
- `postgres` — including a `Embedded=true` mode that bundles its own Postgres for single-binary deployments
- `pebble` — embedded KV, single-process, used by the standalone profile
- `mongodb` — document store; BUMP/STUMP blobs in GridFS, no multi-document transactions so a standalone `mongod` suffices

### Endpoint discovery

Static `datahub_urls` from config are seeded into the shared store on startup. The `p2p_client` subscribes to the Teranode `node_status` libp2p topic and adds advertised datahub URLs into the same registry. Every pod's `teranode.Client` polls the store on `refresh_interval_ms` and reconciles, so a freshly-started replica converges to the cluster's current view without restarts.

The propagation client treats endpoints as circuit-breakers: `failure_threshold` consecutive *transport* failures (connection refused, DNS failure, timeout with no bytes) marks an endpoint unhealthy. Application-level rejections (HTTP 4xx/5xx) do **not** count — the peer responded, so it's reachable. A `GET /health` probe runs every `probe_interval_ms` against the unhealthy set; any HTTP response restores the endpoint. Broadcast wall-time tracks the fastest healthy peer via first-success early-cancel.

## Quickstart

### Standalone (no external dependencies)

```bash
cp config.example.standalone.yaml config.yaml
go run ./cmd/bsv-broadcaster --config config.yaml
```

This profile uses:
- in-memory Kafka (no broker)
- Pebble embedded KV (no Aerospike/Postgres)
- libp2p with Teranode bootstrap peers for discovery (optional)

### Docker / Podman compose (microservices)

`docker-compose.yaml` mirrors the Kubernetes deployment in
`bsva-infra-flux/apps/base/arcade-v2` (teratestnet overlay), one replica each:
the seven BSV Broadcaster services (`api-server`, `propagation`, `bump-builder`, `sse`,
`chaintracks`, `watchdog`, `p2p-client`) as separate `--mode` containers, plus
Redpanda (topics pre-created by `compose/topic-init.sh`), Postgres,
merkle-service (with its own Postgres), and the OTel collector. The shared
service config lives in `compose/bsv-broadcaster-config.yaml`, mounted at
`/etc/bsv-broadcaster/config.yaml` in every BSV Broadcaster container.

```bash
make docker-up                            # published ghcr.io/bsv-blockchain/broadcaster:latest
BSV_BROADCASTER_IMAGE=bsv-broadcaster:local make docker-up  # your working tree, after `make docker-build`
make docker-down
```

Published ports: api-server `8080`, sse `8082`, chaintracks `8083`,
merkle-service `18080`, postgres `15432`/`15433`, Redpanda Kafka `19092`
(admin `19644`), OTLP `4317`/`4318`.

`make run` remains the standalone single-binary path (`mode: all`, no
containers needed with the standalone config).

### Build and test

```bash
make build             # go build ./...
make test              # go test ./...
make lint              # golangci-lint run
```

## Configuration

Configuration is loaded by [Viper](https://github.com/spf13/viper) from `config.yaml` (or `--config <path>`). Every value can be overridden with an `BSV_BROADCASTER_`-prefixed environment variable using underscores for nesting — e.g. `BSV_BROADCASTER_KAFKA_BROKERS`, `BSV_BROADCASTER_STORE_BACKEND`, `BSV_BROADCASTER_LOG_LEVEL`.

See [`config.example.yaml`](./config.example.yaml) (production) and [`config.example.standalone.yaml`](./config.example.standalone.yaml) (zero-dependency) for fully-commented references.

Notable knobs:

| Key                                      | Purpose                                                                                                  |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `mode`                                   | Service to run: `all`, `api-server`, `tx-validator`, `propagation`, `bump-builder`, `p2p-client`         |
| `network`                                | `mainnet` / `testnet` / `teratestnet`. Drives p2p topics and chaintracks bootstrap peers.                |
| `storage_path`                           | Shared on-disk root for Pebble, chaintracks headers, libp2p key cache.                                   |
| `kafka.min_partitions`                   | When > 1, startup fails if `block_processed` or `tx_status` is missing or narrower. Leave 0/1 standalone. |
| `propagation.endpoint_health.*`          | Circuit-breaker thresholds and probe cadence for the datahub fleet.                                      |
| `bump_builder.grace_window_ms`           | Delay after `BLOCK_PROCESSED` before reading STUMPs, so late merkle-service retries can land first.      |
| `tx_validator.parallelism`               | Concurrent in-flight validations per flush window; defaults to `runtime.NumCPU()`.                       |
| `chaintracks_server.enabled`             | Mounts the embedded `go-chaintracks` HTTP API under `/chaintracks/v1` and `/chaintracks/v2`.             |

## HTTP API

The `api_server` exposes:

| Method | Path                                  | Purpose                                                                |
| ------ | ------------------------------------- | ---------------------------------------------------------------------- |
| GET    | `/`                                   | HTML route listing                                                     |
| GET    | `/health`                             | Liveness                                                               |
| GET    | `/ready`                              | Readiness                                                              |
| GET    | `/metrics`                            | Prometheus scrape endpoint                                             |
| GET    | `/tx/:txid`                           | Returns current state and merkle proof for a transaction               |
| POST   | `/tx`                                 | Submit one tx (raw bytes, hex, or JSON `{"rawTx":"<hex>"}`)            |
| POST   | `/txs`                                | Submit a batch (concatenated raw tx bytes, `application/octet-stream`) |
| POST   | `/api/v1/merkle-service/callback`     | Callback sink for merkle-service state transitions                     |
| GET    | `/chaintracks/v1/...`, `/v2/...`      | Embedded `go-chaintracks` HTTP API (when enabled)                      |
| GET    | `/chaintracks/<file>`                 | Bulk header file download with HTTP Range support                      |

### Merkle-service callback states

The `/api/v1/merkle-service/callback` endpoint applies SEEN transitions and
stores STUMPs synchronously (a 2xx from BSV Broadcaster implies durability) and hands
`BLOCK_PROCESSED` to Kafka so BUMP construction runs in the bump-builder.
Recognized callback types:

- `SEEN_ON_NETWORK`
- `SEEN_MULTIPLE_NODES`
- `STUMP` — stored keyed by block hash and subtree index
- `BLOCK_PROCESSED` — triggers BUMP construction after `grace_window_ms`

With several merkle-services configured every callback arrives once per
service; a delivery that carries nothing new is a no-op (see
`docs/merkle-lifecycle-durability.md`).

## Observability

Prometheus metrics are exposed at `/metrics` on both the API server (port `api.port`, default 8080) and the per-pod health server (port `health.port`, default 8081). All metric names are namespaced under `bsv_broadcaster_`.

A full alerting / dashboarding playbook lives in [`metrics/README.md`](./metrics/README.md), including:

- key SLI metrics (validate p95, broadcast p95, merkle register p95)
- alert rules for split-brain reaper leases, BUMP build failures, datahub flapping
- cardinality guard rails (per-endpoint labels are bounded; txids and offsets are never labels)

## Deployment

Kubernetes manifests for each service live in [`deploy/`](./deploy/):

- `all.yaml` — single-pod, all services (dev)
- `api-server.yaml`, `tx-validator.yaml`, `propagation.yaml`, `bump-builder.yaml`, `p2p-client.yaml` — one Deployment per service
- `aerospike.yaml`, `kafka.yaml`, `namespace.yaml` — supporting infra

In horizontally-scaled deployments, set `kafka.min_partitions` to at least the replica count of the largest bump-builder or SSE/webhook consumer. Startup then fails if `block_processed` or `tx_status` is missing or has fewer partitions. Leave it at 0 or 1 for standalone and single-replica deployments. `propagation` is checked separately via `propagation.partitions`.

## Repository layout

```
cmd/bsv-broadcaster/             entry point, service wiring, signal handling
config/                 viper-backed config + network resolution
services/
  api_server/             HTTP, callbacks, chaintracks routes
  tx_validator/           parallel batch validator
  propagation/            broadcaster + reaper + per-endpoint backoff
  bump_builder/           STUMP → BUMP construction
  p2p_client/             libp2p datahub discovery
kafka/                  broker abstraction (sarama + in-memory), topics, DLQ
store/                  Store interface, batch helpers, lease, tx tracker
  aerospike/              Aerospike backend
  postgres/               Postgres backend, optional embedded
  pebble/                 Pebble embedded KV
  mongodb/                MongoDB backend (GridFS for blobs)
  factory/                backend dispatch
teranode/               datahub HTTP client + per-endpoint health
merkleservice/          merkle-service HTTP client
bump/                   BUMP/STUMP types and helpers
validator/              transaction validation policy
metrics/                Prometheus instrumentation
models/                 shared block/tx/callback types
errors/                 typed sentinel errors
deploy/                 Kubernetes manifests
```

## Related projects

- [Teranode](https://github.com/bsv-blockchain/teranode) — high-throughput Bitcoin SV node; BSV Broadcaster follows its daemon/service patterns
- [Merkle Service](https://github.com/bsv-blockchain/merkle-service) — registers transactions and emits state callbacks
- [go-teranode-p2p-client](https://github.com/bsv-blockchain/go-teranode-p2p-client), [go-p2p-message-bus](https://github.com/bsv-blockchain/go-p2p-message-bus) — libp2p plumbing reused for discovery
- [BRC-74: BUMP](https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md) — full block merkle-path format
