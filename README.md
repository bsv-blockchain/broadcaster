<div align="center">

# 📡&nbsp;&nbsp;BSV Broadcaster

**A P2P-first Bitcoin transaction broadcast client for Teranode with Arc-compatible API**

<br/>

<a href="https://github.com/bsv-blockchain/broadcaster/releases"><img src="https://img.shields.io/github/release-pre/bsv-blockchain/broadcaster?include_prereleases&style=flat-square&logo=github&color=black" alt="Release"></a>
<a href="https://golang.org/"><img src="https://img.shields.io/github/go-mod/go-version/bsv-blockchain/broadcaster?style=flat-square&logo=go&color=00ADD8" alt="Go Version"></a>
<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square&logo=springsecurity&logoColor=white" alt="License"></a>

<br/>

<table align="center" border="0">
  <tr>
    <td align="right">
       <code>CI / CD</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/bsv-blockchain/broadcaster/actions"><img src="https://img.shields.io/github/actions/workflow/status/bsv-blockchain/broadcaster/fortress.yml?branch=main&label=build&logo=github&style=flat-square" alt="Build"></a>
       <a href="https://github.com/bsv-blockchain/broadcaster/actions"><img src="https://img.shields.io/github/last-commit/bsv-blockchain/broadcaster?style=flat-square&logo=git&logoColor=white&label=last%20update" alt="Last Commit"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Quality</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://codecov.io/gh/bsv-blockchain/broadcaster"><img src="https://codecov.io/gh/bsv-blockchain/broadcaster/branch/main/graph/badge.svg?style=flat-square" alt="Coverage"></a>
    </td>
  </tr>

  <tr>
    <td align="right">
       <code>Security</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://scorecard.dev/viewer/?uri=github.com/bsv-blockchain/broadcaster"><img src="https://api.scorecard.dev/projects/github.com/bsv-blockchain/broadcaster/badge?style=flat-square" alt="Scorecard"></a>
       <a href=".github/SECURITY.md"><img src="https://img.shields.io/badge/policy-active-success?style=flat-square&logo=security&logoColor=white" alt="Security"></a>
    </td>
    <td align="right">
       &nbsp;&nbsp;&nbsp;&nbsp; <code>Community</code> &nbsp;&nbsp;
    </td>
    <td align="left">
       <a href="https://github.com/bsv-blockchain/broadcaster/graphs/contributors"><img src="https://img.shields.io/github/contributors/bsv-blockchain/broadcaster?style=flat-square&color=orange" alt="Contributors"></a>
       <a href="https://deepwiki.com/bsv-blockchain/broadcaster"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
    </td>
  </tr>
</table>

</div>

<br/>
<br/>

<div align="center">

### <code>Project Navigation</code>

</div>

<table align="center">
  <tr>
    <td align="center" width="33%">
       📦&nbsp;<a href="#-installation"><code>Installation</code></a>
    </td>
    <td align="center" width="33%">
       🚀&nbsp;<a href="#-quick-start"><code>Quick&nbsp;Start</code></a>
    </td>
    <td align="center" width="33%">
       📚&nbsp;<a href="#-documentation"><code>Documentation</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🐳&nbsp;<a href="DOCKER.md"><code>Docker&nbsp;Guide</code></a>
    </td>
    <td align="center">
       ☸️&nbsp;<a href="doc.md#deployment"><code>Deployment&nbsp;Guide</code></a>
    </td>
    <td align="center">
       🏗️&nbsp;<a href="#-architecture"><code>Architecture</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🧪&nbsp;<a href="#-examples--tests"><code>Examples&nbsp;&&nbsp;Tests</code></a>
    </td>
    <td align="center">
       ⚡&nbsp;<a href="#-benchmarks"><code>Benchmarks</code></a>
    </td>
    <td align="center">
       🛠️&nbsp;<a href="#-code-standards"><code>Code&nbsp;Standards</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       🤖&nbsp;<a href="#-ai-usage--assistant-guidelines"><code>AI&nbsp;Usage</code></a>
    </td>
    <td align="center">
       📚&nbsp;<a href="#-resources"><code>Resources</code></a>
    </td>
    <td align="center">
       🤝&nbsp;<a href="#-contributing"><code>Contributing</code></a>
    </td>
  </tr>
  <tr>
    <td align="center">
       👥&nbsp;<a href="#-maintainers"><code>Maintainers</code></a>
    </td>
    <td align="center">
       📝&nbsp;<a href="#-license"><code>License</code></a>
    </td>
    <td align="center">
    </td>
  </tr>
</table>
<br/>

## 📦 Installation

**BSV Broadcaster** requires a [supported release of Go](https://golang.org/doc/devel/release.html#policy).
```shell script
go get -u github.com/bsv-blockchain/broadcaster
```

<br/>

## 🚀 Quick Start

> Looking for per-network setup (mainnet / testnet / teratestnet / regtest)? See [`docs/getting-started.md`](docs/getting-started.md).

### Prerequisites

- **Go 1.27+** (see `go.mod` for exact version)
- **SQLite** (included with most systems)
- **Teranode broadcast URL** (e.g., `https://arc.taal.com`)

### Build from Source

```bash
git clone https://github.com/bsv-blockchain/broadcaster.git
cd broadcaster
go build -o bsv-broadcaster ./cmd/bsv-broadcaster
```

### Configuration

Create `config.yaml` with your Teranode broadcast URL:

```yaml
# Minimal working configuration with no dependencies
mode: all
network: main
storage_path: ~/.bsv-broadcaster

callback_url: "https://myhostname.com"

kafka:
  backend: memory
  consumer_group: bsv-broadcaster

store:
  backend: pebble
  pebble:
    path: ~/.bsv-broadcaster/pebble
    memtable_size_mb: 64
    l0_compaction_threshold: 4
    sync_writes: false

merkle_service:
  url: "https://merkle-service-us-1.bsvb.tech"
  # optional extra instances for redundancy; see docs/getting-started.md
  # urls: ["https://merkle-service-us-2.bsvb.tech"]

p2p:
  datahub_discovery: true

chaintracks_server:
  enabled: true
```

See `config.example.yaml` for all available options.

### Run

```bash
bsv-broadcaster --config config.yaml
```

You should see output indicating the server is running on port 8080.

<br/>

## 📚 Documentation

### Overview

BSV Broadcaster is a lightweight transaction broadcast service that:
- Registers transactions with Merkle Service
- Provides Arc-compatible HTTP API for easy migration
- Supports pluggable storage and event backends (in-memory)
- Tracks transaction status through the complete lifecycle
- Delivers notifications via webhooks and Server-Sent Events (SSE)

**What is [Teranode](https://bsv-blockchain.github.io/teranode/topics/teranodeIntro/)?** Teranode is the next-generation BSV node implementation designed for enterprise-scale transaction processing. BSV Broadcaster acts as a bridge between your application and the Teranode network, handling transaction submission and status tracking.

<br>

### Features

- **Arc-Compatible API** - Drop-in replacement for Arc clients
- **Chain Tracking** - Blockchain header management with merkle proof validation
- **Flexible Storage** - Storage interface for different scaling needs
- **Event Streaming** - In-memory pub/sub for event distribution
- **Webhook Delivery** - Async notifications with retry logic
- [TODO] **SSE Streaming** - Real-time status updates with automatic catchup on reconnect
- **Transaction Validation** - Local validation before network submission
- **Status Tracking** - Complete audit trail of transaction lifecycle
- **Extensible** - All packages public, easy to customize
- **API Reference** – Dive into the godocs at [pkg.go.dev/github.com/bsv-blockchain/broadcaster](https://pkg.go.dev/github.com/bsv-blockchain/broadcaster)

<br>

### API Usage

#### Submit Transaction (`POST /tx`)

Submissions must be in Extended Format (EF): validation needs each input's source output, and a
plain raw transaction is rejected with ARC code 460. BEEF is not accepted; intake does not decode
it, so a BEEF body is rejected with HTTP 400. `GET /policy` advertises this as
`standardFormatSupported: false`.

```bash
curl -X POST http://localhost:3011/tx \
  -H "Content-Type: text/plain" \
  -H "X-CallbackUrl: https://myapp.com/webhook" \
  --data "<hex-encoded-transaction>"
```

#### Get Transaction Status (`GET /tx/{txid}`)

```bash
curl http://localhost:3011/tx/{txid}
```

#### Health Check (`GET /health`)

```bash
curl http://localhost:3011/health
```

#### API Docs (`GET /docs`)

Interactive API documentation (Scalar UI) available at `http://localhost:3011/docs`

### Transaction Status Flow

```
Client → BSV Broadcaster
   ↓
RECEIVED (validated locally, queued for propagation)
   ↓
ACCEPTED_BY_NETWORK (acknowledged by a Teranode datahub)
   ↓
SEEN_ON_NETWORK → SEEN_MULTIPLE_NODES (heard in subtree gossip - in mempool)
   ↓
MINED (merkle proof delivered - included in a block)
```

Side states:
```
PENDING_RETRY   no network verdict yet (for example the parent is not accepted); parked for the
                propagation reaper, which rebroadcasts it until it gets a verdict or exhausts
                its retry budget (propagation.pending_retry_max_attempts, default 288 - about
                24h at the 5-minute backoff cap)
REJECTED        a validator failure at intake, a Teranode rejection, or an exhausted retry
                budget. The ARC code, when there is one, is in `status` and the reason in
                `extraInfo`
```

`REJECTED` is not always a verdict on the bytes. When the durable retry budget runs out the
reaper stops rebroadcasting, clears the retry state and writes `REJECTED` even though no peer
gave a verdict; `extraInfo` then starts with `no network verdict after N durable retry attempts`
and quotes the last network response, if any.

A double spend is reported as `REJECTED` with ARC code `466` - the conflict signal. Teranode's
reason line is in `extraInfo`; when that line names the spender (`UTXO_SPENT ... already spent by
tx <txid>`), the spending transaction is in `competingTxs`. `TX_CONFLICTING`,
`TX_INVALID_DOUBLE_SPEND` and `TX_LOCKED` also map to `466` but do not name a spender, so
`competingTxs` is omitted: do not rely on the winner being in the payload. Both conflicting
submissions get `202` - the broadcaster is asynchronous and the network decides - so submit order
is not a tiebreak: watch the status on `GET /tx`, SSE or a webhook. `SENT_TO_NETWORK` and
`DOUBLE_SPEND_ATTEMPTED` stay in the status enum for ARC compatibility but are not emitted.
`IMMUTABLE` is also in the enum but has no producer today: transactions stay `MINED`, so do not
wait for an `IMMUTABLE` event after 100 confirmations.

### Differences from Arc

- **Simpler Deployment** - Single binary with SQLite, no PostgreSQL/NATS required
- **P2P-First** - Direct gossip listening instead of node callbacks
- **Teranode-Only** - Designed for Teranode, no legacy node support
- **Extensible** - All packages public for customization

<details>
<summary><strong>Configuration Reference</strong></summary>

#### Required Fields

| Field                     | Description                             | Default      |
|---------------------------|-----------------------------------------|--------------|
| `teranode.broadcast_urls` | Teranode broadcast service URLs (array) | **Required** |

#### Optional Fields

| Field                       | Description                                  | Default               |
|-----------------------------|----------------------------------------------|-----------------------|
| `mode`                      | Operating mode: `embedded`, `remote`         | `embedded`            |
| `url`                       | BSV Broadcaster server URL (required for remote mode) | -                     |
| `network`                   | Bitcoin network: `main`, `test`, `stn`       | `main`                |
| `storage_path`              | Data directory for persistent files          | `~/.bsv-broadcaster`           |
| `log_level`                 | Log level: `debug`, `info`, `warn`, `error`  | `info`                |
| `server.address`            | HTTP server listen address                   | `:3011`               |
| `server.read_timeout`       | HTTP read timeout                            | `30s`                 |
| `server.write_timeout`      | HTTP write timeout                           | `30s`                 |
| `server.shutdown_timeout`   | Graceful shutdown timeout                    | `10s`                 |
| `database.type`             | Storage backend: `sqlite`                    | `sqlite`              |
| `database.sqlite_path`      | SQLite database file path                    | `~/.bsv-broadcaster/bsv-broadcaster.db` |
| `events.type`               | Event backend: `memory`                      | `memory`              |
| `events.buffer_size`        | Event channel buffer size                    | `1000`                |
| `teranode.datahub_urls`     | DataHub URLs for fetching block data (array) | -                     |
| `teranode.auth_token`       | Authentication token for Teranode API        | -                     |
| `teranode.timeout`          | HTTP request timeout                         | `30s`                 |
| `validator.max_tx_size_policy` | Maximum transaction size (bytes); `0` tracks the network | `0` (→ `10485760`) |
| `validator.max_script_size_policy` | Maximum script size (bytes); `0` tracks the network | `0` (→ `500000`) |
| `validator.max_tx_sigops_counts_policy` | Maximum signature operations (`0` = unlimited) | `0`         |
| `validator.min_fee_per_kb`  | Minimum fee per KB (satoshis); `0` tracks the network | `0` (→ `100`)      |
| `webhook.max_retries`       | Max webhook retry attempts                   | `10`                  |
| `auth.enabled`              | Identify callers by API key (`Authorization: Bearer bsvb_…`); unkeyed callers stay anonymous. See [docs/api-keys.md](docs/api-keys.md) | `false` |
| `ratelimit.enabled`         | Per-tier rate limits for keyed and anonymous callers (requires `auth.enabled`) | `false` |
| `auth.store.mongodb.uri`    | Dedicated MongoDB for API keys + quota, shared across deployments (`BSV_BROADCASTER_AUTH_STORE_MONGODB_URI`); unset = main store. See [docs/api-keys.md](docs/api-keys.md) | - |
| `auth.store.mongodb.database` | Database for `api_keys` / `api_quota` (required with `uri`) | -            |

**Planned but not yet implemented:**
- `database.type: postgres` - PostgreSQL storage backend
- `events.type: redis` - Redis event backend for distributed deployments

</details>

<details>
<summary><strong>HTTP Headers</strong></summary>

BSV Broadcaster supports Arc-compatible headers:

- `X-CallbackUrl` - Webhook URL for async status updates
- `X-CallbackToken` - Token for webhook auth and SSE stream filtering
- `X-FullStatusUpdates` - Include all intermediate statuses (default: final only)
- `X-SkipFeeValidation` - Skip fee validation
- `X-SkipScriptValidation` - Skip script validation

</details>

<details>
<summary><strong>Webhook Notifications</strong></summary>

When you provide `X-CallbackUrl`, BSV Broadcaster will POST status updates:

```json
{
  "timestamp": "2024-03-26T16:02:29.655390092Z",
  "txid": "...",
  "txStatus": "SEEN_ON_NETWORK",
  "blockHash": "...",
  "blockHeight": 123456,
  "merklePath": "..."
}
```

**Features:**
- Automatic retries with linear backoff (1min, 2min, 3min, etc.)
- Configurable max retries via `webhook.max_retries`
- Every POST carries `User-Agent: bsv-broadcaster-webhook/<version>` — allowlist the
  `bsv-broadcaster-webhook/` prefix at your edge so bot/WAF rules don't silently
  reject deliveries
- Delivery self-diagnosis: `GET /tx/{txid}?callbackToken=<token>` returns a
  `callbacks` array with `attempts`, `lastAttemptAt`, and `lastResult`
  (`"delivered"` or the last failure, e.g. `"status 403"`) for your
  submissions — if `attempts` climbs while your endpoint sees nothing,
  something in front of it is rejecting the POSTs

</details>

<details>
<summary><strong>SSE Streaming</strong></summary>

BSV Broadcaster provides real-time transaction status updates via SSE, offering an alternative to webhook callbacks.

#### How It Works

1. **Submit Transaction with Callback Token:**
   ```bash
   curl -X POST http://localhost:3011/tx \
     -H "Content-Type: text/plain" \
     -H "X-CallbackToken: my-token-123" \
     --data "<hex-encoded-transaction>"
   ```

2. **Connect SSE Client:**
   ```javascript
   const eventSource = new EventSource('http://localhost:3011/events?callbackToken=my-token-123');

   eventSource.addEventListener('status', (e) => {
     const update = JSON.parse(e.data);
     console.log(`${update.txid}: ${update.status}`);
   });
   ```

3. **Receive Real-Time Updates:**
   ```
   id: 1699632123456789000
   event: status
   data: {"txid":"abc123...","txStatus":"ACCEPTED_BY_NETWORK","timestamp":"2024-11-10T12:00:00Z"}
   ```

#### Catchup on Reconnect

When the SSE connection drops, the browser's EventSource automatically reconnects and sends the `Last-Event-ID` header. BSV Broadcaster replays all missed events since that timestamp.

**No client code needed** - this is handled automatically by the EventSource API.

#### Event ID Format

Event IDs are nanosecond timestamps (`time.UnixNano()`), ensuring:
- Chronological ordering
- Virtually collision-free IDs
- Easy catchup queries by time

#### Use Cases

- **Webhooks:** Server-to-server notifications with retry logic
- **SSE:** Real-time browser updates with automatic reconnection
- **Both:** Use the same `X-CallbackToken` for webhooks AND SSE streaming

#### Filtering

Each SSE connection only receives events for transactions submitted with the matching callback token. This allows:
- Multiple users/sessions with isolated event streams
- Scoped access without complex authentication
- Simple token-based routing

See [examples/sse_client.html](examples/sse_client.html) and [examples/sse_client.go](examples/sse_client.go) for complete examples.

</details>

<details>
<summary><strong>Remote Client</strong></summary>

For applications that need BSV Broadcaster functionality without running a full server, use the REST client:

```go
import "github.com/bsv-blockchain/broadcaster/client"

c := client.New("http://bsv-broadcaster-server:3011")
defer c.Close() // Always close when done to clean up SSE connections

// Submit a single transaction
status, err := c.SubmitTransaction(ctx, rawTxBytes, nil)

// Submit multiple transactions
statuses, err := c.SubmitTransactions(ctx, [][]byte{rawTx1, rawTx2}, nil)

// Get transaction status
status, err := c.GetStatus(ctx, "txid...")

// Get policy configuration
policy, err := c.GetPolicy(ctx)

// Subscribe to status updates for a callback token
statusChan, err := c.Subscribe(ctx, "my-callback-token")
for status := range statusChan {
    fmt.Printf("Status: %s %s\n", status.TxID, status.Status)
}
```

</details>

<details>
<summary><strong>Extending BSV Broadcaster</strong></summary>

All packages are public and designed for extension:

#### Custom Storage Backend

```go
import "github.com/bsv-blockchain/broadcaster/store"

type MyStore struct {}

func (s *MyStore) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
    // Your implementation - returns (existing status, was inserted, error)
    // If transaction already exists, return existing status with inserted=false
}

// Implement other store.Store methods...
```

#### Custom Event Handler

```go
import "github.com/bsv-blockchain/broadcaster/events"

type MyHandler struct {
    publisher events.Publisher
}

func (h *MyHandler) Start(ctx context.Context) error {
    ch, _ := h.publisher.Subscribe(ctx)
    for update := range ch {
        // Handle status update
    }
}
```

</details>

<details>
<summary><strong>Troubleshooting</strong></summary>

**Server fails to start with "no teranodes configured"**
- Ensure `teranode.broadcast_urls` is set in your config file with at least one valid URL

**Transactions stuck in RECEIVED or PENDING_RETRY**
- Check the datahub URLs are reachable (`GET /health` lists each one with its health)
- Verify network connectivity and firewall rules
- `GET /tx/{txid}` carries the last network answer in `extraInfo` and, for PENDING_RETRY, the next rebroadcast time in `nextRetryAt`

**SQLite errors**
- Ensure `storage_path` directory exists and is writable
- Check disk space availability

</details>

<br/>

## 🏗️ Architecture

### Key Components

- **P2P client** (`services/p2p_client/`) - Core P2P listener that subscribes to block, subtree, and rejected-tx gossip topics
- **Store** (`store/`) - SQLite storage for transaction statuses and webhook tracking
- **TxTracker** (`store/tracker.go`) - In-memory index of transactions being monitored
- **Event Publisher** (`events/`) - In-memory pub/sub for distributing status updates
- **Webhook Handler** (`handlers/webhook.go`) - Delivers status updates with retry logic
- **HTTP Routes** (`routes/fiber/`) - Arc-compatible REST API including SSE streaming

### Chain Tracking

BSV Broadcaster uses [go-chaintracks](https://github.com/bsv-blockchain/go-chaintracks) for blockchain header tracking and merkle proof validation. Headers are loaded from embedded checkpoint files on startup and updated via P2P block announcements.

For detailed architecture documentation, see [doc.md](doc.md).

<br/>

## 🧪 Examples & Tests

All unit tests and examples run via [GitHub Actions](https://github.com/bsv-blockchain/broadcaster/actions) and use [Go version 1.27.x](https://go.dev/doc/go1.27). View the [configuration file](.github/workflows/fortress.yml).

Run all tests (fast):

```bash script
magex test
```

Run all tests with race detector (slower):
```bash script
magex test:race
```

See [examples/](examples) for SSE client examples and the [combined server example](examples/combined_server).

<br/>

## ⚡ Benchmarks

Run the Go benchmarks:

```bash script
magex bench
```

> **Note:** Comprehensive benchmarks for P2P operations (peer discovery, message throughput, connection establishment) are planned for future releases. The current focus is on correctness and stability of the networking implementation.

<br/>

## 🛠️ Code Standards
Read more about this Go project's [code standards](.github/CODE_STANDARDS.md).

<br/>

## 🤖 AI Usage & Assistant Guidelines
Read the [AI Usage & Assistant Guidelines](.github/tech-conventions/ai-compliance.md) for details on how AI is used in this project and how to interact with AI assistants.

<br/>

## 📚 Resources

- [Migrating from arcade](docs/migration-arcade-to-bsv-broadcaster.md): upgrade notes for deployments that predate the rename
- [Architecture Documentation](doc.md)
- [Observability](docs/observability.md) — OTLP traces/metrics, structured log field canon, and transaction-lifecycle logging
- [Teranode Documentation](https://docs.bsvblockchain.org/)
- [Arc API Reference](https://github.com/bitcoin-sv/arc)

<br/>

## 🤝 Contributing
View the [contributing guidelines](.github/CONTRIBUTING.md) and please follow the [code of conduct](.github/CODE_OF_CONDUCT.md).

### How can I help?
All kinds of contributions are welcome :raised_hands:!
The most basic way to show your support is to star :star2: the project, or to raise issues :speech_balloon:.

[![Stars](https://img.shields.io/github/stars/bsv-blockchain/broadcaster?label=Please%20like%20us&style=social&v=1)](https://github.com/bsv-blockchain/broadcaster/stargazers)

<br/>

## 👥 Maintainers
| [<img src="https://github.com/icellan.png" height="50" alt="Siggi" />](https://github.com/icellan) | [<img src="https://github.com/galt-tr.png" height="50" alt="Galt" />](https://github.com/galt-tr) | [<img src="https://github.com/mrz1836.png" height="50" alt="MrZ" />](https://github.com/mrz1836) |
|:--------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------:|
|                                [Siggi](https://github.com/icellan)                                 |                                [Dylan](https://github.com/galt-tr)                                |                                [MrZ](https://github.com/mrz1836)                                 |

<br/>

## 📝 License

[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat&logo=springsecurity&logoColor=white)](LICENSE)
