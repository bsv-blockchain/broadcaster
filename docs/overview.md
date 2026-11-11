# Announcing Merkle Service and BSV Broadcaster v2

We're releasing **Merkle Service** and **BSV Broadcaster v2** — a redesign that decouples merkle proof construction from Teranode-grade infrastructure, making Teranode-native transaction broadcast and proof retrieval practical for application developers running on normal hardware.

## Where BSV Broadcaster started

BSV Broadcaster was built as a Teranode-native replacement for [ARC](https://github.com/bsv-blockchain/arc). Where ARC sits as a translation layer in front of node infrastructure, BSV Broadcaster was designed to integrate directly with Teranode and produce merkle proofs natively from Teranode's own data model.

The v1 approach was straightforward and, for what it was, elegant: subscribe to a Teranode instance, listen for subtrees in real time, and store every subtree as it's announced. When a block is found, every piece needed to construct a merkle proof for any transaction in that block is already in memory or on disk. Proofs come out instantly.

This works extremely well at low transaction volume. It is also the source of v1's scaling problem.

## The real v1 problem

Listening to every subtree in real time and storing it means BSV Broadcaster is doing roughly the same data ingest work as a Teranode itself. At low volume that's fine; at scale it isn't. As BSV block sizes grow into the millions of transactions, a BSV Broadcaster v1 instance effectively requires **Teranode-grade hardware** — the same disk throughput, the same network capacity, the same memory footprint — to keep up with the firehose.

That breaks the use case. An application developer who wants to broadcast a few hundred transactions and receive merkle proofs for them shouldn't need to provision a Teranode-class machine to do it. The economics don't work, and the operational burden defeats the point of having a broadcast service in the first place.

The v1 architecture coupled "I want proofs for my transactions" to "I must ingest and store every subtree on the network." Merkle Service breaks that coupling.

## What Merkle Service is

Merkle Service is a standalone, horizontally scalable service that pools the Teranode-scale ingest and proof construction work, then delivers per-transaction proofs over HTTP callbacks. It is built to run on the same kind of infrastructure as Teranode itself, using the same patterns and primitives Teranode uses to handle massive blocks.

It is designed around two assumptions:

1. **Blocks contain millions of transactions; subtrees contain thousands.** Anything that scales linearly with transaction count without batching or partitioning is a non-starter.
2. **Most transactions in a block are not registered with this Merkle Service instance.** The service must be able to ingest the full subtree firehose, but only do meaningful work for the small subset of transactions any given BSV Broadcaster instance cares about.

The architecture follows Teranode's daemon/service pattern: a set of cooperating microservices that can also run as a single all-in-one binary for smaller deployments. The service-level decomposition is what makes horizontal scaling possible.

### Component architecture

```
                BSV network
                     │
                     ▼
           ┌──────────────────┐                ┌──────────────────┐
           │   P2P Client     │                │   API Server     │
           │ (Teranode libp2p)│                │  (POST /watch)   │
           │                  │                │                  │
           │ Listens for      │                │ Registers        │
           │ subtrees, blocks │                │ {txid, callback} │
           └────────┬─────────┘                └────────┬─────────┘
                    │                                   │
                    │ publishes                         │ writes
                    ▼                                   ▼
           ┌────────────────────────┐         ┌──────────────────┐
           │         Kafka          │         │    Aerospike     │
           │  topics:               │         │  (callback       │
           │   • subtree            │         │   registry,      │
           │   • block              │◄────────│   txid → URL)    │
           │   • stumps             │  reads  └──────────────────┘
           └────┬───────┬───────┬───┘
                │       │       │
                ▼       ▼       ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Subtree  │ │  Block   │ │ Callback │
        │Processor │ │Processor │ │ Service  │
        └──────────┘ └──────────┘ └────┬─────┘
                                       │ HTTP POST
                                       ▼
                              BSV Broadcaster instances
```

**API Server.** Accepts `POST /watch {txid, callbackUrl}` from BSV Broadcaster. Writes the registration to Aerospike, a key-value store chosen for the same reason Teranode chose it: it scales to billions of keys with predictable sub-millisecond reads under heavy concurrency. The registry schema is intentionally minimal (`txid → callback_url`) — this is a hot-path lookup, not a relational query.

**P2P Client.** A Teranode libp2p client that subscribes to the BSV network for subtree and block messages. It does not do any processing itself — it publishes raw subtree announcements and block messages onto Kafka topics. This decoupling is critical: ingest cannot be slowed down by downstream processing latency.

**Kafka.** Three topics — `subtree`, `block`, and `stumps` — form the backbone of the service. Kafka gives Merkle Service the same horizontal scaling primitive Teranode uses internally: any consumer can be parallelized by adding partitions and consumer instances. If subtree processing is the bottleneck, scale the Subtree Processor consumer group; if callback delivery is the bottleneck, scale the Callback service.

**Subtree Processor.** Subscribes to the `subtree` topic. For each incoming subtree, it stores the subtree to a subtree store (with a one-block TTL — these are evicted automatically), enumerates every transaction, and looks up registrations in Aerospike. **All Aerospike calls are batched**, following the same pattern Teranode uses in its block assembler: instead of N round-trips for a subtree of N transactions, one batch read returns all matching registrations in a single network call. For any tracked transaction, a `SEEN_ON_NETWORK` callback fires.

A future optimization layer — modeled on Teranode's `txmetacache` — deduplicates redundant Aerospike calls and supports counter-based statuses like `SEEN_MULTIPLE_NODES`, where a transaction crossing a propagation threshold triggers an event without re-querying the registry every time.

**Block Processor / Block Subtree Processor.** When a block message arrives on the `block` topic, the Block Processor fans out work across **multiple Block Subtree Processors running in parallel — one logical worker per subtree in the block**. Each Block Subtree Processor:

- Loads the subtree (from the subtree store populated in real time by the Subtree Processor — already in memory or on local disk).
- Enumerates transactions and batch-queries Aerospike for registered callback URLs.
- Builds the STUMP (Subtree Unified Merkle Path, BRC-74 binary format) for each subtree that contains tracked transactions.
- Publishes one STUMP message per `(callback_url, subtree)` pair to the `stumps` Kafka topic.
- Extends the TTL on registered transactions to 30 minutes to absorb potential forks and orphans without retaining state forever.

Subtree 0 is processed with the coinbase placeholder intact — the Merkle Service does not perform coinbase transaction replacement. That happens in BSV Broadcaster during compound BUMP construction, where the coinbase BEEF is fetched from the Teranode DataHub.

Because subtrees within a block are independent, the parallelism is embarrassingly parallel: a 1000-subtree block becomes 1000 independent units of work, distributable across as many workers as available. This is the same partitioning strategy Teranode uses to assemble blocks, applied in reverse for proof extraction.

**Callback Service.** Subscribes to the `stumps` topic. For each STUMP message, it issues an HTTP POST to the registered callback URL. Failed deliveries are republished to Kafka for retry, so transient BSV Broadcaster outages or network blips don't lose proofs.

### Why this scales

Three properties together let Merkle Service handle Teranode-scale blocks while serving any number of lightweight BSV Broadcaster instances:

**Decoupled ingest and processing.** The P2P Client's only job is to publish raw messages onto Kafka. Network ingest cannot stall on processing latency, and processing latency cannot stall on registry queries. Each stage has independent backpressure characteristics.

**Per-subtree parallelism inside each block.** A block of millions of transactions is decomposed into thousands of independent subtree-processing tasks. There is no global coordination point inside block processing — each Block Subtree Processor reads its own subtree, performs its own batched Aerospike lookup, and emits its own STUMPs.

**Batched Aerospike access on every hot path.** No code path issues per-transaction Aerospike calls. Subtree processing batches every transaction in a subtree into one read; block processing batches every transaction in a subtree into one read. Aerospike's batch API is designed for exactly this — a single round-trip across thousands of keys — and it's the same primitive Teranode uses for high-throughput block assembly.

**Fan-out at the right layer.** A single tracked transaction shared across many BSV Broadcaster instances would, in a naive design, multiply work proportionally. In Merkle Service, the expensive operations — subtree ingest, STUMP construction — happen once per subtree regardless of how many subscribers care about transactions inside it. Fan-out happens only at the cheapest layer, the Callback Service issuing HTTP POSTs.

**Configurable deployment topology.** Following Teranode's pattern, every component can run as part of a single all-in-one binary or as independently scaled microservices. A small deployment runs everything in one process; a production deployment scales the bottleneck stage independently.

The end result is a service whose scaling profile follows block structure rather than subscriber count or transaction-of-interest count — which is exactly what's needed to make Teranode-native proof delivery economically viable for application developers.

## How BSV Broadcaster v2 uses Merkle Service

With the heavy ingest work pooled in Merkle Service, BSV Broadcaster v2 itself becomes a much lighter component. It is still a BSV transaction broadcast and status tracking service — accepting transactions, validating against policy, broadcasting to Teranode, tracking lifecycle, and emitting events via webhooks and SSE — but it no longer needs to ingest the network firehose to do any of that. It registers transactions of interest with Merkle Service and reacts to the callbacks it receives.

```
Client ──► BSV Broadcaster ──► Teranode (broadcast)
             │
             ├──► POST /watch ──► Merkle Service
             │                         │
             └◄── HTTP callbacks ◄─────┘
                  (SEEN_ON_NETWORK,
                   STUMP, BLOCK_PROCESSED)
```

The end result: one Merkle Service deployment handles the heavy hardware footprint for many BSV Broadcaster instances. Each BSV Broadcaster runs on commodity infrastructure and only processes proofs for its own tracked subset.

## Transaction lifecycle

A transaction's journey through BSV Broadcaster v2 has five phases.

### Phase 1 — Submission

Client calls `SubmitTransaction()`. BSV Broadcaster parses the submitted bytes (Extended Format only; a plain raw transaction carries no input source data and is rejected with ARC 460, because validation needs each input's parent output, and BEEF is not decoded), deduplicates via `store.GetOrInsertStatus()`, validates against policy (fee rates, script rules, size limits), tracks it in TxTracker (an in-memory O(1) hash map), registers it with every configured Merkle Service via `POST /watch {txid, callbackUrl}` (a tx is registered once any endpoint accepts it; a tx no endpoint accepted is requeued and never broadcast, so a proof can always be delivered for a broadcast tx), and broadcasts concurrently to Teranode endpoints, returning on first success.

Status: `RECEIVED` → `ACCEPTED_BY_NETWORK`.

### Phase 2 — Network propagation

Merkle Service emits informational callbacks as the transaction propagates:

- `SEEN_ON_NETWORK` — transaction detected in a miner's subtree. Status updates to `SEEN_ON_NETWORK`, TxTracker updates, event publishes.
- `SEEN_MULTIPLE_NODES` — multiple miners have seen it. Informational only; no status change.

### Phase 3 — Mining confirmation via STUMPs

When a block is mined containing tracked transactions, Merkle Service sends **one STUMP callback per subtree, not per transaction**. STUMP — Subtree Unified Merkle Path — is a BRC-74 binary-encoded merkle path covering an entire subtree.

The callback payload deliberately has no `txid` field:

```json
{
  "type": "STUMP",
  "blockHash": "...",
  "subtreeIndex": 3,
  "stump": "<BRC-74 binary>"
}
```

Transaction discovery happens at parse time inside BSV Broadcaster. `handleStump()`:

1. Parses the STUMP via `transaction.NewMerklePathFromBinary()` to extract every level-0 leaf hash in the subtree.
2. Filters tracked transactions via `TxTracker.FilterTrackedHashes()` — O(1) map lookup per hash, single RWMutex lock for the entire batch.
3. Updates each tracked transaction to `MINED` in the store and `STUMP_PROCESSING` in TxTracker, publishes events.
4. Stores the STUMP **once**, keyed by `(blockHash, subtreeIndex)`, with `ON CONFLICT DO UPDATE` for idempotency.

A block with 100 tracked transactions across 3 subtrees produces 3 STUMP writes instead of 100 per-tx writes. Duplicate STUMPs from retries collapse via the upsert. SQLITE_BUSY under load is eliminated.

The handler is also backward compatible with older per-tx callback shapes — any `txid` field in the payload is ignored, since discovery always happens from STUMP parsing.

### Phase 4 — Compound BUMP construction

Once Merkle Service has delivered every subtree STUMP for a block, it sends `BLOCK_PROCESSED`:

```json
{ "type": "BLOCK_PROCESSED", "blockHash": "..." }
```

This triggers `ConstructBUMPsForBlock()`. BSV Broadcaster fetches all STUMPs for the block from the database, then fetches block data from Teranode's DataHub:

- **Primary:** `GET /block/{hash}/json` — returns subtree root hashes plus the coinbase BUMP.
- **Fallback:** Binary endpoint — subtree root hashes only, no coinbase BUMP.

`BuildCompoundBUMP()` then assembles a single block-level merkle path containing every tracked transaction in the block. For each STUMP, `AssembleBUMP()`:

- Parses the STUMP binary.
- For subtree 0, replaces coinbase placeholder hashes using the coinbase BUMP from the DataHub response.
- Shifts local subtree offsets to global block offsets.
- Adds subtree root hashes at the upper layers of the tree.
- Computes missing intermediate hashes.
- Extracts the minimal path per transaction.

Tracked txids are discovered from level-0 hashes (again via `TxTracker.FilterTrackedHashes()`), and individual paths are merged into a single compound MerklePath, deduplicated by `(level, offset)`.

The result is **one row in the `bumps` table per block**, containing the compound proof for every tracked transaction in that block. Per-transaction minimal paths are extracted at query time via `extractMinimalPathForTx()` when a client calls `GET /tx/{txid}`. We don't store N separate merkle paths.

After storing the compound BUMP, BSV Broadcaster marks transactions `MINED` via `SetMinedByTxIDs()`, publishes `MINED` events, and deletes the temporary STUMP rows for the block.

### Phase 5 — Confirmation and pruning

As new blocks arrive via the P2P client, Chaintracks tip updates mark blocks `on_chain` (canonical). Reorg detection identifies orphaned blocks: STUMPs for orphaned blocks are deleted, affected transactions reset to `SEEN_ON_NETWORK`, block hash associations cleared.

`MINED` is the last status emitted today. `TxTracker.PruneConfirmed()` exists to drop transactions with 100+ confirmations from memory, but nothing calls it outside tests and no code path writes `IMMUTABLE` (see [observability](observability.md)), so clients must not wait for an `IMMUTABLE` event.

## Status state machine

```
                    RECEIVED ────────► REJECTED (intake verdict)
                       │
                       ├────────────► PENDING_RETRY (no verdict yet; rebroadcast until a verdict,
                       ▼                   │   or REJECTED once the retry cap is spent)
              ACCEPTED_BY_NETWORK ◄────────┘      ─► REJECTED (Teranode verdict,
                       │                              double spend = 466, competingTxs if named)
                       ▼
              SEEN_ON_NETWORK ◄──── (reorg resets MINED)
                       │
                       ▼
              SEEN_MULTIPLE_NODES
                       │
                       ▼
              STUMP_PROCESSING        (TxTracker only, not persisted)
                       │
                       ▼
                     MINED            (last status emitted today)
                       ┆
                       ▼
                   IMMUTABLE          (reserved: no producer yet)
```

Transitions are guarded by `DisallowedPreviousStatuses()` in the SQL UPDATE query, preventing backward transitions — `MINED` cannot revert to `SEEN_ON_NETWORK` except through explicit reorg handling.

`SENT_TO_NETWORK` and `DOUBLE_SPEND_ATTEMPTED` are part of the status enum for ARC compatibility but no code path emits them: the broadcast is acknowledged straight into `ACCEPTED_BY_NETWORK`, and a double spend surfaces as `REJECTED` (see below). A producer for `DOUBLE_SPEND_ATTEMPTED` would be Teranode's rejected-tx gossip, tracked as issue #325. `IMMUTABLE` is in the enum and the lattice as well, but has no producer yet (see Phase 5).

### Rejection semantics: verdicts vs. conditions

BSV Broadcaster distinguishes two kinds of submit failure (issue #254):

- **Verdicts about the transaction bytes** — validator failures (script, fee, size, malformation) and network rejections — are terminal: a `REJECTED` row is persisted with the reason in `extraInfo` and, when classifiable, the numeric ARC code (460-476) in `status`. `GET /tx/{txid}` serves the verdict for as long as the row is retained: forever by default, or the submitter's tier `retention_days` (see [docs/api-keys.md](api-keys.md#data-retention-per-tier)).
- **Conditions of BSV Broadcaster's chain view** are not verdicts and persist nothing. The intake nLockTime/BIP113 finality gate is the canonical case: BSV Broadcaster's view of the tip can trail the network, so a "not final yet" answer is a `400` with ARC code `476` and an actionable reason — but no `REJECTED` row, no status event. `GET /tx/{txid}` keeps returning `404` ("no verdict") for a first submission, so clients that resubmit on no-verdict recover automatically once the locktime expires, and a resubmission evaluated against a stale tip can never clobber an in-flight row.

Recovery is client-driven by design — BSV Broadcaster keeps no rejected-tx tracker (a tx that mined before BSV Broadcaster ever saw it has no oracle to consult):

- **Resubmitting a `REJECTED` transaction re-validates and re-broadcasts it.** If the original cause has cleared (e.g. a parent has since confirmed), the row advances through the normal `SEEN_ON_NETWORK → MINED` flow; the lattice permits every forward transition out of `REJECTED`.
- **Retry exhaustion is a `REJECTED` without a verdict.** A `PENDING_RETRY` row is rebroadcast until a peer gives a verdict or the durable budget (`propagation.pending_retry_max_attempts`, default 288 — about 24h at the 5-minute backoff cap) runs out. On exhaustion `schedulePendingRetry` clears the retry state and writes `REJECTED` with no peer verdict behind it; `extraInfo` begins `no network verdict after N durable retry attempts` and quotes the last network response, if there was one.
- **Cascade rejections are queue state, not verdicts about the child.** A child held behind a rejected ancestor gets `extraInfo` prefixed `parent rejected`, naming the ancestor — resubmit the child after the ancestor is accepted.
- **A double spend is a `REJECTED` verdict with ARC code `466`.** `466` is the conflict signal, so a wallet can tell "my inputs are gone" from "my bytes are wrong" without a follow-up query. The losing transaction carries Teranode's reason line in `extraInfo`. Only a line that names the spender — `UTXO_SPENT … already spent by tx <txid>` — fills `competingTxs`; `TX_CONFLICTING`, `TX_INVALID_DOUBLE_SPEND` and `TX_LOCKED` also map to `466` but name no spender, so `competingTxs` is omitted and the winner is not in the payload. Both conflicting submissions are answered `202` — intake has no outpoint index and the broadcaster is asynchronous by design — and the network decides which one it keeps. Submit order is therefore not a tiebreak: two conflicting transactions sent milliseconds apart can resolve either way, and only the status (`GET /tx`, SSE, webhook) says which one won.

Operators can detect the stale-chain-view condition before it causes non-final rejections: `GET /health` reports `blockHeight` (BSV Broadcaster's processed tip), and the `bsv_broadcaster_chain_tip_height` / `bsv_broadcaster_p2p_peer_best_height` gauges expose height lag between BSV Broadcaster and its datahub peers.

## Storage layout

| Table | Primary key | Purpose |
|---|---|---|
| `transactions` | `txid` | Status, block hash, competing txs |
| `submissions` | `submission_id` | Client callback registrations per submission |
| `stumps` | `(block_hash, subtree_index)` | Temporary STUMP storage between STUMP and BLOCK_PROCESSED |
| `bumps` | `block_hash` | Compound BUMP per block |
| `processed_blocks` | `block_hash` | Block tracking for reorg detection |

## Key design decisions

**Heavy ingest moves to a shared service.** The Teranode-scale subtree ingest workload runs once, in Merkle Service. BSV Broadcaster instances become lightweight proof recipients.

**Per-subtree STUMPs, not per-transaction.** The Stump model has no TxID field. Discovery happens at parse time. One SQLite write per subtree, idempotent upserts, no SQLITE_BUSY under concurrent callbacks.

**Compound BUMPs.** One `bumps` row per block, regardless of how many tracked transactions it contains. Per-tx minimal paths extracted at query time. Avoids storing N separate merkle paths per block.

**TxTracker as source of truth for tracked transactions.** Both STUMP processing and BUMP construction use `TxTracker.FilterTrackedHashes()` to discover which level-0 hashes correspond to tracked transactions. The tracker is an in-memory concurrent hash map loaded from the store at startup.

**Registration before broadcast, with redundancy.** A tx is broadcast only after at least one Merkle Service accepted its `/watch`; several Merkle Services can be configured and each one delivers callbacks, with duplicates handled idempotently. In-flight transactions are re-registered on startup and whenever an endpoint comes back from an outage.

**Backward compatibility.** STUMP callback handling works against both old per-tx and new per-subtree Merkle Service implementations.

## What this unlocks

For application developers, the operational story changes completely. You no longer need Teranode-class hardware to broadcast transactions and get merkle proofs back. A BSV Broadcaster v2 instance can run on commodity infrastructure and receive proofs only for the transactions you care about, regardless of whether those transactions land in a block of a thousand or a block of a hundred million.

For the network, the heavy work happens once in a service designed for it, instead of being duplicated by every integrator who needs proofs.

This is what makes Teranode-native broadcast and proof retrieval practical at the application layer.

## Resources

- **Merkle Service:** https://github.com/bsv-blockchain/merkle-service
- **BSV Broadcaster v2:** https://github.com/bsv-blockchain/arcade-refactor
- **Full design document:** https://github.com/bsv-blockchain/merkle-service/blob/master/docs/design.md
- **ARC (the predecessor BSV Broadcaster was built to replace):** https://github.com/bsv-blockchain/arc
- **Swimlane Diagram** https://github.com/bsv-blockchain/broadcaster/blob/main/docs/swimlanes.png
