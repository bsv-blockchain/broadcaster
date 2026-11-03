# Production Kafka topics

BSV Broadcaster does **not** create Kafka topics itself — it relies on broker-side
auto-creation on first publish, which is typically disabled in production
clusters. One topic also has a hard correctness constraint (`bsv-broadcaster.propagation`
must have **at least `propagation.partitions`**) that BSV Broadcaster enforces at startup
with a fail-closed check. This guide lists every topic an operator needs to
pre-create, with partition, replication, and retention guidance.

This guide assumes a production deployment with `kafka.backend: sarama` pointed
at an external Kafka cluster. Standalone profiles (`kafka.backend: memory`)
don't need any of this — see [`getting-started.md`](getting-started.md).

## Topics BSV Broadcaster uses

| Topic                          | Partitions                | Consumer group(s)                  | Purpose                                                              |
| ------------------------------ | ------------------------- | ---------------------------------- | -------------------------------------------------------------------- |
| `bsv-broadcaster.propagation`           | **≥ `propagation.partitions`** (default 1) | `<consumer_group>-propagation`     | API server → propagation: txs to broadcast to datahubs.              |
| `bsv-broadcaster.block_processed`       | ≥ N (N = bump-builder replicas) | `<consumer_group>-bump-builder` | API server → bump-builder: BLOCK_PROCESSED signals from Merkle Service. |
| `bsv-broadcaster.tx_status`             | ≥ N (N = max concurrent SSE/webhook fan-outs) | ephemeral per-subscriber groups | All services → SSE/webhook: tx status mutations.                     |
| `bsv-broadcaster.transaction`           | 1 (currently unused)      | none                               | Defined in `AllTopics()` but retired with `tx_validator`. Create or skip. |
| `bsv-broadcaster.propagation.dlq`       | 1                         | none (operator-inspected)          | Failed propagation messages after max retries.                       |
| `bsv-broadcaster.block_processed.dlq`   | 1                         | none (operator-inspected)          | Failed BLOCK_PROCESSED messages after max retries.                   |
| `bsv-broadcaster.tx_status.dlq`         | 1                         | none (operator-inspected)          | Failed status-update messages after max retries.                     |
| `bsv-broadcaster.transaction.dlq`       | 1                         | none                               | DLQ for retired topic; safe to skip.                                 |

Consumer-group base name comes from `kafka.consumer_group` in config (default
`bsv-broadcaster`). Service-specific suffixes (`-propagation`, `-bump-builder`) are
applied automatically — operators only configure the base name.

Topic names are `<kafka.topic_prefix>.<name>` (default prefix
`bsv-broadcaster`). Deployments that predate the BSV Broadcaster rebrand set
`kafka.topic_prefix: arcade` to keep their existing `arcade.*` topics; see
[the migration guide](migration-arcade-to-bsv-broadcaster.md).

## Partition counts

### `bsv-broadcaster.propagation` — at least `propagation.partitions`

The propagation service runs one dep-aware dispatcher per partition claim.
Each dispatcher owns its own in-memory dependency index, so a parent and its
child must land on the **same** partition for the child to be held behind its
parent.

Intake guarantees that for transactions submitted together: `POST /txs`
computes a partition key per **dependency family** — every tx in the request
connected through in-batch spends shares one key (the lexicographically
smallest member txid, so a shuffled resubmission maps to the same partition).
A whole family therefore lands on one partition, and per-partition order
delivers parents before children.

Chains that span requests get the same treatment through the store: every row
persists the partition key its message was published with, and a single
`POST /tx` whose input spends a transaction still held by a dispatcher
(`RECEIVED` or `SENT_TO_NETWORK`) reuses that parent's key
(`propagation.parent_key_lookup_max_inputs` bounds how many inputs are
consulted). A parent parked at `PENDING_RETRY` is not followed: the dispatcher
has released it and the reaper rebroadcasts it outside Kafka, so a child of a
parked parent goes through the missing-parent safety net. A wallet paying out of its own
unconfirmed change one request at a time therefore lands its whole chain on
one partition, and each hop is held behind the previous one instead of racing
it to the datahub. A `POST /txs` family whose parent outside the batch is in
flight joins that parent's partition the same way.

A resubmission keeps the key its row was stored with, so it rejoins its earlier
message even if a parent has been accepted in between: a single `POST /tx`
retry, the loser of two identical concurrent submissions (it republishes under
the winner's stored key, even if that row has advanced since the CAS), and a `POST /txs` retry, whose whole family follows
any member still in flight or being republished. The one gap is two concurrent
batches that straddle a parent's acceptance and share a family: a member the
losing batch inserted keeps the key it chose, and a later child following it
falls back to the safety net below.

What remains is the genuine race: a child submitted before its parent, a
parent that is already `ACCEPTED_BY_NETWORK` (Teranode has it, so the child can
go anywhere), or a parent submitted through another region. Such a child can
reach a datahub before its parent and draw `TX_MISSING_PARENT`. That is
treated as a **condition, not a verdict** — the tx rides the bounded requeue,
then parks at `PENDING_RETRY` for the reaper's durable retry queue. The only
path to `REJECTED` is BSV Broadcaster's own store showing an ancestor
terminally rejected.

BSV Broadcaster enforces the floor at startup with `kafka.CheckMinPartitions`. A missing
topic, or a topic with fewer partitions than `propagation.partitions`, aborts
startup. More partitions than configured is allowed — but note the producer
hashes over the topic's **actual** partition count, so extra partitions carry
real traffic and get their own dispatchers. `propagation.partitions` is a
fail-closed assertion, not a placement control.

Sizing: run at most `propagation.partitions` propagation replicas. Extra
replicas idle as standbys for failover. A single request whose transactions
are all one dependency family lands entirely on one partition, so batches that
are one long chain — or that contain a consolidation tx spending many
in-request outputs — serialize regardless of partition count.

### Widening an existing deployment

Partition counts can be increased in place. Do it **topic first**, so BSV Broadcaster is
never configured for more partitions than the topic has (that combination is a
boot-time hard fail):

```bash
# 1. Widen the topic.
rpk topic add-partitions bsv-broadcaster.propagation --num 3   # 1 -> 4 total

# 2. Raise propagation.partitions to the new count (config or
#    BSV_BROADCASTER_PROPAGATION_PARTITIONS), then roll the propagation pods.

# 3. Scale replicas up to at most the partition count.
kubectl scale deploy/propagation --replicas=4
```

Between steps 1 and 2 the producer already hashes over the wider topic, so a
family submitted before the widen and its continuation submitted after can map
to different partitions. That window is what the missing-parent safety net
exists for. During it, watch:

- `bsv_broadcaster_propagation_missing_parent_total{outcome="requeued"}` — a burst is
  expected; it should return to baseline once the remap window passes.
- `bsv_broadcaster_propagation_parked_depth` — a transient rise is normal; sustained
  growth means chains are not converging.
- `bsv_broadcaster_propagation_outcome_total{outcome="rejected"}` — should **not**
  move. A spike would mean ordering artifacts reached a verdict, which the
  condition-not-verdict rule is designed to make impossible.

For a zero-race alternative, drain the topic and recreate it at the target
width before rolling pods.

### `bsv-broadcaster.block_processed` and `bsv-broadcaster.tx_status` — match consumer scale

These topics fan out to consumer groups, so partition count should be
**≥ the maximum concurrent consumer pod count**. A topic with fewer
partitions than consumers in its group leaves some consumers permanently
idle.

Worked example: if you run 4 bump-builder replicas, create
`bsv-broadcaster.block_processed` with at least 4 partitions. Sizing higher than the
current pod count is fine and gives headroom to scale out without recreating
the topic. The same logic applies to `bsv-broadcaster.tx_status`, where each SSE/
webhook subscriber creates its own ephemeral group — partition count caps
the per-subscriber parallelism but does not block additional subscribers.

### Dead-letter topics — 1 partition is fine

DLQ topics receive failed messages after the in-process consumer exhausts
`kafka.max_retries` (default 5). BSV Broadcaster never reads from them; operators
inspect manually. One partition is sufficient.

## Replication and durability

BSV Broadcaster's sync producer uses `RequiredAcks: WaitForAll` — every produce
waits for all in-sync replicas. Under-replicated partitions will stall
writes, so size your replication factor and `min.insync.replicas` for
your cluster's availability target.

Suggested for a typical 3-broker production cluster:

- `--replication-factor 3`
- `min.insync.replicas=2` (tolerates one broker down without stalling writes)

## Retention

BSV Broadcaster does not enforce or assume any retention policy. Suggested defaults:

- **Hot topics** (`bsv-broadcaster.propagation`, `bsv-broadcaster.block_processed`,
  `bsv-broadcaster.tx_status`) — 24h to 7d. These are operational queues; once
  consumed they have no replay value beyond troubleshooting.
- **DLQ topics** — 14d to 30d so you have a reasonable inspection window
  before failed messages age out.

`cleanup.policy=delete` (the broker default) is correct for all topics;
none of them are log-compacted.

## Creating the topics

### Using `rpk` (Redpanda CLI)

```bash
# Hot path
# --partitions must be >= propagation.partitions (default 1). To go wider
# later, see "Widening an existing deployment" above.
rpk topic create bsv-broadcaster.propagation \
  --partitions 1 \
  --replicas 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000   # 7 days

rpk topic create bsv-broadcaster.block_processed \
  --partitions 4 \
  --replicas 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

rpk topic create bsv-broadcaster.tx_status \
  --partitions 4 \
  --replicas 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000    # 1 day

# DLQs
for t in bsv-broadcaster.propagation.dlq bsv-broadcaster.block_processed.dlq bsv-broadcaster.tx_status.dlq; do
  rpk topic create "$t" \
    --partitions 1 \
    --replicas 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=2592000000   # 30 days
done
```

Adjust `--partitions 4` for `bsv-broadcaster.block_processed` / `bsv-broadcaster.tx_status` to
match your consumer-pod headroom.

### Using `kafka-topics.sh` (Apache Kafka CLI)

```bash
BS=localhost:9092

# Hot path
kafka-topics.sh --bootstrap-server "$BS" --create \
  --topic bsv-broadcaster.propagation \
  --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

kafka-topics.sh --bootstrap-server "$BS" --create \
  --topic bsv-broadcaster.block_processed \
  --partitions 4 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

kafka-topics.sh --bootstrap-server "$BS" --create \
  --topic bsv-broadcaster.tx_status \
  --partitions 4 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000

# DLQs
for t in bsv-broadcaster.propagation.dlq bsv-broadcaster.block_processed.dlq bsv-broadcaster.tx_status.dlq; do
  kafka-topics.sh --bootstrap-server "$BS" --create \
    --topic "$t" \
    --partitions 1 --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=2592000000
done
```

## Consumer-group naming

Every BSV Broadcaster service derives its consumer group from the single
`kafka.consumer_group` config value, with a service-specific suffix:

- `<group>-propagation` reads `bsv-broadcaster.propagation`
- `<group>-bump-builder` reads `bsv-broadcaster.block_processed`
- `bsv-broadcaster.tx_status` subscribers (SSE manager, webhook service) use unique
  ephemeral group IDs (`bsv-broadcaster-events-<hex>`) so every subscriber sees every
  message.

Set `kafka.consumer_group` to a value unique per environment
(e.g. `bsv-broadcaster-mainnet-prod`, `bsv-broadcaster-testnet`) so groups don't collide if
multiple deployments share a Kafka cluster.

## What BSV Broadcaster checks at startup

- `kafka.CheckMinPartitions(bsv-broadcaster.propagation, propagation.partitions)` — **hard fail** if
  the topic is missing or has fewer partitions than `propagation.partitions`.
- `kafka.CheckPartitions(...)` — soft warning path used when
  `kafka.min_partitions > 1`. Existing topics with fewer partitions cause a
  startup error; missing topics only log a warning ("will be auto-created
  on first publish").

This means a forgotten `bsv-broadcaster.propagation` will fail loudly, but a
forgotten `bsv-broadcaster.block_processed` will silently auto-create with the
broker's default partition count on first publish. If your broker has
`auto.create.topics.enable=false` (recommended for production), forgotten
topics will instead surface as produce errors at first traffic. Either way,
pre-creating every topic in the table above is the only way to be sure
partition counts match your deployment.
