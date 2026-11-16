# Async durable intake: region-symmetric submit latency

Status: proposal · 2026-11-16 · tracking: 99.99% uptime of TX Broadcasting

## Summary

`POST /tx` and `POST /txs` answer only after every transaction is written to
MongoDB Atlas. Atlas has one primary (us-east-1), and writes wait for a
majority of a 2 + 2 + 1 replica set, which always includes an eu-north-1 node.
So every submission pays a cross-region round trip that no amount of Atlas
capacity removes, and clients in Europe and Asia pay a second one to reach the
primary.

This proposal acknowledges a submission once it is durable in the region that
received it, on that region's Redpanda (3 replicas, multi-AZ). A consumer then
writes it to Atlas in batches, off the request path. Submit latency becomes
roughly the same in every region (~10–30 ms server-side). Atlas cost stays the
same, and the regional failover design is kept.

## Measurements (2026-11-16)

Measured on broadcast.whatsonchain.com, mainnet, `bsva-us-1`:

| Signal | Value |
|---|---|
| `POST /txs`, 10 txs (Coralogix trace `c96d7e61…`) | 1,504 ms; 1,490 ms before the first Kafka produce |
| `POST /txs` average (`api_request_duration_seconds`) | 1,466 ms |
| `POST /tx` average | 338 ms |
| Store write average (`store_updatestatus_duration_seconds`, n = 851) | 167 ms |
| Atlas slow log, `transactions` writes from us-1 | 204 ms total, of which 203 ms `waitForWriteConcern` and < 1 ms CPU |
| Kafka publish (`events_publish_duration_seconds`) | 4–5 ms |

The write cost is the `w:majority` wait. CPU, memory and disk on Cluster0 sit
under 5% (see the 2026-11-11 rightsizing review), so a larger tier does not
help.

eu-1 and ap-1 have too little traffic to measure directly. Their figures below
are estimates: AWS inter-region round trip to us-east-1 (us-east-2 ≈ 12 ms,
eu-west-2 ≈ 75 ms, ap-southeast-1 ≈ 215 ms) plus the ~150 ms majority wait
measured above.

Request sizes, 7 days, all networks (`api_request_bytes`): `/tx` median 640 B,
p99 13.6 KB. Of 250,755 requests, 78 (0.03%) were over 1 MiB and 7 over 4 MiB.

## Request-path ledger

Every remote call `/txs` makes before it answers:

| Step | Remote? | Cost today |
|---|---|---|
| API-key resolution | WoC introspection, cached | ~0 (20 hits / 2 misses) |
| Quota reservation | Atlas, leased in fractions of a key's minute budget | 126 ms, amortised (2 reservations for 22 requests) |
| Parse, policy, script, fee validation | local CPU | small |
| Finality check | chaintracks, in-namespace | small |
| Partition-key inheritance (`LookupPartitionKeys`) | Atlas primary read, one per batch | ~12 / 75 / 215 ms (us / eu / ap) |
| Dedup CAS (`GetOrInsertStatus`) per tx | Atlas majority write | ~165 / 225 / 365 ms per round |
| Retention extend on duplicates | Atlas write, duplicates only | as above |
| `RECEIVED` event + propagation message | region-local Redpanda | ~5 ms |

Async intake only gives every region the same latency if **every** Atlas touch
leaves the request path. The partition-key read and the CAS both move to the
consumer. The quota reservation is already amortised and stays.

## Options

Estimated server-side latency for a 10-tx `/txs`. Cloudflare edge time is not
included.

| Option | us-1 | eu-1 | ap-1 | Region loss | Cost |
|---|---|---|---|---|---|
| Today: sequential CAS | 1.5 s (measured) | ~2.4 s | ~3.9 s | survives | ~$115/mo |
| 1. Concurrent CAS (branch `perf/parallel-intake-writes`) | ~0.2–0.35 s | ~0.3–0.55 s | ~0.6–1.0 s | survives | same |
| 1 + Atlas relayout (majority reached inside the US) | ~30–50 ms | ~90–170 ms | ~230–460 ms | survives | same |
| **Async durable intake (this doc)** | **~10–30 ms** | **~10–30 ms** | **~10–30 ms** | survives; see Durability | same + Redpanda RF 3 |
| Atlas Global Cluster, region-local majorities | ~20–40 ms | ~20–40 ms | ~20–40 ms | a region's shard stops accepting writes | ~$1.2–1.4k/mo |
| Global Cluster with cross-region shard majorities | ≈ today | ≈ today | ≈ today | survives | ~$2–2.4k/mo |

For option 1, the range depends on `batch_concurrency`: the default `NumCPU`
(8 on m8i.2xlarge) gives two rounds for 10 txs, and 16 gives one.

Only async intake gives equal latency in every region while keeping regional
failover. Option 1 should ship regardless: the consumer below needs it, and it
covers the synchronous fallback path.

## Design

### Flow

```
client ─► api-server (region R)
            1. auth (cached), quota (amortised), parse, validate, finality   [local]
            2. produce IntakeEnvelope to arcade.intake (acks=all)            [region-local]
            3. 202 Accepted
                         │
                         ▼
          intake-writer (region R, consumer group per region)
            4. poll a batch of envelopes
            5. one Atlas round trip: InsertMany(ordered:false) + find duplicates
            6. apply today's CAS outcome rules per tx
            7. write submissions, publish RECEIVED for inserted rows        (#391)
            8. produce txid-only messages to arcade.propagation             (#329 unchanged)
            9. commit offsets
                         │
                         ▼
          propagation (unchanged): hydrate → register merkle → broadcast   (F-024)
```

### Intake topic

- New topic `arcade.intake`: replication factor 3, `min.insync.replicas=2`, and
  `max.message.bytes` sized to the threshold below.
  - Retention of 24 h is enough because the topic only holds bytes until they
    reach Atlas.
  - Partitions match `arcade.propagation` (8), keyed by the family partition
    key, so ordering within a family survives.
- The producer is the existing sync producer (`WaitForAll`, idempotent).
- Redpanda brokers must be spread across availability zones. They are not
  today: on 2026-11-16 all three brokers sat in one zone in every region
  (us-east-2a, eu-west-2c, ap-southeast-1a). RF 3 gives no AZ durability until
  a zone topology spread constraint is added. Their gp3 volumes are
  zone-bound, so moving an existing broker means re-creating its PVC.
- A new topic rather than raising `arcade.propagation` to RF 3: the existing
  topics are RF 1, and the operator treats topic changes as one-way (see
  `_base/topics.yaml`). If the operator can raise RF in place, the other topics
  should follow separately.

### Why a separate topic, given #329

#329 / #402 deliberately took raw transactions out of the propagation topic.
That stays as it is. `arcade.intake` carries raw bytes only between the ack and
the Atlas write, usually under a second. Propagation, the reaper and replay
keep reading raw bytes from the store.

### Size threshold

`intake.max_inline_bytes` (default 768 KiB, under the 1 MiB Kafka default).

- A request where any tx exceeds the threshold takes today's synchronous path,
  with concurrent CAS.
- That is about 0.03% of requests by the 7-day sample. They keep today's
  latency and nothing about them changes.

### Envelope

```
IntakeEnvelope {
  txid, raw_tx, partition_key, family_key
  received_at                       // api-server clock; row timestamp
  expires_at                        // retention (tier)
  callback_url, callback_token, full_status_updates
  key_id, request_id, trace context
}
```

### intake-writer

- Runs as a new mode, or inside `propagation`. A separate mode keeps the
  propagation claim and lease logic untouched.
- **Batch dedup.** Per poll batch, `BatchGetOrInsertStatus` is given a native
  Mongo implementation: `InsertMany(ordered:false)`, then one `find` for the
  rows that hit duplicate keys. Its contract changes to return per-row results
  alongside an error, so a partial failure stays attributable. That makes any
  batch one round trip, and it also fixes large `/txs` batches on the
  synchronous path.
- **CAS outcome rules.** These are today's rules, moved from the handler:
  - a REJECTED resubmit or a stranded RECEIVED row is republished, with its
    raw bytes restored
  - a duplicate is registered with the tracker
  - retention is extended on duplicates
- **Partition-key inheritance and family adoption** (#392) move here and run
  once per poll batch.
- **Ordering.** Submissions are written, then RECEIVED is published, then the
  propagation message is produced. F-033 (callbacks for unknown txids are
  dropped) holds because the row exists before merkle registration.
- **Failures.** A store error leaves the offset uncommitted and the batch is
  retried. Idempotency comes from the txid unique index. Poison envelopes, such
  as an oversized document, go to `arcade.intake.dlq` and are alerted on.

### Contract changes

- **202 means durably queued in region R** (on ≥ 2 brokers in separate AZs).
  It no longer means "written to the shared store".
- **`submitted` / `duplicates` / `total`.** Duplicates are only known after the
  Atlas write.
  - Proposal: `duplicates` counts in-request repeats plus hits in a
    region-local recent-intake cache (best effort).
  - Add `queued: n`, and document that duplicate counts are advisory.
  - Validation, fee and finality rejections stay synchronous 4xx, unchanged.
- **`GET /tx/{txid}` right after submit.**
  - The api-server keeps a bounded recent-intake cache and answers RECEIVED
    from it until the consumer write lands.
  - A GET routed to another region inside that window returns 404.
  - The target is consumer lag p99 < 1 s, documented as "404 within a second
    of submit: retry".
- **SSE.** RECEIVED is emitted by the consumer, typically 50–300 ms after the
  202.

### Durability

**Today:** an acknowledged tx is in a majority of Atlas nodes in two regions.

**Proposed:** an acknowledged tx is on at least two Redpanda brokers in
different AZs of one region until the consumer writes it, typically under a
second.

| Event | Today | Proposed |
|---|---|---|
| One broker or AZ lost | n/a | no loss (RF 3, min ISR 2) |
| Region unreachable (network, control plane) | in-flight work adopted elsewhere after ~1 h | the same, **plus** txs in the unwritten window wait until the region returns. Clients that resubmit elsewhere are deduped |
| Region's storage destroyed (≥ 2 brokers' volumes) | no loss | txs in the unwritten window are lost. The window is consumer lag, and clients must resubmit |

The real regression is the last row: a correlated loss of a region's broker
volumes inside a sub-second window. That is the price of equal latency.
Monitoring envelope age at write keeps the window visible.

## Observability

- `intake_envelope_age_seconds`: time from 202 to Atlas write, as a histogram.
  Alert when p99 > 5 s.
- `intake_consumer_lag`: per partition. Alert on growth.
- `intake_sync_fallback_total`: by reason (`size`, `producer_error`).
- The trace context in the envelope links the request span to the writer span.

## Rollout

1. Ship concurrent CAS (option 1). Set `batch_concurrency: 16` in the broadcast
   configmaps.
2. Native `BatchGetOrInsertStatus` for Mongo, with per-row results on error.
3. Redpanda: AZ topology spread, then create `arcade.intake` (RF 3, min ISR 2)
   per network per region.
4. Ship `intake-writer` with `intake.mode: sync` (default). Deploy consumers
   first.
5. Turn on `intake.mode: async` on tstn, then testnet, then mainnet ap-1
   (largest gain), then eu-1 and us-1.
6. Watch envelope age and lag for a week before calling it done.

## Open questions

1. Is "advisory duplicates" acceptable to existing clients (wallet-toolbox,
   ts-stack ARC broadcaster), or must `duplicates` stay exact?
2. Should the recent-intake cache be shared across regions, so a cross-region
   GET inside the window succeeds? That costs a cross-region call on miss.
3. New mode or inside `propagation`?
4. Can the Redpanda operator raise RF on existing Topic CRs, so RF 1 can be
   retired everywhere?

## Effort

Rough sizing:

| Item | Size |
|---|---|
| Native batch CAS in Mongo + contract change | 2–3 days |
| `intake-writer` mode, envelope, threshold fallback | 5–7 days |
| Contract and cache changes | 2–3 days |
| Redpanda topology and topics in flux | 1–2 days |
| Chaos tests: broker loss, region partition, consumer crash | 2–3 days |
