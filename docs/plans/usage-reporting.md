# Per-key usage metering and reporting

**Status:** the provider push sink is implemented (`services/usage`,
documented in `docs/api-keys.md`). Standalone `usage.sinks` and stored totals
(PR B) are deferred until a consumer other than a provider needs them.
Decisions taken on the open questions: dropping uncertain batches is fine
until a receiver de-duplicates, a `/txs` batch counts as one request, and
`units` is the weighted cost.

**In one line:** the broadcaster meters requests per API key and pushes
daily totals to configured receivers over a small, documented protocol.
Any third party running its own broadcaster gets usage by implementing that
protocol, or by pointing a `usage.sinks` entry at its own billing system.
Stored totals behind an admin API (PR B) are optional on top.

Plan to meter API usage per API key and deliver it to whoever bills or
displays it. That can be a delegated-auth provider (the service that issued
the key, see `auth.introspection`), an operator's own billing system, or the
broadcaster's own store for an admin API to read. All of it is off unless
configured. The provider protocol is generic. Any service that implements it
can receive usage. Its shape matches what the first provider's usage
endpoint already accepts, so that provider is just one configured instance.

The intended reader is the developer implementing it. File targets, the
decisions already made, and the open questions are listed explicitly.

---

## Problem

`auth.introspection` (#411) lets a provider issue keys that the broadcaster
verifies. A provider that sells plans also needs to know what each key used,
so it can show customers their usage and, later, bill or enforce plan caps.
Today the broadcaster records nothing per key. The only per-request record
is the request log, whose INFO lines are sampled downstream, and Prometheus
counters that deliberately have no key label (unbounded cardinality).

Operators who run their own broadcaster with local `bsvb_` keys want the
same thing for their own customers, without depending on any particular
provider.

## Requirements

**Generic**
- Off by default. Nothing is counted or sent until configured.
- Per key, per route, per status class, per UTC day: a request count and
  the cost units charged.
- Pluggable destinations ("sinks"). A provider receives only the keys it
  issued, and it receives them under its own plain `key_id`, never the
  broadcaster's namespaced `<provider>:<key_id>`. Local keys go only to sinks
  configured for local keys.
- Bounded memory, bounded outbound traffic, no per-request network call.
- Correct across replicas and regions: every replica reports its own share,
  and counts add up on the receiving side.
- **Never over-count.** A sum the receiver may already have recorded is not
  resent unless the receiver can de-duplicate it.

**The first provider's usage endpoint**

`POST <usage_url>`, `Authorization: Bearer <service token>` (the same
token as introspect).

```json
{"entries": [{"key_id": "…", "endpoint": "v1/tx", "status_class": "2xx",
              "date_utc": "2026-10-30", "count": 12, "units": 120}]}
```

| Field | Rule |
|---|---|
| `key_id` | The provider's own id from introspect. Unknown or non-broadcaster keys are dropped. |
| `endpoint` | A route **name**: at most 3 segments and 64 characters. Paths with ids or query strings are dropped. Stored as `broadcaster/<endpoint>`. |
| `status_class` | `2xx`, `4xx`, `5xx` or `429` (429 is stored as 4xx for now). |
| `date_utc` | `YYYY-MM-DD`; today if omitted. Always sent. |
| `count` | Greater than 0. **Counts add up, so send each bucket once.** |
| `units` | Weighted count. Accepted now, stored later. |

At most 1000 entries and 1 MiB per call.

| Response | Meaning | Resend? |
|---|---|---|
| `200 {"accepted", "dropped"}` | Recorded. Dropped entries were invalid. | No |
| `503 {"retry": true}` + `Retry-After: 60` | Nothing recorded | Yes |
| `502 {"retry": false}` | May already be counted | **No** |
| `501` | This server does not collect usage | No |
| `401` / `400` | Token or body problem | Fix first |

There is no batch id yet. The provider plans one it will de-duplicate on (see
"Idempotency").

## Design

```
request ──► guarded(route, units, h)
              └─ usage.Recorder (outermost; records after c.Next())
                     │  principal, route name, status class, units charged, UTC day
                     ▼
              usage.Aggregator  (per replica, in memory)
                map[Bucket]Totals   Bucket = {source, key_id, endpoint, class, day}
                     │  every flush_interval (plus on shutdown, plus when full)
                     ▼
              drain ─► one slice per sink, filtered by source
                     ├─► HTTPSink "acme"   (provider keys, plain key_id, chunked ≤1000 / ≤1 MiB)
                     ├─► HTTPSink "billing" (local bsvb_ keys, optional)
                     └─► StoreSink         (optional; $inc daily docs, admin API reads them)
```

### 1. What is counted

A request is counted when it reaches a `guarded` route **and** the caller
presented a key that resolved, so the principal has a `KeyID`.

- Anonymous requests and 401s are not counted. 401s have no key to attribute
  them to, and attributing an invalid key's traffic to anyone is wrong.
- 429s from the limiter **are** counted, with class `429` and units 0:
  providers want to see a customer hitting their limit.
- 503s from key verification (provider or store down) happen before a
  principal exists, so they aren't counted.

**Route names.** `guarded` gains a name argument, so every billable route
declares its stable name next to its cost. No path is ever reported.

| Route | Name |
|---|---|
| `POST /tx` | `v1/tx` |
| `POST /txs` | `v1/txs` |
| `GET /tx/:txid` | `v1/tx/status` |
| `GET /policy`, `GET /v1/policy` | `v1/policy` |
| `GET /api/v1/blocks/processing-status`, `…/:blockHash` | `v1/blocks/status` |

Names are lowercase, at most 3 `/`-separated segments and at most 64
characters, enforced by a unit test over the route table. A new guarded route
cannot be added without a name.

**Status class.** `c.Writer.Status()` after the chain, read the same way
`requestLogger` does (`services/api_server/server.go`, with
`metrics.ObserveStatusClass` as the precedent): <400 → `2xx`, 429 → `429`,
other 4xx (including 499, client closed) → `4xx`, ≥500 → `5xx`.

**Units** are exactly the cost units the request was charged. That is the
route's base cost, charged up front by `guarded`, plus `n × costs.submit_tx`
that `ChargeTxs` adds for `/txs` once the batch is parsed. The limiter
records what it charged on the gin context (`ratelimit.ChargedUnits(c)`).
With rate limiting off, `guarded` and `ChargeTxs` record the nominal cost
instead, so units mean the same thing either way.
- Nothing is invented for uncharged work. A `/txs` rejected after the up-front
  charge (a 413 from `CheckBatch`, or a 400 from parsing) records just the
  base `read` cost it paid.
- A request rejected by the limiter records 0.

### 2. Aggregation

`usage.Aggregator` holds `map[Bucket]Totals{Count, Units int64}` behind a
mutex. A request costs one map increment, and the map key is about five
short strings.

- `Bucket.Source` is `local` or the provider name. `Bucket.KeyID` is the
  **source's own** id: the local `key_…` id, or the provider's plain
  `key_id`. To get this without string parsing, `store.APIKey` and
  `apikey.Principal` gain runtime-only `Provider` and `ProviderKeyID`
  fields, set by the `Introspector` (like `LimitID` today).
- `Bucket.Day` is the UTC date the request **finished**, and it is always
  sent as `date_utc`. It is never left to the receiver's "today", and it is
  never the flush day: a request finishing at 23:59:58 and flushed at
  00:00:30 is reported under the earlier day.
- **Bounded.** At `max_buckets` (default 100k) the next request triggers an
  early flush. If that flush can't run, new buckets are dropped and counted
  in `usage_entries_dropped_total{reason="overflow"}`, and existing buckets
  still increment.
- `Drain()` swaps in a fresh map and returns the old one. The aggregator
  stays a plain map and never takes anything back. Retries live in the
  sinks (§4).

Each replica flushes on its own. Counts add up at the receiver, so 3
regions × N replicas need no coordination. Each replica's flush is jittered
±10% to avoid synchronised bursts at the provider.

### 3. Sinks

```go
type Sink interface {
    Name() string
    Accepts(source string) bool     // which keys this sink receives
    Send(ctx context.Context, entries []Entry) Outcome
}
```

**HTTPSink** speaks the protocol above.
- It chunks to at most 1000 entries and at most 1 MiB, sends chunks
  sequentially, and caps in-flight calls at 1 per sink.
- A **provider sink** comes from `auth.introspection.<name>.usage_url`. It
  reuses that provider's service token, timeout, https/loopback and
  no-userinfo/query checks, and the redirect refusal (shared helpers with
  the Introspector). It accepts `source == <name>`.
- A **standalone sink** comes from `usage.sinks.<name>` with `url`,
  `token`/`token_env` and `source: local`. Any operator billing system that
  implements the protocol can receive local-key usage, and `source: <provider>`
  is also allowed for mirroring.

**StoreSink** (optional, phase 2) persists daily totals with an
`$inc` upsert per bucket. It uses `store.UsageStore`, an optional interface
like `RetentionExtender`, held in the **auth store** when one is configured,
so all networks and regions share one view. An admin endpoint reads it (§7).
Mongo first, with documents in `api_usage` keyed
`{network, source, key_id, endpoint, class, day}`. `network` comes from
the deployment's `network` config, because deployments sharing an auth store
must not merge their totals. Documents expire through a TTL on `day` +
`usage.store_retention_days` (default 400). Other backends log a warning at
boot, as retention does.

### 4. Delivery semantics

The receiver can't de-duplicate yet, so delivery is **at most once**. A
batch whose outcome is unknown is dropped and counted, never resent. If a
choice must be made, under-reporting is safer for billing than charging a
customer twice.

| Outcome | Action | Metric (`usage_flush_total{sink,result}`) |
|---|---|---|
| 200 | done; `dropped > 0` → Warn + `usage_entries_dropped_total{reason="rejected"}` | `ok` |
| 503 with `retry: true` (or 503 without a body), 429 | requeue the chunk; pause the sink for `Retry-After` (≤5 min), else 60s | `retry` |
| 401 / 403 | requeue (nothing recorded); pause 5 min; Error log once per 10s | `unauthorized` |
| dial or TLS failure before the request was written | requeue; pause 30s | `retry` |
| 502, other 5xx, timeout or reset after the request was written | **drop** the chunk; Warn | `uncertain` |
| 400 | drop the chunk; Error log | `rejected` |
| 501 | disable the sink until restart; Warn once | `disabled` |

**Requeue.** Each `HTTPSink` owns a bounded `pending` queue of intact
chunks, separate from the aggregator.
- On each flush it retries `pending`, oldest first, before sending the
  newly drained entries.
- The queue is capped (`max_pending_entries`, default 100k). When full, the
  oldest chunks are dropped first, with `reason="overflow"`.
- Chunks are kept intact, never merged, so a future `idempotent: true` path
  can resend the exact same chunk under the same key.
- With an unknown outcome, unsent later chunks in the same flush go to
  `pending`; they are not dropped. Only the chunk in flight was ambiguous.

**Idempotency (forward-compatible).** Every chunk carries an
`Idempotency-Key: <sink>-<replica>-<flush seq>-<chunk>` header, a random
per-process prefix, and a stable key across retries of the same chunk.
Receivers that ignore it are unaffected. Once a receiver confirms it de-duplicates
on that key (or on a body `batch_id`), a per-sink `idempotent: true` switches
`uncertain` from drop to retry with the same key and chunk. The pending
queue already keeps chunks intact for this. The receiver has to
confirm the header name before we rely on it (open question 1).

### 5. Lifecycle and loss window

- `api_server.Start` builds the aggregator and sinks, and starts one flush
  loop per replica.
- `Stop` runs a final flush **after** the HTTP server has drained, so the
  last requests are counted. It's bounded by `usage.shutdown_flush_timeout_ms`
  (default 10s), and anything left is dropped with `reason="shutdown"`.
- A crash loses at most one `flush_interval` (default 60s) of that replica's
  counts. That's acceptable for a usage dashboard. If it ever isn't, phase 3
  can add a store-backed outbox: `$inc` into a pending collection, and a
  leased reporter that sends and deletes. It's left out here because it puts
  a store write on every request, or every flush, for every key.

### 6. Config

```yaml
usage:
  enabled: false                 # master switch; nothing is metered when off
  flush_interval_ms: 60000       # 10000 .. 3600000
  max_buckets: 100000
  shutdown_flush_timeout_ms: 10000
  store: false                   # phase 2: persist daily totals (StoreSink + admin API)
  store_retention_days: 400
  sinks:                         # optional standalone sinks, e.g. an operator's billing system
    billing:
      url: https://billing.example.com/v1/usage
      token_env: BILLING_USAGE_TOKEN
      source: local              # local | <introspection provider name>
      # A receiver sees the sum from every deployment that reports to it.
      # Deployments sharing one auth store (several networks) all report the
      # same keys, so for per-network totals give each its own sink URL.

auth:
  introspection:
    acme:
      key_prefix: bsv_
      url: https://…/products/introspect
      usage_url: https://…/products/usage   # optional: this provider's keys, its own key_id
      token_env: ACME_INTROSPECTION_TOKEN
      tiers: { … }
```

**Validation.**
- `usage.enabled` requires `auth.enabled`, because without keys there is
  nothing to attribute.
- `usage_url` and sink urls follow the introspection URL rules: https except
  on loopback, no user info, query or fragment.
- A sink's `source` must be `local` or a configured provider.
- Every key gets a `SetDefault`, so `BSV_BROADCASTER_USAGE_*` env overrides
  bind.

### 7. Consuming the data

- **Pushed:** providers and standalone sinks (§3).
- **Pulled (phase 2):** `GET /admin/v1/usage?key_id=&source=&from=&to=` and
  `GET /admin/v1/keys/{id}/usage`, served from `api_usage`. These return
  daily rows `{day, endpoint, status_class, count, units}`. They sit on the
  admin listener behind the admin token, and the CLI gets
  `bsv-broadcaster apikey usage <id>`.
- **Prometheus:** aggregate counters by `source`, `endpoint` and `class`,
  with **no** key label (`usage_requests_total`), plus flush and drop
  metrics. Operators can alert on `usage_flush_total{result!="ok"}` and
  `usage_entries_dropped_total`.

### 8. What this deliberately doesn't do

- **Daily caps.** The published plans' requests/day limits are not enforced
  here. That's a limiter feature (a daily window in `QuotaStore`). It could
  later read the same per-key counters, but it must not depend on an
  at-most-once report path.
- **Anonymous usage per IP.** No key means no customer to report to.
- **SSE `/events` and webhook deliveries.** They aren't `guarded` routes
  today. They can be added as named routes later if a provider wants them.

## Implementation tasks

Each task is TDD: a failing test first, then the code, then a mutation check
that the test catches the defect it targets.

**PR A: metering core and provider push**

1. **Principal source fields.** `store.APIKey` and `apikey.Principal` gain
   runtime-only `Provider` and `ProviderKeyID`. `Introspector` sets them, and
   `Middleware.authenticated` copies them.
   - Files: `store/apikeys.go`, `services/apikey/{introspect,principal,middleware}.go`.
   - Test: an introspected key's principal carries `Provider="acme"` and the
     plain id; a local key has `Provider=""`.
2. **Route names.** Change the signature to `guarded(name string, units int64, h)`.
   - Files: `services/api_server/access.go` and `routes.go`.
   - Test: walk `router.Routes()` and assert every guarded route has a valid
     name (pattern, ≤3 segments, ≤64 characters) and that no name contains
     `:`.
3. **Charged units on the context.** The limiter's `charge` and `ChargeTxs`
   record what they charged. Add `ratelimit.ChargedUnits(c)`, plus the
   nominal cost when no limiter is installed.
   - Test: `/txs` with 3 transactions charges `read + 3×submit_tx` with the
     limiter on and off; a 429 records 0.
4. **`services/usage`:** `Bucket`, `Entry`, `Aggregator` (`Add`, `Drain`,
   the `max_buckets` overflow), and `Classify(status)`.
   - Tests: concurrent `Add` under `-race`; drain additivity; overflow drops
     only new buckets; status-class table; day stamped at completion, not
     at flush.
5. **Recorder middleware,** outermost in `guarded`, recording after
   `c.Next()` and only when the principal is keyed.
   - Test, through the real guard chain (extend
     `TestIntrospectionProviderEndToEnd`): provider key 2xx, a 429 with 0
     units, a local key, anonymous (nothing recorded), a 401 (nothing
     recorded).
6. **`HTTPSink`:** request shape, chunking (1000 entries and 1 MiB), the
   `Idempotency-Key` header, and the full outcome table in §4.
   - A fake provider returns every status: 503 requeues and pauses,
     502/timeout-after-write drops, a dial failure requeues, 501 disables,
     401 requeues and pauses. The pending queue is retried oldest first and
     drops its oldest chunks when full.
   - Assert totals add up across a 503 and then a 200.
   - Mutation check: resending on 502 must fail a test.
7. **Flush loop and lifecycle:** jittered interval, early flush at
   capacity, a final flush in `Stop` after the HTTP drain, and the shutdown
   timeout.
   - Test, with a fake clock: no loss on graceful stop, and bounded loss
     plus a counted drop when the sink hangs at shutdown.
8. **Config and wiring:** the `usage.*` block, `auth.introspection.*.usage_url`
   and `usage.sinks`, with defaults, validation and env-binding tests, built
   in `api_server.Start`.
9. **Metrics:** `usage_requests_total{source,endpoint,class}`,
   `usage_flush_total{sink,result}`, `usage_entries_sent_total{sink}`,
   `usage_entries_dropped_total{sink,reason}` and `usage_pending_buckets`,
   preregistered. No key label anywhere.
10. **Docs:** a "Usage reporting" section in `docs/api-keys.md`. It covers
    the protocol, which acts as the contract for third-party receivers, the
    delivery semantics, and the config.
11. **Live e2e:** the local harness against a real provider's usage endpoint with
    the test key. Submit N requests, flush, and confirm the admin usage page
    shows N under `broadcaster/v1/tx`. Also confirm `accepted == entries`,
    and that a 401 from a bad token requeues and nothing is lost after the
    token is fixed.

**PR B: durable totals and pull API (third-party consumption)**

12. `store.UsageStore` (`AddUsage`, `ListUsage`) on Mongo, in the auth store
    when configured, with a TTL index built when `usage.store` is on (the
    `EnsureRetentionIndexes` pattern). `StoreSink` follows the same outcome
    rules.
13. The admin endpoints and the CLI `apikey usage`, with strict query
    validation and a bounded date range (≤ 400 days).
14. Docs and the tagged Mongo tests.

**Later**
- `idempotent: true` per sink once a receiver de-duplicates.
- A store-backed outbox if the 60s crash window matters.
- Daily plan caps in the limiter.

## Open questions

1. **For the provider:** will the usage endpoint de-duplicate on an
   `Idempotency-Key` header (or a body `batch_id`)? If yes, give the name
   and the de-dup window. Until then we drop on 502 and timeouts.
2. **For the provider (confirmation only):** we send `units` as our weighted cost
   (read 1, submit_tx 10, `/txs` n×10+1), per the guide's "weighted count".
3. **For the provider:** should a batch of n transactions on `/txs` count as 1 request
   (our plan: `count` 1, `units` n×10+1), or as n?
4. **For us:** after PR A, which third-party path comes first? A standalone
   `source: local` push sink is small and already in PR A's design. Stored
   totals plus the admin usage API (PR B) is bigger. Neither is needed for a provider.
5. **For us:** is a 60s flush interval right? It's a trade-off between
   dashboard freshness, the crash-loss window and request volume to the
   provider: 3 regions × replicas × 4 networks, one call a minute each.
