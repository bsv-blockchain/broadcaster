# API keys and rate limits

BSV Broadcaster can authenticate clients with API keys and enforce per-tier quotas.
Everything here is off by default; deployments are unaffected until `auth.enabled` / `ratelimit.enabled` are set.

## For API clients

Send your key as a bearer token, the same header ARC clients already use:

```
Authorization: Bearer bsvb_…
```

Keys issued by a delegated-auth provider the deployment trusts (see
[`auth.introspection`](#delegated-authentication-authintrospection)) are sent
the same way.

Requests without a key are served at the anonymous tier, limited per client
IP by a bucket local to the replica that handled the request (not shared
across replicas). An IPv6 client is bucketed by its `/64`, not its exact
address, so rotating addresses within that block (privacy extensions, CGNAT)
shares one bucket rather than getting fresh quota on every rotation. An
invalid, revoked or expired key gets `401` — it never
falls back to anonymous. A client IP that racks up 60 invalid-key attempts in
a minute gets `429` (`X-RateLimit-Reason: auth_failures`) before the key is
even looked at, unless the request carries a key already proven good in that
replica's cache. If the store can't be reached to verify an uncached (or
fully expired) key, the response is `503` with `Retry-After` — retry.

Quotas are measured in cost units per minute:

| Request | Cost |
|---|---|
| `POST /tx` | `submit_tx` (default 10) |
| `POST /txs` with n transactions | `read` up front, then `n × submit_tx` once the batch is parsed |
| `GET /tx/:txid`, `/policy`, `/v1/policy`, block processing-status | `read` (default 1) |

For a keyed caller the quota window is a fixed UTC minute, shared globally
across replicas; the anonymous tier and the fail-open fallback below are
per-replica token buckets that refill continuously instead. Because of the
fixed window, a keyed caller can burst up to roughly 2× its per-minute limit
across a minute boundary, and a small amount of leased-but-unspent quota can
go undelivered if a replica goes idle mid-window.

A request that reaches a quota decision (allowed or denied by quota) carries
`RateLimit-Policy` / `RateLimit` (draft-ietf-httpapi-ratelimit-headers-11)
and `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
(seconds until the window resets, not an epoch timestamp). `Remaining` is
approximate for keyed callers (replicas lease quota in chunks, so it reports
local state, not the live global total). On `429`, wait `Retry-After`
seconds; `X-RateLimit-Reason` is `quota`, `concurrency`, or `auth_failures`.
A `429` for the concurrency cap, and the `auth_failures` throttle, carry
**no** quota headers — only `Retry-After` and `X-RateLimit-Reason`.

`413` means the request can never succeed as sent, not "try again later":

- A `/txs` batch above the tier's `max_batch` (`maxBatch` in the body).
- A `/txs` body larger than the tier's byte cap, `max_batch × 32 MiB`
  (at most 256 MiB). A body over the 256 MiB ceiling gets a plain `413`
  even with rate limiting off.
- A single request whose cost is above the tier's entire per-minute
  quota. For a tier with a configured `max_batch`, this can only happen
  through misconfiguration (checked at startup). A tier with
  `max_batch: 0` (unlimited batch size) has no such check, so a large
  enough `/txs` batch can still hit this at request time.

A disconnect is logged server-side as status `499`, at `Debug` (not `Warn` —
it's ordinary client behavior, not something this service got wrong); the
client sees nothing (the connection is just gone). This covers a disconnect
both while a request is being charged and while a key is still being
identified (an uncached key, or a stale cache entry mid-refresh, with no
fresher answer to fall back to). The latter also counts as `canceled`, not
`error`, in the `bsv_broadcaster_apikey_cache_total` metric — it's the caller giving
up, not the key store failing.

A tier with `units_per_minute: 0` (unbounded) never carries
`RateLimit-Policy` / `RateLimit` / `X-RateLimit-*` — there's no quota to
report, so the headers are omitted rather than sent with meaningless values.
`max_in_flight` still applies, and its `429`s still carry `Retry-After` and
`X-RateLimit-Reason: concurrency`.

`SSE /events`, the chaintracks endpoints, and `/metrics` are **not** guarded
by API keys or rate limits at all, regardless of `auth.enabled` /
`ratelimit.enabled`: `/events` is served by a separate SSE listener (default
port 8082, scoped instead by its own callback token), chaintracks is its own
server, and `/metrics` is exempted on the main API so scrapers never need a
key. Don't rely on either `auth.enabled` or `ratelimit.enabled` to protect
them — keep them off the public ingress if that matters for your
deployment.

## For operators

```yaml
api:
  trusted_platform: cloudflare   # only when every request arrives via Cloudflare
  trusted_proxies: []            # CIDRs/IPs allowed to set X-Forwarded-For
auth:
  enabled: true
admin:
  enabled: true
  host: 127.0.0.1                # default; kubectl port-forward works with this.
                                 # set 0.0.0.0 only to let in-cluster tooling reach it directly
  port: 8090                     # never route through the public ingress
  token: ""                      # set via BSV_BROADCASTER_ADMIN_TOKEN
ratelimit:
  enabled: true
  expected_replicas: 3
  anonymous: { units_per_minute: 50, max_in_flight: 5, max_batch: 1, retention_days: 30 }
  tiers:
    free:      { units_per_minute: 50,    max_in_flight: 5,   max_batch: 1,     retention_days: 30 }
    standard:  { units_per_minute: 3000,  max_in_flight: 20,  max_batch: 100,   retention_days: 90 }
    high:      { units_per_minute: 30000, max_in_flight: 100, max_batch: 1000,  retention_days: 365 }
    unlimited: { units_per_minute: 0,     max_in_flight: 500, max_batch: 10000, retention_days: 730 }
```

**Trust model — read this before setting either option.** `c.ClientIP()`
(the source of the anonymous tier's bucket key and of every request log's
`client_ip`) trusts `X-Forwarded-For` only from a peer listed in
`api.trusted_proxies` (CIDRs or bare IPs; empty by default, so the header is
never trusted). `api.trusted_platform: cloudflare` is a separate, broader
trust decision: it honors `CF-Connecting-IP` **from any peer**, regardless of
`trusted_proxies`. That is only safe when Cloudflare is the sole ingress path
to the pod (e.g. a Cloudflare Tunnel) — otherwise any caller can set that
header itself and pick its own anonymous-tier bucket or spoof `client_ip` in
logs.

- `units_per_minute: 0` means no quota (an unbounded tier); `max_in_flight` still
  caps concurrency per key, split across `expected_replicas`.
- Tier names are lowercase and must match `^[a-z0-9][a-z0-9_-]*$` (they're
  embedded in response headers and used as a metric label). `free` is
  required whenever `ratelimit.enabled` — it's the fallback for a key whose
  tier was removed from config, and admin key creation defaults new keys to
  it. `anonymous` is reserved; configure unkeyed callers under
  `ratelimit.anonymous` instead.
- Every tier must be payable: `costs.submit_tx` must fit inside
  `units_per_minute`, and `max_batch × costs.submit_tx + costs.read` must
  too (checked at config load; a tier that could never pay for its own
  batch cap fails startup, not the first request).
- `retention_days` is how long the transaction and submission rows a
  tier's callers write are kept; see [Data retention per tier](#data-retention-per-tier).
- Revocations and tier changes take effect within `key_cache_ttl_ms`
  (default 30s) while the key store is healthy. If the store is down, a
  key already resolved once keeps authenticating on its last known value
  for up to `key_cache_stale_max_ms` (default 10m); an uncached or fully
  expired key gets `503` instead.
- A downgrade applies within the current minute: units a replica already
  leased under the old, higher limit are discarded rather than spent, so a
  key that has already leased past its new limit is throttled until the
  next window.
- If the quota store is unavailable, keyed callers fail open to a
  per-replica bucket sized at the tier's own limit
  (`bsv_broadcaster_ratelimit_store_failopen_total`). A per-key breaker backs this:
  a reservation gets 2s to succeed, and after any real store error the
  breaker stays open for 10s (serving fail-open with no further store
  calls) before trying one half-open probe.
- Tiers must be declared in the config file; env vars can only override a
  field that's already present in a configured tier (e.g.
  `BSV_BROADCASTER_RATELIMIT_TIERS_PRO_UNITS_PER_MINUTE`) — they can add neither a
  new tier nor a new field on an existing one.
- With `admin.enabled`, `POST /api/v1/blocks/:blockHash/reprocess` moves to
  `POST /admin/v1/blocks/:blockHash/reprocess` on the admin listener, behind
  the admin token. With `admin.enabled: false` it stays public and
  unauthenticated, same as before this feature.
- Admin key management works independently of `auth.enabled` /
  `ratelimit.enabled` (it only needs a store that implements
  `APIKeyStore`), but a tier assigned to a key must still exist under
  `ratelimit.tiers` — populate that section even in an admin-only,
  quota-off deployment if you plan to issue keys on tiers other than `free`.

### Data retention per tier

Each tier, and `ratelimit.anonymous`, can set `retention_days`: how long the
rows written for its callers (the transaction row and the caller's
submission/callback row) are kept before the store deletes them. `0`, the
default, keeps them forever, so no deployment starts deleting data until a
tier opts in. Otherwise it is between 7 and 36500 days. The 7-day floor
outlives the default retry, parking and confirmation windows. A row deleted
while still in flight (say, a transaction stuck unmined past its
retention) is not an error, but its later status updates are dropped.

- The caller's tier decides: a keyed caller uses its key's tier (a tier
  missing from config falls back to `free`, as for rate limits), and an
  unkeyed caller, or any caller when `auth.enabled` is off, uses
  `ratelimit.anonymous`. Retention applies whether or not
  `ratelimit.enabled` is set.
- The expiry is stamped when a row is written: `now + retention_days`. A later
  tier change does not move it.
- **Resubmits extend, never shorten.** When a caller submits a transaction
  that is already stored, the row's expiry moves out to that caller's, so a
  transaction lives `retention_days` after the latest submission entitled to
  the most. To avoid a write on every resubmit (clients that poll by
  resubmitting), the row is only rewritten when the new expiry is later by
  more than a thirtieth of the retention, at most a day: a 7-day row by
  more than about 5.6 hours, a 90-day row by more than a day. So a row
  can expire up to that much earlier than "retention after my last
  submission".
- A keep-forever caller removes the expiry, and a row already kept forever
  never gains one.
- If the extension fails, `POST /tx` and `POST /txs` answer `503` with
  `Retry-After` instead of acknowledging the submission, and the client's
  retry repeats it (extending is idempotent). The failure is counted in
  `bsv_broadcaster_retention_extend_failures_total`. An intake rejection
  (`400`) only counts it.
- Each caller's submission row keeps its own expiry, normally no later than
  the transaction row's. It can be later only if an extension failed on the
  intake-rejection path; `GET /tx` then answers `404` for that txid once the
  transaction row is gone.
- **Set `ratelimit.anonymous.retention_days`.** Left at `0`, unkeyed callers
  keep their rows forever. Since keep-forever removes an expiry, anyone can
  resubmit a transaction's public raw bytes without a key and make its row
  permanent, whatever tier first submitted it.
- Rows written before retention was configured have no expiry and are
  kept forever.

**Backend support.** Only MongoDB enforces retention, through a TTL index on
`expires_at` for `transactions` and `submissions` (`tx_expires_ttl`,
`sub_expires_ttl`, `expireAfterSeconds: 0`, partial on the field existing).
MongoDB's TTL monitor deletes expired documents in the background (about
once a minute). On Postgres, Pebble and Aerospike, rows are kept forever and
the api-server logs a warning at boot when a tier configures retention.

The indexes are built only once retention is configured, by the
api-server in the background after boot (bounded by
`store.mongodb.index_timeout_ms`), so readiness does not wait on them.
Upgrading without configuring retention builds nothing. A failed or
timed-out build is logged, not fatal: rows are still stamped and expire once
the indexes exist. On a large `transactions` collection, create them ahead of the
deploy (same names, keys and options) so boot finds them in place:

```js
db.transactions.createIndex({expires_at: 1}, {name: "tx_expires_ttl", expireAfterSeconds: 0,
  partialFilterExpression: {expires_at: {$exists: true}}})
db.submissions.createIndex({expires_at: 1}, {name: "sub_expires_ttl", expireAfterSeconds: 0,
  partialFilterExpression: {expires_at: {$exists: true}}})
```

### Sharing one key set across deployments (`auth.store`)

By default keys (`api_keys`) and quota counters (`api_quota`) live in the main
store, so every BSV Broadcaster deployment has its own key set. When one service fronts
several deployments — e.g. one BSV Broadcaster per network (mainnet, testnet,
teratestnet, tstn), each with its own `store.mongodb` database and possibly on
different clusters, replicated across regions — point all of them at one
dedicated MongoDB database instead:

```yaml
auth:
  enabled: true                  # required when auth.store is set
  store:
    mongodb:
      uri: ""                    # set via BSV_BROADCASTER_AUTH_STORE_MONGODB_URI
      database: bsv_broadcaster_auth  # BSV_BROADCASTER_AUTH_STORE_MONGODB_DATABASE
      # Optional, same meaning as under store.mongodb; 0/unset = the values shown.
      # connect_timeout_ms: 10000
      # op_timeout_ms: 3000
      # query_timeout_ms: 8000
      # index_timeout_ms: 300000
      # min_pool_size: 2
      # max_conn_idle_ms: 300000
      max_pool_size: 16          # unset = driver default 100. Auth traffic is light
                                 # and every api-server pod of every network shares
                                 # one cluster, so keep this small.
```

Every key path is `auth.store.mongodb.*`, and each has an env override
(`BSV_BROADCASTER_AUTH_STORE_MONGODB_URI`, `…_DATABASE`, `…_OP_TIMEOUT_MS`, …) that
works even when the config file has no `auth.store` block. With `uri` unset
(the default) nothing changes: keys and quota stay in the main store. Setting
`uri` requires `database` and `auth.enabled`; a `database` without a `uri` is
rejected so a typo'd env var can't silently fall back to the main store.

Only the api-server (`--mode api-server` or `all`) opens it: the auth
middleware, the rate limiter and the admin key endpoints all use it in place
of the main store. Its main store is untouched and still holds the
transactions. The URI is never logged; the startup log names only the
database.

**Database privileges.** The auth store touches only `<database>.api_keys` and
`<database>.api_quota`: it never reads or creates any other collection or
GridFS bucket and never runs a database-wide command (it pings the `admin`
database, which needs no privileges). It therefore runs under a role scoped to
just those two collections, e.g. an Atlas custom role with, on **each** of
`<database>.api_keys` and `<database>.api_quota`:

`find`, `insert`, `update`, `remove`, `createIndex`, `listIndexes`, `createCollection`

and no database-level privileges. On boot it creates the same indexes the main
store creates on those collections, under the same names (`api_key_hash`
unique, `api_key_created`, and the `api_quota_expires_ttl` TTL index), so the
auth database may also be another deployment's main database (e.g. mainnet's)
and both provision it without conflict. (`collMod` is only needed if a future
release changes the `api_quota` TTL; this one doesn't.)

When several deployments share one auth store:

- **A key works on every network that shares it.** That is the point, but
  issue keys knowing a testnet key is also a mainnet key. Manage keys through
  any one deployment's admin listener; they all see the same set (within
  `key_cache_ttl_ms`).
- **Keep `ratelimit.tiers` identical everywhere.** The key stores only its
  tier name; each deployment applies its own config's limits for that name,
  and a tier missing from a deployment's config falls back to `free` there.
- **Quota is one window per key across all of them.** A key's per-minute
  budget is shared by every api-server replica of every network and region,
  not granted per network.
- **Set `expected_replicas` to the total** number of api-server replicas
  across all deployments sharing the store: `max_in_flight` is divided by it.
- **Size `lease_fraction` for that total.** Each replica leases
  `lease_fraction × units_per_minute` at a time until shared usage passes
  `shrink_threshold`; with N replicas each holding a lease, the window can be
  overshot by up to N leases before they shrink to exact cost. Keep
  `lease_fraction ≤ ~shrink_threshold / N` (e.g. 12 replicas with the default
  `shrink_threshold: 0.8` → `lease_fraction` of about `0.06` or less).
- **The auth store is on the request path** for uncached keys and quota
  leases. If it is unreachable the usual degradation applies: cached keys keep
  working up to `key_cache_stale_max_ms`, uncached keys get `503`, and quota
  fails open to a per-replica bucket. Host it where every region reaches it
  within `op_timeout_ms`.

### Delegated authentication (`auth.introspection`)

A deployment can delegate verification of some API keys to another service
(a "provider") that issues and manages them, such as a customer dashboard
or billing system. Each provider is configured with the prefix its keys
carry and an introspection endpoint. Keys under that prefix are verified
there and never reach the key store. Keys under `bsvb_` (the broadcaster's
own) always resolve from the key store, and a key with any other prefix gets
`401`.

```yaml
auth:
  enabled: true                       # required when auth.introspection is set
  introspection:
    acme:                             # provider name: logs, metrics, id namespace
      key_prefix: acme_
      url: https://auth.example.com/v1/introspect
      token_env: ACME_INTROSPECTION_TOKEN  # or token: <literal>; exactly one
      timeout_ms: 3000                # default 3000, max 4500
      tiers:                          # provider tier -> ratelimit.tiers name
        basic: free
        gold: pro
```

- `key_prefix` is lowercase alphanumerics ending in `_`, and must not overlap
  `bsvb_` or another provider's prefix. Provider keys must use the
  broadcaster's key format under it: the prefix, 43 base62 characters, `_`,
  then a 6-character base62 CRC32 of the body. Malformed keys are rejected
  before any network call.
- `url` must be `https` (plain `http` is accepted only on a loopback host):
  the service token travels with every call. It must not carry user info, a
  query or a fragment. Redirects are not followed.
- The service token is trimmed of surrounding whitespace (a secret mounted
  from a file usually ends in a newline) and must not contain any other
  whitespace or control characters.
- `tiers` maps the tier names the provider returns (matched
  case-insensitively) to tiers under `ratelimit.tiers`. A provider tier that
  is not mapped, or a missing tier, gets `free`, so a provider can only grant
  the tiers you map: it cannot claim an internal or unbounded tier by
  returning its name.

**Protocol.** For a key it has not seen (or whose cached answer is older than
`key_cache_ttl_ms`), the api-server sends:

```
POST <url>
Authorization: Bearer <service token>
Content-Type: application/json

{"key_hash": "<hex SHA-256 of the full key, prefix included>"}
```

The key itself never leaves the broadcaster. The provider answers:

| Response | Meaning |
|---|---|
| `200 {"valid": true, "key_id", "tier"?, "limit_id"?, "expires_at"?}` | Usable key. `key_id` identifies the caller. `tier` is mapped through `tiers`. `expires_at` is an RFC 3339 timestamp, or null/absent for no expiry, and is enforced on every request, even from cache. Other fields are ignored. |
| `200 {"valid": false}` | Unknown, revoked or out-of-scope key: `401`, cached for `key_cache_ttl_ms`. |
| `401` / `403` | The provider rejected the service token. |
| `429` / `5xx`, a timeout, any other status, or an answer that can't be decoded | The key can't be verified right now. |

Every failure other than `{"valid": false}` is treated as an outage, never
as an unknown key: cached keys keep working up to `key_cache_stale_max_ms`,
uncached keys get `503`, and the cause is logged (at most once per 10s per
kind).

**Ids are namespaced.** The broadcaster records a provider key as
`<provider>:<key_id>` (in logs, traces and quota accounting), and its
`limit_id` as `<provider>:<limit_id>`. Two providers, or a provider and a
local key, can never collide on an id.

**Shared allowances.** When the answer carries a `limit_id`, every key with
that `limit_id` shares one quota window, one `max_in_flight` budget and one
fail-open bucket, so a customer cannot raise their limits by creating more
keys. Without a `limit_id`, each key is its own allowance, like a `bsvb_` key.
The provider must return the same tier for every key on one `limit_id`.

**Backoff.** After a failed call the api-server stops calling that provider
for a while and fails fast instead:

| Failure | Backoff |
|---|---|
| `401` / `403`, or an unexpected status such as `404` or a redirect | 30s (a configuration fix is needed) |
| `429` | `Retry-After` (seconds or an HTTP date, capped at 60s), else 5s |
| `5xx` | `Retry-After` (capped at 60s), else 2s |
| timeout, connection error, undecodable answer | 2s |
| a valid answer without `key_id`, or an `expires_at` that isn't RFC 3339 | none: only that key gets `503` |

A backoff only ever extends. During it, cached keys keep working and uncached
keys get `503`. This keeps an outage or a bad token from sending every
request to the provider, which matters for providers that rate-limit per
source IP: a burst from one replica would otherwise throttle every replica
behind the same egress. Each replica also makes at most 16 concurrent calls
to one provider (a cap on calls, not just connections, so it holds over
HTTP/2); further lookups queue for a free slot.

Revoking a provider key takes effect within the provider's own propagation
delay plus `key_cache_ttl_ms`. The admin endpoints (`/admin/keys`) manage
only `bsvb_` keys.

Metric: `bsv_broadcaster_apikey_introspect_total{provider,result}`, where
`result` is `valid`, `invalid`, `unauthorized` (bad service token), `error`,
or `backoff` (not called because of an earlier failure).

### Managing keys

```bash
kubectl port-forward deploy/api-server 8090:8090
export BSV_BROADCASTER_ADMIN_TOKEN=…
bsv-broadcaster apikey create --name acme --tier high      # prints the key ONCE
bsv-broadcaster apikey list
bsv-broadcaster apikey set-tier key_… unlimited
bsv-broadcaster apikey revoke key_…
```

The admin JSON API is strict: unknown fields are rejected, request bodies
are capped at 64 KiB, `name` is capped at 256 characters, and field names
are camelCase (`name`, `tier`, `expiresAt`). Tier names are lowercased on
write. The CLI reads the token from `--admin-token`, then
`BSV_BROADCASTER_ADMIN_TOKEN`, then the pre-rebrand `ARCADE_ADMIN_TOKEN` —
the same precedence the server applies to `admin.token`, where an `ARCADE_*`
variable is honored only while its `BSV_BROADCASTER_*` twin is unset (see
[the migration notes](migration-arcade-to-bsv-broadcaster.md)). Prefer an
env var over the flag, since a flag value ends up in shell history.
`--expires-in` rejects a negative duration.
