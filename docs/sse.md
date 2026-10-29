# Integrating with the `/events` SSE endpoint

BSV Broadcaster exposes a [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html) stream at `GET /events` that pushes a frame every time a tracked transaction changes status. This document covers the wire format, how to scope the stream to your own transactions, and how reconnection / replay works.

## Endpoint

```
GET /events
```

| Param | Where | Purpose |
|---|---|---|
| `callbackToken` | query string | Filters the stream to txids that were submitted under this token. Omit to receive every status update the server sees. |
| `Last-Event-ID` | request header | Nanosecond timestamp from a prior `id:` line. On reconnect, the server replays updates that occurred after this timestamp. **Only takes effect when `callbackToken` is also set** — without a token there is no way to scope replay to your txids. |

Response is `Content-Type: text/event-stream` and stays open until either side disconnects.

## Submitting transactions under a token

Pick any opaque string as your token (`crypto.randomUUID()` works) and send it as a header on every submit:

```bash
curl -X POST https://broadcaster.example.com/tx \
  -H "Content-Type: application/octet-stream" \
  -H "X-CallbackToken: my-session-abc123" \
  --data-binary @tx.bin
```

The server records a submission row keyed by `(txid, callbackToken)`. The same token then scopes the SSE stream:

```
GET /events?callbackToken=my-session-abc123
```

You may also set `X-CallbackUrl` to additionally receive status updates as outbound webhooks; that is independent of SSE.

## Frame format

```
id: 1745870456123456789
event: status
data: {"txid":"abc...","txStatus":"SEEN_ON_NETWORK","timestamp":"2026-04-28T18:20:56.123456789Z"}

```

- `id` is a Unix nanosecond timestamp. Clients should remember the most recent `id` and send it back as `Last-Event-ID` on reconnect (browser `EventSource` does this automatically).
- `event: status` is the only event type emitted for transaction updates.
- The first frame for a submitted txid is `RECEIVED`, published once intake has stored the transaction and before it is queued for propagation, so it always precedes the network's verdict; `ACCEPTED_BY_NETWORK`, `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES` and `MINED` follow as the network reports them (`PENDING_RETRY` and `REJECTED` are the side states). Open the stream before submitting to see the lifecycle from its first state. Resubmitting a `REJECTED` transaction re-broadcasts it without a new `RECEIVED` frame; the next frame is the network's new verdict, or none if it is rejected again.
- `data` is one JSON object per frame. Every frame carries `txid`, `txStatus`, and `timestamp` (RFC 3339, with fractional seconds when the event has sub-second precision — the same instant as `id`, so latency can be measured from the payload alone). Fractional seconds are optional in RFC 3339; a whole-second timestamp is still rendered as `2026-04-28T18:20:56Z`.
- A blank line terminates each frame.

### Mined frames carry the merkle proof

When `txStatus` is `MINED` (or `IMMUTABLE`), the frame additionally carries the block context and the transaction's merkle proof — the same fields the webhook callback and `GET /tx/{txid}` return, so a push-only client never has to poll for the proof:

```
id: 1745870512987654321
event: status
data: {"txid":"abc...","txStatus":"MINED","timestamp":"2026-04-28T18:21:52.987654321Z","blockHash":"0000...","blockHeight":870123,"merklePath":"<BUMP hex>"}

```

- `blockHash` / `blockHeight` — the block the transaction was mined into.
- `merklePath` — the transaction's minimal [BUMP](https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md) (Bitcoin Unified Merkle Path), hex-encoded. Verify it by recomputing the block merkle root from the txid.

These three fields are omitted from non-mined frames, so pre-`MINED` frames keep the original three-field shape. On a `Last-Event-ID` reconnect, replayed `MINED` frames are enriched with `merklePath` best-effort: to keep the replay path bounded, a single reconnect enriches at most a fixed number of distinct blocks, after which replayed `MINED` frames still carry `blockHash`/`blockHeight` but omit `merklePath` — recover it with `GET /tx/{txid}`.

### Rejected frames carry the reason

When `txStatus` is `REJECTED`, the frame carries the same rejection detail as `GET /tx/{txid}`:

```
data: {"txid":"abc...","txStatus":"REJECTED","timestamp":"2026-08-10T09:15:02Z","status":466,"extraInfo":"UTXO_SPENT (70): ... utxo already spent by tx 7dfb...[0]"}
```

- `extraInfo` — the verbatim Teranode validator line that caused the rejection (best available line when peers disagree).
- `status` — the ARC status code when the line maps to one (466 conflict/double-spend, 467 generic invalid, 476 not-final); omitted when no confident mapping exists.

Both fields are omitted when empty, so existing consumers see no shape change. Reorg-correction frames (see `MINED`/`SEEN_ON_NETWORK` re-anchoring) reuse `extraInfo` for the `reorg_reanchor` / `reorg_unmined` markers.

Every ~15 seconds the server emits a `: keepalive` comment frame so idle proxies don't kill the connection. Comment frames have no `event:` and should be ignored by clients.

## Browser (`EventSource`)

```js
const es = new EventSource(
  'https://broadcaster.example.com/events?callbackToken=my-session-abc123'
);

es.addEventListener('status', (ev) => {
  const { txid, txStatus, timestamp } = JSON.parse(ev.data);
  console.log(txid, '→', txStatus, '@', timestamp);
});

es.onerror = (err) => console.error('SSE error', err);
```

`EventSource` reconnects automatically and sends the most recent `id` it observed back as `Last-Event-ID`, so dropped connections replay missed updates without any extra code (provided the original request supplied a `callbackToken`).

`EventSource` does not allow custom headers, so authentication / token scoping must be in the URL — which is exactly what the `callbackToken` query string is for.

## Node.js / non-browser clients

`EventSource` is a browser API. From Node.js or other backends, either:

- use a polyfill like [`eventsource`](https://www.npmjs.com/package/eventsource) on npm, or
- read the response stream directly:

```ts
const res = await fetch('https://broadcaster.example.com/events?callbackToken=my-session-abc123', {
  headers: { 'Last-Event-ID': lastSeenID ?? '' },
});

const reader = res.body!.getReader();
const decoder = new TextDecoder();
let buf = '';
for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });
  let sep;
  while ((sep = buf.indexOf('\n\n')) !== -1) {
    const frame = buf.slice(0, sep);
    buf = buf.slice(sep + 2);
    handleFrame(frame); // parse `id:` / `event:` / `data:` lines
  }
}
```

When implementing manually, remember to:

- Track the most recent `id:` line and resend it as `Last-Event-ID` on reconnect.
- Reconnect with backoff on transport errors.
- Discard `: keepalive` comments.

## `curl` for sanity checks

```bash
curl -N "https://broadcaster.example.com/events?callbackToken=my-session-abc123"
```

`-N` disables curl's output buffering so you see frames live as they arrive.

## Reconnection and replay

The server only replays missed events when **both** of these are present on the reconnect:

- `?callbackToken=<token>` query string
- `Last-Event-ID: <ns-timestamp>` header

Replay walks the submissions registered under the token and emits the current status for each txid whose `timestamp` is later than `Last-Event-ID`. There is no replay for unfiltered (no-token) consumers — that mode is for live monitoring only.

Note that replay emits each txid's **current** status, not the sequence of transitions you missed. If a transaction went `ACCEPTED → SEEN_ON_NETWORK → MINED` while you were disconnected, you get one `MINED` frame, not three.

Slow consumers will drop frames: the fan-out is non-blocking, so if your client doesn't drain the connection fast enough, the server skips that update for that client rather than stalling. Reconnecting with `Last-Event-ID` is the recovery path.

### `event: gap` — the stream is discontiguous

Whenever the server knows it handed you an incomplete stream, it says so in-band rather than leaving you to assume you are whole:

```
event: gap
data: {"reason":"catchup_truncated","from":"1786493743374103486","count":10000,"action":"reconcile"}
```

| `reason` | Meaning |
|---|---|
| `catchup_truncated` | A reconnect replay hit the server's frame budget. Matching history after `from` was not sent. |
| `firehose_drop` | A no-token stream dropped a frame. No token-scoped replay can recover it, so this is the only recovery signal available to unfiltered consumers. |
| `replay_unavailable` | The store could not serve the replay. The connection continues with live frames only. |

`from` is the last nanosecond cursor you can trust; everything between it and the live stream may be missing. `action` is always `reconcile`: re-fetch the affected transactions with `GET /tx/{txid}`.

Treat a gap as an event to act on, not a warning to log. It is the difference between bounded, self-healing loss and silent loss — a stream that never emits `gap` is complete, and one that does tells you exactly where to look.

Unknown `event:` types must be ignored by conformant clients, so this frame is safe for existing consumers: it costs them nothing and they behave exactly as before.

## Failure modes

| Status | Body | Meaning |
|---|---|---|
| `503` | `{"error":"events stream not enabled"}` | The deployment didn't wire a publisher. Nothing to subscribe to. |
| `500` | `{"error":"streaming unsupported"}` | Response writer doesn't support flushing. Shouldn't happen behind a normal HTTP server. |

## End-to-end flow

1. Generate a `callbackToken` once per session (e.g. `crypto.randomUUID()`).
2. Open `GET /events?callbackToken=<token>` and start handling `event: status` frames.
3. Submit transactions via `POST /tx` (or `POST /txs`) with `X-CallbackToken: <token>`.
4. Each submitted txid will produce a sequence of status frames on the open stream, starting with `RECEIVED` and continuing as it propagates and gets mined.
5. On reconnect, your client (or `EventSource`) sends `Last-Event-ID` automatically, and the server fills in any updates you missed.
