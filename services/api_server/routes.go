package api_server

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RouteHeader documents a single HTTP request header for the API docs page.
// Requirement is one of "Required", "Recommended", "Optional" — these map
// directly onto pill styling in the rendered docs.
type RouteHeader struct {
	Name        string
	Requirement string
	Description string
}

// RouteBody documents one acceptable request body shape for a route. Routes
// with multiple bodies (e.g. POST /tx) render a content-type selector so
// clients only see one example at a time.
type RouteBody struct {
	ContentType string
	Description string
	Example     string
}

// RouteDoc describes a single API endpoint for the rendered docs page.
// Summary is the short tagline shown in the collapsed card header;
// Description is the long-form explanation revealed on expand.
type RouteDoc struct {
	Method         string
	Path           string
	Summary        string
	Description    string
	Headers        []RouteHeader
	RequestBodies  []RouteBody
	ResponseStatus string
	ResponseBody   string
	Notes          string
}

// docsPage is the template data root: the route list plus any page-level
// fields we want to expose later (e.g. version, environment).
type docsPage struct {
	Routes []RouteDoc
}

var callbackSubscriptionHeaders = []RouteHeader{
	{
		Name:        "X-CallbackUrl",
		Requirement: "Optional",
		Description: "Webhook URL BSV Broadcaster should POST status events to as this transaction progresses through the network. Must be a public HTTPS endpoint; private/loopback hosts are rejected unless the deployment is configured to allow them. Every POST carries User-Agent: bsv-broadcaster-webhook/<version> — allowlist the bsv-broadcaster-webhook/ prefix so edge bot rules don't reject deliveries; per-submission delivery health is inspectable via GET /tx/:txid?callbackToken=<token>.",
	},
	{
		Name:        "X-CallbackToken",
		Requirement: "Recommended",
		Description: "Opaque bearer token. Sent on every outbound webhook as Authorization: Bearer <token>, and used to scope Server-Sent Events streams when the client subscribes via SSE.",
	},
	{
		Name:        "X-FullStatusUpdates",
		Requirement: "Optional",
		Description: "Set to \"true\" to receive every status transition (RECEIVED, SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, MINED, IMMUTABLE). Default behavior delivers only terminal/notable transitions.",
	},
}

var routeDocs = []RouteDoc{
	{
		Method:         "GET",
		Path:           "/health",
		Summary:        "Liveness probe",
		Description:    "Reports liveness of the API server process and the upstream Datahub endpoints it depends on. Suitable as a Kubernetes liveness probe. blockHeight is this instance's processed chain-tip height — a freshness signal (datahub_urls[].healthy is reachability-only), letting clients detect a stale chain view and route around it. Each datahub_urls entry also carries the transaction policy that node advertised, so a policy rejection can be traced to the endpoint that enforced it; policy is absent for endpoints no node_status has announced.",
		ResponseStatus: "200 OK",
		ResponseBody:   "{\n  \"healthy\": true,\n  \"version\": \"v0.8.0\",\n  \"status\": \"ok\",\n  \"blockHeight\": 958779,\n  \"datahub_urls\": [\n    {\n      \"url\": \"https://...\",\n      \"source\": \"discovered\",\n      \"healthy\": true,\n      \"policy\": {\n        \"miningFee\": { \"satoshis\": 100, \"bytes\": 1000 },\n        \"maxtxsizepolicy\": 100000000,\n        \"maxscriptsizepolicy\": 500000,\n        \"maxtxsigopscountspolicy\": 4294967295\n      }\n    }\n  ]\n}",
	},
	{
		Method:         "GET",
		Path:           "/ready",
		Summary:        "Readiness probe",
		Description:    "Indicates that the process has finished start-up and is ready to accept traffic. Suitable as a Kubernetes readiness probe.",
		ResponseStatus: "200 OK",
		ResponseBody:   "{\n  \"status\": \"ready\"\n}",
	},
	{
		Method:         "GET",
		Path:           "/policy",
		Summary:        "Discover the current fee and size policy (ARC-compatible)",
		Description:    "Returns BSV Broadcaster's effective transaction policy in the ARC schema so clients can discover the fee rate and size limits before building transactions. Every value reflects exactly what intake enforces. When the operator hasn't pinned a fee, miningFee tracks the lowest fee the network will accept — the minimum advertised across peer node_status announcements — and BSV Broadcaster enforces that same floor at intake; it falls back to the built-in default until a peer is heard. Also served at /v1/policy.",
		ResponseStatus: "200 OK",
		ResponseBody:   "{\n  \"policy\": {\n    \"miningFee\": { \"satoshis\": 100, \"bytes\": 1000 },\n    \"maxtxsizepolicy\": 10485760,\n    \"maxscriptsizepolicy\": 500000,\n    \"maxtxsigopscountspolicy\": 4294967295,\n    \"standardFormatSupported\": false\n  },\n  \"timestamp\": \"2026-08-07T12:00:00Z\"\n}",
		Notes:          "maxtxsigopscountspolicy of 4294967295 means unlimited (the BSV post-Genesis default), matching the ARC convention. Always returns 200.",
	},
	{
		Method:      "GET",
		Path:        "/tx/:txid",
		Summary:     "Look up transaction status by txid",
		Description: "Returns the current lifecycle state of a previously submitted transaction, including its merkle path once mined. With ?callbackToken=<token> the response adds a callbacks array: the webhook delivery state (attempts, lastAttemptAt, lastResult, nextRetryAt) of that token's submissions — lastResult \"status 403\" with climbing attempts means BSV Broadcaster is POSTing and the receiver's edge is refusing.",
		Headers: []RouteHeader{
			{
				Name:        "Accept",
				Requirement: "Optional",
				Description: "application/json (default).",
			},
		},
		ResponseStatus: "200 OK",
		ResponseBody:   "{\n  \"txid\": \"<hex>\",\n  \"txStatus\": \"SEEN_ON_NETWORK\",\n  \"status\": \"SEEN_ON_NETWORK\",\n  \"timestamp\": \"2026-05-20T12:00:00Z\",\n  \"blockHash\": \"<hex|null>\",\n  \"blockHeight\": 870123,\n  \"merklePath\": \"<BUMP hex|null>\",\n  \"extraInfo\": \"\",\n  \"competingTxs\": [],\n  \"callbacks\": [\n    {\n      \"callbackUrl\": \"https://...\",\n      \"lastDeliveredStatus\": \"SEEN_ON_NETWORK\",\n      \"attempts\": 14,\n      \"lastAttemptAt\": \"2026-05-20T12:00:00Z\",\n      \"lastResult\": \"status 403\",\n      \"nextRetryAt\": \"2026-05-20T12:05:00Z\"\n    }\n  ]\n}",
		Notes:          "Returns 404 if the txid has never been submitted to this BSV Broadcaster instance. The callbacks array appears only with a matching ?callbackToken= and covers only that token's submissions.",
	},
	{
		Method:      "GET",
		Path:        "/events",
		Summary:     "Stream status updates over Server-Sent Events · separate service, default port 8082",
		Description: "Long-lived Server-Sent Events stream that pushes lifecycle updates for every transaction submitted under the supplied callback token. Hosted by BSV Broadcaster's standalone SSE service — a separate listener from the main API (default port 8082, fronted by its own Kubernetes Service in production). CORS is permissive on this endpoint so browsers can connect directly with the EventSource API.",
		Headers: []RouteHeader{
			{
				Name:        "Accept",
				Requirement: "Recommended",
				Description: "text/event-stream",
			},
			{
				Name:        "Last-Event-ID",
				Requirement: "Optional",
				Description: "Nanosecond timestamp returned by BSV Broadcaster as the id of the last event the client received. Triggers a catchup replay of every status update that occurred strictly after this timestamp. Omit on first connect to receive the current persisted status of every txid registered under the token.",
			},
		},
		RequestBodies: []RouteBody{
			{
				ContentType: "(query parameters)",
				Description: "?callbackToken=<token>   the same opaque token sent as X-CallbackToken when submitting transactions. Scopes the stream to that token's transactions.",
				Example:     "GET /events?callbackToken=my-token",
			},
			{
				ContentType: "(browser, EventSource)",
				Description: "Open a stream from JavaScript. Last-Event-ID is handled automatically by the browser on reconnect.",
				Example:     "const es = new EventSource(\n  \"https://broadcaster.example.com/events?callbackToken=my-token\"\n);\nes.addEventListener(\"status\", (e) => {\n  const status = JSON.parse(e.data);\n  console.log(status.txid, status.txStatus);\n});",
			},
			{
				ContentType: "(cli, curl)",
				Description: "Tail the stream from a shell.",
				Example:     "curl -N \"https://broadcaster.example.com/events?callbackToken=my-token\"",
			},
		},
		ResponseStatus: "200 OK · text/event-stream",
		ResponseBody:   "id: 1716205200123456789\nevent: status\ndata: {\"txid\":\"<hex>\",\"txStatus\":\"SEEN_ON_NETWORK\",\"timestamp\":\"2026-05-20T12:00:00Z\"}\n\nid: 1716205260987654321\nevent: status\ndata: {\"txid\":\"<hex>\",\"txStatus\":\"MINED\",\"timestamp\":\"2026-05-20T12:01:00Z\"}\n\n: keepalive\n\n",
		Notes:          "Lines prefixed with ':' are SSE comments — BSV Broadcaster sends a `: keepalive` comment every 15 seconds to hold the connection open through intermediaries. The 'id' field is the event timestamp in nanoseconds; clients reconnecting after a network blip should send it back as Last-Event-ID. The SSE service exposes its own /health and /ready endpoints on the same port. Possible txStatus values: RECEIVED, SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, MINED, IMMUTABLE, REJECTED.",
	},
	{
		Method:      "POST",
		Path:        "/tx",
		Summary:     "Submit a single transaction",
		Description: "Submits one Bitcoin SV transaction for validation, propagation, and lifecycle tracking. Synchronous policy validation runs before the 202 is returned; fee and script checks are delegated to the network. Pick the body encoding that matches your client — raw bytes are the most efficient.",
		Headers: append(
			[]RouteHeader{
				{
					Name:        "Content-Type",
					Requirement: "Required",
					Description: "One of application/octet-stream, text/plain (hex), or application/json — see the request body tabs below.",
				},
			},
			callbackSubscriptionHeaders...,
		),
		RequestBodies: []RouteBody{
			{
				ContentType: "application/octet-stream",
				Description: "Raw serialized transaction bytes. Most efficient — no encoding overhead. Supports Extended Format; the canonical txid is derived from the parsed transaction structure, not from a hash of the wire bytes.",
				Example:     "<binary raw transaction bytes>",
			},
			{
				ContentType: "text/plain",
				Description: "Hex-encoded serialized transaction as a UTF-8 string. Whitespace is trimmed before decoding.",
				Example:     "0100000001abc...def00000000",
			},
			{
				ContentType: "application/json",
				Description: "JSON envelope carrying the hex-encoded transaction. Useful for clients that cannot easily send a raw or text/plain body.",
				Example:     "{\n  \"rawTx\": \"0100000001abc...def00000000\"\n}",
			},
		},
		ResponseStatus: "202 Accepted",
		ResponseBody:   "{\n  \"txid\": \"<hex>\",\n  \"status\": 202,\n  \"txStatus\": \"RECEIVED\"\n}\n\n// Idempotent re-submit of a txid BSV Broadcaster has already seen (txStatus echoes the existing state):\n{\n  \"txid\": \"<hex>\",\n  \"status\": 202,\n  \"txStatus\": \"SEEN_ON_NETWORK\"\n}",
		Notes:          "Returns 400 with a reason field on validation failure (also persisted as a terminal REJECTED row, so subscribers see the outcome). This includes non-final transactions (nLockTime ahead of the chain's median-time-past with a non-final input sequence): the reason states when the tx becomes final — resubmit it then and it re-enters the broadcast pipeline. 503 with Retry-After: 1 is returned when the broker is under backpressure; the transaction was not queued and a retry is safe.",
	},
	{
		Method:      "POST",
		Path:        "/txs",
		Summary:     "Submit a batch of transactions",
		Description: "Submits a batch of concatenated raw transactions in a single HTTP request. Each transaction is parsed, validated, dedup-CAS'd, and published as part of one fan-out. Bodies are capped at 256 MiB.",
		Headers: append(
			[]RouteHeader{
				{
					Name:        "Content-Type",
					Requirement: "Required",
					Description: "application/octet-stream — the only encoding accepted for batches.",
				},
			},
			callbackSubscriptionHeaders...,
		),
		RequestBodies: []RouteBody{
			{
				ContentType: "application/octet-stream",
				Description: "Concatenated raw transaction bytes. No length prefixes or separators — each transaction is parsed sequentially using the stream parser.",
				Example:     "<binary raw tx 1><binary raw tx 2>...<binary raw tx N>",
			},
		},
		ResponseStatus: "202 Accepted",
		ResponseBody:   "{\n  \"submitted\": 42,\n  \"duplicates\": 3,\n  \"total\": 45\n}",
		Notes:          "If any transaction in the batch fails validation the whole call returns 400 with the offending txid and reason; the failed transaction is recorded as REJECTED.",
	},
	{
		Method:      "POST",
		Path:        "/api/v1/merkle-service/callback",
		Summary:     "Internal — receive callbacks from Merkle Service",
		Description: "Internal endpoint used by the Merkle Service to deliver SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES, STUMP and BLOCK_PROCESSED events. Bearer authentication is mandatory; the deployment refuses to start without a configured callback token.",
		Headers: []RouteHeader{
			{
				Name:        "Authorization",
				Requirement: "Required",
				Description: "Bearer <callback_token>. Compared in constant time against the configured token.",
			},
			{
				Name:        "Content-Type",
				Requirement: "Required",
				Description: "application/json",
			},
		},
		RequestBodies: []RouteBody{
			{
				ContentType: "application/json",
				Description: "CallbackMessage envelope; the type field discriminates the variant.",
				Example:     "{\n  \"type\": \"SEEN_ON_NETWORK\",\n  \"txid\": \"<hex>\",\n  \"txids\": [\"<hex>\", \"...\"],\n  \"blockHash\": \"<hex>\",\n  \"subtreeIndex\": 0,\n  \"stump\": \"<base64>\"\n}",
			},
		},
		ResponseStatus: "200 OK; 500 Internal Server Error",
		ResponseBody:   "200 OK:\n(empty body)\n\n500 Internal Server Error:\n{\n  \"error\": \"failed to store seen status\"\n}",
		Notes:          "Request bodies are capped (default 16 MiB) to bound memory cost of embedded STUMP blobs; oversize bodies return 413 Payload Too Large. A SEEN_ON_NETWORK or SEEN_MULTIPLE_NODES callback whose store write fails returns 500 so Merkle retries; a successful or duplicate callback stays 200. An unknown type is acknowledged with 200 and a warning: a 5xx would retry a message this build cannot apply.",
	},
	{
		Method:      "GET",
		Path:        "/api/v1/blocks/processing-status",
		Summary:     "List block processing milestones",
		Description: "Lists block processing milestones (header seen, BLOCK_PROCESSED received, compound BUMP built) in descending-height order. Paginates via a height-cursor; pass the lowest height returned from the previous page as before-height to fetch the next page.",
		Headers: []RouteHeader{
			{
				Name:        "Accept",
				Requirement: "Optional",
				Description: "application/json (default).",
			},
		},
		RequestBodies: []RouteBody{
			{
				ContentType: "(query parameters)",
				Description: "?limit=<int>   default 50, max 200.\n?before-height=<int>   cursor — pass the lowest height from the previous page.",
				Example:     "GET /api/v1/blocks/processing-status?limit=50&before-height=870000",
			},
		},
		ResponseStatus: "200 OK",
		ResponseBody:   "{\n  \"blocks\": [\n    {\n      \"blockHash\": \"<hex>\",\n      \"blockHeight\": 870123,\n      \"headerSeenAt\": \"2026-05-20T12:00:00Z\",\n      \"processedAt\": \"2026-05-20T12:00:30Z\",\n      \"bumpBuiltAt\": \"2026-05-20T12:00:45Z\",\n      \"status\": \"compound-bump-built\",\n      \"orphanedAt\": null,\n      \"hasBlockProcessed\": true,\n      \"hasCompoundBUMP\": true\n    }\n  ],\n  \"nextCursor\": 870000\n}",
	},
	{
		Method:         "GET",
		Path:           "/api/v1/blocks/processing-status/:blockHash",
		Summary:        "Get processing status for a single block",
		Description:    "Returns the block processing milestones for one block, looked up by block hash.",
		ResponseStatus: "200 OK",
		ResponseBody:   "{\n  \"blockHash\": \"<hex>\",\n  \"blockHeight\": 870123,\n  \"headerSeenAt\": \"2026-05-20T12:00:00Z\",\n  \"processedAt\": \"2026-05-20T12:00:30Z\",\n  \"bumpBuiltAt\": \"2026-05-20T12:00:45Z\",\n  \"status\": \"compound-bump-built\",\n  \"orphanedAt\": null,\n  \"hasBlockProcessed\": true,\n  \"hasCompoundBUMP\": true\n}",
		Notes:          "Returns 404 if BSV Broadcaster has no record of the block hash.",
	},
	{
		Method:         "POST",
		Path:           "/api/v1/blocks/:blockHash/reprocess",
		Summary:        "Ask Merkle Service to re-emit STUMP + BLOCK_PROCESSED",
		Description:    "Requests that the upstream Merkle Service re-emit STUMP and BLOCK_PROCESSED callbacks for the given block hash. Useful for healing a deployment whose callback stream missed a block.",
		ResponseStatus: "202 Accepted",
		ResponseBody:   "{\n  \"status\": \"accepted\",\n  \"blockHash\": \"<hex>\"\n}",
		Notes:          "Returns 503 if Merkle Service is not configured for this deployment. When admin.enabled, this route is served only on the admin listener as POST /admin/v1/blocks/{blockHash}/reprocess and requires the admin token.",
	},
}

func (s *Server) registerRoutes(r *gin.Engine) {
	r.GET("/", s.handleDocs)

	r.GET("/health", s.handleHealth)
	r.GET("/ready", s.handleReady)
	// Prometheus scrape endpoint. Mounted on the API server (in addition to
	// the per-pod health server) so api-server-only deployments still expose
	// metrics.
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Internal: authenticated by callback_token, never rate limited.
	r.POST("/api/v1/merkle-service/callback", s.handleCallback)

	// Public routes. guarded() adds API-key identification and the tier
	// quota when auth/ratelimit are enabled, charging the given cost units.
	read := s.cfg.RateLimit.Costs.Read
	r.GET("/api/v1/blocks/processing-status", s.guarded("v1/blocks/status", read, s.handleListBlockProcessingStatus)...)
	r.GET("/api/v1/blocks/processing-status/:blockHash", s.guarded("v1/blocks/status", read, s.handleGetBlockProcessingStatus)...)
	// With the admin listener enabled, reprocess is served only there
	// (admin.go), behind the admin token.
	if !s.cfg.Admin.Enabled {
		r.POST("/api/v1/blocks/:blockHash/reprocess", s.handleReprocessBlock)
	}
	r.GET("/tx/:txid", s.guarded("v1/tx/status", read, s.handleGetTransaction)...)
	r.POST("/tx", s.guarded("v1/tx", s.cfg.RateLimit.Costs.SubmitTx, s.handleSubmitTransaction)...)
	// /txs pays the read cost up front, then ChargeTxs(n × submit_tx) once
	// the batch is parsed (handleSubmitTransactions).
	r.POST("/txs", s.guarded("v1/txs", read, s.handleSubmitTransactions)...)
	// ARC-compatible policy discovery (issue #212). Bare /policy matches the
	// path clients 404'd on; /v1/policy is the alias used by the ARC spec.
	r.GET("/policy", s.guarded("v1/policy", read, s.handlePolicy)...)
	r.GET("/v1/policy", s.guarded("v1/policy", read, s.handlePolicy)...)
}
