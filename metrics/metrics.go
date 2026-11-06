// Package metrics defines the Prometheus metrics surface BSV Broadcaster exposes for
// scrape via the /metrics endpoint on the health server.
//
// Conventions
//
//   - Every metric is prefixed `bsv_broadcaster_` so a multi-tenant Prometheus can
//     filter on it cleanly.
//   - Counters end in `_total`. Histograms end in `_seconds` (latency) or
//     `_bytes` (sizes). Gauges end in a noun (e.g. `_depth`, `_count`).
//   - Labels are kept low-cardinality. Endpoint URLs are labeled because the
//     fleet is small (handful of datahubs); txids and Kafka offsets are
//     never used as labels.
//   - Buckets for latency histograms span 1ms..30s in coarse Fibonacci-ish
//     steps so a 1ms validate and a 30s reaper rebroadcast both land in
//     useful buckets.
//   - Buckets for size histograms span 1..10000 since that's the range we
//     see for batch sizes from a 1-tx single submit up to a 1000-tx flush.
//
// Service ownership
//
//   - propagation: batch size, broadcast latency per outcome, chunk count
//     and chunk payload bytes, dispatcher pending depth, deferred-requeue gauge, reaper lease and
//     tick outcomes, narrowed-reaper rebroadcast depth, merkle registration
//     latency.
//   - bump_builder: build duration, blocks processed, BUMP outcomes, STUMP
//     and grace-window stats.
//   - api_server: request latency by route + status, in-flight gauge.
//   - teranode (HTTP client): per-endpoint request latency by op + status,
//     endpoint health gauge.
//   - kafka: produce/consume/DLQ counters, message size histogram.
//   - p2p_client: node_status messages received, datahub URL discovery
//     outcomes.
//
// Most metrics live as package-level vars so any service can update them
// without plumbing a registry through. Tests use the default registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// labelOutcome is the conventional label name for counters that partition
// their measurements by a coarse success/failure-class enum. Centralized so
// every metric uses the same label key (and goconst stays quiet).
const labelOutcome = "outcome"

// Standard latency buckets for histograms measuring durations from very
// short (DB lookup, validate) up to long (reaper tick, bump build).
var latencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
}

// Standard size buckets for histograms measuring batch sizes.
var sizeBuckets = []float64{
	1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
}

// Standard byte-size buckets for HTTP / message payloads.
var bytesBuckets = []float64{
	256, 1024, 4096, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024,
}

// ---------------------------------------------------------------------------
// propagation
// ---------------------------------------------------------------------------

// PropagationBatchSize measures how many txs landed in each processBatch call
// — i.e. the size at the entrypoint to the broadcast pipeline.
var PropagationBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_propagation_batch_size",
	Help:    "Number of txs in each processBatch call.",
	Buckets: sizeBuckets,
})

// PropagationBroadcastConsensus counts broadcasts by network-level outcome.
// "unanimous_reject" means every endpoint that responded returned non-2xx —
// the network agrees the tx is bad, so the slow-track breaker is NOT
// charged against the responding peers (they're behaving correctly). This
// metric is the diagnostic for the resilience tunable: if it's growing
// quickly, the tx generator is producing rejectable txs (double-spends,
// invalid signatures, insufficient fees, …) — not a peer-health problem.
// A peer that refused the chunk by shape (PropagationChunkTotal
// {fallback="size_rejected"}) cast no verdict and is excluded: a broadcast
// every responder refused by shape counts under no verdict label at all.
var PropagationBroadcastConsensus = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_broadcast_consensus_total",
	Help: "Per-broadcast consensus outcome across all responding endpoints.",
}, []string{"verdict"}) // accepted, unanimous_reject, mixed, unreachable

// PropagationPendingDepth gauges how many propagationMsgs the dep-aware
// dispatchers are currently holding in their pendingMsgs accumulators
// awaiting the next flush, summed across this pod's partition
// dispatchers (#295: one dispatcher per assigned partition claim; each
// delta-adds its own contribution and removes it on claim teardown).
// Sustained growth indicates downstream (teranode broadcast or
// merkle-service register) is not keeping up with ingest.
var PropagationPendingDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_propagation_pending_depth",
	Help: "Propagation messages buffered awaiting flush, summed across this pod's partition dispatchers.",
})

// PropagationPendingRequeues gauges how many delayed-requeue goroutines
// are currently parked waiting for their flat requeueDelay to elapse
// before pushing back onto the dispatcher. Each Teranode infra-failure
// (no peer reachable, parseable 500, per-slot PROCESSING) drives a
// new requeue, so a sustained high value points to upstream pressure
// — pair it with TeranodeEndpointHealth to confirm. Inc'd on entry,
// Dec'd via defer regardless of whether the goroutine exits via timer
// or ctx.Done.
var PropagationPendingRequeues = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_propagation_pending_requeues",
	Help: "Number of requeueAfterDelay goroutines currently awaiting their delay before re-admitting messages.",
})

// PropagationInflightBatches gauges how many flushBatch goroutines are
// currently running their register+broadcast pipeline. Capped at
// PropagationConfig.MaxConcurrentBatches; sustained saturation means the
// pipeline cannot keep up with the kafka drain rate and pendingMsgs will
// grow until backpressure forces the consumer to block.
var PropagationInflightBatches = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_propagation_inflight_batches",
	Help: "Number of propagation batches currently mid-pipeline.",
})

// PropagationBroadcastDuration measures end-to-end wall time of broadcasting
// a single chunk to all healthy endpoints (the inner /tx or /txs path).
var PropagationBroadcastDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_propagation_broadcast_duration_seconds",
	Help:    "Wall time of one chunk broadcast across all healthy endpoints.",
	Buckets: latencyBuckets,
}, []string{"path"}) // batch, single

// PropagationOutcomeTotal counts per-tx outcomes from the propagation step.
var PropagationOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_outcome_total",
	Help: "Per-tx propagation outcome counts.",
}, []string{labelOutcome}) // accepted, rejected, retryable, no_verdict

// PropagationChunkTotal counts how many chunk broadcasts were issued. Combined
// with PropagationBatchSize this surfaces whether teranode_max_batch_size and
// teranode_max_batch_bytes are well-tuned. "none" is one POST as planned;
// "narrowed" is a re-split after a peer response that could not be bound to
// individual transactions; "size_rejected" is a chunk a peer refused by
// shape (too many txs, too many bytes, or a 413) — a non-zero rate means the
// configured caps are above what some peer accepts.
var PropagationChunkTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_chunk_total",
	Help: "Number of chunk broadcasts issued, by fallback decision.",
}, []string{"fallback"}) // none, narrowed, size_rejected

// chunkBytesBuckets diverges from bytesBuckets on purpose: the shared set
// jumps 4 MiB → 16 MiB → 64 MiB, which cannot tell an 8 MiB chunk from one
// pinned just under the 16 MiB default of propagation.teranode_max_batch_bytes
// — and that distinction is the whole tuning question. These add resolution
// across 1–32 MiB, the band between BSV Broadcaster's default and Teranode's hard
// 32 MiB maxDataPerRequest.
var chunkBytesBuckets = []float64{
	4 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024,
	8 * 1024 * 1024, 12 * 1024 * 1024, 16 * 1024 * 1024, 24 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024,
}

// PropagationChunkBytes measures the payload of each POST /txs chunk — the
// concatenated raw tx bytes, which is exactly the request Content-Length.
// Read next to propagation.teranode_max_batch_bytes: a p99 pinned just under
// the cap means bytes, not teranode_max_batch_size, decide where chunks end.
// Observed per POST, including the halves produced by narrowing.
var PropagationChunkBytes = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_propagation_chunk_bytes",
	Help:    "Payload bytes (concatenated raw txs) of each POST /txs chunk broadcast.",
	Buckets: chunkBytesBuckets,
})

// PropagationMerkleRegisterDuration measures the merkle-service registration
// wall time for one flushBatch — a single bounded-concurrency fan-out over
// every tx in the batch, observed once per batch. Slow merkle calls are a
// common bottleneck; under burst ingest this histogram is the canonical
// p50/p99 signal.
var PropagationMerkleRegisterDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_propagation_merkle_register_duration_seconds",
	Help:    "Wall time of one batched merkle-service registration fan-out.",
	Buckets: latencyBuckets,
})

// PropagationMerkleRegisterFailures counts per-tx merkle-service registration
// failures by reason. Sustained values indicate the merkle service is
// unhealthy. The label set is closed and bounded: claim_revoked (not a
// real registration failure — the Kafka claim died), auth_error (401/403),
// http_5xx, timeout, transport, and register_error (any other /watch
// failure, including other 4xx). Do not add a txid label.
var PropagationMerkleRegisterFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_merkle_register_failures_total",
	Help: "Per-tx merkle-service Register failures, by reason (claim_revoked, auth_error, http_5xx, timeout, transport, register_error).",
}, []string{"reason"})

// PropagationMerkleRegisterBatchOutcomeTotal counts each flushBatch's merkle
// registration result. "fully_ok" = every tx registered, "partial" = some
// succeeded and some routed to PENDING_RETRY, "all_failed" = nothing
// broadcast this flush. Lets dashboards distinguish a one-off RTT blip
// (single "partial" tick) from a sustained outage (rising "all_failed"
// rate) — a signal the per-tx failure counter alone obscures.
var PropagationMerkleRegisterBatchOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_merkle_register_batch_outcome_total",
	Help: "Per-batch merkle-service registration outcome.",
}, []string{labelOutcome}) // fully_ok, partial, all_failed

// MerkleEndpointRequestsTotal counts every request the merkle-service pool
// issued to (or skipped for) one endpoint, by operation and outcome. The
// existing pool-level merkle metrics answer "did the tx get registered";
// this one answers "which endpoint is failing". op is watch, catchup
// (re-sending a registration the endpoint missed), reprocess or probe; outcome is ok, err_auth, err_4xx, err_5xx, err_network, canceled or
// skipped_open (breaker open, request not sent). The endpoint label is the
// configured base URL, so cardinality is bounded by the config.
var MerkleEndpointRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_merkle_endpoint_requests_total",
	Help: "Per-endpoint merkle-service request outcomes by operation.",
}, []string{"endpoint", "op", labelOutcome})

// MerkleEndpointCatchupPending is how many registrations the merkle-service
// pool still owes an endpoint: txs some other endpoint accepted that this
// one refused, timed out on, or was skipped for while its breaker was open.
// The pool re-sends them in the background; a value that never returns to 0
// means the endpoint keeps refusing /watch.
var MerkleEndpointCatchupPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_merkle_endpoint_catchup_pending",
	Help: "Registrations queued for re-delivery to a merkle-service endpoint that missed them.",
}, []string{"endpoint"})

// MerkleEndpointHealthy is 1 while the endpoint's circuit breaker is closed
// (requests are sent to it) and 0 while it is open (skipped until the
// background /health probe answers). Each process runs its own pool, and
// ones that never send /watch stay at 1, so alert on min by (endpoint) == 0
// rather than on a sum across pods.
var MerkleEndpointHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_merkle_endpoint_healthy",
	Help: "1 while the merkle-service endpoint's circuit breaker is closed, 0 while open.",
}, []string{"endpoint"})

// PropagationReaperLease is 1 when this pod holds the reaper lease, 0 otherwise.
// In K8s, sum across pods should always equal 1 (or 0 during failover).
var PropagationReaperLease = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_propagation_reaper_lease_held",
	Help: "1 if this pod holds the reaper lease, 0 otherwise.",
})

// PropagationReaperTickTotal tracks reaper ticks by outcome.
var PropagationReaperTickTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_reaper_tick_total",
	Help: "Reaper tick outcomes.",
}, []string{labelOutcome}) // ran, skipped_no_leader, lease_error

// PropagationReaperReadyDepth is the count of stale SEEN_ON_NETWORK /
// SEEN_MULTIPLE_NODES rows the last reaper tick observed as candidates
// for rebroadcast. Set on every tick (including to 0 when the queue
// clears) so dashboards reflect current state, not the last non-zero
// scan. Sustained high values indicate a struggling downstream
// (datahubs flapping, merkle slow) blocking SEEN_ON_NETWORK txs from
// reaching ACCEPTED.
var PropagationReaperReadyDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_propagation_reaper_ready_depth",
	Help: "Number of stale SEEN_ON_NETWORK rows ready for rebroadcast at the last reaper tick.",
})

// PropagationClaimRevokedBatchesTotal counts processBatch pipelines aborted
// because their Kafka claim context was already canceled — the broker revoked
// the partition claim mid-batch (consumer-group rebalance) or the service is
// shutting down. Aborting is the durable path: nothing in the batch has been
// marked on the revoked claim's offset tracker, so every tx replays under the
// next claim. An occasional tick during a rolling restart is normal; a
// sustained rate means macro-batch cycles are overrunning the consumer-group
// session timeout and the group is thrashing.
var PropagationClaimRevokedBatchesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_claim_revoked_batches_total",
	Help: "Propagation batches aborted mid-pipeline because the Kafka claim was revoked; their txs stay uncommitted for replay.",
})

// PropagationInflightDepth gauges the dispatchers' full in-flight census:
// every tx admitted from Kafka that has not yet reached a terminal verdict —
// pending flush, mid-broadcast, held behind an in-flight parent, or parked in
// a delayed requeue — summed across this pod's partition dispatchers
// (#295 delta accounting, same as PropagationPendingDepth). Unlike
// PropagationPendingDepth, which admission backpressure caps at
// max_pending per dispatcher, this gauge is uncapped, so it is the one
// that shows a real backlog growing. Each in-flight entry pins its Kafka
// offset below its partition's commit watermark, so this depth also
// approximates the pod's uncommitted-offset backlog.
var PropagationInflightDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_propagation_inflight_depth",
	Help: "Transactions in the in-flight set (admitted but not yet terminal), summed across this pod's partition dispatchers; each pins an uncommitted Kafka offset.",
})

// PropagationConflictAttributionTotal counts conflict-family ("alien") Teranode
// failure lines by whether BSV Broadcaster could resolve them back to a submitted tx.
//
// Teranode renders the conflict family (UTXO_SPENT / TX_CONFLICTING /
// TX_INVALID_DOUBLE_SPEND, HTTP 409) as the deepest public cause, which names
// the SPENT OUTPOINT and the competing spender — never the submitted txid — so
// the line keys under a hash that is not in the batch. BSV Broadcaster holds the
// submitted raw transactions, so it can usually match "<outpoint>:<vout>"
// against a submitted tx's inputs and attribute the verdict (result=attributed
// → terminal REJECTED). result=unattributable means the outpoint matched no
// submitted input, or matched more than one (an intra-batch double spend), so
// the verdict is deliberately NOT guessed: implicit accepts stay withheld and
// the affected txs fall to the bounded requeue.
//
// A sustained unattributable rate is the signal that Teranode changed the
// conflict-line format (or is reporting on outpoints BSV Broadcaster never submitted) —
// exactly the shape that wedged the dev-ovh-1 propagation consumer on
// 2026-08-11 at 337,247 frozen messages of Kafka lag.
var PropagationConflictAttributionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_conflict_attribution_total",
	Help: "Conflict-family Teranode failure lines by whether the spent outpoint resolved to a submitted transaction.",
}, []string{"result"}) // attributed, unattributable

// PropagationMissingParentTotal counts Teranode missing-parent responses
// (TX_MISSING_PARENT / TX_NOT_FOUND) by how BSV Broadcaster resolved them. With a
// multi-partition bsv-broadcaster.propagation topic (#295) a child submitted in a
// later request than its parent can reach Teranode first; that condition is
// retryable, never a verdict on its own (#254).
//
// outcome="requeued": the parent wasn't terminally REJECTED in BSV Broadcaster's own
// store, so the child went back through the bounded requeue (and, on budget
// exhaustion, PENDING_RETRY + reaper). A short burst is expected during a
// partition-count change (key remap window); a sustained rate at steady
// state means family keying is not co-locating same-submission chains —
// check the intake path. outcome="rejected_ancestor": BSV Broadcaster's store showed
// a parent terminally REJECTED, so the child inherited REJECTED via the
// standard cascade reason — the cross-partition analog of the
// dispatcher's same-partition cascade.
var PropagationMissingParentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_missing_parent_total",
	Help: "Teranode missing-parent responses by resolution (requeued vs durable reaper retry vs rejected via a REJECTED ancestor in BSV Broadcaster's store).",
}, []string{"outcome"}) // requeued, reaper_retry, rejected_ancestor

// PropagationHydrateDroppedTotal counts propagation messages the consumer
// dropped because it could not load their raw transaction from the store
// (#329: messages carry the txid only). reason="row_missing": no row for the
// txid. reason="terminal": the row is terminal and its raw_tx was cleared —
// redelivered work that is already done, expected. reason="raw_tx_missing":
// a non-terminal row with no raw_tx; intake always persists raw_tx before
// publishing, so this should stay at zero. reason="lookup_failed": the batch
// read kept failing and this message's own read failed while others
// succeeded; the reaper rebroadcasts it. reason="malformed": the message did
// not decode or carried no txid.
var PropagationHydrateDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_hydrate_dropped_total",
	Help: "Propagation messages dropped because their raw transaction could not be loaded from the store, by reason.",
}, []string{"reason"}) // row_missing, terminal, raw_tx_missing, lookup_failed, malformed

// PropagationHydrateErrorsTotal counts failed store reads while hydrating
// propagation messages. The consumer retries with backoff and does not
// advance past the batch, so a sustained rate means propagation is stalled
// on the store.
var PropagationHydrateErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_hydrate_errors_total",
	Help: "Failed store reads while loading raw transactions for propagation messages.",
})

// PropagationRequeueExhaustedTotal counts transactions parked at PENDING_RETRY
// because they burned their whole in-memory NETWORK requeue budget
// (propagation.retry_max_attempts) without ever producing a network verdict.
// Merkle /watch exhaustion is PropagationMerkleRetryExhaustedTotal, not this
// series — a registration outage must not page as "no network verdict".
//
// This is the poison-batch escape hatch. Before it existed a batch that
// permanently failed with no attributable per-tx verdict requeued forever:
// every tx stayed in the dispatcher's inFlight map, so its Kafka offset kept
// pinning LowestUnfinished() and the single-partition bsv-broadcaster.propagation topic
// could not commit. Observed live on dev-ovh-1 (2026-08-11, BSV Broadcaster v0.11.5):
// 337,247 messages of lag frozen for minutes, 4,679 txs in-flight, ~2,410
// UTXO_SPENT 409s in two minutes across 43 batches of which 42 requeued, while
// the leader pod burned 4.2 cores achieving nothing.
//
// Any non-zero rate here is worth an alert: it means transactions are getting
// no verdict from any peer. They are NOT lost — the row keeps its raw bytes and
// the reaper rebroadcasts PENDING_RETRY rows — but the fast path has given up.
var PropagationRequeueExhaustedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_requeue_exhausted_total",
	Help: "Transactions parked at PENDING_RETRY after exhausting the in-memory network requeue budget with no network verdict.",
})

// PropagationRetryTotal counts fast-path retry decisions by stage.
// stage=merkle is a Merkle /watch requeue and is not a network propagation
// attempt. stage=network is a Teranode/network requeue only. The label set
// is closed (two values). Txids must not be added as labels.
var PropagationRetryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_retry_total",
	Help: "Fast-path retry decisions by stage (merkle registration vs network propagation).",
}, []string{"stage"})

// PropagationMerkleRetryExhaustedTotal counts transactions parked at
// PENDING_RETRY after exhausting fast-path Merkle /watch retries. The
// network propagation budget is not consumed, and the tx is not REJECTED.
var PropagationMerkleRetryExhaustedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_merkle_retry_exhausted_total",
	Help: "Transactions parked at PENDING_RETRY after exhausting fast-path Merkle /watch retries. The network propagation budget is not consumed.",
})

// PropagationPendingRetryTotal counts durable-retry lifecycle events for
// transactions parked at PENDING_RETRY.
//
// outcome="scheduled": a parked tx was booked for another reaper attempt with
// an exponential-backoff next_retry_at. outcome="exhausted": it burned
// propagation.pending_retry_max_attempts and was terminalized REJECTED
// because a parent it spends never reached the network.
//
// A rising "exhausted" rate means clients are submitting transactions whose
// parents BSV Broadcaster never sees — before #299 those rows were not terminalized at
// all: their timestamp_at never moved, so at 24h they fell out of the reaper's
// scan window and stayed PENDING_RETRY permanently, invisible to every gauge.
var PropagationPendingRetryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_propagation_pending_retry_total",
	Help: "Durable-retry lifecycle events for PENDING_RETRY transactions (scheduled for another attempt vs terminalized after exhausting the budget).",
}, []string{"outcome"}) // scheduled, exhausted

// PropagationParkedDepth is the number of transactions currently parked at
// PENDING_RETRY, and PropagationParkedOldestAgeSeconds the age of the oldest.
//
// PENDING_RETRY is deliberately absent from the stuck-transient census
// (reaper.go stuckTransientStatuses tracks RECEIVED and ACCEPTED_BY_NETWORK
// only), and reaper_ready_depth saturates at the per-tick rebroadcast cap, so
// before these existed the parked set was both unbounded and unmeasurable —
// while #299 makes parking an expected steady state for cross-submission
// chains rather than a poison-batch rarity.
var (
	PropagationParkedDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bsv_broadcaster_propagation_parked_depth",
		Help: "Transactions currently parked at PENDING_RETRY awaiting durable reaper rebroadcast.",
	})
	PropagationParkedOldestAgeSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bsv_broadcaster_propagation_parked_oldest_age_seconds",
		Help: "Age of the oldest transaction parked at PENDING_RETRY.",
	})
)

// APITxsSubmittedTotal counts individual transactions submitted through the
// API, by route and dedup result. Unlike the HTTP request histogram (one
// sample per request), this counts PER TRANSACTION — the /txs batch route
// submits many txs in one request, so request counts undercount submissions
// by the batch factor. result=new is the "unique txids submitted" series
// dashboards should use; duplicate = idempotent resubmit of a known txid;
// retry_rejected = resubmit of a previously rejected txid (re-enters the
// broadcast pipeline).
var APITxsSubmittedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_api_txs_submitted_total",
	Help: "Transactions submitted via the API by route and dedup result (new|duplicate|retry_rejected). Counted per transaction, not per request.",
}, []string{"route", "result"})

// APIFinalityRejectionsTotal counts transactions rejected at intake by the
// nLockTime/BIP113 finality gate, by route. These are terminal REJECTED
// verdicts with an actionable reason; the submitter is expected to resubmit
// once the lock expires (issue #245).
var APIFinalityRejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_api_finality_rejections_total",
	Help: "Transactions rejected at intake as non-final (nLockTime/BIP113), by route.",
}, []string{"route"})

// APIFinalityPrecheckUnavailableTotal counts finality pre-checks skipped
// because chain state (tip/headers via chaintracks) was unavailable. The
// gate fails open — teranode remains the authority — so a sustained rate
// here means non-final txs are once again bouncing off the network with
// generic errors instead of being caught at intake.
var APIFinalityPrecheckUnavailableTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_api_finality_precheck_unavailable_total",
	Help: "Finality pre-checks skipped (failed open) because chain state was unavailable.",
})

// StuckTransientTxs counts transactions sitting in a transient status
// (RECEIVED, ACCEPTED_BY_NETWORK) for longer than the stale-transient
// threshold (1h), within the reaper's 24h scan lookback. Unlike
// PropagationReaperReadyDepth it is uncapped, split per status, and covers
// ACCEPTED_BY_NETWORK (which the rebroadcast path deliberately ignores).
// Computed on the leader's reaper tick; non-leaders report 0, so aggregate
// with max() across pods. A sustained non-zero value means the SEEN
// state-transfer for those txs never arrived — the primary alerting signal
// for a stalled subtree-callback pipeline.
var StuckTransientTxs = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_stuck_transient_txs",
	Help: "Transactions stuck in a transient status for over 1h (24h lookback), by status. Leader-computed; aggregate with max().",
}, []string{"status"})

// OldestTransientTxAge reports the age in seconds of the oldest transaction
// currently stuck in each transient status (0 when none). Same computation
// cadence and aggregation semantics as StuckTransientTxs.
var OldestTransientTxAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_oldest_transient_tx_age_seconds",
	Help: "Age of the oldest transaction stuck in each transient status (seconds; 0 when none). Leader-computed; aggregate with max().",
}, []string{"status"})

// StuckTransientTxsByCallback breaks the StuckTransientTxs census down by the
// callback host of the submission(s) that registered each stuck tx, so an
// operator can see WHICH client's transactions are stuck (e.g. one overlay
// service dominating the ACCEPTED_BY_NETWORK backlog) rather than just the
// aggregate. callback_host is the hostname of the submission's CallbackURL, or
// "none" for txs submitted without a callback. Leader-computed and fully reset
// each tick (so a host that drains stops reporting); aggregate with max() and
// sum by (callback_host). Attribution is capped per tick
// (stuckAttributionCap) to bound the per-tick submission lookups — the
// uncapped totals stay on StuckTransientTxs.
var StuckTransientTxsByCallback = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_stuck_transient_txs_by_callback",
	Help: "Stuck transient txs (see bsv_broadcaster_stuck_transient_txs) attributed to the submitting client's callback host. Leader-computed, reset each tick; aggregate with max().",
}, []string{"status", "callback_host"})

// ---------------------------------------------------------------------------
// bump_builder
// ---------------------------------------------------------------------------

// BumpBuilderBuildDuration measures end-to-end wall time from BLOCK_PROCESSED
// receipt to terminal disposition — including the grace-window wait when one
// was needed (completeness-first ordering skips the wait entirely when
// merkle's expected-STUMP set is already satisfied on arrival).
//
// Every terminal disposition of handleMessage lands here exactly once. Two
// outcomes mean a BUMP was built and persisted: `finalized_complete_no_grace`
// (expected-STUMP set complete on the first read — no grace wait) and
// `grace_waited` (built via the grace-window path; also stamped when the
// window is configured to 0). Three more are benign non-failures:
// `short_circuited` (a BUMP already existed and the redelivery added no
// leaves, so the rebuild was skipped), `rebuilt` (a redelivery from another
// merkle-service carried leaves the stored BUMP lacked, so the compound was
// rebuilt), `no_stumps` (block contains no tracked txs) and
// `context_canceled` (shutdown). The rest are failures:
//
//	parse_failed, deferred_incomplete, fetch_failed, no_subtrees,
//	build_failed, validation_failed, store_failed
//
// Alert by matching the failure outcomes positively — outcome=~"parse_failed|
// deferred_incomplete|fetch_failed|no_subtrees|build_failed|validation_failed|
// store_failed" (the list above). Do NOT negate the benign labels (e.g.
// outcome!~"finalized_complete_no_grace|grace_waited"): a negation also
// matches short_circuited/no_stumps/context_canceled, and it silently starts
// matching any new benign outcome someone adds. Keep this list and the rule
// in README.md in sync when adding an outcome.
var BumpBuilderBuildDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_bump_builder_build_duration_seconds",
	Help:    "Time to build and persist one compound BUMP, by outcome.",
	Buckets: latencyBuckets,
}, []string{labelOutcome})

// BumpBuilderBlocksProcessedTotal counts BLOCK_PROCESSED messages handled.
var BumpBuilderBlocksProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_blocks_processed_total",
	Help: "BLOCK_PROCESSED messages handled by bump-builder.",
})

// BumpBuilderStumpCount is the histogram of how many STUMPs each block had.
// Useful for spotting blocks with unusual tracking patterns.
var BumpBuilderStumpCount = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_bump_builder_stump_count",
	Help:    "Number of STUMPs per block at BUMP construction time.",
	Buckets: sizeBuckets,
})

// BumpBuilderTxidsMined counts the txs marked MINED across all builds.
var BumpBuilderTxidsMinedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_txids_mined_total",
	Help: "Tracked transactions marked MINED via BUMP construction.",
})

// BumpBuilderDatahubFetchDuration measures the round-trip to the datahub for
// subtree hashes + coinbase BUMP + header merkle root.
var BumpBuilderDatahubFetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_bump_builder_datahub_fetch_seconds",
	Help:    "Datahub fetch latency for block data needed by BUMP construction.",
	Buckets: latencyBuckets,
})

// BumpBuilderBlockDataSourceTotal counts which source supplied the subtree
// hashes + coinbase BUMP + header merkle root used to build a compound BUMP:
// "callback" when merkle-service enriched BLOCK_PROCESSED (datahub-independent
// path, issue #195) or "datahub" when bump-builder fell back to fetching the
// block. A healthy fully-rolled-out deployment trends to source="callback"; a
// sustained source="datahub" rate signals an un-upgraded merkle-service or
// blocks whose enrichment fields couldn't be built/validated upstream.
var BumpBuilderBlockDataSourceTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_block_data_source_total",
	Help: "Block data source used for compound BUMP construction, by source (callback|datahub).",
}, []string{"source"})

// BumpBuilderGraceWaitTotal counts BLOCK_PROCESSED handlers that actually
// waited the grace window before re-reading STUMPs — i.e. completeness could
// not be verified from the first STUMP read (expected set unsatisfied, or
// absent with STUMPs present). Handlers whose expected-STUMP set was already
// complete on arrival skip the wait and never increment this, so the ratio of
// this counter to blocks_processed_total tracks how often the window still
// earns its latency cost.
var BumpBuilderGraceWaitTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_grace_window_waits_total",
	Help: "BLOCK_PROCESSED handlers that waited the grace window before re-reading STUMPs.",
})

// BumpBuilderEmptyStumpBlocksTotal counts BLOCK_PROCESSED messages that
// arrived with zero stored STUMPs for the block. The "expected" case is a
// block with no tracked transactions, but a sustained non-zero rate while
// BSV Broadcaster has watched txs is a strong signal that STUMP callbacks are being
// lost upstream (merkle-service callback_dedup suppression, delivery DLQ,
// callback URL outage). Surfaces silent drops that would otherwise only show
// up as "tx stuck in SEEN_MULTIPLE_NODES" days later.
var BumpBuilderEmptyStumpBlocksTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_empty_stump_blocks_total",
	Help: "BLOCK_PROCESSED messages handled with zero STUMPs in the store for the block.",
})

// BumpBuilderIncompleteStumpsTotal counts BLOCK_PROCESSED messages whose
// merkle-supplied expected-STUMP set (CallbackMessage.ExpectedSubtreeIndices)
// was NOT fully satisfied by the STUMPs BSV Broadcaster had stored once the grace window
// elapsed. Such a block is deliberately left un-finalized (processed_at stays
// NULL) so the watchdog re-drives it via merkle's /reprocess, which re-emits the
// missing STUMPs and BLOCK_PROCESSED. On BumpBuilderBuildDuration these land
// as outcome="deferred_incomplete" (renamed from "incomplete_stumps" when
// completeness-first grace handling landed). This is the metric that makes the
// previously-silent partial-STUMP drop visible: unlike the all-missing case
// (empty_stump_blocks_total), a block missing only SOME of its STUMPs used to
// build a valid-looking BUMP and lose the absent subtree's txs with no signal.
var BumpBuilderIncompleteStumpsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_incomplete_stumps_total",
	Help: "BLOCK_PROCESSED messages left un-finalized because expected STUMPs were still missing after the grace window.",
})

// BumpBuilderShortCircuitTotal counts BLOCK_PROCESSED messages handled by the
// short-circuit path — the BUMP already exists in the store and this is a
// redelivery (typically from /reprocess re-emitting BLOCK_PROCESSED). Tracks
// how much work the short-circuit saves vs. re-fetching datahub.
var BumpBuilderShortCircuitTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_short_circuit_total",
	Help: "BLOCK_PROCESSED messages skipped because a compound BUMP already exists for the block.",
})

// BumpBuilderAnchorGuardDeniedTotal counts blocks the anchor guard refused
// to mark MINED because chaintracks' active chain has a DIFFERENT block at
// their height (issue #279 — same-height competition loser). path
// distinguishes the fresh-build path from the short-circuit redelivery
// path. Each denial routes the block into the anchor reconciler's queue.
var BumpBuilderAnchorGuardDeniedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_bump_builder_anchor_guard_denied_total",
	Help: "Blocks refused MINED anchoring because the active chain has a different block at their height.",
}, []string{"path"})

// ---------------------------------------------------------------------------
// anchor reconciler (bump-builder-hosted — reorg tx re-anchoring, issue #279)

// ReconcilerBlocksTotal counts orphaned blocks processed by the anchor
// reconciler, by terminal outcome: reanchored (all affected txs moved to a
// canonical block), reverted (all reverted to SEEN_ON_NETWORK), mixed,
// empty (nothing anchored to the orphan anymore), resurrected (the block is
// active again — stale orphan mark), deferred (waiting on the canonical
// block's BUMP), parked (canonical BUMP unavailable at the defer cap — txs
// left MINED, NOT reverted, awaiting a later canonical BUMP; issue #282),
// stale (the reconciled_at compare-and-set found the row reactivated or
// orphaned again mid-pass — nothing stamped; issue #339), held (waiting for
// the merkle-service re-watch of the orphan's txs, under its cap), error.
var ReconcilerBlocksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_blocks_total",
	Help: "Orphaned blocks processed by the anchor reconciler, by outcome.",
}, []string{"outcome"})

// ReconcilerTxsReanchoredTotal counts transactions the reconciler moved from
// an orphaned block to the active-chain block containing them.
var ReconcilerTxsReanchoredTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_txs_reanchored_total",
	Help: "Transactions re-anchored from an orphaned block to a canonical block.",
})

// ReconcilerTxsRevertedTotal counts transactions the reconciler reverted to
// SEEN_ON_NETWORK because no canonical block contains them.
var ReconcilerTxsRevertedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_txs_reverted_total",
	Help: "Transactions reverted to SEEN_ON_NETWORK because their only block was orphaned.",
})

// ReconcilerTxsParkedTotal counts transactions left MINED against an
// orphaned block because its canonical BUMP was unavailable at the defer cap
// (issue #282). Parked txs are NOT reverted to SEEN_ON_NETWORK — a later
// stored/rebuilt canonical BUMP re-anchors them through the normal mine
// path. A steady climb here means canonical BUMPs are not arriving (see the
// merkle-service /reprocess dependency, bsv-blockchain/merkle-service#208).
var ReconcilerTxsParkedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_txs_parked_total",
	Help: "Transactions left MINED against an orphan because no canonical BUMP was available (not reverted).",
})

// ReconcilerTxsRewatchedTotal counts transactions anchored to an orphaned
// block whose merkle-service watch the reconciler refreshed, by outcome
// (ok|failed). The refresh keeps merkle-service reporting a tx past its
// post-mine TTL so a later block that re-mines it re-anchors it; a climbing
// "failed" series means orphans are parking without that cover.
var ReconcilerTxsRewatchedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_txs_rewatched_total",
	Help: "Orphan-anchored transactions re-registered with merkle-service by the reconciler, by outcome.",
}, []string{"outcome"})

// ReconcilerRemineRequeuedTotal counts canonical block rows the reconciler
// re-orphaned in order to retry a re-mine that failed — part-way, before it
// could read the block's BUMP, or on a BUMP that does not parse — AFTER the
// block-status tracker had reactivated the row mid-pass (issue #339
// review). The row was active and off the durable queue with some or all of
// its transactions un-remined, and the reconciler's queue — whose predicate
// is status='orphaned' — is the only in-store path that retries; each count
// is therefore a canonical block reading orphaned for up to one tick, until
// the next pass re-mines it and reactivates it. Non-zero is worth a look:
// it means a store call failed while a reorg was being healed.
var ReconcilerRemineRequeuedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_remine_requeue_total",
	Help: "Canonical block rows re-orphaned to retry a partial re-mine after a concurrent reactivation.",
})

// ReconcilerRemineHandoffFailedTotal counts failed re-mines whose durable
// hand-off (RequeueOrphanedBlock, the row read, or the re-orphan above) the
// store still refused after the in-process retries. The block is then held
// in the reconciler's in-memory pending set and retried on every tick until
// it heals or the hand-off lands; that set does not survive a restart, so a
// non-zero count during an outage is the signal to check
// ReconcilerRemineHandoffPending before restarting the process.
var ReconcilerRemineHandoffFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_reconciler_remine_handoff_failed_total",
	Help: "Failed re-mines whose durable hand-off the store refused after retries; the block fell back to the in-memory pending set.",
})

// ReconcilerRemineHandoffPending is the size of that in-memory pending set:
// canonical blocks with un-remined txs that no in-store queue currently
// holds — either because the store refused the hand-off, or because the
// block-status tracker reactivated the row before its re-mine could run and
// re-orphaning it would be wrong. Should be 0 in steady state and drain to
// 0 as soon as the store recovers.
var ReconcilerRemineHandoffPending = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_reconciler_remine_handoff_pending",
	Help: "Blocks awaiting a re-mine retry that only the reconciler's in-memory pending set holds.",
})

// ReconcilerBlockDuration observes wall time per reconciled block.
var ReconcilerBlockDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_reconciler_block_duration_seconds",
	Help:    "Wall time spent reconciling one orphaned block's transactions.",
	Buckets: prometheus.ExponentialBuckets(0.01, 4, 8), // 10ms .. ~11m
})

// ---------------------------------------------------------------------------
// block-status projection (chaintracks_server tracker + reconciler full-scan)

// Label values for BlockStatusTransitionsTotal. The source names the
// detection edge that wrote the transition: the ReorgEvent handler and the
// tie-scan live in services/chaintracks_server; the startup full-scan and
// the resurrection short-circuit in services/bump_builder's reconciler.
const (
	BlockTransitionOrphaned    = "orphaned"
	BlockTransitionReactivated = "reactivated"

	BlockTransitionSourceReorgEvent = "reorg_event"
	BlockTransitionSourceTieScan    = "tie_scan"
	BlockTransitionSourceFullScan   = "full_scan"
	BlockTransitionSourceReconciler = "reconciler"
)

// BlockStatusTransitionsTotal counts block_processing status transitions by
// direction and detection edge. transition=orphaned is a row demoted because
// the active chain holds a different block at its height; transition=
// reactivated is an orphaned row reset to active because it IS the
// active-chain block at its height again — a same-height flip-flop (issue
// #339). Every pair counts APPLIED transitions, and the signal comes from the
// write itself, not from a pre-read: store.MarkBlocksOrphaned returns how
// many rows its write actually moved to orphaned (hashes with no row, and
// rows already orphaned, are written or skipped without being counted), and
// store.ReactivateBlock reports whether its generation-checked write applied
// (a row another edge already reactivated, or re-orphaned with a newer
// generation, is not counted). Under concurrent writers this is exact: two
// replicas moving the same row report one transition between them. The
// anchor guard's write-time denials are counted separately by
// BumpBuilderAnchorGuardDeniedTotal.
var BlockStatusTransitionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_block_status_transitions_total",
	Help: "block_processing status transitions by direction (orphaned|reactivated) and detection edge (reorg_event|tie_scan|full_scan|reconciler).",
}, []string{"transition", "source"})

// ---------------------------------------------------------------------------
// watchdog (standalone service — block-processing recovery)
// ---------------------------------------------------------------------------

// WatchdogTickTotal tracks watchdog tick outcomes.
var WatchdogTickTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_watchdog_tick_total",
	Help: "Watchdog tick outcomes.",
}, []string{labelOutcome}) // ran, skipped_no_leader, lease_error

// WatchdogStaleCount is the number of stale block_processing rows the last
// tick observed (post-recency-window filter, pre-backoff filter).
var WatchdogStaleCount = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_watchdog_stale_count",
	Help: "Stale block_processing rows observed by the last watchdog tick.",
})

// WatchdogReprocessTotal counts /reprocess outcomes by reason.
var WatchdogReprocessTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_watchdog_reprocess_total",
	Help: "Watchdog /reprocess call outcomes.",
}, []string{labelOutcome}) // success, err_auth, err_4xx, err_5xx, err_network

// WatchdogBackoffDepth is the size of the in-memory attempts map.
// Sustained growth implies blocks are persistently failing to recover —
// inspect logs for the 4xx/5xx outcome breakdown.
var WatchdogBackoffDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_watchdog_backoff_depth",
	Help: "Number of blocks currently held in the watchdog's in-memory backoff map.",
})

// WatchdogParkedTotal counts blocks the watchdog has parked (stopped
// re-driving) because they hit the reprocess cap. A non-zero value means
// blocks are permanently un-finalizable — inspect bump-builder for the
// underlying reason (e.g. incomplete STUMPs / merkle-root mismatch).
var WatchdogParkedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_watchdog_parked_total",
	Help: "Blocks parked by the watchdog (reprocessing stopped), by reason.",
}, []string{"reason"}) // max_attempts, max_age

// ---------------------------------------------------------------------------
// api_server
// ---------------------------------------------------------------------------

// APIRequestDuration measures HTTP request latency by route and status class.
// route is the gin route pattern (not the resolved URL) so cardinality stays
// bounded.
var APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_api_request_duration_seconds",
	Help:    "API request latency by route + method + status class.",
	Buckets: latencyBuckets,
}, []string{"route", "method", "status_class"}) // status_class = 2xx, 3xx, 4xx, 5xx

// APISubmissionRecorderDropTotal counts submission rows dropped by the async
// recorder pool because its bounded queue was full. Non-zero is acceptable
// (recordSubmission is best-effort) but sustained drops mean the recorder
// pool is undersized relative to the inbound submit rate — raise worker
// count or queue depth in server.go.
var APISubmissionRecorderDropTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_api_submission_recorder_drop_total",
	Help: "Submission rows dropped because the async recorder queue was full.",
})

// APIRequestsInFlight tracks how many requests are currently being handled.
var APIRequestsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_api_requests_in_flight",
	Help: "API requests currently being handled.",
})

// APIRequestBytes tracks request body size — surfaces oversized clients early.
var APIRequestBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_api_request_bytes",
	Help:    "API request body size in bytes, by route.",
	Buckets: bytesBuckets,
}, []string{"route"})

// APISSEDroppedTotal counts SSE fan-out events that were dropped without
// being delivered to a client. Reasons:
//   - "slow_client": the client's send buffer was full. The label name is
//     historical and is KEPT for dashboard/alert compatibility, but it is NOT
//     a diagnosis — the buffer fills whenever production outruns drain, and
//     the fan-out goroutine itself has been the producer-side bottleneck.
//     Read it together with SSEFanoutDuration (is fan-out itself slow?) and
//     SSEConnectedClients before attributing fault to a consumer.
//   - "client_gone": the client was unregistering concurrently and its context
//     had already been canceled by the time fan-out reached it.
//   - "stale_received": a RECEIVED reached a client's writer after that
//     stream had already carried a later status for the txid (it trailed
//     the verdict on the events topic), so it was skipped rather than
//     regress the stream. Counted per client.
//
// A non-zero "client_gone" rate is normal under churn.
var APISSEDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_api_sse_dropped_total",
	Help: "SSE fan-out events dropped without delivery, by reason.",
}, []string{"reason"}) // slow_client, client_gone, stale_received

// SSEFanoutDuration measures how long the manager's single fan-out goroutine
// spends on one event, split by kind:
//   - "single": one per-tx status event.
//   - "bulk": one bulk event (a block's MINED unfan), covering the batched
//     token resolution AND every per-tx delivery it expands to.
//
// This is THE signal for the fan-out's own throughput ceiling: the goroutine
// is serial, so sustained 1/p50 below the publish rate means events are
// queueing upstream and will eventually be dropped — regardless of how fast
// any consumer drains. It is the metric that was missing when 1.6M drops were
// attributed to a "slow SSE client" that was idle.
var SSEFanoutDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_sse_fanout_duration_seconds",
	Help:    "Time the SSE fan-out goroutine spent dispatching one event, by kind.",
	Buckets: latencyBuckets,
}, []string{"kind"}) // single, bulk

// SSEConnectedClients tracks how many /events connections are registered on
// this pod. Fan-out cost per event scales with this, and it disambiguates
// "one chronically slow consumer" from "many consumers".
var SSEConnectedClients = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_sse_connected_clients",
	Help: "SSE /events clients currently registered on this pod.",
})

// SSETokenCacheTotal counts txid→token-set cache outcomes on the fan-out
// path ("hit" / "miss"). Each miss is one store round-trip on the serial
// fan-out goroutine, so the miss rate multiplied by SSETokenLookupDuration is
// the store's contribution to fan-out latency. Bulk events bypass the cache
// by design and are not counted here.
var SSETokenCacheTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_sse_token_cache_total",
	Help: "SSE fan-out txid→token-set cache lookups, by result.",
}, []string{"result"}) // hit, miss

// SSETokenCacheEntries reports the cache's resident entry count. Sitting at
// the entry cap means the working set is larger than the cache and hits are
// being lost to eviction; sitting well below it while the byte budget binds
// means unusually large token strings.
var SSETokenCacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_sse_token_cache_entries",
	Help: "Entries resident in the SSE fan-out txid→token-set cache.",
})

// SSETokenLookupDuration measures one Store.TokensForTxIDs call from the
// fan-out path, by kind ("single" for a cache miss on one txid, "batch" for a
// bulk event's chunked resolution). This is the per-event probe latency that
// had to be measured out-of-band before.
var SSETokenLookupDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_sse_token_lookup_duration_seconds",
	Help:    "Store.TokensForTxIDs latency from the SSE fan-out path, by kind.",
	Buckets: latencyBuckets,
}, []string{"kind"}) // single, batch

// SSEMidstreamCatchupsTotal counts store-backed catchup rounds run on a LIVE
// /events connection after fan-out overflowed the client's send channel
// (reason="slow_client" drops above). One drop episode can take several
// rounds (each bounded by the per-round frame cap), so this counts rounds,
// not episodes. A sustained rate means a consumer chronically drains slower
// than the publish rate and is being served store-paced replay instead of
// live frames.
var SSEMidstreamCatchupsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_sse_midstream_catchups_total",
	Help: "Mid-stream SSE catchup rounds run after a client's send buffer overflowed.",
})

// SSEMidstreamCatchupFramesTotal counts frames replayed from the store by
// mid-stream catchup rounds. Includes duplicates of frames the client already
// received live (the replay window is deliberately rewound past the drop
// boundary; clients dedupe by txid+status).
var SSEMidstreamCatchupFramesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_sse_midstream_catchup_frames_total",
	Help: "Frames replayed from the store by mid-stream SSE catchup rounds.",
})

// SSEClientLagSeconds measures, per client per keepalive tick, how far behind
// the present that client's last delivered frame is.
//
// This is the single most useful health signal for the push path and the one
// whose absence let 1.6M dropped events go unnoticed for a whole 45-minute
// run: drops and fan-out duration each describe one component, while lag
// describes what a consumer actually EXPERIENCES. It is nearly free — the
// writer goroutine already maintains the lastDelivered watermark, and this
// samples it once per client per keepalive rather than per frame.
//
// A client with nothing to receive reads as lag, so interpret it against
// SSEConnectedClients and the publish rate: it is a "is the stream keeping
// up" signal, not a liveness check.
var SSEClientLagSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_sse_client_lag_seconds",
	Help:    "Age of the last frame delivered to an SSE client, sampled per keepalive tick.",
	Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
})

// SSEConnectionsTotal counts finished /events connections by how they ended:
//   - "client_closed": the request context was canceled (normal disconnect).
//   - "drain":         released by a graceful shutdown.
//   - "write_error":   the connection died mid-write.
//
// The rate is the reconnect rate, and the label says why. A reconnect storm
// with outcome="write_error" is a very different problem from one with
// outcome="drain", and before this the two were indistinguishable.
var SSEConnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_sse_connections_total",
	Help: "Finished SSE /events connections, by how they ended.",
}, []string{"outcome"}) // client_closed, drain, write_error

// SSEGapEventsTotal counts `event: gap` frames written to /events clients —
// every occasion on which the server KNOWS it handed a client a discontiguous
// stream and said so. Reasons:
//   - "catchup_truncated": a connect-time replay stopped at the frame cap with
//     matching history still beyond the cursor.
//   - "firehose_drop": a tokenless client's frame was dropped on channel
//     overflow. There is no bounded store query that reconstructs an
//     unfiltered stream, so a gap notice is the only recovery signal these
//     clients can be given.
//   - "replay_unavailable": the store could not serve the replay (iteration
//     error, or a backend that refused an unbounded scan).
//
// This is the counterpart to APISSEDroppedTotal: drops say the server lost
// something, gaps say the CLIENT was told. A drop rate without a matching gap
// rate is silent loss, which is the failure mode this metric exists to make
// impossible to miss. The client's contract on receiving one is to reconcile
// the window over REST.
var SSEGapEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_sse_gap_events_total",
	Help: "SSE `event: gap` frames written, by reason.",
}, []string{"reason"}) // catchup_truncated, firehose_drop, replay_unavailable

// EventsSubscriberDroppedTotal counts events.Publisher.Subscribe channel
// drops, labeled by which caller's channel filled. The publisher emits a
// drop when the per-subscriber buffer is at capacity and the kafka handler
// goroutine can't enqueue the next message without blocking. A sustained
// non-zero rate on a particular caller (e.g. "webhook") points to that
// subscriber's downstream draining slower than the producer rate — typical
// causes are synchronous I/O in the channel reader or a CPU-pressured pod.
var EventsSubscriberDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_events_subscriber_dropped_total",
	Help: "events.Publisher subscriber-channel drops, labeled by caller (e.g. sse, webhook).",
}, []string{"caller"})

// WebhookPoolSaturatedTotal counts status updates that the webhook
// service dropped because its bounded delivery worker pool was full when
// the channel reader tried to enqueue them. A non-zero rate means
// MaxConcurrentDeliveries is too low for the current rate of slow
// callbacks: workers are blocked on http.Client.Do and incoming statuses
// pile up faster than the work channel can hold them. Distinguishes
// pool-pressure drops from upstream subscriber-channel drops
// (bsv_broadcaster_events_subscriber_dropped_total{caller="webhook"}) so the two
// failure modes can be tuned independently.
var WebhookPoolSaturatedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_webhook_pool_saturated_total",
	Help: "Status updates dropped by the webhook service because its delivery worker pool was full.",
})

// WebhookCASLostTotal counts claim-then-POST attempts that lost the CAS to
// another replica — the other pod already advanced LastDeliveredStatus for
// the same submission, so this pod silently skipped its POST. With N
// horizontally-scaled api-server replicas, this counter is expected to
// increase at roughly (N - 1) × deliveries: the winner POSTs, the (N - 1)
// losers count here. A flat zero on a multi-replica deployment means events
// are not actually flowing through CAS — either the schema migration is
// missing or only one pod is producing events.
var WebhookCASLostTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_webhook_cas_lost_total",
	Help: "Webhook deliveries skipped because another replica won the LastDeliveredStatus CAS.",
})

// MinedPushWithoutMerklePathTotal counts MINED (or IMMUTABLE) statuses pushed
// on a channel ("webhook" | "sse") whose body/frame went out without a
// merklePath despite enrichment being attempted. Because the compound BUMP is
// persisted before the MINED event is published, a nonzero value points to a
// cache-eviction/reorg race or a missing/unparseable BUMP — the proof is still
// recoverable via GET /tx/:txid, but a sustained rate warrants investigation.
var MinedPushWithoutMerklePathTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_mined_push_without_merkle_path_total",
	Help: "MINED/IMMUTABLE statuses pushed without a merklePath after enrichment, by channel.",
}, []string{"channel"})

// WebhookCASErrorTotal counts CAS attempts that failed with a real infra
// error rather than a generation mismatch — surfaced separately so a flat
// WebhookCASLostTotal can't mask a backend that's silently failing every
// write. The Aerospike and MongoDB backends emit it: Aerospike's CAS path
// collapses gen-mismatch and infra errors into the same (false, nil) return
// shape, so the metric is the one observable signal that distinguishes them;
// MongoDB propagates the error as well but counts it so the two document
// backends stay comparable. Postgres and Pebble propagate infra errors
// through the function's `err` return and the caller already logs those.
var WebhookCASErrorTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_webhook_cas_error_total",
	Help: "Webhook CAS writes that failed with an infra error (distinct from generation mismatch).",
})

// WebhookReaperLease is 1 when this pod holds the webhook-reaper lease, 0
// otherwise. With N replicas, exactly one is expected to be 1 at any time.
var WebhookReaperLease = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_webhook_reaper_lease",
	Help: "1 when this pod holds the webhook-reaper lease, 0 otherwise.",
})

// WebhookReaperTickTotal tracks reaper ticks by outcome (ran /
// skipped_no_leader / lease_error). Skipped_no_leader is the expected steady
// state on N-1 replicas; a sustained lease_error rate points at the store
// backend being unhealthy.
var WebhookReaperTickTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_webhook_reaper_tick_total",
	Help: "Webhook reaper ticks by outcome (ran / skipped_no_leader / lease_error).",
}, []string{labelOutcome})

// WebhookReaperReadyDepth is the number of submissions the most recent reaper
// tick observed as ready-for-retry. A sustained non-zero value means the
// backlog of failed POSTs is growing faster than the reaper can drain it.
var WebhookReaperReadyDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_webhook_reaper_ready_depth",
	Help: "Submissions the last webhook-reaper tick observed as ready-for-retry.",
})

// ---------------------------------------------------------------------------
// teranode (HTTP client)
// ---------------------------------------------------------------------------

// TeranodeRequestDuration measures HTTP latency for outbound calls to a
// datahub endpoint, by op and status code class.
var TeranodeRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_teranode_request_duration_seconds",
	Help:    "HTTP request latency from BSV Broadcaster to a datahub endpoint, by op and status class.",
	Buckets: latencyBuckets,
}, []string{"op", "status_class"}) // op = submit_tx, submit_txs, probe; status_class = 2xx/4xx/5xx/transport_error

// TeranodeEndpointHealth is per-endpoint circuit-breaker state. 1 = healthy,
// 0 = unhealthy. Endpoint URL is the label so dashboards can per-endpoint
// alert; URL count is bounded by the size of the datahub fleet.
var TeranodeEndpointHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_teranode_endpoint_healthy",
	Help: "1 if the endpoint is currently in the healthy set, 0 if circuit-breaker tripped.",
}, []string{"endpoint", "source"}) // source = configured, discovered

// TeranodeEndpointCount is the total count of registered endpoints, separated
// by source. Surfaces whether p2p discovery is finding peers.
var TeranodeEndpointCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_teranode_endpoint_count",
	Help: "Number of registered datahub endpoints, by source.",
}, []string{"source"})

// TeranodeEndpointRefreshRejectedTotal counts registry rows rejected by URL
// validation at refresh time (app.endpointSource). Nonzero means the shared
// DatahubEndpoint registry contains rows that predate discovery-time
// validation (or a peer's DNS broke after registration) — candidates for
// pruning.
var TeranodeEndpointRefreshRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_teranode_endpoint_refresh_rejected_total",
	Help: "Registry datahub endpoints rejected at refresh time by URL validation.",
}, []string{labelOutcome}) // blocked, invalid

// ---------------------------------------------------------------------------
// kafka
// ---------------------------------------------------------------------------

// KafkaMessagesTotal counts produce / consume / DLQ events by topic.
var KafkaMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_kafka_messages_total",
	Help: "Kafka messages produced, consumed, or DLQ-routed, by topic and op.",
}, []string{"topic", "op"}) // op = produce, consume, dlq

// KafkaMessageBytes measures message payload size.
var KafkaMessageBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_kafka_message_bytes",
	Help:    "Kafka message size in bytes, by topic and op.",
	Buckets: bytesBuckets,
}, []string{"topic", "op"})

// KafkaBackpressureTotal counts Send() calls that returned
// ErrBrokerBackpressure. A sustained non-zero rate means a consumer is too
// slow to keep up with the producer at the broker's configured buffer/timeout
// — investigate the corresponding consumer's pending-depth gauge.
var KafkaBackpressureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_kafka_backpressure_total",
	Help: "Producer Send calls that returned ErrBrokerBackpressure, by topic.",
}, []string{"topic"})

// KafkaProduceErrors counts producer failures by topic.
var KafkaProduceErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_kafka_produce_errors_total",
	Help: "Kafka producer error count, by topic.",
}, []string{"topic"})

// KafkaDLQPublishFailures counts DLQ publish failures by original topic. A
// non-zero rate means the DLQ topic is rejecting publishes — investigate Kafka
// availability. The consumer leaves the offset uncommitted on these failures,
// so they correlate with rising consumer lag on the primary topic until
// publishing recovers.
var KafkaDLQPublishFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_kafka_dlq_publish_failures_total",
	Help: "Kafka DLQ publish failure count, by original topic.",
}, []string{"topic"})

// ---------------------------------------------------------------------------
// p2p_client
// ---------------------------------------------------------------------------

// P2PNodeStatusMessagesTotal counts node_status messages received from the
// teranode pubsub topic.
var P2PNodeStatusMessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_p2p_node_status_messages_total",
	Help: "node_status messages received from teranode peers.",
})

// P2PEndpointDiscoveryTotal counts datahub URL discovery outcomes.
var P2PEndpointDiscoveryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_p2p_endpoint_discovery_total",
	Help: "Datahub URL discovery outcomes from peer announcements.",
}, []string{labelOutcome}) // registered, invalid, blocked, no_url, no_store, error

// P2PPeerBestHeight reports the best block height each datahub peer
// advertises in its node_status messages. Compared against
// bsv_broadcaster_chain_tip_height (BSV Broadcaster's own processed tip) this is the
// height-lag signal that was previously invisible: a reachable-but-stalled
// peer showed healthy on every existing metric while its chain view froze
// 50+ blocks behind (issue #254). Labelled by the peer's base_url — the
// small, stable datahub set — and never by libp2p peer id, whose
// restart-churn would grow the series set unboundedly; peers announcing no
// base_url are skipped.
var P2PPeerBestHeight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_p2p_peer_best_height",
	Help: "Best block height advertised by a datahub peer via p2p node_status, by base_url.",
}, []string{"base_url"})

// P2PPeerMinMiningFee reports the minimum mining fee (satoshis per 1000 bytes)
// each peer advertises in its node_status FeePolicy. The GET /policy endpoint
// advertises the network-wide minimum of these (issue #212); this gauge makes
// the per-peer inputs visible. Labelled by base_url for the same
// bounded-cardinality reason as P2PPeerBestHeight; peers with no base_url are
// still persisted for the minimum but not surfaced here.
var P2PPeerMinMiningFee = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_p2p_peer_min_mining_fee",
	Help: "Minimum mining fee (satoshis per 1000 bytes) advertised by a peer via p2p node_status, by base_url.",
}, []string{"base_url"})

// P2PPeerMaxTxSizePolicy and P2PPeerMaxScriptSizePolicy report the size limits
// each peer advertises in its node_status FeePolicy. GET /policy advertises the
// network-wide maximum of these — the most permissive peer, mirroring the
// cheapest-peer rule used for the fee — and intake enforces it, so these gauges
// are how an operator sees which peer is setting the ceiling and spots one
// advertising an outlier. Labelled by base_url for the same
// bounded-cardinality reason as P2PPeerMinMiningFee. Only set for peers that
// actually advertise a limit: a legacy peer's absent limit would otherwise show
// as 0, reading as "accepts nothing".
var P2PPeerMaxTxSizePolicy = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_p2p_peer_max_tx_size_policy",
	Help: "Maximum transaction size (bytes) advertised by a peer via p2p node_status, by base_url.",
}, []string{"base_url"})

var P2PPeerMaxScriptSizePolicy = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_p2p_peer_max_script_size_policy",
	Help: "Maximum script size (bytes) advertised by a peer via p2p node_status, by base_url.",
}, []string{"base_url"})

// ChainTipHeight reports BSV Broadcaster's own view of the active chain tip — the
// highest block_processing row marked active. Refreshed by the api-server's
// /health handler (which also returns it as blockHeight); alert on this
// flatlining, or on bsv_broadcaster_p2p_peer_best_height pulling ahead of it, to
// catch stale-chain-view conditions before they surface as non-final
// rejections (issue #254).
var ChainTipHeight = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "bsv_broadcaster_chain_tip_height",
	Help: "BSV Broadcaster's active processed chain-tip height (max active block_processing row).",
})

// ---------------------------------------------------------------------------
// callback path — inbound merkle-service callbacks
// ---------------------------------------------------------------------------

// statusTransitionAgeBuckets covers RECEIVED→SEEN_ON_NETWORK style
// transitions. Wider than latencyBuckets because the tail can stretch into
// the multi-second range under merkle-service congestion (the slow case is
// the one we care most about catching with a histogram).
var statusTransitionAgeBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
}

// StatusTransitionAge measures the wall-clock age of the previous status
// row at the moment a new transition is applied. Wired into the SEEN_ON_NETWORK
// callback handler so {from="RECEIVED",to="SEEN_ON_NETWORK"} surfaces the
// time between BSV Broadcaster receiving a tx and the merkle-service callback
// landing in BSV Broadcaster's store + publish pipeline. Naturally extensible to
// other transitions (RECEIVED→REJECTED, SEEN_ON_NETWORK→MINED, ...) without
// new metric definitions.
var StatusTransitionAge = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_status_transition_age_seconds",
	Help:    "Wall-clock age of the previous status row at the moment a new transition is applied.",
	Buckets: statusTransitionAgeBuckets,
}, []string{"from", "to"})

// CallbackHandlerDuration measures end-to-end handler latency for one
// inbound /api/v1/merkle-service/callback request, partitioned by the
// callback type so a slow STUMP path doesn't get conflated with a slow
// SEEN_ON_NETWORK path. result ∈ {success, partial, error}.
var CallbackHandlerDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_callback_handler_duration_seconds",
	Help:    "End-to-end duration of one inbound merkle-service callback handler, by type.",
	Buckets: latencyBuckets,
}, []string{"type", "result"})

// CallbackBatchSize records len(TxIDs) per inbound callback so we can see
// how aggressively the upstream is batching. Informs whether bulk-publish
// optimizations are paying off.
var CallbackBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_callback_batch_size",
	Help:    "Number of txids in one inbound merkle-service callback, by type.",
	Buckets: sizeBuckets,
}, []string{"type"})

// CallbackUnknownTxIDTotal counts txids referenced by a callback that
// BSV Broadcaster's store has no record of. Already logged at WARN; the counter
// makes the rate scrapable so an operator can spot upstream drift.
var CallbackUnknownTxIDTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_callback_unknown_txid_total",
	Help: "Inbound callbacks that named a txid BSV Broadcaster's store doesn't know.",
}, []string{"type"})

// CallbackStaleTotal counts txids in inbound callbacks whose store row is
// already past the target status (lattice short-circuited the update). The
// underlying signal also lives in store_updatestatus_duration_seconds_count
// with outcome=skipped_lattice, but that histogram bakes in the duration
// label set; a dedicated counter is cheaper to alert on and makes
// "merkle-service is sending us stale callbacks" a first-class number.
// prev_status carries the lattice-blocked previous state (e.g. MINED) so
// operators can tell apart "callback for already-mined tx" from "duplicate
// SEEN_ON_NETWORK".
var CallbackStaleTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_callback_stale_total",
	Help: "Inbound callbacks whose target status was already eclipsed by the stored row's status.",
}, []string{"type", "prev_status"})

// ---------------------------------------------------------------------------
// store hot-path — per-call latency
// ---------------------------------------------------------------------------

// StoreUpdateStatusDuration decomposes the per-call UpdateStatus latency so
// we can tell whether a long callback handler is paying disk-write cost or
// publish cost. from_status/to_status label cardinality is bounded by the
// status lattice (~10 values each) so the matrix stays small.
var StoreUpdateStatusDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_store_updatestatus_duration_seconds",
	Help:    "Duration of one store.UpdateStatus call, by from/to status and outcome.",
	Buckets: latencyBuckets,
}, []string{"from_status", "to_status", labelOutcome}) // outcome: applied, skipped_lattice, not_found, error

// ---------------------------------------------------------------------------
// events publisher — Kafka send latency
// ---------------------------------------------------------------------------

// EventsPublishDuration measures the latency of one Publisher.Publish or
// PublishBulk call. Currently the path is dark — when the in-memory broker
// applies backpressure (a stalled consumer makes Send take 2s), nothing
// surfaces in metrics. kind ∈ {single, bulk}.
var EventsPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_events_publish_duration_seconds",
	Help:    "Duration of one Publisher.Publish/PublishBulk call.",
	Buckets: latencyBuckets,
}, []string{"kind", labelOutcome}) // outcome: success, error

// ObserveStatusClass returns the bucket label ("2xx", "3xx", "4xx", "5xx",
// "transport_error") for a given HTTP status code. Used by HTTP-latency
// histograms to keep label cardinality bounded.
func ObserveStatusClass(statusCode int) string {
	switch {
	case statusCode == 0:
		return "transport_error"
	case statusCode >= 200 && statusCode < 300:
		return "2xx"
	case statusCode >= 300 && statusCode < 400:
		return "3xx"
	case statusCode >= 400 && statusCode < 500:
		return "4xx"
	case statusCode >= 500 && statusCode < 600:
		return "5xx"
	default:
		return "other"
	}
}

// ---------------------------------------------------------------------------
// ratelimit / apikey
// ---------------------------------------------------------------------------

// RateLimitDecisionsTotal counts rate-limit and API-key decisions by tier and
// result: allowed | quota | concurrency | unauthorized | batch_too_large |
// unpayable (a single charge's cost exceeds the tier's entire per-minute
// budget — a 413, since a 429 here could never resolve itself) |
// auth_failures (the per-IP invalid-key throttle tripped, before the key was
// even looked at). Counted per charge, not per request: POST /txs charges
// twice (its base request cost, then its transactions). tier is "none" for
// unauthorized and auth_failures.
var RateLimitDecisionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_ratelimit_decisions_total",
	Help: "Rate-limit decisions by tier and result (allowed|quota|concurrency|unauthorized|batch_too_large|unpayable|auth_failures), per charge.",
}, []string{"tier", "result"})

// RateLimitReservationsTotal counts ReserveQuota calls against the shared
// store by result: granted (in full) | partial | exhausted (zero) | error |
// canceled (the caller's own context was done, not a store outage).
var RateLimitReservationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_ratelimit_reservations_total",
	Help: "Shared quota reservations by result (granted|partial|exhausted|error|canceled).",
}, []string{"result"})

// RateLimitReservationDuration is the latency of shared quota reservations —
// the store round trip a lease miss pays.
var RateLimitReservationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "bsv_broadcaster_ratelimit_reservation_duration_seconds",
	Help:    "Latency of shared quota reservations.",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
})

// RateLimitStoreFailOpenTotal counts keyed charges served from the local
// fail-open bucket because the quota store errored.
var RateLimitStoreFailOpenTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bsv_broadcaster_ratelimit_store_failopen_total",
	Help: "Keyed rate-limit charges served by the local fail-open bucket because the quota store errored.",
})

// APIKeyCacheTotal counts API-key resolutions by cache result:
// hit | miss | negative (cached "no such key") | failing (a synchronous
// refresh of a stale entry just failed, newly marking the key as failing
// and falling back to the stale value) | stale (served non-blocking,
// within key_cache_stale_max, because the key is already known failing) |
// error (a synchronous attempt failed -- refreshing an uncached key, or a
// key already past staleMax -- with no stale value available to fall back
// to) | canceled (same as error, but the caller's own context ended the
// wait -- a client disconnect, not a store failure).
var APIKeyCacheTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bsv_broadcaster_apikey_cache_total",
	Help: "API key resolutions by cache result (hit|miss|negative|failing|stale|error|canceled).",
}, []string{"result"})
