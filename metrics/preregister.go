package metrics

import "github.com/bsv-blockchain/broadcaster/models"

// Vec children (labeled series) only come into existence on their first
// observation. A series born between two scrapes starts at its burst value,
// and PromQL increase()/rate() can never count anything before a series'
// first sample — so every deploy or pod restart silently swallowed the first
// burst per label pair (a single block marking hundreds of txs MINED
// registered as ~0 on dashboards). Services call these helpers at
// construction time so every series they can emit is exported at 0 from the
// first scrape.

// PreRegisterStatusTransitions instantiates the StatusTransitionAge children
// for every lattice-valid (from → to) pair of the given target statuses.
// Each service passes only the transitions it actually observes to keep the
// exported-series count bounded per pod.
func PreRegisterStatusTransitions(tos ...models.Status) {
	for _, to := range tos {
		for _, from := range models.AllStatuses() {
			if to.CanTransitionFrom(from) {
				StatusTransitionAge.WithLabelValues(string(from), string(to))
			}
		}
	}
}

// PreRegisterTxSubmissions instantiates every {route, result} child of
// APITxsSubmittedTotal. The api-server calls this at construction time.
func PreRegisterTxSubmissions() {
	for _, route := range []string{"/tx", "/txs"} {
		for _, result := range []string{"new", "duplicate", "retry_rejected"} {
			APITxsSubmittedTotal.WithLabelValues(route, result)
		}
		APIFinalityRejectionsTotal.WithLabelValues(route)
	}
}

// bumpBuildOutcomes is the closed set of outcome labels handleMessage can
// stamp on BumpBuilderBuildDuration. Keep it in sync with the outcome
// assignments in services/bump_builder/builder.go and the doc comment on
// BumpBuilderBuildDuration. A failure alert that never fires because its
// outcome series was never born is exactly the blind spot pre-registration
// exists to close, so the failure outcomes matter most here. Unexported so no
// other package can mutate the shared slice.
var bumpBuildOutcomes = []string{
	// benign — the two build dispositions (finalized_complete_no_grace when
	// the expected-STUMP set was already complete on arrival and the grace
	// window was skipped, grace_waited otherwise) plus the non-build ones
	"finalized_complete_no_grace", "grace_waited",
	"short_circuited", "no_stumps", "context_canceled",
	// a redelivered BLOCK_PROCESSED (another merkle-service) carried STUMPs
	// with leaves the stored BUMP lacked, so the compound was rebuilt
	"rebuilt",
	// the anchor guard refused to mark the block's txs MINED because the
	// active chain has a different block at its height (issue #279)
	"skipped_inactive_chain",
	// failures
	"parse_failed", "deferred_incomplete", "fetch_failed",
	"no_subtrees", "build_failed", "validation_failed", "store_failed",
}

// PreRegisterBumpOutcomes instantiates every outcome child of
// BumpBuilderBuildDuration so each series is exported from the first scrape.
// The bump-builder calls this at construction time. Without it a
// first-occurrence failure (e.g. the first validation_failed after a deploy)
// is invisible to increase()/rate() until its second sample — precisely when
// an operator most needs the alert to fire.
func PreRegisterBumpOutcomes() {
	for _, outcome := range bumpBuildOutcomes {
		BumpBuilderBuildDuration.WithLabelValues(outcome)
	}
}

// blockStatusTransitions is the closed set of {transition, source} pairs the
// block-status tracker (services/chaintracks_server) and the anchor
// reconciler (services/bump_builder) emit on BlockStatusTransitionsTotal.
// The reconciler orphans in exactly one place — re-orphaning a canonical row
// the tracker reactivated while its re-mine was failing part-way, so the
// remaining txs get retried (keepPartialRemineQueued) — which is why the
// reconciler/orphaned pair is here. Keep in sync with the emitters.
var blockStatusTransitions = [][2]string{
	{BlockTransitionOrphaned, BlockTransitionSourceReorgEvent},
	{BlockTransitionOrphaned, BlockTransitionSourceTieScan},
	{BlockTransitionOrphaned, BlockTransitionSourceFullScan},
	{BlockTransitionOrphaned, BlockTransitionSourceReconciler},
	{BlockTransitionReactivated, BlockTransitionSourceReorgEvent},
	{BlockTransitionReactivated, BlockTransitionSourceTieScan},
	{BlockTransitionReactivated, BlockTransitionSourceFullScan},
	{BlockTransitionReactivated, BlockTransitionSourceReconciler},
}

// PreRegisterBlockStatusTransitions instantiates every {transition, source}
// child of BlockStatusTransitionsTotal. chaintracks_server and the
// bump-builder call this at construction time: a same-height flip-flop
// (issue #339) is rare enough that the reactivated series would otherwise be
// born mid-incident, and increase() would swallow the one burst that matters.
func PreRegisterBlockStatusTransitions() {
	for _, pair := range blockStatusTransitions {
		BlockStatusTransitionsTotal.WithLabelValues(pair[0], pair[1])
	}
}

// PreRegisterPropagationRetryOutcomes instantiates every outcome child of the
// missing-parent and durable-retry counters. The propagation service calls
// this at construction time.
//
// These matter more than most: the interesting series are the ones that stay
// at zero in a healthy deployment (rejected_ancestor, exhausted), and an alert
// on a series that was never born returns no-data rather than 0 — which most
// alerting rules treat as "nothing to see" instead of "healthy".
func PreRegisterPropagationRetryOutcomes() {
	for _, outcome := range []string{"requeued", "reaper_retry", "rejected_ancestor"} {
		PropagationMissingParentTotal.WithLabelValues(outcome)
	}
	for _, reason := range []string{"row_missing", "terminal", "raw_tx_missing", "malformed"} {
		PropagationHydrateDroppedTotal.WithLabelValues(reason)
	}
	for _, outcome := range []string{"scheduled", "exhausted"} {
		PropagationPendingRetryTotal.WithLabelValues(outcome)
	}
	for _, stage := range []string{"merkle", "network"} {
		PropagationRetryTotal.WithLabelValues(stage)
	}
	for _, reason := range []string{"claim_revoked", "auth_error", "http_5xx", "timeout", "transport", "register_error"} {
		PropagationMerkleRegisterFailures.WithLabelValues(reason)
	}
}

// PreRegisterRateLimit exports every per-tier rate-limit decision series,
// plus the reservation-result series, at 0 from the first scrape, for the
// configured tiers. Call PreRegisterAPIKey too whenever auth.enabled -- this
// helper alone does not cover the apikey-identification series ({none,*}
// decisions, the cache results), which exist independently of whether
// ratelimit.enabled is on.
func PreRegisterRateLimit(tiers []string) {
	for _, tier := range tiers {
		for _, result := range []string{"allowed", "quota", "concurrency", "batch_too_large", "unpayable"} {
			RateLimitDecisionsTotal.WithLabelValues(tier, result)
		}
	}
	for _, result := range []string{"granted", "partial", "exhausted", "error", "canceled"} {
		RateLimitReservationsTotal.WithLabelValues(result)
	}
}

// PreRegisterAPIKey exports every series apikey.Middleware and
// apikey.Resolver can emit at 0 from the first scrape: the {none,*} decision
// pairs recorded before any tier is known (an invalid key, or an IP
// throttled for repeated invalid-key attempts), and every API-key cache
// result. Call this whenever auth.enabled, independent of whether
// ratelimit.enabled is also on -- identification runs either way.
func PreRegisterAPIKey() {
	for _, result := range []string{"unauthorized", "auth_failures"} {
		RateLimitDecisionsTotal.WithLabelValues("none", result)
	}
	for _, result := range []string{"hit", "miss", "negative", "failing", "stale", "error", "canceled"} {
		APIKeyCacheTotal.WithLabelValues(result)
	}
}

// PreRegisterUsage exports the usage-reporting series for each provider
// with a usage sink at 0.
func PreRegisterUsage(providers []string) {
	for _, p := range providers {
		for _, result := range []string{"ok", "retry", "unauthorized", "misconfigured", "uncertain", "rejected", "disabled"} {
			UsageFlushTotal.WithLabelValues(p, result)
		}
		for _, reason := range []string{"uncertain", "rejected", "invalid", "overflow", "disabled", "shutdown"} {
			UsageEntriesDroppedTotal.WithLabelValues(p, reason)
		}
		UsageEntriesSentTotal.WithLabelValues(p)
		UsagePendingEntries.WithLabelValues(p)
	}
}

// PreRegisterIntrospection exports every APIKeyIntrospectTotal series for
// the configured providers at 0.
func PreRegisterIntrospection(providers []string) {
	for _, p := range providers {
		for _, result := range []string{"valid", "invalid", "unauthorized", "error", "backoff"} {
			APIKeyIntrospectTotal.WithLabelValues(p, result)
		}
	}
}
