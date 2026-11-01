package bump_builder

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/bump"
	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services"
	"github.com/bsv-blockchain/broadcaster/store"
)

// ReconcilerLeaseName coordinates a single reconciler across bump-builder
// replicas via the standard store.Leaser primitive. The work is idempotent
// (re-anchor and revert both converge), so brief dual-holding during a
// failover window is redundant work, not corruption.
const ReconcilerLeaseName = "anchor-reconciler"

// defaultFullScanDepth bounds the startup full-scan when
// Reconciler.FullScanDepth is unset: only rows within this many heights of
// the active tip are considered. 144 ≈ one day of BSV blocks — the same
// recency window the watchdog uses — and stops the deep backstop from
// re-orphaning a long chain's entire history oldest-first (issue #282).
const defaultFullScanDepth = 144

// maxStartupFullScanAttempts bounds how many times a tick re-runs the
// one-shot startup full-scan after an incomplete run. Enough to ride out a
// transient store failure; low enough that a persistent one does not re-page
// the scan window on every tick for the life of the process.
const maxStartupFullScanAttempts = 5

// defaultFullScanChaintracksReadyTimeout bounds the initial wait for the
// chain-header source to sync up to the store's active tip before the startup
// full-scan runs, when Reconciler.FullScanChaintracksReadyTimeoutMs is unset.
// The embedded chaintracks resyncs from genesis on every deploy; two minutes
// covers a typical catch-up while never blocking startup indefinitely — a
// slower resync self-heals through the tick loop instead.
const defaultFullScanChaintracksReadyTimeout = 2 * time.Minute

// errMalformedBUMP marks a stored compound BUMP that does not parse. It is
// distinct from store.ErrNotFound (nothing stored) and from a transient read
// failure: the blob exists and is corrupt, so it will not fix itself, and
// only a rebuild (a BLOCK_PROCESSED redelivery via /reprocess) replaces it.
var errMalformedBUMP = errors.New("stored compound BUMP is malformed")

// reconcileBlock outcomes — the label values of ReconcilerBlocksTotal.
const (
	outcomeError       = "error"       // a store or header failure; the block stays queued and the next tick retries
	outcomeDeferred    = "deferred"    // the canonical BUMP is not usable yet; waiting (and asking) for it, under the cap
	outcomeResurrected = "resurrected" // the block is canonical again; re-mined and reactivated
	outcomeStale       = "stale"       // the row changed generation mid-pass; nothing stamped or reactivated
	outcomeParked      = "parked"      // canonical BUMP unavailable at the cap; stamped, txs left MINED@orphan
	outcomeMixed       = "mixed"       // some txs re-anchored, some reverted
	outcomeReanchored  = "reanchored"  // every affected tx re-anchored to a canonical block
	outcomeReverted    = "reverted"    // every affected tx reverted to SEEN_ON_NETWORK
	outcomeEmpty       = "empty"       // nothing was anchored to the orphan
)

// Reconciler heals the transactions of orphaned blocks (issue #279): for
// every block_processing row with status='orphaned' and no reconciled_at,
// it re-anchors the txs still MINED against the orphan to the active-chain
// block that contains them (using the canonical block's already-stored
// compound BUMP — no external calls in the common case), reverts the txs
// proven to be in no canonical block to SEEN_ON_NETWORK, publishes corrected
// bulk status events, and stamps reconciled_at. When the canonical block's
// BUMP is unavailable it PARKS the block instead of reverting (issue #282):
// the txs stay MINED against the orphan until a later canonical BUMP can
// re-anchor them — un-mining a tx that is very likely in the not-yet-fetched
// canonical block would be a downgrade, not a repair.
//
// Detection is elsewhere: the chaintracks_server tracker marks blocks
// orphaned from ReorgEvents and its tie-scan, and bump-builder's anchor
// guard marks the blocks it refuses. The reconciler is remediation only —
// plus its startup full-scan, the deep backstop that also detects orphans
// predating this code (the height-764 incident block on the scaling
// cluster heals through exactly that path).
type Reconciler struct {
	services.ReadyHook

	cfg         *config.Config
	logger      *zap.Logger
	store       store.Store
	publisher   events.Publisher
	chainHeader ChainHeaderReader
	merkle      merkleservice.Service // nil ⇒ the /reprocess defer path is disabled
	leaser      store.Leaser          // nil ⇒ single-replica mode, no lease gating
	holderID    string

	// defers counts how many ticks each orphan has waited for its height's
	// canonical BUMP. In-memory by design: a restart resets the counts and
	// the worst case is a bounded number of duplicate /reprocess calls —
	// the same trade-off the watchdog makes with its attempt state.
	defers map[string]int

	// rewatched records, per orphan, the generation (orphaned_at) whose txs
	// have had their merkle-service watch refreshed — see rewatchOrphanTxs.
	// In-memory like defers: a restart re-sends one idempotent /watch per
	// anchored tx for each still-queued orphan, nothing worse.
	rewatched map[string]time.Time

	// startupScanDone flips true only after a startup full-scan that ran while
	// the chain-header source was synced to the store's active tip. Until then
	// the tick loop keeps re-attempting it (lease-gated) so the deep backstop
	// eventually runs once even if chaintracks was mid-resync at process start
	// and the initial readiness wait timed out. In-memory by design: a restart
	// re-arms the one-shot, which is idempotent (fullScan only marks, and marks
	// are convergent).
	startupScanDone bool

	// startupScanAttempts counts full-scan runs that ended incomplete; the
	// one-shot is retired at maxStartupFullScanAttempts. Touched only from
	// the reconcile goroutine, like startupScanDone.
	startupScanAttempts int

	// leaseTTLOverride replaces the derived lease TTL (3× the tick interval,
	// floored at a minute). Tests only — it makes the renewal heartbeat run
	// at an observable cadence.
	leaseTTLOverride time.Duration

	// pendingRemine holds canonical blocks (hash → height) whose own re-mine
	// failed part-way AND whose durable hand-off (keepPartialRemineQueued)
	// then failed too, after retries: the row is active and off every
	// in-store queue with only some of its txs re-mined, and nothing in the
	// store would revisit it. Each tick drains this set before the store
	// queue — re-mining directly while the block is canonical, or landing
	// the hand-off — until it is empty. In-memory only: it exists for the
	// window in which the store is refusing single-row writes, and the
	// moment the store recovers a tick either heals the block or hands it
	// to the durable queue. It does not survive a restart; the /reprocess
	// redelivery requested at hand-off time is the only cover then, and it
	// is best-effort. Bounded by maxPendingRemines. Touched only from the
	// reconcile goroutine, like defers.
	pendingRemine map[string]uint64

	now    func() time.Time
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReconciler wires the anchor reconciler. Returns nil (service skipped)
// when disabled by config or when no chain-header source exists — without
// chaintracks there is no canonicality oracle, which is the same condition
// under which the anchor guard and tie-scan are inert, so the process
// degrades to legacy behavior as one coherent unit.
func NewReconciler(
	cfg *config.Config,
	logger *zap.Logger,
	st store.Store,
	publisher events.Publisher,
	chainHeader ChainHeaderReader,
	merkle merkleservice.Service,
	leaser store.Leaser,
) *Reconciler {
	if !cfg.BumpBuilder.Reconciler.Enabled {
		logger.Info("anchor reconciler disabled by config")
		return nil
	}
	if chainHeader == nil {
		logger.Info("anchor reconciler skipped: no chaintracks header source (enable chaintracks_server)")
		return nil
	}
	host, _ := os.Hostname()
	return &Reconciler{
		cfg:           cfg,
		logger:        logger.Named("anchor-reconciler"),
		store:         st,
		publisher:     publisher,
		chainHeader:   chainHeader,
		merkle:        merkle,
		leaser:        leaser,
		holderID:      fmt.Sprintf("%s-%d", host, os.Getpid()),
		defers:        make(map[string]int),
		rewatched:     make(map[string]time.Time),
		pendingRemine: make(map[string]uint64),
		now:           time.Now,
		done:          make(chan struct{}),
	}
}

// Name implements services.Service.
func (r *Reconciler) Name() string { return "anchor-reconciler" }

// Start runs the reconcile loop until ctx is canceled. The optional startup
// full-scan runs once (lease-gated), but only after the chain-header source
// has synced up to the store's active tip — see waitForChaintracksReady. If it
// is still catching up when the bounded wait elapses, the scan is deferred to a
// later tick rather than run against a header source that would fail open on
// every row (issue #279 follow-up).
func (r *Reconciler) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)
	defer close(r.done)

	interval := time.Duration(r.cfg.BumpBuilder.Reconciler.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// The startup full-scan waits on chaintracks (default two minutes) and
	// is explicitly allowed to defer. It is recovery work, not a gate on
	// serving: holding the process readiness probe for it would keep an
	// all-in-one pod's API out of service for that whole wait. This matches
	// the API server, which is ready once it is listening while its
	// background refreshers are still running.
	r.SignalReady()

	if r.cfg.BumpBuilder.Reconciler.StartupFullScan && r.acquireLease(ctx) {
		if r.waitForChaintracksReady(ctx) {
			r.startupScanUnderLease(ctx)
		} else {
			r.logger.Warn("startup full-scan: chain-header source not ready within timeout; "+
				"deferring to a later tick (self-heals once chaintracks catches up to the active tip)",
				zap.Int("timeout_ms", r.cfg.BumpBuilder.Reconciler.FullScanChaintracksReadyTimeoutMs))
		}
	}
	r.tick(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if r.leaser != nil {
				releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				_ = r.leaser.Release(releaseCtx, ReconcilerLeaseName, r.holderID)
				cancel()
			}
			return nil
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// Stop implements services.Service.
func (r *Reconciler) Stop() error {
	if r.cancel != nil {
		r.cancel()
	}
	<-r.done
	return nil
}

// leaseTTL is how long one TryAcquireOrRenew holds the reconciler lease: 3×
// the tick interval, floored at a minute, so a lease survives two missed
// ticks before another replica may take it.
func (r *Reconciler) leaseTTL() time.Duration {
	if r.leaseTTLOverride > 0 {
		return r.leaseTTLOverride
	}
	interval := time.Duration(r.cfg.BumpBuilder.Reconciler.IntervalMs) * time.Millisecond
	ttl := 3 * interval
	if ttl < time.Minute {
		ttl = time.Minute
	}
	return ttl
}

// tryLease acquires or renews the reconciler lease and reports the expiry
// the store granted, plus whether this replica leads. A nil leaser means
// single-replica mode (always lead, zero expiry); a lease error is reported
// as not leading.
func (r *Reconciler) tryLease(ctx context.Context) (time.Time, bool) {
	if r.leaser == nil {
		return time.Time{}, true
	}
	heldUntil, err := r.leaser.TryAcquireOrRenew(ctx, ReconcilerLeaseName, r.holderID, r.leaseTTL())
	if err != nil {
		r.logger.Warn("lease check failed, skipping tick", zap.Error(err))
		return time.Time{}, false
	}
	return heldUntil, !heldUntil.IsZero()
}

// acquireLease reports whether this replica currently leads.
func (r *Reconciler) acquireLease(ctx context.Context) bool {
	_, ok := r.tryLease(ctx)
	return ok
}

// startupScanUnderLease runs the startup full-scan once the chain-header
// source is ready. The readiness wait can outlast the lease TTL (default: up
// to 2 min of waiting against a 90 s TTL), so leadership is re-asserted
// first — without this two replicas can run the full-scan at once,
// convergent but double-publishing MINED corrections now that the scan
// re-mines and reactivates rather than only marking — and the scan runs
// under the heartbeat, since it can outlive the TTL by minutes (see
// holdLease).
func (r *Reconciler) startupScanUnderLease(ctx context.Context) {
	heldUntil, ok := r.tryLease(ctx)
	if !ok {
		r.logger.Info("startup full-scan: lease lost while waiting for the chain-header source; " +
			"deferring to a later tick")
		return
	}
	leaseCtx, release := r.holdLease(ctx, heldUntil)
	defer release()
	r.runStartupFullScan(leaseCtx)
}

// holdLease keeps the reconciler lease renewed for as long as the returned
// context lives, and cancels that context if the lease is lost. The caller
// must already hold the lease — heldUntil is the expiry that acquisition
// reported — and must call the returned CancelFunc when its pass ends.
//
// The lease is acquired once per tick, but a pass can outlive the TTL by a
// wide margin: the full-scan re-mines a large BUMP for minutes and a tick's
// reconcileBlock does the same for a big canonical block, against a TTL of
// 3× the tick interval (90 s by default). Without renewal another replica
// acquires the lease mid-pass and runs the same work concurrently —
// convergent, but it double-publishes MINED corrections and events (issue
// #339 review). So a heartbeat renews at TTL/3, the cadence store.Leaser
// documents, and cancels the pass when the lease is gone: when a renewal
// reports another holder, or when renewals keep failing past the last
// confirmed expiry (a single failed renewal inside a live TTL is retried,
// not fatal). That expiry is always the one the store granted, never a
// local now+TTL: a slow acquire or a skewed backend clock means the real
// lease lapses earlier than the local estimate, and a run of renewal errors
// would otherwise keep the pass alive past the point another replica could
// have taken the lease. Cancellation makes the store calls in flight return
// ctx errors, so the full-scan reports itself incomplete and stays armed and
// the tick's block stays queued — for whichever replica now leads. A nil
// leaser (single-replica mode) returns ctx unchanged.
//
// Two guards make that hold regardless of whether the renewal RPC ever
// returns. Each renewal runs under a deadline of min(heldUntil, now+TTL/3),
// so a renewal that hangs — a pool wait, a network or database stall; the
// Postgres leaser hands the context straight to pgx with no deadline of its
// own — comes back with DeadlineExceeded no later than the lease's expiry,
// and earlier when a retry inside the TTL is still possible. Independently
// of that, an expiry timer armed for heldUntil cancels the pass when it
// fires; every successful renewal moves it to the new expiry. The timer runs
// on its own goroutine, so it fires even while the heartbeat is blocked
// inside the RPC, and a renewal that returns late and successfully after it
// fired cannot resurrect the pass: leaseCtx is already cancelled, and the
// heartbeat re-checks it before trusting any result.
func (r *Reconciler) holdLease(ctx context.Context, heldUntil time.Time) (context.Context, context.CancelFunc) {
	if r.leaser == nil {
		return ctx, func() {}
	}
	ttl := r.leaseTTL()
	beat := ttl / 3
	leaseCtx, cancel := context.WithCancel(ctx)
	// The independent expiry: fires at heldUntil unless a successful renewal
	// moved it. Stopped when the pass ends so it cannot outlive it.
	expiry := time.AfterFunc(time.Until(heldUntil), func() {
		r.logger.Error("lease expired with no successful renewal; abandoning the in-flight pass " +
			"so it cannot overlap another replica's")
		cancel()
	})
	go func() {
		defer expiry.Stop()
		ticker := time.NewTicker(beat)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
			}
			// Bound the RPC: never past the lease, and no longer than one
			// beat when that leaves room for another attempt inside it.
			deadline := heldUntil
			if next := time.Now().Add(beat); next.Before(deadline) {
				deadline = next
			}
			renewCtx, renewCancel := context.WithDeadline(leaseCtx, deadline)
			until, err := r.leaser.TryAcquireOrRenew(renewCtx, ReconcilerLeaseName, r.holderID, ttl)
			renewCancel()
			if leaseCtx.Err() != nil {
				return // the pass ended (or the expiry fired) while the renewal was in flight
			}
			switch {
			case err != nil && time.Now().Before(heldUntil):
				r.logger.Warn("lease renewal failed; the pass continues until the last confirmed expiry",
					zap.Time("held_until", heldUntil), zap.Error(err))
				continue
			case err != nil:
				r.logger.Error("lease renewal kept failing past the lease expiry; abandoning the in-flight pass "+
					"so it cannot overlap another replica's", zap.Error(err))
			case until.IsZero():
				r.logger.Warn("lease taken by another replica mid-pass; abandoning the in-flight pass")
			default:
				heldUntil = until
				// A Stop that returns false means the timer fired while the
				// renewal was in flight: leaseCtx is cancelled, and the next
				// iteration's Done check ends the heartbeat. Re-arming then is
				// harmless (a second cancel is a no-op), so no special case.
				expiry.Stop()
				expiry.Reset(time.Until(heldUntil))
				continue
			}
			cancel()
			return
		}
	}()
	return leaseCtx, cancel
}

// tick consumes one batch of the orphaned-block queue.
func (r *Reconciler) tick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	heldUntil, ok := r.tryLease(ctx)
	if !ok {
		return
	}
	// Everything below runs under the lease-scoped context: the heartbeat
	// renews the lease for as long as the pass takes and cancels the pass if
	// the lease is lost, so a long re-mine never runs alongside another
	// replica's.
	ctx, release := r.holdLease(ctx, heldUntil)
	defer release()
	// Self-heal the startup full-scan: if it was deferred because the embedded
	// chaintracks was still resyncing from genesis at process start (its
	// storage is ephemeral, so every deploy wipes headers), run it now that the
	// lease is held and chaintracks has caught up to the active tip. Guarded by
	// startupScanDone so it runs at most once successfully; short-circuits
	// before the readiness probe once done or when the scan is disabled, so
	// steady-state ticks pay nothing.
	if r.cfg.BumpBuilder.Reconciler.StartupFullScan && !r.startupScanDone && r.chaintracksReady(ctx) {
		r.runStartupFullScan(ctx)
		if ctx.Err() != nil {
			return // the lease was lost (or the process is stopping) during the scan
		}
	}
	// Blocks only this process remembers come first: nothing in the store
	// will retry them, and they are canonical blocks with a partial tx set.
	r.drainPendingRemines(ctx)
	if ctx.Err() != nil {
		return
	}
	blocksPerTick := r.cfg.BumpBuilder.Reconciler.BlocksPerTick
	if blocksPerTick <= 0 {
		blocksPerTick = 4
	}
	rows, err := r.store.ListOrphanedBlocksToReconcile(ctx, blocksPerTick)
	if err != nil {
		r.logger.Warn("failed to list orphaned blocks", zap.Error(err))
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		start := r.now()
		outcome := r.reconcileBlock(ctx, row)
		metrics.ReconcilerBlocksTotal.WithLabelValues(outcome).Inc()
		metrics.ReconcilerBlockDuration.Observe(time.Since(start).Seconds())
	}
}

// runStartupFullScan runs the one-shot startup full-scan and retires it only
// once a scan actually completes. An incomplete scan (paging error, or a
// repair whose re-mine or write failed) leaves rows that need healing still
// orphaned — and, when they were already stamped reconciled, off the tick's
// durable queue — so retiring the one-shot there would strand exactly the
// deep incident the scan exists for until someone restarts the process
// (issue #339 review). Later ticks retry, capped so a persistently failing
// store cannot re-page the window on every tick forever.
func (r *Reconciler) runStartupFullScan(ctx context.Context) {
	if r.fullScan(ctx) {
		r.startupScanDone = true
		return
	}
	if ctx.Err() != nil {
		// Shutdown, or the lease heartbeat abandoned the pass: not a failed
		// attempt, so it does not count toward the cap. The one-shot stays
		// armed for whichever replica leads next.
		r.logger.Warn("startup full-scan: interrupted; leaving the one-shot armed", zap.Error(ctx.Err()))
		return
	}
	r.startupScanAttempts++
	if r.startupScanAttempts >= maxStartupFullScanAttempts {
		r.logger.Error("startup full-scan: still incomplete at the attempt cap; retiring the one-shot. "+
			"Rows needing repair remain orphaned — re-run with a targeted "+
			"BSV_BROADCASTER_BUMP_BUILDER_RECONCILER_FULL_SCAN_{MIN,MAX}_HEIGHT window once the cause is cleared",
			zap.Int("attempts", r.startupScanAttempts),
			zap.Int("attempt_cap", maxStartupFullScanAttempts))
		r.startupScanDone = true
		return
	}
	r.logger.Warn("startup full-scan: incomplete; retrying on a later tick",
		zap.Int("attempts", r.startupScanAttempts),
		zap.Int("attempt_cap", maxStartupFullScanAttempts))
}

// fullScan pages the block_processing table and re-judges every 'active'
// and 'orphaned' row within the scan bounds against the active chain:
// 'active' rows whose height is provably held by a different block are
// orphan-marked (then consumed by the normal tick), and 'orphaned' rows that
// ARE the active-chain block at their height are reset to active — including
// rows already stamped reconciled, which the tick's queue can never revisit
// (issue #339) — and re-mined from their retained compound BUMP so the heal
// does not depend on a competitor's row existing. It is the deep backstop
// that picks up transitions predating the detection edges (guard, tie-scan,
// ReorgEvents) and the operator lever for an old incident.
//
// The scan is BOUNDED (issue #282): rather than re-judging the entire
// history oldest-first (which on a long chain grinds through hundreds of
// historical competition losers, starving the actual recent incident), it
// considers only heights within FullScanDepth of the active tip — or an
// explicit FullScan{Min,Max}Height range for operator-targeted recovery.
//
// Returns whether the scan COMPLETED: it paged the whole window and every
// repair it decided on was applied. A false return means at least one row
// that needs healing is still orphaned — and, if it was already stamped
// reconciled, off the tick's durable queue — so the caller must retry rather
// than retire the one-shot (issue #339 review).
func (r *Reconciler) fullScan(ctx context.Context) bool {
	const page = 1000
	minHeight, maxHeight, mode := r.fullScanBounds(ctx)
	r.logger.Info("startup full-scan: scanning",
		zap.String("bound_mode", mode),
		zap.Uint64("min_height", minHeight),
		zap.Uint64("max_height", maxHeight))
	// Sets, not slices: the boundary-safe pager may visit a row more than
	// once at page boundaries (see store.ForEachBlockProcessing); the first
	// judgement of a hash wins. Parked rows belong to the watchdog and are
	// never judged here.
	markedSet := make(map[string]uint64) // hash → height, for the pre-write re-check
	resurrectSet := make(map[string]*models.BlockProcessingStatus)
	// Candidate rows the chain-header source could not judge. Absence of
	// evidence never orphans or reactivates a row, but it is not a verdict
	// either: a transient per-height read failure, or an embedded
	// chaintracks a few headers behind the store's tip, must leave the
	// one-shot armed rather than retire it with a stamped orphan untouched.
	unjudged := 0
	err := store.ForEachBlockProcessing(ctx, r.store, minHeight, page, func(row *models.BlockProcessingStatus) error {
		if (row.Status != models.BlockStatusActive && row.Status != models.BlockStatusOrphaned) ||
			row.BlockHeight == 0 || row.BlockHeight > math.MaxUint32 {
			return nil
		}
		if maxHeight > 0 && row.BlockHeight > maxHeight {
			return nil // above the targeted range's upper bound
		}
		if _, seen := markedSet[row.BlockHash]; seen {
			return nil
		}
		if _, seen := resurrectSet[row.BlockHash]; seen {
			return nil
		}
		// Through activeHashAt, the one place the header source's answers
		// are classified. Absence and failure both leave the row unjudged
		// here — the scan retries either way — but neither is ever evidence.
		canonical, found, hdrErr := r.activeHashAt(ctx, row.BlockHeight)
		if hdrErr != nil || !found {
			unjudged++
			return nil //nolint:nilerr // fail-open by contract: never judge on absence of evidence
		}
		matches := canonical == row.BlockHash
		switch {
		case row.Status == models.BlockStatusActive && !matches:
			markedSet[row.BlockHash] = row.BlockHeight
		case row.Status == models.BlockStatusOrphaned && matches:
			resurrectSet[row.BlockHash] = row
		}
		return nil
	})
	complete := true
	if err != nil {
		r.logger.Warn("startup full-scan: incomplete", zap.Error(err))
		// Whatever was found before the failure still routes into healing,
		// but the window was not fully judged: keep the one-shot armed.
		complete = false
	}
	if unjudged > 0 {
		r.logger.Warn("startup full-scan: some rows could not be judged against the active chain; "+
			"keeping the one-shot armed for a retry",
			zap.Int("unjudged_rows", unjudged))
		complete = false
	}
	if len(markedSet) == 0 && len(resurrectSet) == 0 {
		r.logger.Info("startup full-scan: no stale anchors found")
		return complete
	}
	// Both halves always run; && would skip the resurrections whenever the
	// orphan marking failed.
	markedOK := r.fullScanMarkOrphaned(ctx, markedSet)
	resurrectedOK := r.fullScanResurrect(ctx, resurrectSet)
	return complete && markedOK && resurrectedOK
}

// fullScanMarkOrphaned demotes the off-chain 'active' rows the scan found,
// reporting whether the scan is complete for them: the write landed, and
// every candidate could be judged.
//
// Candidates were judged during the paging walk, which can take a while,
// and MarkBlocksOrphaned has no canonicality guard of its own — so each one
// is re-checked against the active chain immediately before the write. A
// reorg during the walk can have made a candidate canonical again; marking
// it on that stale evidence would demote a canonical row (and regress a
// reactivation the tracker may have applied meanwhile). Only rows the
// header source positively places off-chain NOW are marked; a row it can no
// longer judge — a lookup failure, or a height it cannot serve — is left
// alone and the scan reported incomplete so it is retried: absence of
// evidence never orphans.
func (r *Reconciler) fullScanMarkOrphaned(ctx context.Context, set map[string]uint64) bool {
	if len(set) == 0 {
		return true
	}
	complete := true
	marked := make([]string, 0, len(set))
	for hash, height := range set {
		canonical, found, hdrErr := r.activeHashAt(ctx, height)
		switch {
		case hdrErr != nil:
			r.logger.Warn("startup full-scan: chain-header lookup failed before the orphan write; "+
				"leaving the row for the next scan",
				logfields.BlockHash(hash), logfields.BlockHeight(height), zap.Error(hdrErr))
			complete = false
		case !found:
			r.logger.Warn("startup full-scan: chain-header source can no longer judge the row's height; "+
				"leaving it for the next scan",
				logfields.BlockHash(hash), logfields.BlockHeight(height))
			complete = false
		case canonical == hash:
			r.logger.Info("startup full-scan: candidate became the active-chain block at its height during "+
				"the walk; not orphaning it",
				logfields.BlockHash(hash), logfields.BlockHeight(height))
		default:
			marked = append(marked, hash)
		}
	}
	if len(marked) == 0 {
		return complete
	}
	applied, err := r.store.MarkBlocksOrphaned(ctx, marked, r.now())
	// Observed before the error check: the backends report the transitions
	// that landed before a failure alongside the error, and those rows are
	// orphaned in the store regardless of how the call ended.
	recordOrphanTransitions(metrics.BlockTransitionSourceFullScan, applied)
	if err != nil {
		r.logger.Warn("startup full-scan: failed to mark orphaned",
			zap.Int("applied_before_failure", applied), zap.Error(err))
		return false
	}
	r.logger.Info("startup full-scan: marked off-chain blocks orphaned",
		zap.Strings("block_hashes", marked))
	return complete
}

// recordOrphanTransitions adds applied orphaned transitions to the
// block-status metric for the given detection edge. Callers pass the count
// MarkBlocksOrphaned returned whether or not it also returned an error: the
// count is what landed.
func recordOrphanTransitions(source string, applied int) {
	if applied > 0 {
		metrics.BlockStatusTransitionsTotal.
			WithLabelValues(metrics.BlockTransitionOrphaned, source).
			Add(float64(applied))
	}
}

// requeueForTick puts a resurrect candidate that is off the tick's durable
// queue (stamped by a previous reconciliation) back on it, as a
// compare-and-set on the generation the paging walk read:
// RequeueOrphanedBlock clears reconciled_at only while the row is still
// orphaned with that generation, and never transitions a row. That matters
// because the tracker runs alongside this scan and may legitimately have
// reactivated this canonical row between the walk's read and now — a
// MarkBlocksOrphaned here would flip that active row back to orphaned. A row
// that is no longer the judged orphan needs nothing from us: reactivated,
// it is where the repair wanted it; re-orphaned with a newer generation, it
// is already queued (MarkBlocksOrphaned clears the stamp). So a not-applied
// result is a benign hand-off, and only a store error returns false.
func (r *Reconciler) requeueForTick(ctx context.Context, logger *zap.Logger, row *models.BlockProcessingStatus) bool {
	applied, err := r.store.RequeueOrphanedBlock(ctx, row.BlockHash, row.OrphanGeneration())
	if err != nil {
		logger.Error("startup full-scan: failed to requeue the block for the reconciler", zap.Error(err))
		return false
	}
	if !applied {
		logger.Info("startup full-scan: row changed since it was judged (reactivated, or re-orphaned with a " +
			"newer generation); nothing to requeue")
	}
	return true
}

// fullScanResurrect re-mines each resurrected block's txs from its retained
// compound BUMP — the same fuel reconcileBlock burns for a canonical block,
// with onlyChanged so rows already anchored right produce no events — and
// then resets the row to active through ReactivateBlock, a compare-and-set
// on the orphan generation the paging walk read (it also clears
// orphaned_at/reconciled_at and preserves the milestone timestamps). The
// re-mine runs FIRST: a failed batch leaves the row orphaned so the next
// full-scan retries the whole heal, whereas reactivating first would leave
// the txs with nothing left to revisit them. Reports whether every row in
// the set was healed or handed off, so a failed repair keeps the one-shot
// armed.
func (r *Reconciler) fullScanResurrect(ctx context.Context, set map[string]*models.BlockProcessingStatus) bool {
	if len(set) == 0 {
		return true
	}
	complete := true
	batchSize := r.cfg.BumpBuilder.Reconciler.BatchSize
	if batchSize <= 0 {
		batchSize = maxTxIDsPerBulkEvent
	}
	resurrected := make([]string, 0, len(set))
	for hash, row := range set {
		height := row.BlockHeight
		logger := r.logger.With(logfields.BlockHash(hash), logfields.BlockHeight(height))
		n, ok, remineErr := r.remineWithRetry(ctx, logger, hash, batchSize)
		switch {
		case errors.Is(remineErr, errMalformedBUMP):
			// The block's own stored BUMP is corrupt. Retrying the scan
			// cannot fix that, and once the attempt cap retired the one-shot
			// the row would sit orphaned, stamped and off every queue. So
			// hand it to the tick: requeued, the row reaches the
			// resurrection short-circuit, whose own re-mine hits the same
			// corrupt BUMP and takes the deferral path — the row stays
			// queued and /reprocess is asked to redeliver the block, which
			// rebuilds the BUMP — until the BUMP parses (see resurrect).
			// That is a durable retry with its own cap. The hand-off is
			// generation-aware: if the tracker reactivated the row since the
			// walk read it, a plain requeue would find nothing to do and
			// leave an ACTIVE row with a corrupt BUMP, un-remined txs and no
			// rebuild requested. keepRemineRetried holds such a row in the
			// pending set instead — it stays active (nothing was written, and
			// this path must never flip a canonical row back to orphaned),
			// the rebuild is requested, and the drain keeps asking under the
			// defer cap. The scan is not incomplete for the row unless the
			// hand-off itself failed, in which case the pending set holds it
			// anyway and the scan retries too.
			logger.Error("startup full-scan: resurrected block's stored BUMP is malformed; "+
				"handing the row to the reconciler so its deferral drives the rebuild", zap.Error(remineErr))
			if !r.keepRemineRetried(ctx, logger, hash, height, row.OrphanGeneration()) {
				metrics.ReconcilerRemineHandoffFailedTotal.Inc()
				r.holdPendingRemine(ctx, logger, hash, height)
				complete = false
			}
			continue
		case remineErr != nil && ok:
			// Failed part-way after the in-process retries, and the tracker
			// may have reactivated the row while the batches ran — in which
			// case a scan retry would no longer select it (active and
			// canonical). Hand the remainder to a path that retries it
			// whatever the row's state; see keepPartialRemineQueued. The
			// repair then has an owner, so the scan is not incomplete for it
			// unless that hand-off itself failed.
			metrics.ReconcilerTxsReanchoredTotal.Add(float64(n))
			logger.Error("startup full-scan: re-mine from the resurrected block's BUMP failed part-way; "+
				"handing the remainder to the reconciler", zap.Int("txs_reanchored", n), zap.Error(remineErr))
			if !r.keepPartialRemineQueued(ctx, logger, hash, row.OrphanGeneration()) {
				// The store refused the hand-off too. The scan retries (it
				// re-selects the row while it is still orphaned), and the
				// pending set covers the case where it is not.
				metrics.ReconcilerRemineHandoffFailedTotal.Inc()
				r.holdPendingRemine(ctx, logger, hash, height)
				complete = false
			}
			continue
		case remineErr != nil:
			// Nothing was read, so nothing was written — but "the row is as
			// the walk found it" is not a given: the tracker may have
			// reactivated this canonical row since the paging read, and a
			// scan retry would then skip it (active rows are never
			// candidates) with its txs never re-mined, until the attempt cap
			// retired the one-shot. So the row is re-checked at the
			// generation the walk read and handed to a path that retries it
			// whatever its state: still ours → requeued for the tick;
			// reactivated → held in the pending set, where the drain re-mines
			// it directly while it is canonical (nothing was written, so
			// unlike the partial-re-mine branch there is no reason to flip
			// the canonical row back to orphaned); hand-off refused → the
			// pending set as well. The scan ALSO stays incomplete, so it
			// retries the read itself while the row is still orphaned.
			logger.Error("startup full-scan: could not read the resurrected block's BUMP; "+
				"handing the row to the reconciler and retrying the scan", zap.Error(remineErr))
			if !r.keepRemineRetried(ctx, logger, hash, height, row.OrphanGeneration()) {
				metrics.ReconcilerRemineHandoffFailedTotal.Inc()
				r.holdPendingRemine(ctx, logger, hash, height)
			}
			complete = false
			continue
		}
		// The canonical judgement that put this hash in the set was made
		// during the paging walk, and the re-mine above can run for minutes.
		// Re-read the chain immediately before the write: a reorg during the
		// repair would otherwise have this scan reactivate a row that is now
		// genuinely orphaned AND clear the fresh orphan generation, taking
		// it off the reconcile queue with nothing left to revisit it.
		canonical, canonicalKnown, hdrErr := r.activeHashAt(ctx, height)
		if hdrErr != nil {
			logger.Warn("startup full-scan: chain-header lookup failed before the reactivation; "+
				"leaving the row orphaned for the next scan", zap.Error(hdrErr))
			complete = false
			continue
		}
		if !canonicalKnown {
			logger.Warn("startup full-scan: chain-header source can no longer judge the block's height; " +
				"leaving the row orphaned for the next scan")
			complete = false
			continue
		}
		if canonical != hash {
			// The re-mine above may have anchored txs to a block that is now
			// off-chain, and this row — stamped by its previous
			// reconciliation — is off the tick's durable queue, so nothing
			// would revisit them. Requeue it: with reconciled_at cleared it
			// is back on the queue, where the next tick re-anchors those txs
			// to the new canonical block (or reverts them) through the
			// ordinary reconcile path. The ReorgEvent the tracker records
			// for the same flip re-orphans it with a newer generation, which
			// requeues it too; either way the repair is handed off, so the
			// scan is not incomplete for it unless the requeue write itself
			// failed.
			logger.Warn("startup full-scan: block lost its height to a competitor during the re-mine; "+
				"requeueing it so the reconciler re-anchors its txs to the new canonical block",
				zap.String("canonical_block_hash", canonical))
			if !r.requeueForTick(ctx, logger, row) {
				// Its txs stay MINED against an off-chain block until a
				// ReorgEvent or the next scan revisits it.
				complete = false
			}
			continue
		}
		applied, err := r.store.ReactivateBlock(ctx, hash, height, row.OrphanGeneration())
		if err != nil {
			logger.Warn("startup full-scan: failed to reactivate resurrected block", zap.Error(err))
			complete = false
			continue
		}
		if !applied {
			// The row is no longer the one the walk judged: the tracker
			// reactivated it meanwhile (fine — its txs are re-mined either
			// way), or a ReorgEvent orphaned it again with a newer
			// generation, which cleared its stamp and put it back on the
			// tick's queue. Not a transition, and nothing left for this scan
			// to do with it.
			logger.Info("startup full-scan: row changed since it was judged; not reactivated")
			continue
		}
		r.forgetOrphan(hash)
		metrics.BlockStatusTransitionsTotal.
			WithLabelValues(metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceFullScan).
			Inc()
		resurrected = append(resurrected, hash)
		if !ok {
			logger.Warn("startup full-scan: resurrected block has no stored compound BUMP; " +
				"its txs heal through the competitor's orphan row or a BLOCK_PROCESSED redelivery")
			continue
		}
		metrics.ReconcilerTxsReanchoredTotal.Add(float64(n))
		if n > 0 {
			logger.Info("startup full-scan: re-mined txs against resurrected block", zap.Int("txs_reanchored", n))
		}
	}
	if len(resurrected) > 0 {
		r.logger.Warn("startup full-scan: reactivated resurrected canonical blocks that were marked orphaned",
			zap.Strings("block_hashes", resurrected))
	}
	return complete
}

// fullScanBounds resolves the height window the startup full-scan considers.
// An explicit FullScan{Min,Max}Height target (either > 0) wins — operator-
// pointed recovery, e.g. a known old incident. Otherwise the scan is bounded
// to within FullScanDepth of the active tip (store.GetActiveTipBlockHeight).
// Returns (0, 0) — unbounded, the pre-#282 behavior — when the tip is unknown
// or the chain is shorter than the horizon (a short table is cheap to sweep
// whole); this keeps the fail-open contract so a missing tip never hides an
// incident. maxHeight 0 means "no upper bound".
func (r *Reconciler) fullScanBounds(ctx context.Context) (minHeight, maxHeight uint64, mode string) {
	rc := r.cfg.BumpBuilder.Reconciler
	if rc.FullScanMinHeight > 0 || rc.FullScanMaxHeight > 0 {
		return rc.FullScanMinHeight, rc.FullScanMaxHeight, "targeted"
	}
	depth := rc.FullScanDepth
	if depth <= 0 {
		depth = defaultFullScanDepth
	}
	tip, err := r.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		r.logger.Warn("startup full-scan: active-tip lookup failed; scanning unbounded", zap.Error(err))
		return 0, 0, "unbounded"
	}
	if tip == 0 || uint64(depth) >= tip {
		return 0, 0, "unbounded"
	}
	return tip - uint64(depth), 0, "horizon"
}

// chaintracksReady reports whether the chain-header source has caught up to the
// store's active tip — the precondition for a MEANINGFUL startup full-scan. In
// the bump-builder deployment r.chainHeader is an embedded chaintracks with
// ephemeral storage that resyncs from genesis on every deploy; until it reaches
// the tip, GetHeaderByHeight returns nil for every height and the scan would
// fail open on every row (marking nothing) — silently breaking reorg recovery.
//
// Readiness is TRIVIALLY satisfied when the active tip is unknown/zero (nothing
// to compare against — preserves the pre-gate behavior on an empty table) or
// when the tip lookup fails (the scan itself still fails open per-row, so a
// store hiccup never blocks it forever). Otherwise chaintracks is ready exactly
// when it can return a non-nil, non-error header at the tip height.
func (r *Reconciler) chaintracksReady(ctx context.Context) bool {
	tip, err := r.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		// A canceled/expired context is shutdown, not evidence — report
		// not-ready so a cancellation mid-wait never masquerades as "ready"
		// and triggers a scan on the way down. A genuine store error still
		// fails open (treat as ready; the scan itself fails open per-row).
		if ctx.Err() != nil {
			return false
		}
		r.logger.Warn("chaintracks readiness: active-tip lookup failed; treating as ready", zap.Error(err))
		return true
	}
	if tip == 0 || tip > math.MaxUint32 {
		return true
	}
	hdr, err := r.chainHeader.GetHeaderByHeight(ctx, uint32(tip))
	return err == nil && hdr != nil
}

// waitForChaintracksReady blocks — with bounded exponential backoff — until
// chaintracksReady reports true or the configured timeout elapses, whichever
// comes first. Returns true if chaintracks became ready, false on timeout or
// context cancellation. It NEVER blocks indefinitely: a false return is the
// caller's signal to defer the startup scan to the tick-loop self-heal rather
// than run it against a not-yet-synced header source.
func (r *Reconciler) waitForChaintracksReady(ctx context.Context) bool {
	timeout := time.Duration(r.cfg.BumpBuilder.Reconciler.FullScanChaintracksReadyTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultFullScanChaintracksReadyTimeout
	}
	const (
		initialBackoff = 250 * time.Millisecond
		maxBackoff     = 5 * time.Second
	)
	deadline := time.Now().Add(timeout)
	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return false
		}
		if r.chaintracksReady(ctx) {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		wait := backoff
		if wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// reconcileBlock heals one orphaned block O and returns the metric outcome.
//
// Ordering matters: re-anchor runs BEFORE revert, so txs moved to a
// canonical block have left O's index by the time SetStatusByBlockHash
// computes the remainder. Crash-resume is idempotent for the same reason —
// reconciled_at is stamped last, and a re-run finds only whatever the crash
// left anchored to O.
func (r *Reconciler) reconcileBlock(ctx context.Context, row *models.BlockProcessingStatus) string {
	orphan := row.BlockHash
	logger := r.logger.With(logfields.BlockHash(orphan))
	// The orphan generation this pass reconciles. The final stamp — and the
	// resurrection write below — are a CAS on it, so a row the block-status
	// tracker reactivated, or that was orphaned again, while this pass ran
	// is never marked reconciled or reactivated by stale work (issue #339).
	orphanedAt := row.OrphanGeneration()

	// Resolve the orphan's height — the row may be a height-0 placeholder
	// created by MarkBlockProcessed before any header arrived; the stored
	// BUMP knows the height. Only a positively missing BUMP leaves the
	// height unresolved: a transient read failure would otherwise make the
	// canonical block unidentifiable below, and the pass would park the
	// row — stamping it off the queue with its txs still anchored to the
	// orphan — over a hiccup. The block stays queued instead.
	height := row.BlockHeight
	if height == 0 {
		h, _, err := r.store.GetBUMP(ctx, orphan)
		switch {
		case err == nil:
			height = h
		case errors.Is(err, store.ErrNotFound):
			// A placeholder with no BUMP either: nothing resolves its height.
		default:
			logger.Warn("failed to read the orphan's BUMP to resolve its height; leaving the block queued for retry",
				zap.Error(err))
			return outcomeError
		}
	}

	batchSize := r.cfg.BumpBuilder.Reconciler.BatchSize
	if batchSize <= 0 {
		batchSize = maxTxIDsPerBulkEvent
	}

	// The active-chain block at O's height. A lookup FAILURE is not a
	// judgement: with no canonical hash the pass below would find nothing
	// to re-anchor to and park the row — stamped, off the queue, txs still
	// anchored to the orphan — so the block stays queued and the next tick
	// retries. Absence (chaintracks cannot serve the height) keeps the
	// pre-existing fail-open path.
	canonicalHash, canonicalKnown, hdrErr := r.activeHashAt(ctx, height)
	if hdrErr != nil {
		logger.Warn("chain-header lookup failed; leaving the block queued for retry",
			logfields.BlockHeight(height), zap.Error(hdrErr))
		return outcomeError
	}

	// Resurrection short-circuit: the block is the active-chain block at
	// its height again (flip-flop). Re-mine its txs from its own retained
	// BUMP — the same heal the full-scan applies, onlyChanged so rows already
	// anchored right produce no events — and reset the row to active (the
	// write also clears orphaned_at/reconciled_at). The COMPETITOR is now
	// the orphan and heals through its own row; the re-mine here is what
	// makes the heal independent of that row existing, and what makes a
	// requeued row with a corrupt BUMP cycle through the deferral below
	// rather than be reactivated with its txs never re-mined.
	if canonicalKnown && canonicalHash == orphan {
		logger.Warn("orphan mark is stale — block is on the active chain; re-mining and resetting to active")
		return r.resurrect(ctx, logger, orphan, height, orphanedAt, batchSize)
	}

	// Keep merkle-service watching O's txs before anything below can defer
	// or park: parking relies on a LATER block's STUMPs re-anchoring them,
	// and merkle-service stops reporting a tx postMineTTL (30 min) after it
	// first saw it mined — O's own mine (mainnet 973522, 2026-10-31).
	r.rewatchOrphanTxs(ctx, logger, orphan, orphanedAt)

	// The canonical block at O's height: re-mine its FULL BUMP set first.
	// This simultaneously (a) re-anchors O's txs that are in it, (b) mines
	// txs the anchor guard correctly refused while the canonical block was
	// still an alternate, and (c) is a no-op for rows already anchored
	// right (onlyChanged filters their events). Missing canonical BUMP ⇒
	// defer (optionally poking merkle-service /reprocess) up to the cap.
	reanchored := 0
	canonicalReady := false
	if canonicalKnown {
		n, bumpOK, remineErr := r.remineFromStoredBUMP(ctx, logger, canonicalHash, batchSize)
		// n is added either way: rows this pass really did re-anchor are
		// re-anchored whether or not the rest of the batch landed, and the
		// error/deferred returns below are the only paths that would
		// otherwise drop them from the metric.
		reanchored += n
		switch {
		case errors.Is(remineErr, errMalformedBUMP):
			// A corrupt canonical BUMP proves nothing, exactly like a missing
			// one, so fall through to the defer below: its /reprocess poke
			// has merkle-service redeliver the block, which rebuilds and
			// overwrites the BUMP — the one remedy for this state. Error
			// rather than Info because, unlike a BUMP that is merely not
			// stored yet, this does not fix itself.
			logger.Error("stored canonical BUMP is malformed; deferring and requesting a rebuild",
				zap.String("canonical_block_hash", canonicalHash), zap.Error(remineErr))
		case remineErr != nil:
			// A failed batch must not fall through to the revert below — that
			// would un-mine txs that ARE in the canonical block — nor stamp
			// the row. Leave it queued; the next tick retries.
			metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
			logger.Warn("re-mine from the canonical BUMP failed; leaving the block queued for retry",
				zap.String("canonical_block_hash", canonicalHash),
				zap.Int("txs_reanchored", reanchored),
				zap.Error(remineErr))
			return outcomeError
		default:
			canonicalReady = bumpOK
		}
	}
	if !canonicalReady && canonicalHash != "" {
		if r.deferForCanonicalBUMP(ctx, logger, orphan, canonicalHash) {
			metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
			return outcomeDeferred
		}
	}

	// Deep-reorg neighborhood: txs re-binned into LATER canonical blocks.
	// Only the txs still anchored to O are candidates; membership is an
	// O(1) lookup against each neighbor's stored compound BUMP. This runs
	// regardless of the height-O canonical BUMP so we still re-anchor every
	// tx we CAN prove before deciding the remainder's fate.
	affected, err := r.store.GetTxIDsByBlockHash(ctx, orphan)
	if err != nil {
		logger.Warn("failed to read affected txids", zap.Error(err))
		return outcomeError
	}
	n, neighborErr := r.reanchorNeighborhood(ctx, logger, affected, height, batchSize)
	reanchored += n
	if neighborErr != nil {
		// Leave reconciled_at unstamped and the block queued, exactly as a
		// failed GetTxIDsByBlockHash above does. Neither the park nor the
		// revert below is safe here: park would retire the block with rows
		// stranded MINED@orphan that a neighbour's BUMP proves belong
		// elsewhere, and revert would un-mine them outright.
		metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
		logger.Warn("neighborhood re-anchor failed; leaving the block queued for retry",
			zap.Int("txs_reanchored", reanchored), zap.Error(neighborErr))
		return outcomeError
	}

	// Park vs revert for whatever is still anchored to O (issue #282). When
	// the canonical block's BUMP was NOT available (canonicalReady == false),
	// we have no proof of where these txs belong — the canonical block simply
	// hasn't been fetched/rebuilt yet. Reverting them to SEEN_ON_NETWORK
	// would UN-MINE txs that are almost certainly in that not-yet-fetched
	// block: a downgrade, not a repair. So by default we PARK the block
	// instead — leave the txs MINED@O and stamp reconciled_at so it drops out
	// of the queue (bounded — the defer cap already fired; no infinite retry
	// and no further /reprocess spam). A later stored/rebuilt canonical BUMP
	// re-anchors them through the normal mine path (MINED@O → MINED@canonical
	// is lattice-legal). RevertWhenUnreconcilable restores the old revert-all
	// fallback as an opt-in.
	if !canonicalReady && !r.cfg.BumpBuilder.Reconciler.RevertWhenUnreconcilable {
		return r.parkBlock(ctx, logger, orphan, orphanedAt, height, canonicalHash, reanchored)
	}

	// Revert the remainder: the canonical BUMP WAS available and these txs
	// are provably in no canonical block (or the operator opted into the
	// legacy fallback) — SEEN_ON_NETWORK puts them back in flight
	// (rebroadcast/propagation owns them from here), and the store appends O
	// to their orphaned-anchor history.
	// Publish BEFORE the error check. SetStatusByBlockHash returns the rows it
	// rewrote alongside any error — a page that failed mid-walk, or a block
	// still taking mines faster than it can be retired — and those rows are
	// already SEEN_ON_NETWORK in the store. They leave the block's index with
	// that write, so a later retry will not find them again: if their event is
	// not published here it is never published, and subscribers keep believing
	// the txs are MINED. Publishing is idempotent for the subscriber, so the
	// only wrong move is to skip it.
	reverted, err := r.store.SetStatusByBlockHash(ctx, orphan, models.StatusSeenOnNetwork)
	r.publishReverted(ctx, logger, reverted)
	if err != nil {
		logger.Warn("failed to revert remaining txs", zap.Int("published", len(reverted)), zap.Error(err))
		return outcomeError
	}

	// Cleanup: STUMPs are per-subtree intermediates, safe to drop. The
	// compound BUMP is deliberately RETAINED — it serves the historical
	// orphanedProofs on GET /tx and is the store-local re-anchor fuel if
	// this block wins a later flip-flop.
	if delErr := r.store.DeleteStumpsByBlockHash(ctx, orphan); delErr != nil {
		logger.Warn("failed to clean up orphan STUMPs", zap.Error(delErr))
	}
	stamped, err := r.store.MarkBlockReconciled(ctx, orphan, orphanedAt, r.now())
	if err != nil {
		logger.Warn("failed to stamp reconciled_at", zap.Error(err))
		return outcomeError
	}
	r.forgetOrphan(orphan)
	// Observe the tx work BEFORE the stale check: those writes landed
	// regardless of whether this pass got to stamp the row.
	metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
	metrics.ReconcilerTxsRevertedTotal.Add(float64(len(reverted)))
	if !stamped {
		return r.staleOutcome(logger, height)
	}

	logger.Info(
		"orphaned block reconciled",
		logfields.BlockHeight(height),
		zap.String("canonical_block_hash", canonicalHash),
		zap.Int("txs_reanchored", reanchored),
		zap.Int("txs_reverted", len(reverted)),
	)
	switch {
	case reanchored > 0 && len(reverted) > 0:
		return outcomeMixed
	case reanchored > 0:
		return outcomeReanchored
	case len(reverted) > 0:
		return outcomeReverted
	default:
		return outcomeEmpty
	}
}

// resurrect is the tick's half of the #339 repair for a queued row whose
// block IS the active-chain block at its height: re-mine the block's txs
// from its own retained BUMP, then return the row to active with a CAS on
// the generation this pass dequeued (like the reconciled_at stamp: a row the
// tracker already reactivated, or that a reorg orphaned again with a newer
// generation in between, is left as it is and not counted).
//
// The re-mine's failure modes drive the outcome, so the row is never
// reactivated with its txs un-remined and no durable retry:
//   - no BUMP stored (ok=false): nothing to re-mine from; reactivate. The
//     txs heal through the competitor's orphan row or a BLOCK_PROCESSED
//     redelivery, as tracker-side reactivations rely on.
//   - errMalformedBUMP: the stored blob is corrupt and will not fix
//     itself. Do NOT reactivate; take the same deferral the canonical path
//     takes for an unusable BUMP — the row stays queued and /reprocess is
//     asked to redeliver the block, which rebuilds the BUMP — until the
//     BUMP parses or the defer cap is reached. At the cap the row IS
//     reactivated, loudly: the alternative (park: stamp and leave it
//     orphaned) would leave a canonical block orphaned, which is the bug
//     this PR exists to close; its txs stay on the redelivery path.
//   - any other error (transient read, failed batch): "error", row queued,
//     next tick retries.
func (r *Reconciler) resurrect(ctx context.Context, logger *zap.Logger, orphan string, height uint64, orphanedAt time.Time, batchSize int) string {
	n, ok, remineErr := r.remineWithRetry(ctx, logger, orphan, batchSize)
	switch {
	case errors.Is(remineErr, errMalformedBUMP):
		if r.deferForCanonicalBUMP(ctx, logger, orphan, orphan) {
			logger.Error("resurrected block's stored BUMP is malformed; deferring the reactivation and "+
				"requesting a rebuild so its txs are re-mined before the row returns to active",
				zap.Error(remineErr))
			return outcomeDeferred
		}
		logger.Error("resurrected block's stored BUMP is still malformed at the defer cap; reactivating the row "+
			"anyway so a canonical block is not left orphaned — its txs heal on a BLOCK_PROCESSED redelivery",
			zap.Error(remineErr))
	case remineErr != nil && ok:
		// The BUMP was read and the re-mine failed part-way, after the
		// in-process retries. Some txs are re-mined, the rest are not, and
		// "the row is still ours and queued" is not a given: the tracker may
		// have reactivated it while the batches were in flight. Hand the
		// remainder to a path that WILL retry it, whatever the row's state.
		metrics.ReconcilerTxsReanchoredTotal.Add(float64(n))
		logger.Warn("re-mine from the resurrected block's BUMP failed part-way",
			zap.Int("txs_reanchored", n), zap.Error(remineErr))
		if !r.keepPartialRemineQueued(ctx, logger, orphan, orphanedAt) {
			// The store refused the hand-off too: the row may be active and
			// off every in-store queue. Remember it here so the next tick
			// retries, regardless of what the store holds.
			metrics.ReconcilerRemineHandoffFailedTotal.Inc()
			r.holdPendingRemine(ctx, logger, orphan, height)
		}
		return outcomeError
	case remineErr != nil:
		// Nothing was read, so nothing was written: the row is exactly as
		// dequeued and stays queued for the next tick.
		logger.Warn("re-mine from the resurrected block's BUMP failed before any write; leaving the block queued for retry",
			zap.Error(remineErr))
		return outcomeError
	case !ok:
		logger.Warn("resurrected block has no stored compound BUMP; " +
			"its txs heal through the competitor's orphan row or a BLOCK_PROCESSED redelivery")
	default:
		metrics.ReconcilerTxsReanchoredTotal.Add(float64(n))
		if n > 0 {
			logger.Info("re-mined txs against the resurrected block", zap.Int("txs_reanchored", n))
		}
	}
	applied, err := r.store.ReactivateBlock(ctx, orphan, height, orphanedAt)
	if err != nil {
		logger.Warn("failed to reset resurrected block", zap.Error(err))
		return outcomeError
	}
	if !applied {
		return r.staleOutcome(logger, height)
	}
	r.forgetOrphan(orphan)
	metrics.BlockStatusTransitionsTotal.
		WithLabelValues(metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReconciler).
		Inc()
	return outcomeResurrected
}

// remineRetryAttempts bounds the in-process retries of a block's own re-mine
// (the resurrection paths) after a batch fails part-way. Immediate retries
// ride out a transient blip without a full tick of latency; anything longer
// is handed to a durable path by keepPartialRemineQueued.
const remineRetryAttempts = 3

// remineRetryBackoff separates those attempts.
const remineRetryBackoff = 50 * time.Millisecond

// remineWithRetry is remineFromStoredBUMP with bounded in-process retries of
// a re-mine that failed part-way (ok=true, err non-nil). It never retries a
// read failure or a malformed BUMP — neither is helped by trying again
// immediately — and stops on a cancelled context. The returned count sums
// the rows every attempt landed.
func (r *Reconciler) remineWithRetry(ctx context.Context, logger *zap.Logger, blockHash string, batchSize int) (int, bool, error) {
	total := 0
	for attempt := 1; ; attempt++ {
		n, ok, err := r.remineFromStoredBUMP(ctx, logger, blockHash, batchSize)
		total += n
		if err == nil || !ok || errors.Is(err, errMalformedBUMP) || attempt >= remineRetryAttempts || ctx.Err() != nil {
			return total, ok, err
		}
		logger.Warn("re-mine failed part-way; retrying in-process",
			zap.Int("attempt", attempt), zap.Int("max_attempts", remineRetryAttempts), zap.Error(err))
		select {
		case <-ctx.Done():
			return total, ok, err
		case <-time.After(remineRetryBackoff):
		}
	}
}

// handOffAttempts bounds the in-process retries of the single-row hand-off
// writes in keepPartialRemineQueued; handOffBackoff separates them. A blip
// is ridden out here; anything longer falls back to the pending set.
const (
	handOffAttempts = 3
	handOffBackoff  = 50 * time.Millisecond
)

// maxPendingRemines bounds the in-memory pending set. It only grows while
// the store is refusing single-row writes to blocks whose re-mine also
// failed part-way — a store outage, in which the queue reads that feed the
// tick fail too — so the bound is a safety net, not a working limit; a block
// that does not fit is logged at Error and left to the /reprocess
// redelivery requested for it.
const maxPendingRemines = 256

// keepPartialRemineQueued makes sure a block whose own re-mine failed
// part-way stays on a path that retries it, whatever happened to its row in
// the meantime. Reports whether it is: false means every hand-off write was
// refused through handOffAttempts tries, and the caller must hold the block
// in the pending set. The generation the caller judged decides:
//
//   - still ours (RequeueOrphanedBlock applies): the row is orphaned at that
//     generation and now unstamped — on the tick's durable queue, whose
//     resurrection short-circuit re-mines it (idempotently) next pass.
//   - re-orphaned with a newer generation: already queued for its own pass,
//     which re-mines it. Nothing to do.
//   - reactivated by the tracker mid-re-mine: the row is active and off the
//     queue with only some of its txs re-mined. The tracker's reactivation
//     leaves txs to the competitor's orphan row, which need not exist (a
//     dropped tip header), and the watchdog will not re-drive a block that
//     already has processed_at — so the reconciler's queue is the only
//     in-store path that WILL retry, and its predicate is status='orphaned'.
//     The row is re-orphaned with a fresh generation: the next tick's
//     short-circuit re-mines the remainder and reactivates it, so the
//     canonical block reads orphaned for at most one tick — longer only
//     while the store keeps failing, when an un-healed projection is the
//     honest state. Logged at Error and counted
//     (ReconcilerRemineRequeuedTotal, from the transition the write
//     reports, so a refused write counts nothing), and a /reprocess
//     redelivery is requested as well — belt and braces, never the only
//     path.
//   - parked or gone: the watchdog owns it / nothing to requeue.
func (r *Reconciler) keepPartialRemineQueued(ctx context.Context, logger *zap.Logger, hash string, orphanedAt time.Time) bool {
	for attempt := 1; ; attempt++ {
		done, err := r.handOffPartialRemine(ctx, logger, hash, orphanedAt)
		if err == nil {
			return done
		}
		if attempt >= handOffAttempts || ctx.Err() != nil {
			logger.Error("the durable hand-off of a partial re-mine failed through every retry",
				zap.Int("attempts", attempt), zap.Error(err))
			return false
		}
		logger.Warn("hand-off of a partial re-mine failed; retrying in-process",
			zap.Int("attempt", attempt), zap.Int("max_attempts", handOffAttempts), zap.Error(err))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(handOffBackoff):
		}
	}
}

// handOffPartialRemine is one attempt of keepPartialRemineQueued. done=true
// means the block is on a path that retries it (or needs none); a non-nil
// error means a store call was refused and the attempt should be repeated.
func (r *Reconciler) handOffPartialRemine(ctx context.Context, logger *zap.Logger, hash string, orphanedAt time.Time) (bool, error) {
	applied, err := r.store.RequeueOrphanedBlock(ctx, hash, orphanedAt)
	if err != nil {
		return false, fmt.Errorf("requeue: %w", err)
	}
	if applied {
		return true, nil // still the judged orphan, and on the queue
	}
	row, err := r.store.GetBlockProcessingStatus(ctx, hash)
	switch {
	case errors.Is(err, store.ErrNotFound):
		logger.Warn("block row vanished during a partial re-mine; nothing to requeue")
		return true, nil
	case err != nil:
		return false, fmt.Errorf("read row: %w", err)
	case row.Status == models.BlockStatusOrphaned:
		logger.Info("block was orphaned again with a newer generation during the re-mine; that pass re-mines it")
		return true, nil
	case row.Status != models.BlockStatusActive:
		logger.Warn("block row is no longer active or orphaned; leaving it to its owner",
			logfields.Status(string(row.Status)))
		return true, nil
	}
	logger.Error("the tracker reactivated the block while its re-mine was failing; re-orphaning it so the " +
		"reconciler retries the un-remined txs next tick (the block reads orphaned until then)")
	r.requestRebuild(ctx, logger, hash)
	transitions, err := r.store.MarkBlocksOrphaned(ctx, []string{hash}, r.now())
	// Counted from what the write reports — a transition that landed
	// alongside an error included, a refused write not at all.
	recordOrphanTransitions(metrics.BlockTransitionSourceReconciler, transitions)
	metrics.ReconcilerRemineRequeuedTotal.Add(float64(transitions))
	if err != nil {
		return false, fmt.Errorf("re-orphan: %w", err)
	}
	return true, nil
}

// keepRemineRetried makes sure a resurrect candidate whose own re-mine could
// not RUN — its BUMP unreadable or malformed, so nothing was written — stays
// on a path that retries it, whatever happened to its row since the scan
// read it. Reports false only when the store refused every check through
// handOffAttempts tries. The generation the scan judged decides:
//
//   - still ours (RequeueOrphanedBlock applies): on the tick's durable queue,
//     whose resurrection short-circuit re-mines it (or defers on a corrupt
//     BUMP, asking for the rebuild under the defer cap) next pass.
//   - re-orphaned with a newer generation: already queued for its own pass.
//   - reactivated by the tracker: the row is active and off every in-store
//     queue with its txs un-remined, and no scan will select it again. It
//     is held in the pending set, where the drain re-mines it directly while
//     it is canonical — or, for a corrupt BUMP, keeps asking for the rebuild
//     under the defer cap. Unlike keepPartialRemineQueued it never
//     re-orphans that row: nothing was written, so there is no half-applied
//     state to force back onto the durable queue at the cost of a canonical
//     block reading orphaned — the invariant the malformed path has kept
//     since round 6 of this PR's review.
//   - parked or gone: the watchdog owns it / nothing to retry.
func (r *Reconciler) keepRemineRetried(ctx context.Context, logger *zap.Logger, hash string, height uint64, orphanedAt time.Time) bool {
	for attempt := 1; ; attempt++ {
		applied, err := r.store.RequeueOrphanedBlock(ctx, hash, orphanedAt)
		if err == nil {
			if applied {
				return true // still the judged orphan, and on the queue
			}
			var row *models.BlockProcessingStatus
			row, err = r.store.GetBlockProcessingStatus(ctx, hash)
			switch {
			case errors.Is(err, store.ErrNotFound):
				logger.Warn("block row vanished before its re-mine could run; nothing to retry")
				return true
			case err != nil:
				err = fmt.Errorf("read row: %w", err)
			case row.Status == models.BlockStatusOrphaned:
				logger.Info("block was orphaned again with a newer generation; that pass re-mines it")
				return true
			case row.Status == models.BlockStatusActive:
				logger.Warn("the tracker reactivated the block before its re-mine could run; " +
					"holding it so the reconciler re-mines it directly while it is canonical")
				r.holdPendingRemine(ctx, logger, hash, height)
				return true
			default:
				logger.Warn("block row is no longer active or orphaned; leaving it to its owner",
					logfields.Status(string(row.Status)))
				return true
			}
		}
		if attempt >= handOffAttempts || ctx.Err() != nil {
			logger.Error("the hand-off of an un-run re-mine failed through every retry",
				zap.Int("attempts", attempt), zap.Error(err))
			return false
		}
		logger.Warn("hand-off of an un-run re-mine failed; retrying in-process",
			zap.Int("attempt", attempt), zap.Int("max_attempts", handOffAttempts), zap.Error(err))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(handOffBackoff):
		}
	}
}

// holdPendingRemine records a block whose re-mine no in-store path will
// retry — its hand-off was refused, or it is active with un-remined txs — so
// the next tick retries it from memory (drainPendingRemines). It also asks
// merkle-service to redeliver the block right here — a refused hand-off may
// not have reached its own request — because that redelivery (the builder
// re-mines the stored BUMP's full level-0 set on it) is the only cover left
// if this process restarts before the set drains; best-effort, never the
// only path while the process lives. Logged at Error; a full set drops the
// block, loudly, leaving it to that redelivery alone. Callers count a
// refused durable hand-off on ReconcilerRemineHandoffFailedTotal themselves;
// the gauge here covers every held block.
func (r *Reconciler) holdPendingRemine(ctx context.Context, logger *zap.Logger, hash string, height uint64) {
	r.requestRebuild(ctx, logger, hash)
	if r.pendingRemine == nil {
		r.pendingRemine = make(map[string]uint64)
	}
	if _, held := r.pendingRemine[hash]; !held && len(r.pendingRemine) >= maxPendingRemines {
		logger.Error("partial re-mine could not be handed off and the in-memory pending set is full; "+
			"the block's remaining txs depend on the BLOCK_PROCESSED redelivery requested for it",
			zap.Int("pending", len(r.pendingRemine)))
		return
	}
	r.pendingRemine[hash] = height
	metrics.ReconcilerRemineHandoffPending.Set(float64(len(r.pendingRemine)))
	logger.Error("partial re-mine could not be handed off to the store; holding the block in memory and "+
		"retrying every tick until it heals or the hand-off lands (lost on restart — the requested "+
		"BLOCK_PROCESSED redelivery is the only cover then)",
		zap.Int("pending", len(r.pendingRemine)))
}

// drainPendingRemines retries every block in the pending set once per tick.
// While the block is canonical its own BUMP is re-mined directly (the heal
// itself, no detour through the queue); a failure after the in-process
// retries goes through the durable hand-off again and, if that lands, the
// queue owns the block. A block that is no longer canonical is left to the
// tracker's orphaning and dropped once its row reads orphaned (that
// generation is queued). A malformed BUMP is asked to be rebuilt and kept.
// The set shrinks only when a block is healed or durably queued.
func (r *Reconciler) drainPendingRemines(ctx context.Context) {
	if len(r.pendingRemine) == 0 {
		return
	}
	batchSize := r.cfg.BumpBuilder.Reconciler.BatchSize
	if batchSize <= 0 {
		batchSize = maxTxIDsPerBulkEvent
	}
	for hash, height := range r.pendingRemine {
		if ctx.Err() != nil {
			return
		}
		logger := r.logger.With(logfields.BlockHash(hash), logfields.BlockHeight(height))
		canonical, found, hdrErr := r.activeHashAt(ctx, height)
		switch {
		case hdrErr != nil:
			logger.Warn("pending re-mine: chain-header lookup failed; keeping the block for the next tick", zap.Error(hdrErr))
			continue
		case !found:
			continue // cannot judge the height yet; keep it
		case canonical != hash:
			// Not canonical any more: the tracker's ReorgEvent / tie-scan
			// orphan the row, and that generation is on the durable queue.
			// Dropped once that has happened.
			if row, err := r.store.GetBlockProcessingStatus(ctx, hash); err == nil && row.Status == models.BlockStatusOrphaned {
				logger.Info("pending re-mine: block lost its height and its row is orphaned and queued; the queue owns it")
				delete(r.pendingRemine, hash)
			}
			continue
		}
		n, ok, err := r.remineWithRetry(ctx, logger, hash, batchSize)
		metrics.ReconcilerTxsReanchoredTotal.Add(float64(n))
		switch {
		case err == nil:
			if ok {
				logger.Info("pending re-mine: healed", zap.Int("txs_reanchored", n))
			} else {
				logger.Warn("pending re-mine: the block has no stored BUMP any more; nothing left to re-mine from")
			}
			r.forgetOrphan(hash)
			delete(r.pendingRemine, hash)
		case errors.Is(err, errMalformedBUMP):
			// A corrupt BUMP on a canonical, active row: ask for the rebuild
			// under the same per-block cap the tick's deferral uses — never
			// re-orphan the row for it — and at the cap leave the block to
			// the redelivery already requested, loudly.
			if r.deferForCanonicalBUMP(ctx, logger, hash, hash) {
				logger.Error("pending re-mine: stored BUMP is malformed; rebuild requested, keeping the block",
					zap.Int("rebuild_requests", r.defers[hash]), zap.Error(err))
				continue
			}
			logger.Error("pending re-mine: stored BUMP still malformed at the rebuild-request cap; "+
				"leaving the block to the BLOCK_PROCESSED redelivery", zap.Error(err))
			r.forgetOrphan(hash)
			delete(r.pendingRemine, hash)
		default:
			logger.Warn("pending re-mine: re-mine failed again", zap.Int("txs_reanchored", n), zap.Error(err))
			if r.keepPartialRemineQueued(ctx, logger, hash, time.Time{}) {
				logger.Info("pending re-mine: handed off to the durable queue")
				delete(r.pendingRemine, hash)
			}
		}
	}
	metrics.ReconcilerRemineHandoffPending.Set(float64(len(r.pendingRemine)))
}

// parkBlock is the issue-#282 fallback for an orphan whose canonical BUMP is
// unavailable at the defer cap: it takes the block off the reconcile queue
// WITHOUT downgrading any transaction. The txs still anchored to O stay
// MINED@O (not reverted to SEEN_ON_NETWORK) — a later stored or rebuilt
// canonical BUMP re-anchors them through the normal mine path. STUMPs are
// pruned and the compound BUMP retained, identical to the healed-terminal
// path; only reconciled_at is stamped so the row leaves the queue (bounded:
// no infinite retry, no repeated /reprocess). Returns the "parked" outcome.
func (r *Reconciler) parkBlock(ctx context.Context, logger *zap.Logger, orphan string, orphanedAt time.Time, height uint64, canonicalHash string, reanchored int) string {
	// Count what remains MINED@O purely for the metric/log — the neighborhood
	// pass already re-anchored everything it could prove, so this is the
	// still-unresolvable set we are deliberately leaving in place.
	parked := 0
	if remaining, err := r.store.GetTxIDsByBlockHash(ctx, orphan); err == nil {
		parked = len(remaining)
	}

	if err := r.store.DeleteStumpsByBlockHash(ctx, orphan); err != nil {
		logger.Warn("failed to clean up orphan STUMPs", zap.Error(err))
	}
	stamped, err := r.store.MarkBlockReconciled(ctx, orphan, orphanedAt, r.now())
	if err != nil {
		logger.Warn("failed to stamp reconciled_at on parked block", zap.Error(err))
		return outcomeError
	}
	r.forgetOrphan(orphan)
	// As above: the re-anchors and the park already happened.
	metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
	metrics.ReconcilerTxsParkedTotal.Add(float64(parked))
	if !stamped {
		return r.staleOutcome(logger, height)
	}

	logger.Warn(
		"orphaned block parked (unreconcilable): canonical BUMP unavailable at the defer cap — "+
			"txs left MINED against the orphan, NOT reverted; a later canonical BUMP re-anchors them (issue #282)",
		logfields.BlockHeight(height),
		zap.String("canonical_block_hash", canonicalHash),
		zap.Int("txs_reanchored", reanchored),
		zap.Int("txs_parked", parked),
	)
	return outcomeParked
}

// rewatchOrphanTxs re-registers every tx still anchored to an orphan with
// merkle-service, once per orphan generation.
//
// When merkle-service finds a registered tx in a block it emits the STUMP and
// then touches the registration with block.postMineTTLSec (1800 s deployed),
// so it stops reporting the tx 30 minutes after that first mine. A reorg
// that orphans the block does not reach merkle-service, and nothing else on
// the broadcaster re-registers a tx it already holds as MINED. A tx re-mined
// in a later block after the TTL ran out therefore produces no STUMP: the
// later block's compound BUMP is built without it, the neighborhood walk
// cannot claim it, and the park leaves it MINED against the orphan for good.
// That is what stranded ~8,750 txs at mainnet 973522 (orphaned 22:09,
// re-mined in 973524 at 22:31:40, 80 s after their registrations lapsed). A
// fresh /watch rewrites the registration without that TTL, so the block that
// re-mines the tx reports it through the normal STUMP → mine path, which
// moves it off the orphan (MINED@O → MINED@N is lattice-legal).
//
// Best-effort: a failure never holds the pass up, and the generation is only
// recorded once every registration landed, so a failed one is retried on the
// next tick. Disabled with the rest of the merkle integration.
func (r *Reconciler) rewatchOrphanTxs(ctx context.Context, logger *zap.Logger, orphan string, orphanedAt time.Time) {
	if r.merkle == nil || r.cfg.CallbackURL == "" {
		return
	}
	if gen, ok := r.rewatched[orphan]; ok && gen.Equal(orphanedAt) {
		return
	}
	txids, err := r.store.GetTxIDsByBlockHash(ctx, orphan)
	if err != nil {
		logger.Warn("failed to read the orphan's txids to refresh their merkle-service watch; retrying next tick",
			zap.Error(err))
		return
	}
	if len(txids) == 0 {
		r.rewatched[orphan] = orphanedAt
		return
	}
	regs := make([]merkleservice.Registration, len(txids))
	for i, txid := range txids {
		regs[i] = merkleservice.Registration{
			TxID:          txid,
			CallbackURL:   r.cfg.CallbackURL,
			CallbackToken: r.cfg.CallbackToken,
		}
	}
	concurrency := r.cfg.Propagation.MerkleConcurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	failed := 0
	var firstErr error
	for _, regErr := range r.merkle.RegisterBatchWithResults(ctx, regs, concurrency) {
		if regErr == nil {
			continue
		}
		if firstErr == nil {
			firstErr = regErr
		}
		failed++
	}
	metrics.ReconcilerTxsRewatchedTotal.WithLabelValues("ok").Add(float64(len(txids) - failed))
	if failed > 0 {
		metrics.ReconcilerTxsRewatchedTotal.WithLabelValues("failed").Add(float64(failed))
		logger.Warn("failed to refresh the merkle-service watch for some of the orphan's txs; retrying next tick",
			zap.Int("txs", len(txids)), zap.Int("failed", failed), zap.Error(firstErr))
		return
	}
	logger.Info("refreshed the merkle-service watch for the orphan's txs", zap.Int("txs", len(txids)))
	r.rewatched[orphan] = orphanedAt
}

// forgetOrphan drops the per-orphan in-memory bookkeeping once a block has
// left the reconcile queue (stamped, reactivated, or healed).
func (r *Reconciler) forgetOrphan(hash string) {
	delete(r.defers, hash)
	delete(r.rewatched, hash)
}

// staleOutcome is returned when the reconciled_at CAS found a different
// orphan generation than the one this pass processed: the block-status
// tracker reactivated the row, or it was orphaned again, while the pass ran.
// Nothing is stamped — an active row stays clean and a newer orphan
// generation stays queued for its own pass. The tx writes this pass made
// were correct against the chain state it observed; the competitor's orphan
// row (marked by the same reorg) heals them on a later tick.
func (r *Reconciler) staleOutcome(logger *zap.Logger, height uint64) string {
	logger.Warn("stale reconcile: block row changed generation while reconciling (reactivated or re-orphaned); not stamping",
		logfields.BlockHeight(height))
	return outcomeStale
}

// activeHashAt resolves the active-chain block hash at height. Tri-state:
// found=false with a nil error means the chain-header source positively has
// no header at that height (above its tip, unknown, or out of range) —
// callers fail open, never treating absence of evidence as evidence; a
// non-nil error means the lookup FAILED, which is not a judgement either
// way, and callers must keep their row for a retry rather than act on it.
// Collapsing the two made a transient header read look like the end of the
// chain (issue #339 review).
//
// This is the one place the source's answers are classified, per the
// ChainHeaderReader contract: absence is (nil, nil) OR an error that
// errors.Is chaintrackslib.ErrHeaderNotFound. go-chaintracks' ChainManager
// reports every height at or beyond its tip with that sentinel — so a walk
// upward from a block reaches it on every tick as the ordinary end of the
// chain — and treating it as a failure kept every orphan's neighborhood
// walk in "error" forever, with its off-chain txs never reverted.
func (r *Reconciler) activeHashAt(ctx context.Context, height uint64) (hash string, found bool, err error) {
	if height == 0 || height > math.MaxUint32 {
		return "", false, nil
	}
	active, err := r.chainHeader.GetHeaderByHeight(ctx, uint32(height))
	switch {
	case errors.Is(err, chaintrackslib.ErrHeaderNotFound):
		return "", false, nil // the source has no header here: absence, not a failure
	case err != nil:
		return "", false, fmt.Errorf("look up the active block at height %d: %w", height, err)
	case active == nil:
		return "", false, nil
	}
	return active.Hash.String(), true, nil
}

// deferForCanonicalBUMP handles the canonical-BUMP-missing branch: while
// under the defer cap it bumps the orphan's counter, optionally pokes
// merkle-service /reprocess for the canonical block, and reports true
// (caller returns "deferred" without stamping reconciled_at). At the cap it
// reports false so the caller parks the block (issue #282) — or reverts, if
// RevertWhenUnreconcilable is set.
func (r *Reconciler) deferForCanonicalBUMP(ctx context.Context, logger *zap.Logger, orphan, canonicalHash string) bool {
	maxDefer := r.cfg.BumpBuilder.Reconciler.MaxDeferAttempts
	if maxDefer <= 0 {
		maxDefer = 10
	}
	if r.defers[orphan] >= maxDefer {
		return false
	}
	r.defers[orphan]++
	logger.Info(
		"canonical block not yet usable (BUMP unstored or unparseable) — deferring",
		zap.String("canonical_block_hash", canonicalHash),
		zap.Int("defer_attempt", r.defers[orphan]),
	)
	r.requestRebuild(ctx, logger, canonicalHash)
	return true
}

// requestRebuild asks merkle-service to redeliver BLOCK_PROCESSED for
// blockHash (/reprocess), which has the builder rebuild — or, when a stored
// BUMP does not parse, replace — the block's compound BUMP and re-mine its
// full level-0 set. Best-effort and a no-op when the merkle client or the
// callback URL is not configured; the caller keeps its own retry.
func (r *Reconciler) requestRebuild(ctx context.Context, logger *zap.Logger, blockHash string) {
	if r.merkle == nil || r.cfg.CallbackURL == "" {
		return
	}
	if err := r.merkle.Reprocess(ctx, blockHash, r.cfg.CallbackURL, r.cfg.CallbackToken); err != nil {
		logger.Warn("merkle-service /reprocess failed", zap.String("reprocess_block_hash", blockHash), zap.Error(err))
	}
}

// reanchorNeighborhood walks the heights above the orphan looking for
// canonical blocks whose stored BUMPs contain the still-affected txs (a
// deep reorg re-bins txs into later blocks) and re-anchors what it finds.
// Returns the number of rows moved and the first store write failure, if
// any — an incomplete pass must not reach the caller's revert, for the
// reason spelled out at the failure branch below, so the caller leaves the
// block queued. The affected slice shrinks as txs are claimed; whatever
// remains falls to the caller's revert.
func (r *Reconciler) reanchorNeighborhood(ctx context.Context, logger *zap.Logger, affected []string, height uint64, batchSize int) (int, error) {
	if len(affected) == 0 {
		return 0, nil
	}
	// A placeholder orphan row with no resolvable height (height==0) has no
	// meaningful neighborhood above it — walking heights 1..depth is
	// nonsensical. Skip straight to the caller's revert.
	if height == 0 {
		return 0, nil
	}
	depth := r.cfg.BumpBuilder.Reconciler.NeighborhoodDepth
	if depth < 0 {
		depth = 0
	}
	reanchored := 0
	for h := height + 1; h <= height+uint64(depth) && len(affected) > 0; h++ {
		neighbor, found, hdrErr := r.activeHashAt(ctx, h)
		if hdrErr != nil {
			// A failed lookup at an intermediate height is not the end of
			// the chain: a later neighbor's BUMP may prove the remaining txs
			// mined, and the caller's revert would un-mine them. Leave the
			// block queued instead.
			return reanchored, hdrErr
		}
		if !found {
			break // above the tip — nothing further to check
		}
		_, bumpBytes, bumpErr := r.store.GetBUMP(ctx, neighbor)
		switch {
		case errors.Is(bumpErr, store.ErrNotFound):
			continue // positively nothing stored: this neighbor can claim nothing
		case bumpErr != nil:
			// Only a positively missing BUMP means "this neighbor can claim
			// nothing". A read failure while the neighbor's BUMP IS stored
			// would otherwise leave its txs in `affected`, where the caller's
			// revert un-mines them and the stamp retires the block — the
			// retry is lost with the row off the queue.
			return reanchored, fmt.Errorf("read neighbor %s BUMP: %w", neighbor, bumpErr)
		case len(bumpBytes) == 0:
			// Every backend reports a missing BUMP as store.ErrNotFound, so
			// an empty blob is a stored-but-unusable one: malformed, like a
			// blob that does not parse, and handled below the same way.
			return reanchored, fmt.Errorf("%w: neighbor %s: empty stored blob", errMalformedBUMP, neighbor)
		}
		idx, idxErr := bump.IndexCompound(bumpBytes)
		if idxErr != nil {
			// Same for a stored BUMP that does not parse: it may well hold
			// these txs, so nothing below may revert them. The block stays
			// queued, loudly, until the neighbor's BUMP is rebuilt.
			return reanchored, fmt.Errorf("%w: neighbor %s: %w", errMalformedBUMP, neighbor, idxErr)
		}
		contained := make([]string, 0, len(affected))
		rest := make([]string, 0, len(affected))
		for _, txid := range affected {
			if idx.Contains(txid) {
				contained = append(contained, txid)
			} else {
				rest = append(rest, txid)
			}
		}
		for start := 0; start < len(contained); start += batchSize {
			end := min(start+batchSize, len(contained))
			n, mineErr := setMinedAndPublish(ctx, logger, r.store, r.publisher,
				neighbor, h, contained[start:end], models.ExtraInfoReorgReanchor, true)
			reanchored += n
			if mineErr != nil {
				// Same hazard as a partial canonical re-mine, one height up:
				// the rows this chunk failed to write are proven members of
				// THIS neighbor's BUMP and are still anchored to the orphan,
				// where the caller's revert would take them to
				// SEEN_ON_NETWORK. Stop and report the failure rather than
				// letting the pass look finished.
				//
				// `affected` is not the thing that protects them — it only
				// steers the remaining heights, while the revert works off
				// the store's own block index — so shrinking it is neither
				// the problem nor the fix.
				return reanchored, fmt.Errorf("re-anchor to neighbor %s: %w", neighbor, mineErr)
			}
		}
		affected = rest
	}
	return reanchored, nil
}

// remineFromStoredBUMP re-mines every level-0 txid of blockHash's stored
// compound BUMP against it (onlyChanged — no duplicate events for rows
// already anchored right). ok=false when no BUMP is stored — store.ErrNotFound,
// which every backend returns for a missing BUMP — the caller's defer signal.
//
// A non-nil error means the BUMP could not be READ reliably, is stored but
// unusable (an empty blob, or one that does not parse: both wrap
// errMalformedBUMP, so callers can tell corruption from a transient
// failure), or a store write failed part-way. In every case
// the caller must not treat the block as healed (no revert,
// no reconciled_at stamp, no reactivation) so the work is retried. A partial
// re-mine is the dangerous case, because it can look like success: the rows
// it failed to write are provably IN this canonical BUMP and stay anchored to
// the orphan, the neighborhood pass only walks heights ABOVE the orphan so
// it cannot claim them, and the caller would then revert them to
// SEEN_ON_NETWORK and stamp reconciled_at — un-mining transactions that are
// demonstrably mined, with nothing left to re-drive them: the canonical
// block already has its BUMP and its processed_at, and the orphan has left
// the queue. Surfacing the error keeps the orphan queued instead.
func (r *Reconciler) remineFromStoredBUMP(ctx context.Context, logger *zap.Logger, blockHash string, batchSize int) (int, bool, error) {
	bumpHeight, bumpBytes, bumpErr := r.store.GetBUMP(ctx, blockHash)
	switch {
	case errors.Is(bumpErr, store.ErrNotFound):
		// Genuinely no BUMP stored yet: that is the ok=false signal the
		// caller defers on, not a failure to propagate.
		return 0, false, nil
	case bumpErr != nil:
		// A transient read failure is NOT "no BUMP stored". Collapsing the
		// two would make the caller burn a defer attempt on a healthy block
		// (and eventually revert it), or — in the full-scan — reactivate a
		// row whose txs were never re-mined, which then leaves the only
		// queue that could retry them. Surface it so the caller retries.
		return 0, false, fmt.Errorf("read stored BUMP for %s: %w", blockHash, bumpErr)
	case len(bumpBytes) == 0:
		// Not "no BUMP" either: every backend reports that as ErrNotFound.
		// A stored empty blob is unusable in the same way a blob that does
		// not parse is (the builder never writes one — BuildCompoundBUMP
		// refuses an empty STUMP set), and takes the same path.
		return 0, false, fmt.Errorf("%w: %s: empty stored blob", errMalformedBUMP, blockHash)
	}
	txids, parseErr := levelZeroTxidsFromBUMP(bumpBytes)
	if parseErr != nil {
		// Not the ok=false "nothing stored" signal: a blob IS stored and it
		// is corrupt. The full-scan must not read this as "no BUMP" and
		// reactivate a row whose txs it never re-mined, and it does not
		// fix itself the way a not-yet-built BUMP does. reconcileBlock
		// recognizes errMalformedBUMP and defers (its /reprocess poke is
		// the rebuild); every other caller keeps the row for a retry.
		return 0, false, fmt.Errorf("%w: %s: %w", errMalformedBUMP, blockHash, parseErr)
	}
	changed := 0
	for start := 0; start < len(txids); start += batchSize {
		end := min(start+batchSize, len(txids))
		n, mineErr := setMinedAndPublish(ctx, logger, r.store, r.publisher,
			blockHash, bumpHeight, txids[start:end], models.ExtraInfoReorgReanchor, true)
		changed += n
		if mineErr != nil {
			// Stop at the first failed chunk, like reanchorNeighborhood does.
			// One failure already makes this attempt non-ready, and the
			// retry re-mines the BUMP's full level-0 set, so the remaining
			// chunks would buy no progress that the next tick does not —
			// while during an outage each one pays its own store timeout,
			// turning a large canonical block into a long stall.
			return changed, true, fmt.Errorf("re-mine batch %d..%d (%d of %d txs landed) against %s: %w",
				start, end, changed, len(txids), blockHash, mineErr)
		}
	}
	return changed, true, nil
}

// publishReverted fans out the bulk SEEN_ON_NETWORK correction events for a
// reorg revert, chunked like the MINED fan-out.
func (r *Reconciler) publishReverted(ctx context.Context, logger *zap.Logger, txids []string) {
	if len(txids) == 0 || r.publisher == nil {
		return
	}
	for start := 0; start < len(txids); start += maxTxIDsPerBulkEvent {
		end := min(start+maxTxIDsPerBulkEvent, len(txids))
		template := &models.TransactionStatus{
			Status:    models.StatusSeenOnNetwork,
			Timestamp: r.now(),
			TxIDs:     txids[start:end],
			ExtraInfo: models.ExtraInfoReorgUnmined,
		}
		if err := r.publisher.PublishBulk(ctx, template); err != nil {
			logger.Warn("failed to publish bulk reorg revert",
				zap.Int("chunk_start", start),
				zap.Error(err))
		}
	}
}
