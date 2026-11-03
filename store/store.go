package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// ErrInvalidPeerPolicy is returned by PeerPolicy.Validate, and by every
// backend's UpsertPeerPolicy, for a policy that cannot be stored faithfully.
// Callers treat it as "drop this advertisement", never as a store fault.
var ErrInvalidPeerPolicy = errors.New("invalid peer policy")

// ErrInvalidEndpointPolicy is returned by EndpointPolicy.Validate for an
// advertised policy that cannot be stored faithfully. Callers drop the policy
// and still register the URL, so this never reaches a backend as a write error.
var ErrInvalidEndpointPolicy = errors.New("invalid endpoint policy")

// ErrReplayUnavailable is returned by IterateStatusesByToken when a backend
// cannot serve the replay within its resource bounds — typically a token whose
// submission set is too large for a backend that has no keyset index over
// (token, timestamp, txid) and would otherwise materialize and sort the whole
// thing in memory.
//
// It exists so refusing is a first-class outcome. Attempting an unbounded
// replay is not merely slow: it has OOM-killed the SSE pod and taken every
// other connected client down with it (#237/#238). The SSE service turns this
// error into an explicit `event: gap`, so the client learns it must reconcile
// over REST and the live stream continues — a bounded, declared degradation
// instead of a crash.
var ErrReplayUnavailable = errors.New("replay unavailable for this token on this backend")

// PendingRetry is the lightweight row shape the reaper consumes. It avoids
// pulling full TransactionStatus objects through the retry hot path.
type PendingRetry struct {
	TxID        string
	RawTx       []byte
	RetryCount  int
	NextRetryAt time.Time
	// LastReason is the last non-empty network response recorded for the
	// tx across its durable attempts (SetPendingRetryFields), so the
	// eventual give-up can quote it even when the final attempt drew no
	// response at all. Empty when no peer ever answered.
	LastReason string
}

// DatahubEndpointSourceConfigured marks endpoints seeded from static config.
const DatahubEndpointSourceConfigured = "configured"

// DatahubEndpointSourceDiscovered marks endpoints registered at runtime via
// p2p discovery.
const DatahubEndpointSourceDiscovered = "discovered"

// DatahubEndpoint is a registered datahub URL persisted to the shared store
// so propagation and bump-builder pods running as separate microservices
// converge on the same union of (configured + p2p-discovered) URLs.
//
// Network scopes the entry to a Bitcoin network (mainnet/testnet/teratestnet/
// regtest). It exists so a store reused across network changes — or shared
// between pods on the same persistence backend — never serves a peer from one
// network to a pod configured for another. Legacy rows written before this
// field existed have an empty Network and are filtered out by every read.
//
// Policy is the transaction policy the node advertised in the same node_status
// announcement that carried this URL, or nil when it advertised none. See
// EndpointPolicy for why a write that carries no policy never erases one.
type DatahubEndpoint struct {
	URL      string
	Network  string
	Source   string // DatahubEndpointSourceConfigured or DatahubEndpointSourceDiscovered
	Policy   *EndpointPolicy
	LastSeen time.Time
}

// EndpointPolicy is the transaction policy a teranode advertised in the
// node_status fee_policy object alongside its datahub URL: what that specific
// endpoint will accept. Sizes are in bytes; the mining fee is
// satoshis-per-Bytes, the node_status FeePolicy.MiningFee shape, e.g.
// {Satoshis: 100, Bytes: 1000} == 100 sat/kB.
//
// It is attached to the URL registration purely so operators can see, per
// endpoint, what each node enforces — GET /health reports it. The network-wide
// values BSV Broadcaster itself enforces come from PeerPolicy instead, which is keyed by
// libp2p PeerID and so also covers peers advertising no URL at all. The two are
// deliberately separate: same source, different keys, different questions.
//
// A nil Policy means "this node advertised none" — a statically configured seed
// URL, or a teranode predating fee_policy — which is why it is a pointer rather
// than a zero-valued struct. Every backend treats a nil-policy write as leaving
// any previously recorded policy in place: the configured-URL seed in
// app.BuildServices re-upserts on every process start, and without that rule an
// endpoint that is both configured and announced would lose its policy on each
// restart and regain it seconds later.
type EndpointPolicy struct {
	MiningFeeSatoshis       uint64
	MiningFeeBytes          uint64
	MaxTxSizePolicy         uint64
	MaxScriptSizePolicy     uint64
	MaxTxSigopsCountsPolicy uint64
}

// Validate reports whether ep can be persisted without loss, on the same terms
// as PeerPolicy.Validate: the Postgres and Aerospike backends narrow these to
// signed 64-bit, and node_status is unauthenticated gossip, so the peer chooses
// the numbers. Callers drop an invalid policy to nil rather than failing the
// write — the URL registration is the reason the row exists, and a peer sending
// a garbage size must not be able to remove its own endpoint from the registry.
func (ep EndpointPolicy) Validate() error {
	for _, f := range []struct {
		name  string
		value uint64
	}{
		{"mining fee satoshis", ep.MiningFeeSatoshis},
		{"mining fee byte basis", ep.MiningFeeBytes},
		{"max tx size", ep.MaxTxSizePolicy},
		{"max script size", ep.MaxScriptSizePolicy},
		{"max tx sigops count", ep.MaxTxSigopsCountsPolicy},
	} {
		if f.value > maxStorablePolicyValue {
			return fmt.Errorf("%w: advertised %s %d, above the storable maximum %d",
				ErrInvalidEndpointPolicy, f.name, f.value, maxStorablePolicyValue)
		}
	}
	return nil
}

// StatusCensus is one status's aggregate in a CensusStatusesSince result:
// how many rows sit at that status inside the census window, and the minimum
// timestamp among them (zero when Count is 0).
type StatusCensus struct {
	Count  int64
	Oldest time.Time
}

// PeerPolicy is the transaction policy a peer advertised in its node_status
// gossip announcement, persisted to the shared store so the ARC-compatible
// GET /policy endpoint (served by api-server pods) can compute network-wide
// values even though only the single p2p-client pod observes node_status
// (issue #212).
//
// Keyed by PeerID (a libp2p peer runs on a single network, so PeerID is
// unique); Network is a filter attribute, not part of the key — matching the
// DatahubEndpoint pattern. LastSeen lets readers drop peers not re-heard within
// a TTL so a departed cheap node cannot pin the advertised fee low forever. The
// mining fee is stored as satoshis-per-Bytes (the node_status FeePolicy.MiningFee
// shape), e.g. {Satoshis: 100, Bytes: 1000} == 100 sat/kB.
type PeerPolicy struct {
	PeerID            string
	Network           string
	MiningFeeSatoshis uint64
	MiningFeeBytes    uint64

	// MaxTxSizePolicy and MaxScriptSizePolicy are the peer's advertised size
	// limits in bytes, from node_status FeePolicy. Zero means "not advertised"
	// — either an older teranode with no fee_policy at all, or a value too
	// large to store — and readers skip such rows rather than treating the
	// zero as a real limit.
	MaxTxSizePolicy     uint64
	MaxScriptSizePolicy uint64

	LastSeen time.Time
}

// maxStorablePolicyValue bounds a peer policy's numeric fields. The Postgres
// and Aerospike backends store them in signed 64-bit columns, so a value above
// MaxInt64 wraps negative on write and reads back as an astronomical uint64 — a
// silently corrupted value that GET /policy would then advertise as the network
// consensus.
//
// The bound can only ever reject garbage: the entire money supply, 21 million
// BSV, is 2.1e15 satoshis — some four thousand times below MaxInt64 — and no
// transaction or script size comes remotely close. It has to be checked rather
// than assumed because node_status is unauthenticated gossip: the peer chooses
// the number.
const maxStorablePolicyValue = uint64(math.MaxInt64)

// Validate reports whether pp can be persisted without loss. Every backend
// calls it before writing, so the uint64→int64 narrowing each one performs is
// bounded by a check instead of by assumption.
//
// The size limits are allowed to be zero ("peer did not advertise"), but they
// are still validated for storable range: values above maxStorablePolicyValue
// are rejected by Validate. recordPeerPolicy calls SanitizePolicySizes to zero
// out-of-range sizes so the fee observation can still be persisted.
func (pp PeerPolicy) Validate() error {
	switch {
	case pp.PeerID == "":
		return fmt.Errorf("%w: empty peer id", ErrInvalidPeerPolicy)
	case pp.MiningFeeBytes == 0:
		// A zero byte basis makes the fee meaningless: readers divide by it to
		// normalize to sat/kB, and lowestObservedFeePerKB has to skip such rows.
		// Refusing the write keeps them out of the store in the first place.
		return fmt.Errorf("%w: peer %s has a zero mining fee byte basis", ErrInvalidPeerPolicy, pp.PeerID)
	case pp.MiningFeeSatoshis > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised %d satoshis, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MiningFeeSatoshis, maxStorablePolicyValue)
	case pp.MiningFeeBytes > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised a %d byte basis, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MiningFeeBytes, maxStorablePolicyValue)
	case pp.MaxTxSizePolicy > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised a %d byte max tx size, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MaxTxSizePolicy, maxStorablePolicyValue)
	case pp.MaxScriptSizePolicy > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised a %d byte max script size, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MaxScriptSizePolicy, maxStorablePolicyValue)
	default:
		return nil
	}
}

// SanitizePolicySizes returns a copy of pp with any advertised size limit that
// cannot be stored without loss zeroed, plus the number of fields it dropped.
// Callers use it to keep an implausible size from failing the whole write: the
// peer's fee observation is independently useful and must survive.
func (pp PeerPolicy) SanitizePolicySizes() (PeerPolicy, int) {
	dropped := 0
	if pp.MaxTxSizePolicy > maxStorablePolicyValue {
		pp.MaxTxSizePolicy = 0
		dropped++
	}
	if pp.MaxScriptSizePolicy > maxStorablePolicyValue {
		pp.MaxScriptSizePolicy = 0
		dropped++
	}
	return pp, dropped
}

// BatchInsertResult is one entry in the result slice returned by
// BatchGetOrInsertStatus. Inserted is true when the row was newly written by
// this call; false when an existing row was found and Existing carries it.
// Result ordering matches the input slice ordering.
type BatchInsertResult struct {
	Existing *models.TransactionStatus // populated only when !Inserted
	Inserted bool
}

// StatusUpdate is one row of BatchUpdateStatusReturning.
//
// Prev is non-nil only when this call durably applied the requested
// transition. It is the row immediately before that write. Callers that
// publish a transition or advance an in-memory tracker must use Prev.
//
// Current is the durable row when the txid is known and this call did not
// apply the transition. That is a lattice skip, including a race in which
// another writer moved the row to a later status before this write. Current
// is nil when the txid is absent. Prev and Current are mutually exclusive.
type StatusUpdate struct {
	Prev    *models.TransactionStatus
	Current *models.TransactionStatus
}

// Applied reports whether this call durably wrote the requested transition.
// Only an applied result may be published or used to advance a tracker.
func (u StatusUpdate) Applied() bool { return u.Prev != nil }

// Store handles all persistence operations for transactions and submissions
type Store interface {
	// GetOrInsertStatus inserts a new transaction status or returns the existing one if it already exists.
	// Returns the status, a boolean indicating if it was newly inserted (true) or already existed (false), and any error.
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (existing *models.TransactionStatus, inserted bool, err error)

	// BatchGetOrInsertStatus is the multi-row form of GetOrInsertStatus. The
	// returned slice is in the same order as `statuses` — result[i].Inserted
	// reports whether statuses[i] was newly inserted, and result[i].Existing
	// carries the existing row when !Inserted (it is nil for new inserts).
	//
	// Backends with native batch support (e.g. Postgres via the xmax trick)
	// implement this as a single round-trip; backends without (Aerospike,
	// Pebble) fall back to a bounded-concurrency loop over GetOrInsertStatus.
	BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]BatchInsertResult, error)

	// UpdateStatus updates an existing transaction status (used for P2P, blocks, etc.).
	// If no row exists for status.TxID the call returns ErrNotFound without
	// writing — callers must use GetOrInsertStatus to create new rows. This
	// guards the callback receiver path from creating phantom rows on behalf
	// of unknown txids (F-033 / issue #91).
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error

	// BatchUpdateStatus is the multi-row form of UpdateStatus. Same partial-
	// update semantics as UpdateStatus — empty fields are ignored, non-empty
	// fields overwrite. Rows whose txid is unknown are silently skipped (the
	// per-row ErrNotFound contract from UpdateStatus is collapsed to a no-op
	// here — callers wanting per-row diagnostics use UpdateStatus directly).
	// Postgres implements this in a single round-trip via UPDATE ... FROM
	// (VALUES …); other backends fall back to a bounded-concurrency loop.
	BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error

	// BatchUpdateStatusReturning is the diagnostic-rich form of BatchUpdateStatus.
	// Returns a slice the same length as `statuses`. See StatusUpdate.
	// result[i].Prev is non-nil only when this call durably applied
	// statuses[i]: it is the row as it stood immediately before that write.
	// A non-nil Prev is not "a row existed when we looked." An idempotent
	// re-assert of the same status is an applied write, so Prev.Status may
	// equal the requested status; callers that fan out events skip that case.
	//
	// result[i].Current is the durable row when the txid is known and this
	// call did not apply the transition (lattice skip, including a race lost
	// to a later status). A genuinely absent txid leaves both Prev and
	// Current nil. Callers must not publish or advance a tracker from
	// Current, and must not count Current as an unknown txid.
	//
	// Prev's status metadata (Status, Timestamp, block anchor, extra info)
	// is guaranteed. RawTx is NOT — MongoDB, Postgres and Aerospike read
	// only the status metadata, since no caller needs RawTx and it can be
	// megabytes per row on a hot path, while Pebble happens to return it
	// only because it reads the row whole.
	BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]StatusUpdate, error)

	// GetStatus retrieves the status for a transaction
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)

	// LookupPartitionKeys returns, for every txid in txids that has a row, the
	// row's current status and the propagation partition key its message was
	// published with (ParentKey.PartitionKey is "" for rows written before the
	// key was persisted). A projected read that never touches raw_tx, cheap
	// enough for the intake path: the api-server consults a submission's
	// parents so a child keys like an in-flight parent and lands on the
	// dispatcher that holds it. Unknown txids are absent from the result.
	LookupPartitionKeys(ctx context.Context, txids []string) (map[string]ParentKey, error)

	// EnrichMerklePath populates status.MerklePath in place for a MINED/IMMUTABLE
	// status that already carries a BlockHash, extracting the transaction's
	// minimal merkle path from the block's compound BUMP. It is a no-op when the
	// status is nil, already has a MerklePath, has no BlockHash, is not
	// MINED/IMMUTABLE, or the block's BUMP cannot be retrieved/parsed — so callers
	// on push paths can invoke it unconditionally and treat the proof as
	// best-effort (never a delivery gate). Safe to call repeatedly across all the
	// txids of one block: implementations share the bounded bumpcache of parsed,
	// indexed compound BUMPs (see store/bumpcache), so per-tx enrichment is
	// O(tree depth · log level-size), not a re-parse. Unlike GetStatus this does
	// not read the full row (no RawTx), which keeps it cheap enough for the
	// SSE/webhook fan-out hot path.
	EnrichMerklePath(ctx context.Context, status *models.TransactionStatus)

	// GetStatusesSince retrieves all transactions updated since a given timestamp
	GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error)

	// IterateStatusesSince streams every transaction updated since the given
	// timestamp through fn, one row at a time. Implementations must avoid
	// materializing the full result set in memory. fn returning a non-nil
	// error stops iteration and surfaces that error to the caller.
	//
	// This returns FULL rows (raw_tx, merkle_path, competing_txs, …) and, on
	// Postgres, sorts them. It exists for consumers that genuinely need the
	// whole row — propagation's replay and reaper. Do NOT reach for it from a
	// new bulk or startup scan: reusing it for TxTracker hydration shipped 14
	// columns for ~5M rows on every api-server boot and drove both BSV Broadcaster and
	// its Postgres out of memory (issue #276). New bulk consumers get a
	// projected, server-side-filtered method instead — see IterateTrackerRows,
	// IterateStatusesByToken and CensusStatusesSince.
	IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error

	// IterateTrackerRows streams the minimal (txid, status, block_height)
	// projection TxTracker.LoadFromStore needs, filtering server-side so a
	// multi-million-row history is never shipped to the client (issue #276).
	//
	// Contract:
	//   - MUST emit every row whose status is in TrackerStatuses(), exactly
	//     once, with Status and BlockHeight equal to the stored values.
	//   - MUST NOT emit rows in any other status. Pushing the status filter
	//     server-side is the memory contract, not an optimization: REJECTED
	//     alone is ~44% of a mature store.
	//   - MAY additionally drop MINED rows with
	//     0 < block_height < scan.PruneMinedBelow. Implementations are not
	//     required to; a backend whose index cannot express the height
	//     predicate is correct, just less selective.
	//   - MUST NOT read raw_tx, merkle_path, competing_txs or orphaned_anchors,
	//     MUST NOT enrich merkle paths, and MUST NOT impose an ordering. The
	//     ORDER BY over the full table is what pinned gigabytes.
	//   - fn returning a non-nil error stops iteration and surfaces it.
	//
	// TrackerScan.Keep is the specification; the SQL/index filter is only a
	// pushdown of it. The caller re-applies Keep to every emitted row, so a
	// backend that over-emits is a performance issue only — a backend that
	// under-emits is a correctness bug. store/storetest asserts exact
	// agreement across all three backends.
	IterateTrackerRows(ctx context.Context, scan TrackerScan, fn func(TrackerRow) error) error

	// CensusStatusesSince aggregates the stuck-transient census store-side:
	// for each requested status, the number of transaction rows with
	// timestamp >= since AND timestamp < stuckDeadline, plus the minimum
	// timestamp among them. The returned map has exactly one entry per
	// requested status — zero-valued when nothing matched — so callers can
	// publish gauges without existence checks.
	//
	// This exists so the propagation reaper's census over a multi-million-row
	// table never streams rows: Postgres answers it with one GROUP BY
	// aggregate, and other backends push the status filter into their
	// secondary indexes. Counting inside an IterateStatusesSince walk pinned
	// the reaper's effective cadence to the full-scan time (~4 minutes on a
	// ~1.6M-row store) instead of reaper_interval_ms — see issue #290.
	CensusStatusesSince(ctx context.Context, since, stuckDeadline time.Time, statuses []models.Status) (map[models.Status]StatusCensus, error)

	// SetStatusByBlockHash updates all transactions with the given block hash to a new status.
	// Returns the txids that were updated. For unmined statuses (SEEN_ON_NETWORK),
	// block fields are cleared and the cleared anchor is appended to the row's
	// orphaned-anchor history (reorg revert). For IMMUTABLE, block fields are
	// preserved. IMMUTABLE rows are never touched, and rows whose block_hash no
	// longer matches at write time are skipped — a tx concurrently re-anchored
	// to the canonical block must not be reverted by a stale index read.
	SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error)

	// GetTxIDsByBlockHash returns the txids of every transaction row currently
	// anchored to blockHash (MINED or IMMUTABLE). The reorg reconciler reads
	// this as the affected set of an orphaned block; rows that have already
	// been re-anchored or reverted no longer appear.
	GetTxIDsByBlockHash(ctx context.Context, blockHash string) ([]string, error)

	// InsertBUMP stores a compound BUMP for a block.
	InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error

	// GetBUMP retrieves the compound BUMP for a block.
	GetBUMP(ctx context.Context, blockHash string) (blockHeight uint64, bumpData []byte, err error)

	// DeleteBUMPByBlockHash removes the stored compound BUMP for a block and
	// invalidates any cached parse. Idempotent — deleting a missing BUMP is a
	// no-op. Operator/cleanup lever; note the reorg reconciler deliberately
	// RETAINS orphaned blocks' BUMPs so historical orphaned-anchor proofs
	// stay resolvable (issue #279).
	DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error

	// --- Block processing status ---
	//
	// These methods track which blocks have reached each milestone in the
	// (header observed → BLOCK_PROCESSED → compound BUMP built) pipeline. The
	// table is observability-first: writers must not fail their primary work
	// because of a status-tracking error.

	// UpsertBlockHeaderSeen records that chaintracks observed a tip header.
	// It is the UNCONDITIONAL insert-or-reset; the resurrection paths (the
	// block-status tracker and the anchor reconciler returning an orphaned
	// row to active once the block is the active-chain block at its height
	// again, issue #339) use ReactivateBlock, the generation-checked form,
	// instead. On insert, status='active' and header_seen_at=seenAt. On
	// conflict, implementations MUST overwrite block_height (chaintracks is
	// the authoritative source) and reset status='active' /
	// reconciled_at=NULL (so a later re-orphaning reconciles again), but
	// MUST preserve the existing header_seen_at, processed_at, and
	// bump_built_at so a re-arrival or reorg-resurrection does not erase
	// earlier milestones. The stored orphan generation is RETAINED as the
	// row's high-water mark (see MarkBlocksOrphaned); it is historical on an
	// active row and the model's OrphanedAt reads nil.
	UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error

	// ReactivateBlock returns an orphaned block's row to status='active'
	// because the block is the active-chain block at its height again
	// (issue #339), as a compare-and-set on the orphan generation: it
	// applies only while the row is still status='orphaned' AND its stored
	// generation equals orphanedAt — the value the caller READ from the row
	// (BlockProcessingStatus.OrphanGeneration), never a timestamp of its
	// own; a zero orphanedAt checks status only. On apply, block_height is
	// overwritten (chaintracks is authoritative), status='active',
	// reconciled_at is cleared, the milestone timestamps are preserved, and
	// the generation is retained as the row's high-water mark (historical;
	// OrphanedAt reads nil) — the row shape UpsertBlockHeaderSeen's
	// conflict path produces. Missing rows, rows
	// that are active or parked, and rows orphaned AGAIN with a newer
	// generation since the caller judged them are left untouched; a missing
	// row is never created. Returns whether the transition applied — the
	// applied-transition signal the block-status metric reports, and the
	// guard that keeps a judgement made against one generation from
	// clearing a newer one (the tie-scan and full-scan judge during a paging
	// walk, and the full-scan re-mines for minutes before it writes).
	ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error)

	// RequeueOrphanedBlock puts an orphaned row that a previous
	// reconciliation stamped — and so left the reconciler's durable queue —
	// back on it, as a compare-and-set on the orphan generation: it clears
	// reconciled_at only while the row is still status='orphaned' AND its
	// stored generation equals orphanedAt — the value the caller read from
	// the row, never its own timestamp; a zero orphanedAt checks status
	// only. The generation is NOT changed, so the token the caller holds
	// keeps matching, and it is never a status transition: a row another edge
	// reactivated since the caller judged it (the block-status tracker
	// running alongside the reconciler's full-scan), a row orphaned again
	// with a newer generation (already back on the queue, since
	// MarkBlocksOrphaned clears the stamp) and a missing row are all left
	// untouched. Returns whether the stamp was cleared. This is the
	// full-scan's hand-off to the tick for a repair it cannot finish itself
	// (issue #339 review); MarkBlocksOrphaned is the wrong tool for that,
	// because its transition arm would flip a legitimately reactivated
	// canonical row back to orphaned.
	RequeueOrphanedBlock(ctx context.Context, blockHash string, orphanedAt time.Time) (bool, error)

	// MarkBlockProcessed records that the merkle service delivered
	// BLOCK_PROCESSED for this block. Upsert: when no row exists (callback
	// arrived before chaintracks emitted the header), insert with
	// header_seen_at = processedAt. On conflict, only processed_at is
	// updated — block_height and other milestones are left alone.
	MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error

	// MarkBlockBUMPBuilt records that the compound BUMP was successfully
	// stored for this block. Same upsert-on-missing semantics as
	// MarkBlockProcessed.
	MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error

	// ClearBlockProcessed unsets processed_at on an existing row so the block
	// re-enters ListStaleBlockProcessingStatus and the watchdog re-drives it.
	// The bump-builder calls it when a later merkle-service's BLOCK_PROCESSED
	// turns out incomplete for a block an earlier service's build already
	// finalized. No-op (nil) when the row does not exist; every other column
	// is left alone.
	ClearBlockProcessed(ctx context.Context, blockHash string) error

	// AddBlockExpectedSubtrees unions indices into the block's retained set
	// of expected subtree indices: every subtree index some merkle-service's
	// BLOCK_PROCESSED announced a STUMP for while the block was incomplete.
	// Each service only announces the subtrees IT tracks, so the bump-builder
	// keeps the union to stop a later, smaller announcement from finalizing
	// a block another service is still owed a STUMP for. No-op (nil) when
	// the row does not exist.
	AddBlockExpectedSubtrees(ctx context.Context, blockHash string, indices []int) error

	// GetBlockExpectedSubtrees returns the retained set, ascending; empty
	// (nil, nil) when none is retained or the row does not exist.
	GetBlockExpectedSubtrees(ctx context.Context, blockHash string) ([]int, error)

	// ClearBlockExpectedSubtrees drops the retained set once a build has
	// satisfied it. No-op (nil) when the row does not exist.
	ClearBlockExpectedSubtrees(ctx context.Context, blockHash string) error

	// MarkBlocksOrphaned transitions every named block to status='orphaned',
	// stamps orphaned_at and CLEARS reconciled_at. Clearing is required, not
	// cosmetic: orphaned rows with reconciled_at IS NULL form the anchor
	// reconciler's work queue, so a row orphaned AGAIN after a previous
	// orphaning was reconciled must re-enter that queue — the old stamp
	// describes the old generation and says nothing about the new one.
	// Leaving it set strands the block with no path back (issue #339).
	// Hashes that have no row are silently skipped (chaintracks may emit
	// OrphanedHashes for blocks observed before the service started
	// recording). Returns the number of rows whose status actually CHANGED
	// to 'orphaned' — the applied-transition count callers report to the
	// block-status metric. A hash with no row, or a row already 'orphaned',
	// is still written (generation refreshed, reconciled_at cleared) but is
	// not a transition and is not counted. The count falls out of the write
	// itself, so it costs no extra round-trip; pre-reading every hash would
	// put N of them in front of a latency-critical write. It must also be
	// exact under concurrent writers — two replicas orphaning the same row
	// report ONE transition between them — so the status check belongs IN
	// the write (a filter or a generation-checked CAS), never in a separate
	// read whose result an unconditional write then trusts.
	//
	// The stored orphan GENERATION is minted by the store, with orphanedAt
	// as a lower bound: on every orphan write — the transition of an
	// active/parked row and the refresh of a row already orphaned alike —
	// the new generation is max(orphanedAt, previous + ε), where "previous"
	// is the last generation the row ever had (retained through
	// reactivation as a high-water mark) and ε is the backend's smallest
	// step (1 ns; 1 µs for Postgres' timestamptz). So two orphanings of one
	// row never share a generation even when callers reuse a timestamp —
	// which the contract permits — and a reconciler that read generation G
	// can never stamp, reactivate or requeue a LATER orphaning that a
	// wall-clock token would have let it mistake for its own. Tokens are
	// therefore always the value read back from the row
	// (BlockProcessingStatus.OrphanGeneration), never the caller's own
	// timestamp. The refresh of a row already orphaned is forward-only: a
	// stored generation NEWER than orphanedAt is kept — stamp state and all
	// — so a delayed call carrying an older timestamp cannot touch a newer
	// orphaning; an equal or older stored generation still refreshes (a new
	// generation is minted, reconciled_at is cleared, the row is requeued).
	// Implementations must check ctx between rows (or chunks): a cancelled
	// caller — the reconciler's lease heartbeat on lease loss — must not
	// keep writing, and reports what landed.
	MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) (int, error)

	// MarkBlockReconciled stamps reconciled_at on an orphaned block's row,
	// recording that tx re-anchor/revert for this orphan completed. It is a
	// compare-and-set on the orphan generation the caller processed: the
	// stamp applies only while the row is still status='orphaned' AND its
	// stored generation equals orphanedAt — the value the caller read from
	// the row when it dequeued it, never its own timestamp; a zero
	// orphanedAt checks status only. A row the block-status tracker
	// reactivated — or orphaned again, which always mints a newer
	// generation — while the reconciler was working is left untouched
	// (issue #339), as is a missing row. Returns whether the stamp applied.
	MarkBlockReconciled(ctx context.Context, blockHash string, orphanedAt, at time.Time) (bool, error)

	// ListOrphanedBlocksToReconcile returns up to limit rows with
	// status='orphaned' AND reconciled_at IS NULL, oldest orphaned_at
	// first — the anchor reconciler's durable work queue. limit must be > 0.
	ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error)

	// MarkBlocksParked transitions every named block from status='active' to
	// status='parked' — the watchdog's terminal state for blocks whose
	// reprocess caps (attempts/age) are exhausted. Rows that are missing or
	// not 'active' (orphaned rows stay orphaned) are silently skipped. Parked
	// rows drop out of ListStaleBlockProcessingStatus so they read as an
	// explicit triage backlog rather than perpetual stale churn; a later
	// UpsertBlockHeaderSeen for the same hash resets them to 'active'.
	MarkBlocksParked(ctx context.Context, blockHashes []string) error

	// GetBlockProcessingStatus returns the row keyed by blockHash. Returns
	// ErrNotFound if no row exists.
	GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error)

	// ListBlockProcessingStatus returns up to limit rows ordered by
	// block_height DESC. When beforeHeight > 0, restricts to rows with
	// block_height < beforeHeight (the keyset cursor). limit must be > 0.
	ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error)

	// GetActiveTipBlockHeight returns the highest block_height across rows
	// with status='active'. Returns 0 when the table is empty (or every row
	// is orphaned). The bump-builder watchdog uses this to compute a
	// recency window — only blocks within N of the active tip are eligible
	// for /reprocess, so a long BSV Broadcaster outage that floods the table with
	// historical headers doesn't trigger thousands of reprocess calls.
	GetActiveTipBlockHeight(ctx context.Context) (uint64, error)

	// ListStaleBlockProcessingStatus returns up to limit rows where
	// processed_at IS NULL, status='active', header_seen_at < olderThan,
	// and block_height >= minHeight. Ordered by header_seen_at ASC so the
	// watchdog retries the oldest gap first. limit must be > 0.
	ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error)

	// SetMinedByTxIDs marks transactions as mined for a given block (hash + height)
	// and tx list. blockHeight is required: downstream consumers (SSE, webhooks,
	// BUMP-build dedup) rely on the height to anchor each MINED status to a
	// specific block, and a zero/missing height has historically caused dropped
	// updates and BUMP-build re-work (see issue #87 / F-029). Implementations
	// must persist both blockHash and blockHeight on each updated row, and the
	// returned `mined` TransactionStatus values MUST carry BlockHeight populated.
	// Implementations must only update records that already exist in the store;
	// txids with no existing record should be silently skipped (not created).
	//
	// Returns two parallel slices of equal length: `prevs[i]` is the row as it
	// existed immediately before the MINED write, and `mined[i]` is the row as
	// it exists after. Only txids that were actually updated appear in either
	// slice — unknown-txid skips produce no entry on either side. The prevs
	// slice exists so callers can observe the
	// bsv_broadcaster_status_transition_age_seconds{from=*,to=MINED} metric without an
	// extra round-trip.
	SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) (prevs, mined []*models.TransactionStatus, err error)

	// InsertSubmission creates a new submission record
	InsertSubmission(ctx context.Context, sub *models.Submission) error

	// GetSubmissionsByTxID retrieves all active subscriptions for a transaction
	GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error)

	// GetSubmissionsByToken retrieves all submissions for a callback token
	GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error)

	// TokensForTxIDs returns the DISTINCT non-empty callback tokens registered
	// against each of the supplied txids, keyed by txid. A txid with no
	// submission — or none carrying a token — is ABSENT from the result map
	// (never present with an empty slice), so an absent key means "nobody is
	// subscribed". Duplicate txids in the input are collapsed.
	//
	// This is the SSE fan-out hot path. It replaced a per-(event, client)
	// existence probe: fan-out now resolves a txid's token set ONCE and
	// answers every connected client from that set in memory, and resolves a
	// whole bulk event's txid list in one batch. Implementations MUST resolve
	// from the txid side only (a txid has a handful of submissions) and MUST
	// NOT materialize a token's submission list: a single token can hold
	// millions of submissions, and loading one per event is what OOM-killed
	// the SSE service (#237/#238). Implementations that cannot issue an
	// unbounded batch must chunk internally rather than reject the call.
	TokensForTxIDs(ctx context.Context, txids []string) (map[string][]string, error)

	// IterateStatusesByToken streams the current status of every DISTINCT
	// txid registered under callbackToken through fn, in ascending
	// status-timestamp order. Rows are PROJECTED for streaming delivery —
	// txid, status, timestamp, block hash/height only. Implementations MUST
	// NOT load raw_tx or enrich the merkle path: this is the SSE catchup hot
	// path, called for tokens with millions of submissions, and per-row
	// compound-BUMP parsing is what OOMed the SSE service. Filters: when
	// since is non-zero, only statuses with Timestamp strictly after since
	// are emitted; when onlyStatuses is non-empty, only rows in one of those
	// statuses are emitted. fn returning a non-nil error stops the iteration
	// and surfaces that error to the caller.
	//
	// An implementation that cannot serve the replay within its own resource
	// bounds MUST return ErrReplayUnavailable rather than attempt it. Refusing
	// is a supported outcome — the caller degrades to an explicit gap notice
	// and the client reconciles over REST — whereas attempting it is how this
	// path has taken the whole pod down and every connected client with it.
	IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error

	// UpdateDeliveryStatus updates the delivery tracking for a submission
	UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error

	// RecordDeliveryAttempt stamps the outcome of one webhook POST attempt on
	// the submission row: Attempts is incremented and LastAttemptAt/LastResult
	// are overwritten (result is "delivered" or the failure reason, e.g.
	// "status 403"). Deliberately orthogonal to UpdateDeliveryStatus /
	// UpdateDeliveryStatusCAS: those manage the per-transition retry state the
	// CAS resets (issue #166), while this is monotonic lifetime bookkeeping
	// surfaced to clients via GET /tx?callbackToken=… for delivery
	// self-diagnosis (issue #249).
	RecordDeliveryAttempt(ctx context.Context, submissionID string, at time.Time, result string) error

	// UpdateDeliveryStatusCAS atomically advances LastDeliveredStatus from
	// `expected` to `next` for the given submission. Returns true iff a row
	// was updated; false means another replica has already advanced this
	// submission and the caller should silently skip its POST. The retry
	// counter and next-retry timestamp are cleared on success — those are
	// only meaningful while a delivery is in retry state.
	//
	// Used by webhook delivery to coordinate exactly-once POSTs across
	// horizontally-scaled api-server pods. Each replica's events.Publisher
	// subscription gets its own Kafka consumer group, so every pod sees
	// every status update; the CAS funnels concurrent attempts down to one.
	UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (claimed bool, err error)

	// ListSubmissionsReadyForRetry returns up to limit submissions whose
	// delivery is in retry state (retry_count > 0 and next_retry_at <= now).
	// The webhook reaper consumes this to re-fire deliveries whose POST
	// failed after CAS already advanced LastDeliveredStatus — without this
	// sweep, the retry bookkeeping written by recordFailure would never be
	// consumed and a single-attempt failure would be a permanent loss.
	// Ordered by next_retry_at ASC so the oldest backlog drains first.
	ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error)

	// STUMP operations for Merkle Service integration

	// InsertStump stores a STUMP keyed by (block_hash, subtree_index,
	// content_hash). Inserting identical bytes again is a no-op; a different
	// STUMP for the same subtree (another merkle-service with a different
	// registered set) is stored as a second row. ContentHash is computed from
	// StumpData when the caller left it empty.
	InsertStump(ctx context.Context, stump *models.Stump) error

	// GetStumpsByBlockHash retrieves every STUMP row for a block, ordered by
	// subtree index. There may be several rows per subtree index; callers
	// merge them (bump.BuildCompoundBUMP does).
	GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error)

	// DeleteStumpsByBlockHash removes all STUMPs for a given block hash (used during reorg cleanup).
	DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error

	// BumpRetryCount atomically increments retry_count and returns the new value.
	// Does not touch any other bins — callers combine this with
	// SetPendingRetryFields or ClearRetryState depending on the new count.
	BumpRetryCount(ctx context.Context, txid string) (retryCount int, err error)

	// SetPendingRetryFields writes the durable retry bins: status=PENDING_RETRY,
	// raw_tx, next_retry_at, timestamp and retry_reason. lastReason always
	// replaces the stored reason (empty clears it): carrying a reason across
	// attempts is the caller's job (the reaper re-passes the one
	// GetReadyRetries returned), so a tx parked afresh after leaving the
	// queue — by any exit path — never inherits a response from an earlier
	// stay. retry_count is untouched — use BumpRetryCount first to get the
	// value that feeds next_retry_at backoff.
	SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error

	// GetReadyRetries returns up to limit PENDING_RETRY rows whose
	// next_retry_at has elapsed. Rows include raw_tx, retry_count and
	// retry_reason so the reaper can act without a second read per row.
	GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*PendingRetry, error)

	// ClearRetryState transitions a tx out of PENDING_RETRY (either on success
	// or final rejection) and deletes the raw_tx + next_retry_at +
	// retry_reason bins so the row stops showing up in ready-retry queries.
	ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error

	// MarkMerkleRegisteredByTxIDs records that the given txids have been
	// successfully registered with merkle-service at ts. Unknown txids are
	// silently skipped (matching SetMinedByTxIDs semantics). Used by the
	// startup replay loop to skip rows it already registered recently — see
	// issue #145.
	MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error

	// EnsureIndexes provisions whatever the backend needs for query operations
	// (schema, secondary indexes). ctx bounds the whole operation; backends may
	// layer tighter internal deadlines on top.
	EnsureIndexes(ctx context.Context) error

	// UpsertDatahubEndpoint registers (or refreshes the LastSeen of) a datahub
	// URL. Used by p2p_client to publish discovered URLs and by main to seed
	// statically configured URLs so all pods see the same registry.
	UpsertDatahubEndpoint(ctx context.Context, ep DatahubEndpoint) error

	// ListDatahubEndpoints returns every registered datahub endpoint scoped
	// to the given network. Each pod's teranode.Client polls this on a refresh
	// interval and merges new URLs into its in-memory list. Entries written
	// before the schema gained a network column have an empty Network and are
	// excluded — they will be re-registered with the correct network the next
	// time their peer announces.
	ListDatahubEndpoints(ctx context.Context, network string) ([]DatahubEndpoint, error)

	// UpsertPeerPolicy records (or refreshes) a peer's observed mining fee.
	// Called by p2p_client on each node_status announcement carrying a fee.
	UpsertPeerPolicy(ctx context.Context, pp PeerPolicy) error

	// ListPeerPolicies returns every recorded peer policy scoped to the given
	// network. The GET /policy handler filters these by LastSeen and takes the
	// minimum mining fee. Entries with an empty Network (legacy rows) are excluded.
	ListPeerPolicies(ctx context.Context, network string) ([]PeerPolicy, error)

	// Close closes the database connection
	Close() error
}

// ParentKey is one LookupPartitionKeys result: a row's current status and the
// propagation partition key its message carried.
type ParentKey struct {
	Status       models.Status
	PartitionKey string
}
