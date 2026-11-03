package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// Collection and GridFS bucket names. Bucket names are prefixes: GridFS
// materializes them as <name>.files and <name>.chunks.
const (
	collTransactions     = "transactions"
	collSubmissions      = "submissions"
	collBlockProcessing  = "block_processing"
	collLeases           = "leases"
	collDatahubEndpoints = "datahub_endpoints"
	collPeerPolicies     = "peer_policies"
	collBumpManifests    = "bump_manifests"
	collStumpManifests   = "stump_manifests"
	bucketBumps          = "bumps"
	bucketStumps         = "stumps"
	collAPIKeys          = "api_keys"
	collAPIQuota         = "api_quota"
)

// Field names. Every filter and update spells fields through these so a typo
// is a compile error rather than a silently-empty query, and so the bson tags
// on the doc structs below have exactly one counterpart to stay in sync with.
const (
	fID                 = "_id"
	fStatus             = "status"
	fStatusCode         = "status_code"
	fBlockHash          = "block_hash"
	fBlockHeight        = "block_height"
	fMerklePath         = "merkle_path"
	fExtraInfo          = "extra_info"
	fCompetingTxs       = "competing_txs"
	fRawTx              = "raw_tx"
	fRetryCount         = "retry_count"
	fNextRetryAt        = "next_retry_at"
	fRetryReason        = "retry_reason"
	fPartitionKey       = "partition_key"
	fTimestamp          = "timestamp"
	fCreatedAt          = "created_at"
	fMerkleRegisteredAt = "merkle_registered_at"
	fOrphanedAnchors    = "orphaned_anchors"
	fVersion            = "version"

	fTxID                = "txid"
	fCallbackURL         = "callback_url"
	fCallbackToken       = "callback_token"
	fFullStatusUpdates   = "full_status_updates"
	fLastDeliveredStatus = "last_delivered_status"
	fAttempts            = "attempts"
	fLastAttemptAt       = "last_attempt_at"
	fLastResult          = "last_result"

	fHeaderSeenAt = "header_seen_at"
	fProcessedAt  = "processed_at"
	fContentHash  = "content_hash"
	fBUMPBuiltAt  = "bump_built_at"
	fOrphanedAt   = "orphaned_at"
	fOrphanedGen  = "orphaned_gen"
	fReconciledAt = "reconciled_at"
	// fExpectedSubtrees is the union of announced expected subtree indices
	// (store.Store.AddBlockExpectedSubtrees).
	fExpectedSubtrees = "expected_subtrees"

	fHolder    = "holder"
	fExpiresAt = "expires_at"

	fNetwork  = "network"
	fSource   = "source"
	fLastSeen = "last_seen"
	fPolicy   = "policy"

	// GridFS files-collection fields. metadata.* are ours; uploadDate and
	// length are written by the driver.
	fMetaBlockHash    = "metadata.block_hash"
	fMetaSubtreeIndex = "metadata.subtree_index"
	fUploadDate       = "uploadDate"
	fLength           = "length"
	fFilename         = "filename"
	fFilesID          = "files_id"
	fChunkN           = "n"

	// Blob manifest fields.
	fFileID       = "file_id"
	fSubtreeIndex = "subtree_index"
	fUpdatedAt    = "updated_at"

	// API key and quota fields.
	fAPIKeyHash      = "hash"
	fAPIKeyTier      = "tier"
	fAPIKeyRevokedAt = "revoked_at"
	fQuotaUsed       = "used"
	fQuotaLastGrant  = "last_grant"
)

// Query and update operators.
const (
	opSet         = "$set"
	opUnset       = "$unset"
	opInc         = "$inc"
	opSetOnInsert = "$setOnInsert"
	opAddToSet    = "$addToSet"
	opEach        = "$each"
	opIn          = "$in"
	opNin         = "$nin"
	opNe          = "$ne"
	opGt          = "$gt"
	opGte         = "$gte"
	opLt          = "$lt"
	opLte         = "$lte"
	opOr          = "$or"
	opExists      = "$exists"
	opMatch       = "$match"
	opGroup       = "$group"
	opSum         = "$sum"
	opMin         = "$min"
	opMax         = "$max"
	// Aggregation expressions used by the pipeline-form updates.
	opIfNull   = "$ifNull"
	opCond     = "$cond"
	opAdd      = "$add"
	opSubtract = "$subtract"
	opMultiply = "$multiply"
	opDivide   = "$divide"
	opMod      = "$mod"
	opRound    = "$round"
	opToLong   = "$toLong"
	opToDate   = "$toDate"
)

// txDoc is the transactions collection document. Optional fields carry
// omitempty so a zero value is never written: "absent" is the only encoding
// of "no value", which is what makes the {field: null}, $exists and partial
// index predicates in this package correct. Writers that need to clear a
// field use $unset, never $set to zero.
//
// Version is a write counter, not a CAS token: 1 at insert and bumped by
// every write, but never filtered on. The block-scoped rewrites
// (SetMinedByTxIDs, SetStatusByBlockHash) instead carry their guards in the
// update filter itself — the status lattice as a $nin, "still anchored to
// this block" — and apply them one findAndModify per row, so a concurrent
// UpdateStatus is refused by the guard rather than by a version compare. The
// counter stays because it makes a row's write history cheap to read in the
// shell and leaves room for a future guard.
type txDoc struct {
	TxID               string              `bson:"_id"`
	Status             string              `bson:"status"`
	StatusCode         int                 `bson:"status_code,omitempty"`
	BlockHash          string              `bson:"block_hash,omitempty"`
	BlockHeight        int64               `bson:"block_height,omitempty"`
	MerklePath         []byte              `bson:"merkle_path,omitempty"`
	ExtraInfo          string              `bson:"extra_info,omitempty"`
	CompetingTxs       []string            `bson:"competing_txs,omitempty"`
	RawTx              []byte              `bson:"raw_tx,omitempty"`
	RetryCount         int                 `bson:"retry_count,omitempty"`
	NextRetryAt        time.Time           `bson:"next_retry_at,omitempty"`
	Timestamp          time.Time           `bson:"timestamp"`
	CreatedAt          time.Time           `bson:"created_at"`
	MerkleRegisteredAt time.Time           `bson:"merkle_registered_at,omitempty"`
	OrphanedAnchors    []orphanedAnchorDoc `bson:"orphaned_anchors,omitempty"`
	PartitionKey       string              `bson:"partition_key,omitempty"`
	Version            int64               `bson:"version"`
}

// orphanedAnchorDoc is one entry of txDoc.OrphanedAnchors. MerklePath is
// deliberately absent — it is resolved at read time from the orphaned block's
// retained BUMP and never persisted (models.OrphanedAnchor).
type orphanedAnchorDoc struct {
	BlockHash   string    `bson:"block_hash"`
	BlockHeight int64     `bson:"block_height,omitempty"`
	OrphanedAt  time.Time `bson:"orphaned_at"`
}

// txDocFromStatus maps a model to its insert document. Times are truncated to
// millisecond precision — BSON datetime resolution — and written back into
// st so the caller's struct equals what a later read returns.
func txDocFromStatus(st *models.TransactionStatus) txDoc {
	st.Timestamp = msTrunc(st.Timestamp)
	st.CreatedAt = msTrunc(st.CreatedAt)
	st.NextRetryAt = msTrunc(st.NextRetryAt)
	st.MerkleRegisteredAt = msTrunc(st.MerkleRegisteredAt)
	return txDoc{
		TxID:               st.TxID,
		Status:             string(st.Status),
		StatusCode:         st.StatusCode,
		BlockHash:          st.BlockHash,
		BlockHeight:        heightToInt64(st.BlockHeight),
		MerklePath:         []byte(st.MerklePath),
		ExtraInfo:          st.ExtraInfo,
		CompetingTxs:       st.CompetingTxs,
		RawTx:              []byte(st.RawTx),
		RetryCount:         st.RetryCount,
		NextRetryAt:        st.NextRetryAt,
		Timestamp:          st.Timestamp,
		CreatedAt:          st.CreatedAt,
		MerkleRegisteredAt: st.MerkleRegisteredAt,
		OrphanedAnchors:    anchorsToDocs(st.OrphanedProofs),
		PartitionKey:       st.PartitionKey,
	}
}

// toStatus maps a stored document back to the model.
func (d txDoc) toStatus() *models.TransactionStatus {
	st := &models.TransactionStatus{
		TxID:               d.TxID,
		Status:             models.Status(d.Status),
		StatusCode:         d.StatusCode,
		BlockHash:          d.BlockHash,
		BlockHeight:        heightFromInt64(d.BlockHeight),
		ExtraInfo:          d.ExtraInfo,
		CompetingTxs:       d.CompetingTxs,
		RetryCount:         d.RetryCount,
		NextRetryAt:        d.NextRetryAt,
		Timestamp:          d.Timestamp,
		CreatedAt:          d.CreatedAt,
		MerkleRegisteredAt: d.MerkleRegisteredAt,
		OrphanedProofs:     anchorsFromDocs(d.OrphanedAnchors),
		PartitionKey:       d.PartitionKey,
	}
	if len(d.MerklePath) > 0 {
		st.MerklePath = models.HexBytes(d.MerklePath)
	}
	if len(d.RawTx) > 0 {
		st.RawTx = models.HexBytes(d.RawTx)
	}
	return st
}

func anchorsToDocs(anchors []models.OrphanedAnchor) []orphanedAnchorDoc {
	if len(anchors) == 0 {
		return nil
	}
	out := make([]orphanedAnchorDoc, 0, len(anchors))
	for _, a := range anchors {
		out = append(out, orphanedAnchorDoc{
			BlockHash:   a.BlockHash,
			BlockHeight: heightToInt64(a.BlockHeight),
			OrphanedAt:  msTrunc(a.OrphanedAt),
		})
	}
	return out
}

func anchorsFromDocs(docs []orphanedAnchorDoc) []models.OrphanedAnchor {
	if len(docs) == 0 {
		return nil
	}
	out := make([]models.OrphanedAnchor, 0, len(docs))
	for _, d := range docs {
		out = append(out, models.OrphanedAnchor{
			BlockHash:   d.BlockHash,
			BlockHeight: heightFromInt64(d.BlockHeight),
			OrphanedAt:  d.OrphanedAt.UTC(),
		})
	}
	return out
}

// submissionDoc is the submissions collection document. Pointer times stay
// absent while nil, matching models.Submission's "not yet" semantics.
type submissionDoc struct {
	SubmissionID        string     `bson:"_id"`
	TxID                string     `bson:"txid"`
	CallbackURL         string     `bson:"callback_url,omitempty"`
	CallbackToken       string     `bson:"callback_token,omitempty"`
	FullStatusUpdates   bool       `bson:"full_status_updates,omitempty"`
	LastDeliveredStatus string     `bson:"last_delivered_status,omitempty"`
	RetryCount          int        `bson:"retry_count,omitempty"`
	NextRetryAt         *time.Time `bson:"next_retry_at,omitempty"`
	Attempts            int        `bson:"attempts,omitempty"`
	LastAttemptAt       *time.Time `bson:"last_attempt_at,omitempty"`
	LastResult          string     `bson:"last_result,omitempty"`
	CreatedAt           time.Time  `bson:"created_at"`
}

// submissionDocFromModel truncates in the caller's struct as well as in the
// document, including through the optional pointers, so the model a caller
// hands to InsertSubmission equals what a later read returns.
func submissionDocFromModel(sub *models.Submission) submissionDoc {
	sub.CreatedAt = msTrunc(sub.CreatedAt)
	sub.NextRetryAt = msTruncPtr(sub.NextRetryAt)
	sub.LastAttemptAt = msTruncPtr(sub.LastAttemptAt)
	return submissionDoc{
		SubmissionID:        sub.SubmissionID,
		TxID:                sub.TxID,
		CallbackURL:         sub.CallbackURL,
		CallbackToken:       sub.CallbackToken,
		FullStatusUpdates:   sub.FullStatusUpdates,
		LastDeliveredStatus: string(sub.LastDeliveredStatus),
		RetryCount:          sub.RetryCount,
		NextRetryAt:         sub.NextRetryAt,
		Attempts:            sub.Attempts,
		LastAttemptAt:       sub.LastAttemptAt,
		LastResult:          sub.LastResult,
		CreatedAt:           sub.CreatedAt,
	}
}

func (d submissionDoc) toModel() *models.Submission {
	return &models.Submission{
		SubmissionID:        d.SubmissionID,
		TxID:                d.TxID,
		CallbackURL:         d.CallbackURL,
		CallbackToken:       d.CallbackToken,
		FullStatusUpdates:   d.FullStatusUpdates,
		LastDeliveredStatus: models.Status(d.LastDeliveredStatus),
		RetryCount:          d.RetryCount,
		NextRetryAt:         d.NextRetryAt,
		Attempts:            d.Attempts,
		LastAttemptAt:       d.LastAttemptAt,
		LastResult:          d.LastResult,
		CreatedAt:           d.CreatedAt,
	}
}

// blockProcessingDoc is the block_processing collection document.
// header_seen_at is always present: every upsert path writes it via
// $setOnInsert, so no reader needs a "seen == zero" special case.
type blockProcessingDoc struct {
	BlockHash    string     `bson:"_id"`
	BlockHeight  int64      `bson:"block_height"`
	HeaderSeenAt time.Time  `bson:"header_seen_at"`
	ProcessedAt  *time.Time `bson:"processed_at,omitempty"`
	BUMPBuiltAt  *time.Time `bson:"bump_built_at,omitempty"`
	Status       string     `bson:"status"`
	OrphanedAt   *time.Time `bson:"orphaned_at,omitempty"`
	// OrphanedGen is orphaned_at at full precision (Unix nanoseconds) — the
	// orphan GENERATION the reconciler's compare-and-set writes
	// (MarkBlockReconciled, ReactivateBlock) match on. BSON datetimes hold
	// milliseconds, so two orphanings inside one millisecond would be the
	// same generation if orphaned_at alone were the token, and a stale
	// reconciler could then stamp or reactivate the newer one. orphaned_at
	// stays the sortable, indexed copy; this is the identity. Absent on a
	// row written before the field existed, so readers fall back to
	// orphaned_at.
	OrphanedGen  int64      `bson:"orphaned_gen,omitempty"`
	ReconciledAt *time.Time `bson:"reconciled_at,omitempty"`
}

func (d blockProcessingDoc) toModel() *models.BlockProcessingStatus {
	// The generation survives a reactivation as the high-water mark the
	// next orphaning mints above; it is current — and surfaced — only while
	// the row is orphaned.
	var orphanedAt *time.Time
	if d.Status == string(models.BlockStatusOrphaned) {
		orphanedAt = d.OrphanedAt
		if d.OrphanedGen != 0 {
			// Hand callers the full-precision generation, so the value they
			// read back is exactly the token the CAS writes compare on.
			t := time.Unix(0, d.OrphanedGen).UTC()
			orphanedAt = &t
		}
	}
	return &models.BlockProcessingStatus{
		BlockHash:    d.BlockHash,
		BlockHeight:  heightFromInt64(d.BlockHeight),
		HeaderSeenAt: d.HeaderSeenAt,
		ProcessedAt:  d.ProcessedAt,
		BUMPBuiltAt:  d.BUMPBuiltAt,
		Status:       models.BlockProcessingStatusValue(d.Status),
		OrphanedAt:   orphanedAt,
		ReconciledAt: d.ReconciledAt,
	}
}

// leaseDoc is the leases collection document. expires_at, compared on the
// client, is the only expiry that counts; the TTL index on it reaps rows an
// hour after that (leaseReapGraceSeconds), never at it, so the server's clock
// cannot expire a live lease.
type leaseDoc struct {
	Name      string    `bson:"_id"`
	Holder    string    `bson:"holder"`
	ExpiresAt time.Time `bson:"expires_at"`
}

// datahubEndpointDoc is the datahub_endpoints collection document. Policy is
// a nested document that is ABSENT when the node advertised none and present
// — with explicit zeros where needed — when it did. That distinction is
// load-bearing (store.EndpointPolicy): the upsert only $sets policy when the
// caller carries one, so a policy-less refresh never erases a recorded policy.
type datahubEndpointDoc struct {
	URL      string             `bson:"_id"`
	Network  string             `bson:"network"`
	Source   string             `bson:"source"`
	LastSeen time.Time          `bson:"last_seen"`
	Policy   *endpointPolicyDoc `bson:"policy,omitempty"`
}

// endpointPolicyDoc deliberately has no omitempty: a zero is an advertised
// value, and absence is expressed one level up by a nil Policy.
type endpointPolicyDoc struct {
	MiningFeeSatoshis       int64 `bson:"mining_fee_satoshis"`
	MiningFeeBytes          int64 `bson:"mining_fee_bytes"`
	MaxTxSizePolicy         int64 `bson:"max_tx_size_policy"`
	MaxScriptSizePolicy     int64 `bson:"max_script_size_policy"`
	MaxTxSigopsCountsPolicy int64 `bson:"max_tx_sigops_counts_policy"`
}

// endpointPolicyToDoc narrows a validated policy. Callers run
// EndpointPolicy.Validate first, which bounds every field to MaxInt64.
func endpointPolicyToDoc(p *store.EndpointPolicy) *endpointPolicyDoc {
	if p == nil {
		return nil
	}
	return &endpointPolicyDoc{
		MiningFeeSatoshis:       int64(p.MiningFeeSatoshis),       //nolint:gosec // bounded by EndpointPolicy.Validate
		MiningFeeBytes:          int64(p.MiningFeeBytes),          //nolint:gosec // bounded by EndpointPolicy.Validate
		MaxTxSizePolicy:         int64(p.MaxTxSizePolicy),         //nolint:gosec // bounded by EndpointPolicy.Validate
		MaxScriptSizePolicy:     int64(p.MaxScriptSizePolicy),     //nolint:gosec // bounded by EndpointPolicy.Validate
		MaxTxSigopsCountsPolicy: int64(p.MaxTxSigopsCountsPolicy), //nolint:gosec // bounded by EndpointPolicy.Validate
	}
}

func (d datahubEndpointDoc) toModel() store.DatahubEndpoint {
	ep := store.DatahubEndpoint{
		URL:      d.URL,
		Network:  d.Network,
		Source:   d.Source,
		LastSeen: d.LastSeen,
	}
	if p := d.Policy; p != nil {
		ep.Policy = &store.EndpointPolicy{
			MiningFeeSatoshis:       u64(p.MiningFeeSatoshis),
			MiningFeeBytes:          u64(p.MiningFeeBytes),
			MaxTxSizePolicy:         u64(p.MaxTxSizePolicy),
			MaxScriptSizePolicy:     u64(p.MaxScriptSizePolicy),
			MaxTxSigopsCountsPolicy: u64(p.MaxTxSigopsCountsPolicy),
		}
	}
	return ep
}

// peerPolicyDoc is the peer_policies collection document. Numerics carry no
// omitempty: readers treat 0 and absent identically ("not advertised"), and a
// full overwrite on upsert is the contract shared by every backend.
type peerPolicyDoc struct {
	PeerID              string    `bson:"_id"`
	Network             string    `bson:"network"`
	MiningFeeSatoshis   int64     `bson:"mining_fee_satoshis"`
	MiningFeeBytes      int64     `bson:"mining_fee_bytes"`
	MaxTxSizePolicy     int64     `bson:"max_tx_size_policy"`
	MaxScriptSizePolicy int64     `bson:"max_script_size_policy"`
	LastSeen            time.Time `bson:"last_seen"`
}

// peerPolicyToDoc narrows a validated policy; PeerPolicy.Validate bounds every
// field to MaxInt64 before this runs.
func peerPolicyToDoc(pp store.PeerPolicy) peerPolicyDoc {
	return peerPolicyDoc{
		PeerID:              pp.PeerID,
		Network:             pp.Network,
		MiningFeeSatoshis:   int64(pp.MiningFeeSatoshis),   //nolint:gosec // bounded by PeerPolicy.Validate
		MiningFeeBytes:      int64(pp.MiningFeeBytes),      //nolint:gosec // bounded by PeerPolicy.Validate
		MaxTxSizePolicy:     int64(pp.MaxTxSizePolicy),     //nolint:gosec // bounded by PeerPolicy.Validate
		MaxScriptSizePolicy: int64(pp.MaxScriptSizePolicy), //nolint:gosec // bounded by PeerPolicy.Validate
		LastSeen:            msTrunc(pp.LastSeen),
	}
}

func (d peerPolicyDoc) toModel() store.PeerPolicy {
	return store.PeerPolicy{
		PeerID:              d.PeerID,
		Network:             d.Network,
		MiningFeeSatoshis:   u64(d.MiningFeeSatoshis),
		MiningFeeBytes:      u64(d.MiningFeeBytes),
		MaxTxSizePolicy:     u64(d.MaxTxSizePolicy),
		MaxScriptSizePolicy: u64(d.MaxScriptSizePolicy),
		LastSeen:            d.LastSeen,
	}
}

// --- filters shared by several methods ---

// latticeFilter returns the filter clause that pushes
// models.Status.CanTransitionFrom into a single conditional update: the
// document's current status must not be in target's disallowed-previous set.
//
// The target itself is removed from that set first. Several statuses list
// themselves as a disallowed predecessor, but CanTransitionFrom explicitly
// permits re-asserting the same status (an idempotent no-op for duplicate
// callbacks), so a naive $nin would block a transition the lattice allows.
// $nin also matches a document with no status field at all, which is the
// prev == "" branch. An empty result means "reachable from anything" — the
// IMMUTABLE case — and callers must then treat a zero-match as not-found.
func latticeFilter(target models.Status) bson.D {
	disallowed := target.DisallowedPreviousStatuses()
	banned := make([]string, 0, len(disallowed))
	for _, b := range disallowed {
		if b == target {
			continue
		}
		banned = append(banned, string(b))
	}
	if len(banned) == 0 {
		return bson.D{}
	}
	return bson.D{{Key: fStatus, Value: bson.D{{Key: opNin, Value: banned}}}}
}

// trackerFilter is the server-side pushdown of store.TrackerScan.Keep. It is
// a disjunction of plain conjunctions so each branch gets exact bounds on
// the {status, block_height, _id} index:
//   - status in the tracked set minus MINED;
//   - status MINED with no usable height (absent, null or 0) — never prunable;
//   - status MINED with block_height >= PruneMinedBelow (only when pruning).
//
// Over-emitting is a performance property only — the caller re-applies Keep —
// but under-emitting is a correctness bug, so the unit test in this package
// evaluates the filter in Go against Keep over the full status × height grid.
func trackerFilter(scan store.TrackerScan) bson.D {
	tracked := store.TrackerStatuses()
	nonMined := make([]string, 0, len(tracked))
	hasMined := false
	for _, s := range tracked {
		if s == models.StatusMined {
			hasMined = true
			continue
		}
		nonMined = append(nonMined, string(s))
	}
	branches := bson.A{bson.D{{Key: fStatus, Value: bson.D{{Key: opIn, Value: nonMined}}}}}
	if hasMined {
		mined := string(models.StatusMined)
		if scan.PruneMinedBelow == 0 {
			branches = append(branches, bson.D{{Key: fStatus, Value: mined}})
		} else {
			branches = append(branches,
				bson.D{{Key: fStatus, Value: mined}, {Key: fBlockHeight, Value: bson.D{{Key: opIn, Value: bson.A{nil, int64(0)}}}}},
				bson.D{{Key: fStatus, Value: mined}, {Key: fBlockHeight, Value: bson.D{{Key: opGte, Value: heightToInt64(scan.PruneMinedBelow)}}}},
			)
		}
	}
	return bson.D{{Key: opOr, Value: branches}}
}

// --- scalar helpers ---

// msTrunc truncates t to BSON datetime resolution and normalizes it to UTC.
// Zero stays zero so omitempty still drops it.
//
// The UTC conversion is part of the write-back invariant, not cosmetic: a BSON
// datetime carries no zone, so the driver decodes every stored timestamp as
// UTC. A caller handing us a time.Time in Local (or any other location) would
// otherwise keep its own wall/zone in the struct we wrote back while a later
// read of the same row returns the same instant in UTC — equal under
// time.Time.Equal but NOT under == or reflect.DeepEqual, which is the
// comparison the contract tests and callers actually use. Truncate itself is
// zone-independent (it rounds the absolute time since the zero instant), so
// truncating before or after the conversion gives the same answer.
func msTrunc(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.Truncate(time.Millisecond).UTC()
}

// msCeil rounds t UP to the next millisecond unless it is already aligned.
// Stored timestamps are millisecond-aligned, so an inclusive lower bound
// (>= since) and an exclusive upper bound (< deadline) must round up to keep
// their contract: with since = 12.345 ms a stored 12 ms is not >= since, and
// with deadline = 12.345 ms a stored 12 ms is < deadline. Flooring (msTrunc)
// remains correct for > and <= bounds. Zero stays zero.
func msCeil(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	floored := t.Truncate(time.Millisecond).UTC()
	if floored.Equal(t) {
		return floored
	}
	return floored.Add(time.Millisecond)
}

func msTruncPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := msTrunc(*t)
	return &v
}

// heightToInt64 narrows a block height. Heights are tiny relative to the
// int64 range; a value above MaxInt64 could only be memory corruption, and
// clamping keeps it from wrapping negative into an index.
func heightToInt64(h uint64) int64 {
	if h > uint64(1<<63-1) {
		return 1<<63 - 1
	}
	return int64(h)
}

func heightFromInt64(h int64) uint64 {
	return u64(h)
}

// u64 widens a stored signed value, reading anything negative as 0 (the
// "not advertised" / "no height" value) rather than as an astronomical
// unsigned number.
func u64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
