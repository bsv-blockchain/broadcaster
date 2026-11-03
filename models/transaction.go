// Package models contains data structures for transaction and status management.
package models

import (
	"encoding/hex"
	"time"
)

// HexBytes is a byte slice that marshals to/from hex strings in JSON
type HexBytes []byte

// MarshalJSON converts HexBytes to a JSON-encoded hex string.
func (h HexBytes) MarshalJSON() ([]byte, error) {
	if h == nil {
		return []byte("null"), nil
	}
	return []byte(`"` + hex.EncodeToString(h) + `"`), nil
}

// UnmarshalJSON decodes a JSON-encoded hex string into HexBytes.
func (h *HexBytes) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*h = nil
		return nil
	}
	// Remove quotes
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		data = data[1 : len(data)-1]
	}
	decoded, err := hex.DecodeString(string(data))
	if err != nil {
		return err
	}
	*h = decoded
	return nil
}

// TransactionStatus represents the current status of a transaction.
// RawTx is carried from intake so the propagation reaper can rebroadcast a
// stuck row. NextRetryAt (with the store's retry_reason and the park reason in
// ExtraInfo) is set while the tx waits in PENDING_RETRY and cleared by any
// write that moves it to ACCEPTED_BY_NETWORK or beyond — see
// Status.ClearsRetryState.
type TransactionStatus struct {
	TxID       string    `json:"txid"`
	Status     Status    `json:"txStatus"`
	StatusCode int       `json:"status,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	// TxIDs, when populated, marks this as a bulk-transition event: every
	// txid in the slice carries the same payload — Status, BlockHash,
	// BlockHeight, ExtraInfo and StatusCode all apply to all of them. Used
	// by bump-builder's MINED fan-out to publish ONE Kafka event per block
	// instead of N events per block, which previously saturated the webhook
	// service's bounded work queue (~185k drops/block on testnet bursts).
	// Subscribers detect bulk via len(TxIDs) > 0 and unfan in their own
	// handler, copying the whole struct per txid — so a publisher holding a
	// per-transaction payload must split the event by payload rather than
	// send one whose fields fit only some of its txids (issue #301; see
	// propagation.publishRejections). omitempty so the HTTP API surface
	// (single-tx status reads) is unaffected — TxIDs is never set on rows
	// read from the store, only on transient events on the publisher channel.
	TxIDs        []string `json:"txids,omitempty"`
	BlockHash    string   `json:"blockHash,omitempty"`
	BlockHeight  uint64   `json:"blockHeight,omitempty"`
	MerklePath   HexBytes `json:"merklePath,omitempty"`
	ExtraInfo    string   `json:"extraInfo,omitempty"`
	CompetingTxs []string `json:"competingTxs,omitempty"`
	RawTx        HexBytes `json:"rawTx,omitempty"`
	RetryCount   int      `json:"retryCount,omitempty"`
	// omitzero, not omitempty: omitempty never omits a struct, so a zero
	// time.Time used to serialize as "0001-01-01T00:00:00Z" on every row
	// that had no retry scheduled.
	NextRetryAt time.Time `json:"nextRetryAt,omitzero"`
	CreatedAt   time.Time `json:"-"`
	// OrphanedProofs preserves every block this tx was once MINED against
	// before a reorg superseded the anchor (issue #279). Stores append an
	// entry whenever an existing MINED anchor is overwritten with a
	// different block (re-anchor) or cleared by a reorg revert, capped at
	// MaxOrphanedAnchors. Each entry's MerklePath is enriched at read time
	// from the orphaned block's retained compound BUMP — never persisted —
	// so clients can still verify the historical proof against the
	// orphaned header.
	OrphanedProofs []OrphanedAnchor `json:"orphanedProofs,omitempty"`
	// MerkleRegisteredAt is the wall-clock time of the most recent
	// successful merkle-service /watch registration for this txid. Zero
	// value means "never registered" (or registered before this field was
	// added). The startup replay loop reads this to skip rows registered
	// within MerkleReplaySkipRecentMinutes — see issue #145.
	MerkleRegisteredAt time.Time `json:"merkleRegisteredAt,omitzero"`
	// PartitionKey is the bsv-broadcaster.propagation partition key the row's
	// propagation message was published with. Persisted so a later single-tx
	// submission spending this row's outputs can key itself the same way while
	// the row is still in flight, which puts the child on the dispatcher that
	// holds its parent (api_server.partitionKeyForSubmit). Routing state, not
	// status: never serialized, read only through store.LookupPartitionKeys.
	PartitionKey string `json:"-"`
}

// Status represents the various states a transaction can be in
type Status string

const (
	// StatusUnknown indicates the transaction status is unknown.
	StatusUnknown = Status("UNKNOWN")
	// StatusReceived indicates the transaction was received.
	StatusReceived = Status("RECEIVED")
	// StatusSentToNetwork indicates the transaction was sent to the network.
	StatusSentToNetwork = Status("SENT_TO_NETWORK")
	// StatusAcceptedByNetwork indicates the transaction was accepted by the network.
	StatusAcceptedByNetwork = Status("ACCEPTED_BY_NETWORK")
	// StatusSeenOnNetwork indicates the transaction was seen on the network.
	StatusSeenOnNetwork = Status("SEEN_ON_NETWORK")
	// StatusSeenMultipleNodes indicates the transaction was seen by multiple independent miners.
	StatusSeenMultipleNodes = Status("SEEN_MULTIPLE_NODES")
	// StatusDoubleSpendAttempted indicates a double spend was attempted.
	StatusDoubleSpendAttempted = Status("DOUBLE_SPEND_ATTEMPTED")
	// StatusRejected indicates the transaction was rejected.
	StatusRejected = Status("REJECTED")
	// StatusPendingRetry indicates the transaction broadcast failed with a retryable error and will be retried.
	StatusPendingRetry = Status("PENDING_RETRY")
	// StatusStumpProcessing indicates the transaction has a STUMP and is building the BUMP
	StatusStumpProcessing = Status("STUMP_PROCESSING")
	// StatusMined indicates the transaction was mined.
	StatusMined = Status("MINED")
	// StatusImmutable indicates the transaction is immutable.
	StatusImmutable = Status("IMMUTABLE")
)

// AllStatuses returns every defined Status, in lifecycle order. Callers that
// enumerate the status domain (metric pre-registration, exhaustive lattice
// checks) use this instead of maintaining their own copy of the const block.
func AllStatuses() []Status {
	return []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusDoubleSpendAttempted, StatusRejected, StatusPendingRetry,
		StatusStumpProcessing, StatusMined, StatusImmutable,
	}
}

// terminalStatuses lists statuses that represent a final outcome for a
// transaction. A row already in one of these states must never be overwritten
// by a lower-priority update (e.g. a late SEEN_ON_NETWORK callback arriving
// after MINED). The only allowed transition out of a terminal status is the
// MINED → IMMUTABLE promotion handled explicitly by CanTransitionFrom.
var terminalStatuses = map[Status]struct{}{
	StatusRejected:             {},
	StatusDoubleSpendAttempted: {},
	StatusMined:                {},
	StatusImmutable:            {},
}

// IsTerminal reports whether this status is a final outcome that should not be
// overwritten by later, lower-priority updates.
func (s Status) IsTerminal() bool {
	_, ok := terminalStatuses[s]
	return ok
}

// NonTerminalStatuses returns the statuses a transaction can still move on
// from — the "pending" set a reconnecting client needs replayed. Everything
// terminal (MINED, IMMUTABLE, REJECTED, DOUBLE_SPEND_ATTEMPTED) is excluded:
// terminal history is queryable via the REST API and replaying it wholesale
// is what made SSE catchup unbounded.
func NonTerminalStatuses() []Status {
	return []Status{
		StatusReceived, StatusSentToNetwork, StatusAcceptedByNetwork,
		StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusPendingRetry, StatusStumpProcessing,
	}
}

// ClearsRetryState reports whether a write to s is a positive network verdict
// that retires the row's retry bookkeeping. Once the network has accepted,
// seen or mined the transaction, the park reason in extra_info, the schedule
// in next_retry_at and the last network response in retry_reason describe a
// past the row has left; every store clears them on such a write, replacing
// extra_info with whatever the write itself carries. Without this a row that
// recovered from PENDING_RETRY kept reporting "parent not yet accepted …
// retryable" and a nextRetryAt on GET /tx all the way to MINED. retry_count
// is kept as history and raw_tx because the reaper still rebroadcasts stuck
// rows from it.
func (s Status) ClearsRetryState() bool {
	switch s {
	case StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes, StatusMined, StatusImmutable:
		return true
	default:
		return false
	}
}

// DisallowedPreviousStatuses returns statuses that CANNOT transition to this status.
// Used by stores' UpdateStatus implementations (and CanTransitionFrom) to prevent
// invalid status transitions — most importantly, to prevent terminal statuses
// (MINED, IMMUTABLE, REJECTED, DOUBLE_SPEND_ATTEMPTED) from being silently
// overwritten by a later, lower-priority update such as SEEN_ON_NETWORK.
func (s Status) DisallowedPreviousStatuses() []Status {
	switch s {
	case StatusUnknown, StatusReceived:
		// Going back to UNKNOWN/RECEIVED is never valid once the tx has any
		// further status — only an initial insert should set these.
		return []Status{
			StatusSentToNetwork, StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusPendingRetry, StatusStumpProcessing,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSentToNetwork:
		// PENDING_RETRY → SENT_TO_NETWORK is the reaper republishing a retry-
		// queued tx, so it must be allowed; everything to the right of
		// SENT_TO_NETWORK in the forward order is a regression.
		return []Status{
			StatusSentToNetwork, StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusAcceptedByNetwork:
		return []Status{
			StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSeenOnNetwork:
		// SEEN_ON_NETWORK is a forward step that can follow earlier states or
		// recover a previously-REJECTED tx (e.g., a peer accepted after another
		// peer rejected, or the underlying cause of the rejection cleared).
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusPendingRetry,
			StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSeenMultipleNodes:
		return []Status{
			StatusSeenMultipleNodes,
			StatusPendingRetry,
			StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusPendingRetry:
		// PENDING_RETRY is only valid for txs that are still in flight — never
		// from a terminal state, and never from an already-confirmed state.
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusStumpProcessing:
		// STUMP_PROCESSING relates to mined-block bump building. It is only
		// meaningful for in-flight txs; once a tx is terminal it must not be
		// pushed back into STUMP_PROCESSING.
		return []Status{
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusRejected:
		// REJECTED may overwrite pre-network in-flight state (RECEIVED /
		// SENT / ACCEPTED / PENDING_RETRY), but must not clobber a tx that
		// has already been observed on the network or confirmed. Issue #251:
		// resubmitting an already-broadcast (and often already-mined) tx
		// must not regress SEEN_* → REJECTED when intake validation fails
		// on spent inputs of the same txid.
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusMined, StatusImmutable,
		}
	case StatusDoubleSpendAttempted:
		// Conflict detection can mark a SEEN tx; it must not clobber MINED /
		// IMMUTABLE.
		return []Status{StatusMined, StatusImmutable}
	case StatusMined:
		// MINED can be set from any in-flight state, but a transient miner-
		// reorg-style regression must not pull a tx out of IMMUTABLE.
		return []Status{StatusImmutable}
	case StatusImmutable:
		// IMMUTABLE is the highest-priority sink — reachable from anything.
		return []Status{}
	default:
		return []Status{}
	}
}

// CanTransitionFrom reports whether moving from prev → s is allowed by the
// status lattice. An empty prev (i.e. no existing row) is always allowed —
// the very first write for a txid bypasses the lattice. Re-asserting the same
// status (prev == s) is also allowed: it is an idempotent no-op for callers
// that may receive duplicate callbacks.
func (s Status) CanTransitionFrom(prev Status) bool {
	if prev == "" || prev == s {
		return true
	}
	for _, banned := range s.DisallowedPreviousStatuses() {
		if banned == prev {
			return false
		}
	}
	return true
}

// Submission represents a client's submission and subscription preferences.
// Deliberately carries no json tags: it is never serialized to clients
// directly (CallbackToken must not leak) — the api-server maps it to a
// response DTO where needed.
type Submission struct {
	SubmissionID        string
	TxID                string
	CallbackURL         string
	CallbackToken       string
	FullStatusUpdates   bool
	LastDeliveredStatus Status
	// RetryCount and NextRetryAt are the webhook retry-scheduling state for
	// the current transition: bumped by each failed POST, cleared by the CAS
	// when the next transition is claimed.
	RetryCount  int
	NextRetryAt *time.Time
	// Attempts, LastAttemptAt and LastResult are lifetime delivery
	// bookkeeping, stamped after every POST regardless of outcome and never
	// reset: Attempts counts every POST ever made for this submission,
	// LastResult holds "delivered" or the last failure ("status 403", a
	// transport error). Surfaced on GET /tx?callbackToken=… so receivers can
	// self-diagnose delivery problems (issue #249: a WAF 403-ing every
	// callback is invisible from the receiver's own logs).
	Attempts      int
	LastAttemptAt *time.Time
	LastResult    string
	CreatedAt     time.Time
}

// OrphanedAnchor is one historical MINED anchor a reorg superseded: the
// block the tx was anchored to, and when the anchor was replaced. Kept on
// the transaction row (capped at MaxOrphanedAnchors) so the orphaned
// block's proof remains auditable after re-anchoring — see issue #279.
type OrphanedAnchor struct {
	BlockHash   string    `json:"blockHash"`
	BlockHeight uint64    `json:"blockHeight"`
	OrphanedAt  time.Time `json:"orphanedAt"`
	// MerklePath is the tx's proof against the orphaned block, resolved at
	// read time from that block's retained compound BUMP. Empty when the
	// BUMP is no longer available. Never persisted.
	MerklePath HexBytes `json:"merklePath,omitempty"`
}

// MaxOrphanedAnchors caps the orphaned-anchor history kept per transaction.
// A flip-flopping competition rewrites the same two anchors, so a small cap
// bounds row growth without losing real history.
const MaxOrphanedAnchors = 5

// ExtraInfo markers carried on reorg-correction status events (issue #279).
// They ride the bulk event templates on bsv-broadcaster.tx_status so downstream
// consumers (webhook, SSE) can distinguish a reorg correction from a plain
// transition — in particular, the webhook's same-status dedup makes an
// exception for a MINED event flagged as a re-anchor, since it carries a
// new block hash + merkle path.
const (
	// ExtraInfoReorgReanchor marks a MINED event whose txs were re-anchored
	// from an orphaned block to the active-chain block at its height.
	ExtraInfoReorgReanchor = "reorg_reanchor"
	// ExtraInfoReorgUnmined marks a SEEN_ON_NETWORK event whose txs were
	// reverted out of an orphaned block they do not exist outside of.
	ExtraInfoReorgUnmined = "reorg_unmined"
)

// AppendOrphanedAnchor appends anchor to history, dropping the oldest
// entries beyond MaxOrphanedAnchors. Duplicate consecutive anchors for the
// same block hash are collapsed (idempotent replays must not grow history).
func AppendOrphanedAnchor(history []OrphanedAnchor, anchor OrphanedAnchor) []OrphanedAnchor {
	if anchor.BlockHash == "" {
		return history
	}
	if n := len(history); n > 0 && history[n-1].BlockHash == anchor.BlockHash {
		return history
	}
	history = append(history, anchor)
	if len(history) > MaxOrphanedAnchors {
		history = history[len(history)-MaxOrphanedAnchors:]
	}
	return history
}

// BlockReorg represents a notification that a block was orphaned due to a chain reorganization
type BlockReorg struct {
	BlockHash   string
	BlockHeight uint64
	Timestamp   time.Time
}

// SubmitOptions contains options for transaction submission
type SubmitOptions struct {
	CallbackURL          string // Webhook URL for status callbacks
	CallbackToken        string // Token for SSE event filtering
	FullStatusUpdates    bool   // Send all status updates (not just final)
	SkipFeeValidation    bool   // Skip fee validation
	SkipScriptValidation bool   // Skip script validation
}

// FeeAmount is the ARC mining-fee rate expressed as a number of satoshis per
// a number of bytes, e.g. {Satoshis: 100, Bytes: 1000} == 100 sat/kB.
type FeeAmount struct {
	Satoshis uint64 `json:"satoshis"`
	Bytes    uint64 `json:"bytes"`
}

// Policy is the ARC-compatible transaction policy document returned by
// GET /policy (issue #212). Field names and JSON tags match the ARC contract
// so BSV Broadcaster is a drop-in target for clients that discover fee/size limits
// before building transactions.
type Policy struct {
	MiningFee               FeeAmount `json:"miningFee"`
	MaxTxSizePolicy         uint64    `json:"maxtxsizepolicy"`
	MaxScriptSizePolicy     uint64    `json:"maxscriptsizepolicy"`
	MaxTxSigopsCountsPolicy int64     `json:"maxtxsigopscountspolicy"`
	StandardFormatSupported bool      `json:"standardFormatSupported"`
}

// PolicyResponse wraps Policy in the ARC response envelope with a policy
// timestamp (RFC 3339).
type PolicyResponse struct {
	Policy    Policy    `json:"policy"`
	Timestamp time.Time `json:"timestamp"`
}
