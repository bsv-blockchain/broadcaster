// Package pebble implements store.Store, store.Leaser, store.APIKeyStore
// and store.QuotaStore on top of github.com/cockroachdb/pebble, a pure-Go
// LSM KV. It's the recommended backend for BSV Broadcaster's zero-dependency
// standalone mode: no external services, single binary, durable on disk.
//
// Layout: see keys.go for the key namespaces. Primary records hold
// JSON-encoded values; secondary indexes are empty-value prefix keys whose
// suffix names the primary row (idx:apikey:hash:<hash> is the one
// exception: it stores the key id as its value). Mutations that touch more
// than one key go through a Batch so stale index entries are removed
// atomically with the primary write.
//
// Single-process: Pebble takes an exclusive file lock on its data directory.
// Running two BSV Broadcaster instances against the same path will fail at Open —
// acceptable for standalone, mirrors the plan's single-node constraint.
package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"sync"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/bumpcache"
)

// getOrInsertShards serializes GetOrInsertStatus per-txid so concurrent
// inserts of the same txid collapse to a single write. Pebble has no CAS,
// and a read-then-write pattern races between goroutines. 64 shards keeps
// contention cheap.
const getOrInsertShards = 64

var (
	_ store.Store  = (*Store)(nil)
	_ store.Leaser = (*Store)(nil)
)

// Store is a Pebble-backed implementation of store.Store, store.Leaser,
// store.APIKeyStore and store.QuotaStore.
type Store struct {
	db        *pebbledb.DB
	writeOpts *pebbledb.WriteOptions
	cfg       config.Pebble
	// bumpCache holds parsed+indexed compound BUMPs for merkle-path
	// enrichment — sizing, budget, and singleflight live in bumpcache.
	bumpCache *bumpcache.Cache
	insertMu  [getOrInsertShards]sync.Mutex
	leaseMu   sync.Mutex
	// submissionMu serializes the read-modify-write sequence in
	// UpdateDeliveryStatusCAS. Pebble is embedded and single-process, so
	// a single mutex across all submissions is enough — webhook CAS
	// contention is in the low-Hz range even at peak.
	submissionMu sync.Mutex
	// apiKeyMu serializes API-key read-modify-writes (create's uniqueness
	// checks, tier updates, revocation). Admin traffic only.
	apiKeyMu sync.Mutex
	// quota holds the rate limiter's per-window counters in memory: Pebble is
	// single-process, so they are exact without persistence, and a restart
	// loses at most the current minute. See apikeys.go.
	quotaMu      sync.Mutex
	quota        map[quotaKey]int64
	quotaSweptAt time.Time
}

// storedStatus is the persistence shape for TransactionStatus rows. We keep
// it parallel to models.TransactionStatus but redefine Time fields as int64
// unix-nanoseconds to dodge JSON time-zone drift and to keep retry index
// keys byte-for-byte consistent.
type storedStatus struct {
	TxID                   string                 `json:"txid"`
	Status                 string                 `json:"status"`
	StatusCode             int                    `json:"status_code,omitempty"`
	BlockHash              string                 `json:"block_hash,omitempty"`
	BlockHeight            uint64                 `json:"block_height,omitempty"`
	MerklePath             []byte                 `json:"merkle_path,omitempty"`
	ExtraInfo              string                 `json:"extra_info,omitempty"`
	CompetingTxs           []string               `json:"competing_txs,omitempty"`
	RawTx                  []byte                 `json:"raw_tx,omitempty"`
	RetryCount             int                    `json:"retry_count,omitempty"`
	TimestampUnixNs        int64                  `json:"ts"`
	CreatedUnixNs          int64                  `json:"created_at,omitempty"`
	NextRetryUnixNs        int64                  `json:"next_retry_at,omitempty"`
	RetryReason            string                 `json:"retry_reason,omitempty"`
	MerkleRegisteredUnixNs int64                  `json:"merkle_registered_at,omitempty"`
	OrphanedAnchors        []storedOrphanedAnchor `json:"orphaned_anchors,omitempty"`
	PartitionKey           string                 `json:"partition_key,omitempty"`
}

// storedOrphanedAnchor is the persisted form of models.OrphanedAnchor —
// block identity + when the anchor was superseded. The proof itself is
// never persisted; it is re-derived from the orphaned block's retained
// compound BUMP at read time.
type storedOrphanedAnchor struct {
	BlockHash      string `json:"block_hash"`
	BlockHeight    uint64 `json:"block_height,omitempty"`
	OrphanedUnixNs int64  `json:"orphaned_at"`
}

// appendOrphanedAnchor records that the row's current anchor is being
// superseded (re-anchor to a different block, or reorg revert). Collapses
// consecutive duplicates and caps at models.MaxOrphanedAnchors, mirroring
// models.AppendOrphanedAnchor.
func appendOrphanedAnchor(row *storedStatus, now time.Time) {
	if row.BlockHash == "" {
		return
	}
	if n := len(row.OrphanedAnchors); n > 0 && row.OrphanedAnchors[n-1].BlockHash == row.BlockHash {
		return
	}
	row.OrphanedAnchors = append(row.OrphanedAnchors, storedOrphanedAnchor{
		BlockHash:      row.BlockHash,
		BlockHeight:    row.BlockHeight,
		OrphanedUnixNs: now.UnixNano(),
	})
	if len(row.OrphanedAnchors) > models.MaxOrphanedAnchors {
		row.OrphanedAnchors = row.OrphanedAnchors[len(row.OrphanedAnchors)-models.MaxOrphanedAnchors:]
	}
}

func (s storedStatus) toModel() *models.TransactionStatus {
	out := &models.TransactionStatus{
		TxID:         s.TxID,
		Status:       models.Status(s.Status),
		StatusCode:   s.StatusCode,
		BlockHash:    s.BlockHash,
		BlockHeight:  s.BlockHeight,
		MerklePath:   models.HexBytes(s.MerklePath),
		ExtraInfo:    s.ExtraInfo,
		CompetingTxs: s.CompetingTxs,
		RawTx:        models.HexBytes(s.RawTx),
		RetryCount:   s.RetryCount,
		PartitionKey: s.PartitionKey,
	}
	if s.TimestampUnixNs != 0 {
		out.Timestamp = time.Unix(0, s.TimestampUnixNs)
	}
	if s.CreatedUnixNs != 0 {
		out.CreatedAt = time.Unix(0, s.CreatedUnixNs)
	}
	if s.NextRetryUnixNs != 0 {
		out.NextRetryAt = time.Unix(0, s.NextRetryUnixNs)
	}
	if s.MerkleRegisteredUnixNs != 0 {
		out.MerkleRegisteredAt = time.Unix(0, s.MerkleRegisteredUnixNs)
	}
	for _, a := range s.OrphanedAnchors {
		out.OrphanedProofs = append(out.OrphanedProofs, models.OrphanedAnchor{
			BlockHash:   a.BlockHash,
			BlockHeight: a.BlockHeight,
			OrphanedAt:  time.Unix(0, a.OrphanedUnixNs).UTC(),
		})
	}
	return out
}

func fromModel(m *models.TransactionStatus) storedStatus {
	out := storedStatus{
		TxID:         m.TxID,
		Status:       string(m.Status),
		StatusCode:   m.StatusCode,
		BlockHash:    m.BlockHash,
		BlockHeight:  m.BlockHeight,
		MerklePath:   []byte(m.MerklePath),
		ExtraInfo:    m.ExtraInfo,
		CompetingTxs: m.CompetingTxs,
		RawTx:        []byte(m.RawTx),
		RetryCount:   m.RetryCount,
		PartitionKey: m.PartitionKey,
	}
	if !m.Timestamp.IsZero() {
		out.TimestampUnixNs = m.Timestamp.UnixNano()
	}
	if !m.CreatedAt.IsZero() {
		out.CreatedUnixNs = m.CreatedAt.UnixNano()
	}
	if !m.NextRetryAt.IsZero() {
		out.NextRetryUnixNs = m.NextRetryAt.UnixNano()
	}
	if !m.MerkleRegisteredAt.IsZero() {
		out.MerkleRegisteredUnixNs = m.MerkleRegisteredAt.UnixNano()
	}
	for _, a := range m.OrphanedProofs {
		out.OrphanedAnchors = append(out.OrphanedAnchors, storedOrphanedAnchor{
			BlockHash:      a.BlockHash,
			BlockHeight:    a.BlockHeight,
			OrphanedUnixNs: a.OrphanedAt.UnixNano(),
		})
	}
	return out
}

type storedSubmission struct {
	SubmissionID        string `json:"submission_id"`
	TxID                string `json:"txid"`
	CallbackURL         string `json:"callback_url,omitempty"`
	CallbackToken       string `json:"callback_token,omitempty"`
	FullStatusUpdates   bool   `json:"full_status_updates,omitempty"`
	LastDeliveredStatus string `json:"last_delivered_status,omitempty"`
	RetryCount          int    `json:"retry_count,omitempty"`
	NextRetryUnixNs     int64  `json:"next_retry_at,omitempty"`
	Attempts            int    `json:"attempts,omitempty"`
	LastAttemptUnixNs   int64  `json:"last_attempt_at,omitempty"`
	LastResult          string `json:"last_result,omitempty"`
	CreatedUnixNs       int64  `json:"created_at"`
}

func (s storedSubmission) toModel() *models.Submission {
	out := &models.Submission{
		SubmissionID:        s.SubmissionID,
		TxID:                s.TxID,
		CallbackURL:         s.CallbackURL,
		CallbackToken:       s.CallbackToken,
		FullStatusUpdates:   s.FullStatusUpdates,
		LastDeliveredStatus: models.Status(s.LastDeliveredStatus),
		RetryCount:          s.RetryCount,
		Attempts:            s.Attempts,
		LastResult:          s.LastResult,
	}
	if s.CreatedUnixNs != 0 {
		out.CreatedAt = time.Unix(0, s.CreatedUnixNs)
	}
	if s.NextRetryUnixNs != 0 {
		t := time.Unix(0, s.NextRetryUnixNs)
		out.NextRetryAt = &t
	}
	if s.LastAttemptUnixNs != 0 {
		t := time.Unix(0, s.LastAttemptUnixNs)
		out.LastAttemptAt = &t
	}
	return out
}

type storedLease struct {
	Holder        string `json:"holder"`
	ExpiresUnixNs int64  `json:"expires_at"`
}

type storedBump struct {
	BlockHeight uint64 `json:"block_height"`
	BumpData    []byte `json:"bump_data"`
}

type storedDatahubEndpoint struct {
	URL     string `json:"url"`
	Network string `json:"network"`
	Source  string `json:"source"`
	// Policy is absent on rows written before endpoints carried an advertised
	// policy, and on any endpoint whose node advertised none. A missing JSON
	// key decodes to nil, which is exactly that sentinel — no migration needed.
	Policy         *storedEndpointPolicy `json:"policy,omitempty"`
	LastSeenUnixNs int64                 `json:"last_seen"`
}

type storedEndpointPolicy struct {
	MiningFeeSatoshis       uint64 `json:"mining_fee_satoshis"`
	MiningFeeBytes          uint64 `json:"mining_fee_bytes"`
	MaxTxSizePolicy         uint64 `json:"max_tx_size_policy"`
	MaxScriptSizePolicy     uint64 `json:"max_script_size_policy"`
	MaxTxSigopsCountsPolicy uint64 `json:"max_tx_sigops_counts_policy"`
}

type storedPeerPolicy struct {
	PeerID            string `json:"peer_id"`
	Network           string `json:"network"`
	MiningFeeSatoshis uint64 `json:"mining_fee_satoshis"`
	MiningFeeBytes    uint64 `json:"mining_fee_bytes"`
	// Absent in rows written before the size limits were observed; JSON decodes
	// a missing key as 0, which is exactly the "peer did not advertise"
	// sentinel, so no migration is needed.
	MaxTxSizePolicy     uint64 `json:"max_tx_size_policy,omitempty"`
	MaxScriptSizePolicy uint64 `json:"max_script_size_policy,omitempty"`
	LastSeenUnixNs      int64  `json:"last_seen"`
}

// New opens a Pebble database at cfg.Path and returns a Store ready to use.
// If the directory does not exist it's created. The returned Store takes an
// exclusive file lock on the directory — closing the Store releases it.
func New(cfg config.Pebble) (*Store, error) {
	opts := &pebbledb.Options{}
	if cfg.MemTableSizeMB > 0 {
		opts.MemTableSize = uint64(cfg.MemTableSizeMB) << 20
	}
	if cfg.L0CompactionThreshold > 0 {
		opts.L0CompactionThreshold = cfg.L0CompactionThreshold
	}

	db, err := pebbledb.Open(cfg.Path, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble at %s: %w", cfg.Path, err)
	}

	return &Store{
		db:        db,
		writeOpts: &pebbledb.WriteOptions{Sync: cfg.SyncWrites},
		cfg:       cfg,
		bumpCache: bumpcache.New(),
	}, nil
}

// Close flushes any in-memory writes and releases the file lock.
func (s *Store) Close() error {
	if err := s.db.Flush(); err != nil {
		// Best-effort flush; always try to close.
		_ = s.db.Close()
		return fmt.Errorf("flush on close: %w", err)
	}
	return s.db.Close()
}

// EnsureIndexes is a no-op — Pebble's indexes are just prefix key ranges
// that are written atomically with primary rows. There's nothing to
// provision at startup.
func (s *Store) EnsureIndexes(context.Context) error { return nil }

func (s *Store) shardFor(txid string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(txid))
	return &s.insertMu[h.Sum32()%getOrInsertShards]
}

// --- Transaction status ---

func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Serialize per-txid so concurrent callers don't both see "not found"
	// and both insert. Pebble has no CAS, so the mutex is our ordering.
	mu := s.shardFor(status.TxID)
	mu.Lock()
	defer mu.Unlock()

	if existing, err := s.readStatus(status.TxID); err != nil {
		return nil, false, err
	} else if existing != nil {
		return existing, false, nil
	}

	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	if status.Status == "" {
		status.Status = models.StatusReceived
	}
	status.CreatedAt = now

	if err := s.writeStatusNew(status); err != nil {
		return nil, false, err
	}
	return status, true, nil
}

func (s *Store) readStatus(txid string) (*models.TransactionStatus, error) {
	v, closer, err := s.db.Get(txKey(txid))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get tx %s: %w", txid, err)
	}
	defer func() { _ = closer.Close() }()
	var st storedStatus
	if err := json.Unmarshal(v, &st); err != nil {
		return nil, fmt.Errorf("unmarshal tx %s: %w", txid, err)
	}
	return st.toModel(), nil
}

// writeStatusNew writes a brand-new TransactionStatus row together with all
// of its secondary index entries in a single atomic batch.
func (s *Store) writeStatusNew(status *models.TransactionStatus) error {
	st := fromModel(status)
	payload, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal tx %s: %w", status.TxID, err)
	}

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Set(txKey(status.TxID), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, st)
	return b.Commit(s.writeOpts)
}

// addStatusIndexes records the secondary index entries for a status row.
// Caller must write these atomically with the primary row.
func (s *Store) addStatusIndexes(b *pebbledb.Batch, st storedStatus) {
	_ = b.Set(idxTxStatusKey(st.Status, st.TxID), nil, nil)
	if st.BlockHash != "" {
		_ = b.Set(idxTxBlockKey(st.BlockHash, st.TxID), nil, nil)
	}
	if st.Status == string(models.StatusPendingRetry) && st.NextRetryUnixNs != 0 {
		_ = b.Set(idxTxRetryReadyKey(st.NextRetryUnixNs, st.TxID), nil, nil)
	}
	if st.TimestampUnixNs != 0 {
		_ = b.Set(idxTxUpdatedKey(st.TimestampUnixNs, st.TxID), nil, nil)
	}
}

// removeStatusIndexes removes the secondary index entries for a status row.
func (s *Store) removeStatusIndexes(b *pebbledb.Batch, st storedStatus) {
	_ = b.Delete(idxTxStatusKey(st.Status, st.TxID), nil)
	if st.BlockHash != "" {
		_ = b.Delete(idxTxBlockKey(st.BlockHash, st.TxID), nil)
	}
	if st.Status == string(models.StatusPendingRetry) && st.NextRetryUnixNs != 0 {
		_ = b.Delete(idxTxRetryReadyKey(st.NextRetryUnixNs, st.TxID), nil)
	}
	if st.TimestampUnixNs != 0 {
		_ = b.Delete(idxTxUpdatedKey(st.TimestampUnixNs, st.TxID), nil)
	}
}

// BatchGetOrInsertStatus runs GetOrInsertStatus concurrently for each row.
// Pebble is single-process and operations are already memory-fast; the
// parallel-loop fallback is the simplest correct path here.
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return store.BatchGetOrInsertStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatus runs UpdateStatus concurrently for each row.
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	return store.BatchUpdateStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatusReturning runs the diagnostic-rich form. Same parallelism
// budget as BatchUpdateStatus; per-row previous rows are returned in input
// order so callers can observe transition-age metrics without an extra
// round-trip per txid.
func (s *Store) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]store.StatusUpdate, error) {
	return store.BatchUpdateStatusReturningParallel(ctx, s, statuses)
}

// UpdateStatus replaces the status row for status.TxID. It's a full rewrite:
// any existing secondary index entries for the previous version are deleted
// and the new set is written in the same batch, so an intermediate query
// never sees a stale index pointing at a row with a different status.
//
// UpdateStatus is for *existing* rows only. If no row exists for the given
// txid the call returns store.ErrNotFound without writing anything — callers
// (notably the merkle-service callback receiver) must use GetOrInsertStatus
// to create new rows. This guard closes F-033 / issue #91: previously a
// callback referencing a never-submitted txid would create a phantom row
// with no submission/validation history, turning the callback endpoint into
// a write-anywhere primitive.
func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	_, err := s.UpdateStatusReturning(ctx, status)
	return err
}

// UpdateStatusReturning is UpdateStatus plus the applied-result. Prev is
// the row this call wrote over. Current is the durable row when the lattice
// skipped the write; that row is known, and it is not a transition a caller
// may publish. Transient errors and ctx-cancel return an empty result.
//
// Hoists the JSON marshal of the merged payload OUT of the per-shard lock
// so the critical section is bounded to Pebble I/O + index updates. This is
// the hot-path optimization called out in the latency plan.
func (s *Store) UpdateStatusReturning(ctx context.Context, status *models.TransactionStatus) (store.StatusUpdate, error) {
	if err := ctx.Err(); err != nil {
		return store.StatusUpdate{}, err
	}

	timerStart := time.Now()
	fromLabel := ""
	toLabel := string(status.Status)
	outcome := "applied"
	defer func() {
		metrics.StoreUpdateStatusDuration.WithLabelValues(fromLabel, toLabel, outcome).Observe(time.Since(timerStart).Seconds())
	}()

	// Lock per-txid for consistent read-modify-write. The caller may be
	// partially-filled (e.g., only Status + Timestamp set) — we merge with
	// the existing row so other fields are preserved.
	mu := s.shardFor(status.TxID)
	mu.Lock()
	existing, err := s.readStoredStatus(status.TxID)
	if err != nil {
		mu.Unlock()
		outcome = "error"
		return store.StatusUpdate{}, err
	}
	if existing == nil {
		mu.Unlock()
		outcome = "not_found"
		// Don't create phantom rows. See F-033 / issue #91.
		return store.StatusUpdate{}, store.ErrNotFound
	}
	fromLabel = existing.Status

	// Enforce the status lattice: a later, lower-priority update (e.g. a stray
	// SEEN_ON_NETWORK callback arriving after a tx has already been MINED) must
	// not overwrite a terminal status. See models.Status.CanTransitionFrom and
	// issue #61 / F-003.
	if status.Status != "" {
		if !status.Status.CanTransitionFrom(models.Status(existing.Status)) {
			mu.Unlock()
			outcome = "skipped_lattice"
			// Known row, transition not applied. Current is the durable
			// status; Prev stays nil so callers do not publish.
			return store.StatusUpdate{Current: existing.toModel()}, nil
		}
	}

	merged := mergeStatus(existing, status)
	// Marshal + index-key construction don't depend on shared state — do
	// them under the lock only because the existing-row data is local to
	// this goroutine after the read above. The actual write batch is the
	// thing that must be serialized per shard; marshal could move out, but
	// keeping it here keeps the read→merge→write atomic without extra
	// copies. (Empirically the marshal is ~5µs, the disk write dominates.)
	payload, err := json.Marshal(merged)
	if err != nil {
		mu.Unlock()
		outcome = "error"
		return store.StatusUpdate{}, err
	}

	b := s.db.NewBatch()
	s.removeStatusIndexes(b, *existing)
	if err := b.Set(txKey(status.TxID), payload, nil); err != nil {
		_ = b.Close()
		mu.Unlock()
		outcome = "error"
		return store.StatusUpdate{}, err
	}
	s.addStatusIndexes(b, merged)
	commitErr := b.Commit(s.writeOpts)
	_ = b.Close()
	mu.Unlock()

	if commitErr != nil {
		outcome = "error"
		return store.StatusUpdate{}, commitErr
	}
	return store.StatusUpdate{Prev: existing.toModel()}, nil
}

// mergeStatus applies the fields set on update onto existing. Empty strings,
// zero timestamps, and zero heights are treated as "keep current" — matching
// the Aerospike backend's BinMap behavior where a missing bin is a no-op.
func mergeStatus(existing *storedStatus, update *models.TransactionStatus) storedStatus {
	var out storedStatus
	if existing != nil {
		out = *existing
	}
	out.TxID = update.TxID
	if update.Status != "" {
		out.Status = string(update.Status)
		if update.Status.ClearsRetryState() {
			// A positive verdict retires the retry bookkeeping: the park
			// reason, the schedule and the last network response describe a
			// past the row has left. ExtraInfo becomes what this write
			// carries (possibly nothing). See models.Status.ClearsRetryState.
			out.ExtraInfo = update.ExtraInfo
			out.NextRetryUnixNs = 0
			out.RetryReason = ""
		}
	}
	if update.StatusCode != 0 {
		out.StatusCode = update.StatusCode
	}
	if update.BlockHash != "" {
		out.BlockHash = update.BlockHash
	}
	if update.BlockHeight > 0 {
		out.BlockHeight = update.BlockHeight
	}
	if len(update.MerklePath) > 0 {
		out.MerklePath = []byte(update.MerklePath)
	}
	if update.ExtraInfo != "" {
		out.ExtraInfo = update.ExtraInfo
	}
	if len(update.CompetingTxs) > 0 {
		out.CompetingTxs = update.CompetingTxs
	}
	if len(update.RawTx) > 0 {
		out.RawTx = []byte(update.RawTx)
	}
	if update.RetryCount > 0 {
		out.RetryCount = update.RetryCount
	}
	if update.PartitionKey != "" {
		out.PartitionKey = update.PartitionKey
	}
	if !update.Timestamp.IsZero() {
		out.TimestampUnixNs = update.Timestamp.UnixNano()
	}
	if !update.CreatedAt.IsZero() {
		out.CreatedUnixNs = update.CreatedAt.UnixNano()
	}
	if !update.NextRetryAt.IsZero() {
		out.NextRetryUnixNs = update.NextRetryAt.UnixNano()
	}
	if !update.MerkleRegisteredAt.IsZero() {
		out.MerkleRegisteredUnixNs = update.MerkleRegisteredAt.UnixNano()
	}
	return out
}

func (s *Store) readStoredStatus(txid string) (*storedStatus, error) {
	v, closer, err := s.db.Get(txKey(txid))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get tx %s: %w", txid, err)
	}
	defer func() { _ = closer.Close() }()
	var st storedStatus
	if err := json.Unmarshal(v, &st); err != nil {
		return nil, fmt.Errorf("unmarshal tx %s: %w", txid, err)
	}
	return &st, nil
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st, err := s.readStatus(txid)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, nil
	}
	s.enrichMerklePath(ctx, st)
	s.enrichOrphanedProofs(ctx, st)
	return st, nil
}

// GetStatusesSince walks the updated-at index newest-first. The since filter
// is client-side because index keys are ordered ascending; a ReverseIter
// cutoff at the since timestamp would require a descending encoding we don't
// need given the expected cardinality.
func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := idxTxUpdatedPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var results []*models.TransactionStatus
	sinceNs := since.UnixNano()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		txid := lastSegment(iter.Key())
		st, err := s.readStatus(txid)
		if err != nil || st == nil {
			continue
		}
		if !since.IsZero() && st.Timestamp.UnixNano() < sinceNs {
			continue
		}
		results = append(results, st)
	}
	return results, nil
}

// IterateStatusesSince walks the same updated-at index as GetStatusesSince but
// hands each row to fn directly instead of accumulating a slice. The Pebble
// iterator already streams keys lazily, so peak memory is bounded by whatever
// fn retains rather than the full history depth.
func (s *Store) IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix := idxTxUpdatedPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	sinceNs := since.UnixNano()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		txid := lastSegment(iter.Key())
		st, err := s.readStatus(txid)
		if err != nil || st == nil {
			continue
		}
		if !since.IsZero() && st.Timestamp.UnixNano() < sinceNs {
			continue
		}
		if err := fn(st); err != nil {
			return err
		}
	}
	return nil
}

// IterateTrackerRows walks the per-status index (idx:tx:status:<status>:*) for
// each tracked status, so rows in untracked statuses — REJECTED and IMMUTABLE,
// the overwhelming majority of a mature store — are never visited at all.
// Hydration previously walked idx:tx:updated and did a full readStatus point
// read for every row in the store (issue #276).
//
// Rows are decoded with readStoredStatus rather than readStatus: the latter
// enriches merkle paths and orphaned proofs, none of which the tracker reads.
func (s *Store) IterateTrackerRows(ctx context.Context, scan store.TrackerScan, fn func(store.TrackerRow) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	for _, status := range scan.Statuses() {
		prefix := idxTxStatusPrefix(string(status))
		iter, err := s.db.NewIter(&pebbledb.IterOptions{
			LowerBound: prefix,
			UpperBound: endOfPrefix(prefix),
		})
		if err != nil {
			return err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return err
			}
			st, err := s.readStoredStatus(lastSegment(iter.Key()))
			if err != nil || st == nil {
				continue
			}
			// Trust the row, not the index key. If an index entry is ever stale
			// relative to the row it points at, emitting the key's status would
			// hand the tracker a status the store no longer holds; re-reading
			// makes a stale entry merely wasted work.
			rowStatus := models.Status(st.Status)
			if rowStatus != status {
				continue
			}
			if !scan.Keep(rowStatus, st.BlockHeight) {
				continue
			}
			if err := fn(store.TrackerRow{
				TxID:        st.TxID,
				Status:      rowStatus,
				BlockHeight: st.BlockHeight,
			}); err != nil {
				_ = iter.Close()
				return err
			}
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CensusStatusesSince walks the per-status index (idx:tx:status:<status>:*)
// for each requested status, so rows in other statuses — the overwhelming
// majority of a mature store — are never visited. Each candidate row is read
// via readStoredStatus (no merkle enrichment) purely for its timestamp; the
// count/min aggregation happens in place. This replaces the reaper's in-walk
// census over the full updated-at index (issue #290).
func (s *Store) CensusStatusesSince(ctx context.Context, since, stuckDeadline time.Time, statuses []models.Status) (map[models.Status]store.StatusCensus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make(map[models.Status]store.StatusCensus, len(statuses))
	sinceNs := since.UnixNano()
	deadlineNs := stuckDeadline.UnixNano()

	for _, status := range statuses {
		census := store.StatusCensus{}
		prefix := idxTxStatusPrefix(string(status))
		iter, err := s.db.NewIter(&pebbledb.IterOptions{
			LowerBound: prefix,
			UpperBound: endOfPrefix(prefix),
		})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return nil, err
			}
			st, err := s.readStoredStatus(lastSegment(iter.Key()))
			if err != nil || st == nil {
				continue
			}
			if (!since.IsZero() && st.TimestampUnixNs < sinceNs) || st.TimestampUnixNs >= deadlineNs {
				continue
			}
			census.Count++
			if census.Oldest.IsZero() || st.TimestampUnixNs < census.Oldest.UnixNano() {
				census.Oldest = time.Unix(0, st.TimestampUnixNs)
			}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		out[status] = census
	}
	return out, nil
}

// SetStatusByBlockHash walks idx:tx:block:<blockHash>:* and rewrites each
// referenced row with the new status. For SEEN_ON_NETWORK transitions block
// fields are cleared and the cleared anchor is appended to the row's
// orphaned-anchor history (reorg revert, issue #279); for IMMUTABLE they're
// kept. The index walk is a snapshot: each row is re-checked under its shard
// lock and skipped when its block_hash no longer matches (concurrently
// re-anchored to the canonical block) or it is already IMMUTABLE. Returns
// only the txids actually rewritten.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := idxTxBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}

	var candidates []string
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()
			return nil, err
		}
		candidates = append(candidates, lastSegment(iter.Key()))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	clearBlock := newStatus == models.StatusSeenOnNetwork
	var txids []string
	now := time.Now()
	for _, txid := range candidates {
		mu := s.shardFor(txid)
		mu.Lock()

		existing, err := s.readStoredStatus(txid)
		if err != nil || existing == nil {
			mu.Unlock()
			continue
		}
		// Stale-index guard: the row moved to a different anchor between the
		// index snapshot and this write — reverting it now would undo a
		// concurrent re-anchor to the canonical block.
		if existing.BlockHash != blockHash {
			mu.Unlock()
			continue
		}
		// IMMUTABLE rows are never touched by block-scoped rewrites.
		if existing.Status == string(models.StatusImmutable) {
			mu.Unlock()
			continue
		}

		updated := *existing
		updated.Status = string(newStatus)
		updated.TimestampUnixNs = now.UnixNano()
		if clearBlock {
			appendOrphanedAnchor(&updated, now)
			updated.BlockHash = ""
			updated.BlockHeight = 0
		}

		payload, err := json.Marshal(updated)
		if err != nil {
			mu.Unlock()
			return txids, err
		}

		b := s.db.NewBatch()
		s.removeStatusIndexes(b, *existing)
		if setErr := b.Set(txKey(txid), payload, nil); setErr != nil {
			_ = b.Close()
			mu.Unlock()
			return txids, setErr
		}
		s.addStatusIndexes(b, updated)
		err = b.Commit(s.writeOpts)
		_ = b.Close()
		mu.Unlock()
		if err != nil {
			return txids, err
		}
		txids = append(txids, txid)
	}
	return txids, nil
}

// GetTxIDsByBlockHash walks idx:tx:block:<blockHash>:* and returns every
// txid currently anchored to the block. The reorg reconciler reads this as
// an orphaned block's affected set; rows already re-anchored or reverted
// have left the index and no longer appear.
func (s *Store) GetTxIDsByBlockHash(ctx context.Context, blockHash string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := idxTxBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var txids []string
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return txids, err
		}
		txids = append(txids, lastSegment(iter.Key()))
	}
	return txids, nil
}

// BumpRetryCount is read-modify-write under the per-txid lock. The mutex is
// load-bearing: two concurrent bumps without it would both read the same
// count and both increment to count+1 instead of count+2.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return 0, err
	}
	if existing == nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, store.ErrNotFound)
	}
	existing.RetryCount++
	payload, err := json.Marshal(existing)
	if err != nil {
		return 0, err
	}
	if err := s.db.Set(txKey(txid), payload, s.writeOpts); err != nil {
		return 0, err
	}
	return existing.RetryCount, nil
}

func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, store.ErrNotFound)
	}

	// Lattice guard: the park path writes twice (a guarded status update, then
	// this), so skipping it here would silently undo the first write's
	// protection and drag a MINED / IMMUTABLE / SEEN / REJECTED row back to
	// PENDING_RETRY. Silent skip matches the guarded update's semantics.
	if !models.StatusPendingRetry.CanTransitionFrom(models.Status(existing.Status)) {
		return nil
	}

	updated := *existing
	updated.Status = string(models.StatusPendingRetry)
	updated.RawTx = rawTx
	updated.NextRetryUnixNs = nextRetryAt.UnixNano()
	updated.TimestampUnixNs = time.Now().UnixNano()
	updated.RetryReason = lastReason

	payload, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	s.removeStatusIndexes(b, *existing)
	if err := b.Set(txKey(txid), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, updated)
	return b.Commit(s.writeOpts)
}

// GetReadyRetries uses a snapshot so concurrent BumpRetryCount or index
// rewrites don't produce duplicate or missing entries during the scan.
// The snapshot is held only for the scan window.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	prefix := idxTxRetryReadyPrefix()
	iter, err := snap.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	nowNs := now.UnixNano()
	results := make([]*store.PendingRetry, 0, limit)
	for iter.First(); iter.Valid() && len(results) < limit; iter.Next() {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		// Key shape: idx:tx:retry_ready:<hex-ns>:<txid>. Parse the hex prefix
		// to drop entries that aren't yet due.
		key := iter.Key()
		rest := key[len(prefix):]
		if len(rest) < 17 || rest[16] != ':' {
			continue
		}
		var nextNs int64
		_, err := fmt.Sscanf(string(rest[:16]), "%x", &nextNs)
		if err != nil {
			continue
		}
		if nextNs > nowNs {
			break // remaining entries are in the future
		}
		txid := string(rest[17:])

		v, closer, err := snap.Get(txKey(txid))
		if errors.Is(err, pebbledb.ErrNotFound) {
			continue
		}
		if err != nil {
			return results, err
		}
		var st storedStatus
		if err := json.Unmarshal(v, &st); err != nil {
			_ = closer.Close()
			return results, err
		}
		_ = closer.Close()
		if len(st.RawTx) == 0 {
			continue
		}
		results = append(results, &store.PendingRetry{
			TxID:        st.TxID,
			RawTx:       st.RawTx,
			RetryCount:  st.RetryCount,
			NextRetryAt: time.Unix(0, st.NextRetryUnixNs),
			LastReason:  st.RetryReason,
		})
	}
	return results, nil
}

func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}

	updated := *existing
	updated.Status = string(finalStatus)
	updated.TimestampUnixNs = time.Now().UnixNano()
	updated.RawTx = nil
	updated.NextRetryUnixNs = 0
	updated.RetryReason = ""
	if extraInfo != "" {
		updated.ExtraInfo = extraInfo
	}

	payload, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	s.removeStatusIndexes(b, *existing)
	if err := b.Set(txKey(txid), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, updated)
	return b.Commit(s.writeOpts)
}

// SetMinedByTxIDs updates only rows that already exist — matching the
// Aerospike contract where absent txids are silently skipped. blockHeight is
// persisted on each row and echoed back on the returned status so SSE/webhook
// consumers see the same height that anchors the BUMP (issue #87 / F-029).
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	now := time.Now()
	var prevs, out []*models.TransactionStatus
	for _, txid := range txids {
		if err := ctx.Err(); err != nil {
			return prevs, out, err
		}
		mu := s.shardFor(txid)
		mu.Lock()

		existing, err := s.readStoredStatus(txid)
		if err != nil {
			mu.Unlock()
			return prevs, out, err
		}
		if existing == nil {
			mu.Unlock()
			continue
		}
		// Respect the status lattice: of all statuses only IMMUTABLE may not
		// regress to MINED — a future confirmation-depth promotion must never
		// be pulled back by a replayed BLOCK_PROCESSED (issue #279 hardening).
		if !models.StatusMined.CanTransitionFrom(models.Status(existing.Status)) {
			mu.Unlock()
			continue
		}
		prev := existing.toModel()

		updated := *existing
		// Re-anchor (MINED against a DIFFERENT block): preserve the anchor
		// being superseded so the orphaned block's proof stays auditable
		// (issue #279). First-time MINED and idempotent same-block replays
		// leave history untouched.
		if existing.Status == string(models.StatusMined) && existing.BlockHash != blockHash {
			appendOrphanedAnchor(&updated, now)
		}
		updated.Status = string(models.StatusMined)
		updated.BlockHash = blockHash
		updated.BlockHeight = blockHeight
		updated.TimestampUnixNs = now.UnixNano()
		// MINED is a positive verdict: retire the retry bookkeeping (see
		// models.Status.ClearsRetryState).
		updated.ExtraInfo = ""
		updated.NextRetryUnixNs = 0
		updated.RetryReason = ""

		payload, err := json.Marshal(updated)
		if err != nil {
			mu.Unlock()
			return prevs, out, err
		}

		b := s.db.NewBatch()
		s.removeStatusIndexes(b, *existing)
		if setErr := b.Set(txKey(txid), payload, nil); setErr != nil {
			_ = b.Close()
			mu.Unlock()
			return prevs, out, setErr
		}
		s.addStatusIndexes(b, updated)
		err = b.Commit(s.writeOpts)
		_ = b.Close()
		mu.Unlock()
		if err != nil {
			return prevs, out, err
		}

		prevs = append(prevs, prev)
		out = append(out, &models.TransactionStatus{
			TxID:        txid,
			Status:      models.StatusMined,
			BlockHash:   blockHash,
			BlockHeight: blockHeight,
			Timestamp:   now,
		})
	}
	return prevs, out, nil
}

// MarkMerkleRegisteredByTxIDs stamps merkle_registered_at on every existing row
// in the txid list. Unknown txids are silently no-ops (matching SetMinedByTxIDs).
// Per-shard locking matches the rest of the status writers; an UpdateStatus
// racing with this write may see either ordering but never lose other fields
// since mergeStatus only ever overwrites populated fields. Issue #145.
func (s *Store) MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tsNs := ts.UnixNano()
	for _, txid := range txids {
		if err := ctx.Err(); err != nil {
			return err
		}
		mu := s.shardFor(txid)
		mu.Lock()

		existing, err := s.readStoredStatus(txid)
		if err != nil {
			mu.Unlock()
			return err
		}
		if existing == nil {
			mu.Unlock()
			continue
		}

		updated := *existing
		updated.MerkleRegisteredUnixNs = tsNs

		payload, err := json.Marshal(updated)
		if err != nil {
			mu.Unlock()
			return err
		}
		// No index churn: merkle_registered_at isn't indexed.
		err = s.db.Set(txKey(txid), payload, s.writeOpts)
		mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// --- BUMP / STUMP ---

func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(storedBump{BlockHeight: blockHeight, BumpData: bumpData})
	if err != nil {
		return err
	}
	if err := s.db.Set(bumpKey(blockHash), payload, s.writeOpts); err != nil {
		return err
	}
	// A rebuild can overwrite bump_data for an existing block (e.g. late STUMP
	// callbacks produce a more complete sparse compound). Drop any cached parse
	// so the next enrichment re-fetches the current stored BUMP.
	s.bumpCache.Remove(blockHash)
	return nil
}

func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	v, closer, err := s.db.Get(bumpKey(blockHash))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return 0, nil, store.ErrNotFound
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	defer func() { _ = closer.Close() }()
	var b storedBump
	if err := json.Unmarshal(v, &b); err != nil {
		return 0, nil, err
	}
	return b.BlockHeight, b.BumpData, nil
}

// DeleteBUMPByBlockHash removes the stored compound BUMP for a block and
// drops any cached parse. Idempotent.
func (s *Store) DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Delete(bumpKey(blockHash), s.writeOpts); err != nil {
		return fmt.Errorf("delete bump %s: %w", blockHash, err)
	}
	s.bumpCache.Remove(blockHash)
	return nil
}

func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	hash := stump.EnsureContentHash()
	payload, err := json.Marshal(stump)
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(stumpKey(stump.BlockHash, stump.SubtreeIndex, hash), payload, nil); err != nil {
		return err
	}
	if err := b.Set(idxStumpBlockKey(stump.BlockHash, stump.SubtreeIndex, hash), nil, nil); err != nil {
		return err
	}
	return b.Commit(s.writeOpts)
}

func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := stumpBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var stumps []*models.Stump
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return stumps, err
		}
		var st models.Stump
		if err := json.Unmarshal(iter.Value(), &st); err != nil {
			// An unreadable row means the set is incomplete. Skipping it
			// would let the bump-builder conclude a redelivery adds nothing
			// and stamp a block it never fully built, so fail the read like
			// the other backends do and let the caller retry.
			return nil, fmt.Errorf("get stumps %s: decode %q: %w", blockHash, iter.Key(), err)
		}
		st.EnsureContentHash()
		stumps = append(stumps, &st)
	}
	return stumps, nil
}

// --- Block processing status ---

// storedBlockProcessing is the persistence shape. Times are stored as unix
// nanoseconds (0 == "not yet") to match the conventions used by storedStatus
// and to avoid JSON timezone drift.
type storedBlockProcessing struct {
	BlockHash          string `json:"block_hash"`
	BlockHeight        uint64 `json:"block_height"`
	HeaderSeenUnixNs   int64  `json:"header_seen_at,omitempty"`
	ProcessedUnixNs    int64  `json:"processed_at,omitempty"`
	BUMPBuiltUnixNs    int64  `json:"bump_built_at,omitempty"`
	Status             string `json:"status"`
	OrphanedAtUnixNs   int64  `json:"orphaned_at,omitempty"`
	ReconciledAtUnixNs int64  `json:"reconciled_at,omitempty"`
}

func (b storedBlockProcessing) toModel() *models.BlockProcessingStatus {
	out := &models.BlockProcessingStatus{
		BlockHash:   b.BlockHash,
		BlockHeight: b.BlockHeight,
		Status:      models.BlockProcessingStatusValue(b.Status),
	}
	if b.HeaderSeenUnixNs != 0 {
		out.HeaderSeenAt = time.Unix(0, b.HeaderSeenUnixNs).UTC()
	}
	if b.ProcessedUnixNs != 0 {
		t := time.Unix(0, b.ProcessedUnixNs).UTC()
		out.ProcessedAt = &t
	}
	if b.BUMPBuiltUnixNs != 0 {
		t := time.Unix(0, b.BUMPBuiltUnixNs).UTC()
		out.BUMPBuiltAt = &t
	}
	// The stored generation is retained through a reactivation as the
	// high-water mark MarkBlocksOrphaned mints from; it is current — and
	// surfaced — only while the row is orphaned.
	if b.OrphanedAtUnixNs != 0 && b.Status == string(models.BlockStatusOrphaned) {
		t := time.Unix(0, b.OrphanedAtUnixNs).UTC()
		out.OrphanedAt = &t
	}
	if b.ReconciledAtUnixNs != 0 {
		t := time.Unix(0, b.ReconciledAtUnixNs).UTC()
		out.ReconciledAt = &t
	}
	return out
}

// readBlockProc returns the stored row for blockHash, or nil if absent.
func (s *Store) readBlockProc(blockHash string) (*storedBlockProcessing, error) {
	v, closer, err := s.db.Get(blockProcKey(blockHash))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get block_processing %s: %w", blockHash, err)
	}
	defer func() { _ = closer.Close() }()
	var b storedBlockProcessing
	if err := json.Unmarshal(v, &b); err != nil {
		return nil, fmt.Errorf("unmarshal block_processing %s: %w", blockHash, err)
	}
	return &b, nil
}

// writeBlockProc persists the row plus the descending-height index entry.
// The caller is expected to hold the per-block shard lock so the index does
// not pick up stale entries from a concurrent height change.
func (s *Store) writeBlockProc(prev, current *storedBlockProcessing) error {
	payload, err := json.Marshal(current)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	// If the height changed, the old index entry is now wrong — delete it.
	// In practice this only happens when a row created via MarkBlockProcessed
	// with height=0 is later corrected by UpsertBlockHeaderSeen.
	if prev != nil && prev.BlockHeight != current.BlockHeight {
		if err := batch.Delete(idxBlockProcHeightKey(prev.BlockHeight, prev.BlockHash), nil); err != nil {
			return err
		}
	}
	if err := batch.Set(blockProcKey(current.BlockHash), payload, nil); err != nil {
		return err
	}
	if err := batch.Set(idxBlockProcHeightKey(current.BlockHeight, current.BlockHash), nil, nil); err != nil {
		return err
	}
	return batch.Commit(s.writeOpts)
}

func (s *Store) UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()

	prev, err := s.readBlockProc(blockHash)
	if err != nil {
		return err
	}
	cur := storedBlockProcessing{
		BlockHash:   blockHash,
		BlockHeight: blockHeight,
		Status:      string(models.BlockStatusActive),
	}
	if prev != nil {
		// Preserve milestone timestamps; chaintracks owns block_height and
		// status, which we forcibly reset to active so a returning orphan
		// re-joins the main chain. The orphan generation stays as the row's
		// high-water mark (toModel hides it on an active row); only the
		// reconcile stamp is cleared.
		cur.HeaderSeenUnixNs = prev.HeaderSeenUnixNs
		cur.ProcessedUnixNs = prev.ProcessedUnixNs
		cur.BUMPBuiltUnixNs = prev.BUMPBuiltUnixNs
		cur.OrphanedAtUnixNs = prev.OrphanedAtUnixNs
	}
	if cur.HeaderSeenUnixNs == 0 {
		cur.HeaderSeenUnixNs = seenAt.UnixNano()
	}
	return s.writeBlockProc(prev, &cur)
}

func (s *Store) MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()

	prev, err := s.readBlockProc(blockHash)
	if err != nil {
		return err
	}
	cur := storedBlockProcessing{
		BlockHash:       blockHash,
		BlockHeight:     blockHeight,
		ProcessedUnixNs: processedAt.UnixNano(),
		Status:          string(models.BlockStatusActive),
	}
	if prev != nil {
		// Preserve everything except processed_at.
		cur.BlockHeight = prev.BlockHeight
		cur.HeaderSeenUnixNs = prev.HeaderSeenUnixNs
		cur.BUMPBuiltUnixNs = prev.BUMPBuiltUnixNs
		cur.Status = prev.Status
		cur.OrphanedAtUnixNs = prev.OrphanedAtUnixNs
		cur.ReconciledAtUnixNs = prev.ReconciledAtUnixNs
	} else {
		// Row didn't exist — synthesize header_seen_at = processedAt for
		// observability. The next UpsertBlockHeaderSeen will preserve it.
		cur.HeaderSeenUnixNs = processedAt.UnixNano()
	}
	return s.writeBlockProc(prev, &cur)
}

// ClearBlockProcessed unsets processed_at on an existing row; a missing row
// is a no-op.
func (s *Store) ClearBlockProcessed(ctx context.Context, blockHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()

	prev, err := s.readBlockProc(blockHash)
	if err != nil || prev == nil {
		return err
	}
	cur := *prev
	cur.ProcessedUnixNs = 0
	return s.writeBlockProc(prev, &cur)
}

// AddBlockExpectedSubtrees implements store.Store. Read-modify-write under
// the block's shard lock; a missing block-processing row is a no-op.
func (s *Store) AddBlockExpectedSubtrees(ctx context.Context, blockHash string, indices []int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(indices) == 0 {
		return nil
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()

	row, err := s.readBlockProc(blockHash)
	if err != nil || row == nil {
		return err
	}
	cur, err := s.readBlockExpected(blockHash)
	if err != nil {
		return err
	}
	merged := append(cur, indices...)
	slices.Sort(merged)
	merged = slices.Compact(merged)
	payload, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	return s.db.Set(blockExpectedKey(blockHash), payload, s.writeOpts)
}

// GetBlockExpectedSubtrees implements store.Store.
func (s *Store) GetBlockExpectedSubtrees(ctx context.Context, blockHash string) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.readBlockExpected(blockHash)
}

// ClearBlockExpectedSubtrees implements store.Store.
func (s *Store) ClearBlockExpectedSubtrees(ctx context.Context, blockHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()
	return s.db.Delete(blockExpectedKey(blockHash), s.writeOpts)
}

func (s *Store) readBlockExpected(blockHash string) ([]int, error) {
	v, closer, err := s.db.Get(blockExpectedKey(blockHash))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get block expected subtrees %s: %w", blockHash, err)
	}
	defer func() { _ = closer.Close() }()
	var out []int
	if err := json.Unmarshal(v, &out); err != nil {
		return nil, fmt.Errorf("unmarshal block expected subtrees %s: %w", blockHash, err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *Store) MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()

	prev, err := s.readBlockProc(blockHash)
	if err != nil {
		return err
	}
	cur := storedBlockProcessing{
		BlockHash:       blockHash,
		BlockHeight:     blockHeight,
		BUMPBuiltUnixNs: builtAt.UnixNano(),
		Status:          string(models.BlockStatusActive),
	}
	if prev != nil {
		cur.BlockHeight = prev.BlockHeight
		cur.HeaderSeenUnixNs = prev.HeaderSeenUnixNs
		cur.ProcessedUnixNs = prev.ProcessedUnixNs
		cur.Status = prev.Status
		cur.OrphanedAtUnixNs = prev.OrphanedAtUnixNs
		cur.ReconciledAtUnixNs = prev.ReconciledAtUnixNs
	} else {
		cur.HeaderSeenUnixNs = builtAt.UnixNano()
	}
	return s.writeBlockProc(prev, &cur)
}

func (s *Store) MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	transitions := 0
	gen := orphanedAt.UnixNano()
	for _, h := range blockHashes {
		// Re-checked per row: the reconciler's lease heartbeat cancels the
		// context mid-batch when the lease is lost, and every row written
		// after that overlaps the new holder's pass. The rows already
		// written are reported, as the contract promises.
		if err := ctx.Err(); err != nil {
			return transitions, err
		}
		mu := s.shardFor(h)
		mu.Lock()
		prev, err := s.readBlockProc(h)
		if err != nil {
			mu.Unlock()
			return transitions, err
		}
		if prev == nil {
			mu.Unlock()
			continue
		}
		wasOrphaned := prev.Status == string(models.BlockStatusOrphaned)
		if wasOrphaned && prev.OrphanedAtUnixNs > gen {
			// A newer generation is already in place — a delayed call must
			// not move it backwards, or a reconciler holding this older
			// token could pass the CAS for a generation it never processed.
			// Its stamp state is left alone too.
			mu.Unlock()
			continue
		}
		cur := *prev
		cur.Status = string(models.BlockStatusOrphaned)
		// Mint the generation: strictly greater than any this row has had
		// (the retained high-water mark, whatever its status), with the
		// caller's timestamp as the lower bound. Two orphanings can then
		// never share a generation even when callers reuse a timestamp, so a
		// token read before this write can never pass the CAS after it.
		cur.OrphanedAtUnixNs = mintOrphanGeneration(prev.OrphanedAtUnixNs, gen)
		cur.ReconciledAtUnixNs = 0 // re-enter the reconciler queue for this generation
		if err := s.writeBlockProc(prev, &cur); err != nil {
			mu.Unlock()
			return transitions, err
		}
		mu.Unlock()
		if !wasOrphaned {
			transitions++
		}
	}
	return transitions, nil
}

// MarkBlockReconciled stamps reconciled_at on an orphaned block's row — the
// anchor reconciler finished re-anchoring/reverting its transactions — as a
// compare-and-set on the orphan generation: the row must still be orphaned
// with the orphaned_at the caller observed (a zero orphanedAt checks status
// only). Missing, resurrected and re-orphaned rows are silently skipped and
// reported as not stamped (issue #339). The shard lock makes the
// read-compare-write atomic against the other block_processing writers.
func (s *Store) MarkBlockReconciled(ctx context.Context, blockHash string, orphanedAt, at time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()
	prev, err := s.readBlockProc(blockHash)
	if err != nil {
		return false, err
	}
	if prev == nil || prev.Status != string(models.BlockStatusOrphaned) {
		return false, nil
	}
	if !orphanedAt.IsZero() && prev.OrphanedAtUnixNs != orphanedAt.UnixNano() {
		return false, nil
	}
	cur := *prev
	cur.ReconciledAtUnixNs = at.UnixNano()
	if err := s.writeBlockProc(prev, &cur); err != nil {
		return false, err
	}
	return true, nil
}

// RequeueOrphanedBlock clears reconciled_at on a row that is still orphaned
// with the generation the caller judged (a zero orphanedAt checks status
// only), putting it back on the reconciler's queue without changing the
// generation. Missing, active, parked and re-orphaned rows are left
// untouched and reported as not applied; it never transitions a row. The
// shard lock makes the read-compare-write atomic against the other
// block_processing writers.
func (s *Store) RequeueOrphanedBlock(ctx context.Context, blockHash string, orphanedAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()
	prev, err := s.readBlockProc(blockHash)
	if err != nil {
		return false, err
	}
	if prev == nil || prev.Status != string(models.BlockStatusOrphaned) {
		return false, nil
	}
	if !orphanedAt.IsZero() && prev.OrphanedAtUnixNs != orphanedAt.UnixNano() {
		return false, nil
	}
	if prev.ReconciledAtUnixNs == 0 {
		return true, nil // already queued; nothing to write
	}
	cur := *prev
	cur.ReconciledAtUnixNs = 0
	if err := s.writeBlockProc(prev, &cur); err != nil {
		return false, err
	}
	return true, nil
}

// ReactivateBlock returns an orphaned row to active as a compare-and-set on
// the orphan generation (issue #339): the row must still be orphaned with the
// orphaned_at the caller judged (a zero orphanedAt checks status only).
// Missing, active, parked and re-orphaned rows are left untouched and
// reported as not applied. The shard lock makes the read-compare-write atomic
// against the other block_processing writers; writeBlockProc maintains the
// height index when block_height moves.
func (s *Store) ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	mu := s.shardFor(blockHash)
	mu.Lock()
	defer mu.Unlock()
	prev, err := s.readBlockProc(blockHash)
	if err != nil {
		return false, err
	}
	if prev == nil || prev.Status != string(models.BlockStatusOrphaned) {
		return false, nil
	}
	if !orphanedAt.IsZero() && prev.OrphanedAtUnixNs != orphanedAt.UnixNano() {
		return false, nil
	}
	cur := *prev
	cur.BlockHeight = blockHeight
	cur.Status = string(models.BlockStatusActive)
	// OrphanedAtUnixNs is kept: the high-water mark the next orphaning
	// mints above. toModel hides it on an active row.
	cur.ReconciledAtUnixNs = 0
	if err := s.writeBlockProc(prev, &cur); err != nil {
		return false, err
	}
	return true, nil
}

// mintOrphanGeneration returns the generation an orphan write stores: the
// caller's timestamp (ns) as a lower bound, and strictly greater than the
// row's previous generation — its high-water mark across reactivations — by
// the smallest step this backend can represent.
func mintOrphanGeneration(prevUnixNs, wantUnixNs int64) int64 {
	if prevUnixNs >= wantUnixNs {
		return prevUnixNs + 1
	}
	return wantUnixNs
}

// ListOrphanedBlocksToReconcile scans the block_processing rows for
// status='orphaned' AND reconciled_at unset. Rows that can be re-anchored
// RIGHT NOW — the active (canonical) block at the same height already has a
// stored BUMP — sort ahead of rows that cannot, oldest orphaned_at first
// WITHIN each group. This mirrors the Postgres store: the reconciler consumes
// a small LIMIT per tick, so a large backlog of orphans whose canonical block
// has no stored BUMP (only parkable, never re-anchorable now) must not starve
// a genuinely re-anchorable recent incident sitting behind them.
//
// The re-anchorable signal mirrors reconcileBlock, which resolves the canonical
// block at the orphan's height and re-anchors from that block's stored compound
// BUMP. The active row at a height is the store's record of that canonical
// block, so "an active row at this height whose hash has a stored BUMP" is the
// proxy. Checking specifically the ACTIVE block's BUMP (not any BUMP at the
// height) matters: an orphan retains its own compound BUMP, which must not
// count. The table holds one row per block, so the single prefix scan that
// already backs this query also collects the active-by-height map for free.
func (s *Store) ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	prefix := []byte(prefixBlockProc)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var rows []*storedBlockProcessing
	activeByHeight := make(map[uint64][]string)
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var b storedBlockProcessing
		if err := json.Unmarshal(iter.Value(), &b); err != nil {
			continue
		}
		switch {
		case b.Status == string(models.BlockStatusActive):
			activeByHeight[b.BlockHeight] = append(activeByHeight[b.BlockHeight], b.BlockHash)
		case b.Status == string(models.BlockStatusOrphaned) && b.ReconciledAtUnixNs == 0:
			row := b
			rows = append(rows, &row)
		}
	}

	// reanchorable[i] tracks whether rows[i]'s canonical block already has a
	// stored BUMP. Memoized per height because a backlog is mostly same-height
	// competitions, so distinct heights (and thus BUMP existence probes) are
	// far fewer than rows.
	reanchorableAt := make(map[uint64]bool, len(activeByHeight))
	heightReanchorable := func(height uint64) bool {
		if v, ok := reanchorableAt[height]; ok {
			return v
		}
		v := false
		for _, hash := range activeByHeight[height] {
			if s.hasBUMP(hash) {
				v = true
				break
			}
		}
		reanchorableAt[height] = v
		return v
	}
	reanchorable := make([]bool, len(rows))
	for i, r := range rows {
		reanchorable[i] = heightReanchorable(r.BlockHeight)
	}

	idx := make([]int, len(rows))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ia, ib := idx[a], idx[b]
		if reanchorable[ia] != reanchorable[ib] {
			return reanchorable[ia] // re-anchorable first
		}
		return rows[ia].OrphanedAtUnixNs < rows[ib].OrphanedAtUnixNs
	})
	if len(idx) > limit {
		idx = idx[:limit]
	}
	out := make([]*models.BlockProcessingStatus, len(idx))
	for i, j := range idx {
		out[i] = rows[j].toModel()
	}
	return out, nil
}

// hasBUMP reports whether a compound BUMP is stored for blockHash. It only
// probes for key existence — the ordering signal in
// ListOrphanedBlocksToReconcile mirrors the Postgres EXISTS(... JOIN bumps ...)
// check, which is presence, not content.
func (s *Store) hasBUMP(blockHash string) bool {
	_, closer, err := s.db.Get(bumpKey(blockHash))
	if err != nil {
		return false
	}
	_ = closer.Close()
	return true
}

func (s *Store) MarkBlocksParked(ctx context.Context, blockHashes []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, h := range blockHashes {
		if err := ctx.Err(); err != nil {
			return err // a cancelled caller must not keep writing rows
		}
		mu := s.shardFor(h)
		mu.Lock()
		prev, err := s.readBlockProc(h)
		if err != nil {
			mu.Unlock()
			return err
		}
		// Only 'active' rows park: missing rows are skipped, and an orphaned
		// row is off-chain (a more specific terminal state) that must not be
		// relabeled as a missing-BUMP backlog.
		if prev == nil || prev.Status != string(models.BlockStatusActive) {
			mu.Unlock()
			continue
		}
		cur := *prev
		cur.Status = string(models.BlockStatusParked)
		if err := s.writeBlockProc(prev, &cur); err != nil {
			mu.Unlock()
			return err
		}
		mu.Unlock()
	}
	return nil
}

func (s *Store) GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stored, err := s.readBlockProc(blockHash)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, store.ErrNotFound
	}
	return stored.toModel(), nil
}

// GetActiveTipBlockHeight scans the descending-height index (newest first
// thanks to the inverted uint64 encoding) and returns the first active row's
// height. Pebble has no native MAX so this is the closest equivalent — and
// is still O(active-orphan prefix) at worst, which in practice means the
// first key the iterator sees (orphans are vanishingly rare at the tip).
func (s *Store) GetActiveTipBlockHeight(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	prefix := idxBlockProcHeightPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		stored, err := s.readBlockProc(lastSegment(iter.Key()))
		if err != nil {
			return 0, err
		}
		if stored == nil || stored.Status != string(models.BlockStatusActive) {
			continue
		}
		return stored.BlockHeight, nil
	}
	return 0, nil
}

// ListStaleBlockProcessingStatus iterates every block_processing row and
// filters in-memory. Cardinality is ~144 rows/day and the watchdog tick
// happens on the order of 30 s, so a full scan is comfortably under the
// budget even at multi-week retention.
func (s *Store) ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	threshold := olderThan.UnixNano()
	prefix := idxBlockProcHeightPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var candidates []*storedBlockProcessing
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stored, err := s.readBlockProc(lastSegment(iter.Key()))
		if err != nil {
			return nil, err
		}
		if stored == nil {
			continue
		}
		if stored.Status != string(models.BlockStatusActive) {
			continue
		}
		if stored.ProcessedUnixNs != 0 {
			continue
		}
		if stored.HeaderSeenUnixNs == 0 || stored.HeaderSeenUnixNs >= threshold {
			continue
		}
		if stored.BlockHeight < minHeight {
			continue
		}
		candidates = append(candidates, stored)
	}
	// Oldest header_seen_at first — fair retry order.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].HeaderSeenUnixNs < candidates[j].HeaderSeenUnixNs
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]*models.BlockProcessingStatus, len(candidates))
	for i, c := range candidates {
		out[i] = c.toModel()
	}
	return out, nil
}

func (s *Store) ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	prefix := idxBlockProcHeightPrefix()
	// Inverted-height encoding means descending height = ascending bytes.
	// To filter "block_height < beforeHeight" we lower-bound the iterator at
	// the inverted (beforeHeight-1) so heights >= beforeHeight are skipped.
	lower := prefix
	if beforeHeight > 0 {
		lower = []byte(fmt.Sprintf("%s%016x:", prefixIdxBlockProcHeight, ^(beforeHeight - 1)))
	}
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: lower,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	out := make([]*models.BlockProcessingStatus, 0, limit)
	for iter.First(); iter.Valid() && len(out) < limit; iter.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		blockHash := lastSegment(iter.Key())
		stored, err := s.readBlockProc(blockHash)
		if err != nil {
			return out, err
		}
		if stored == nil {
			// Stale index entry — skip rather than fail the whole list.
			continue
		}
		out = append(out, stored.toModel())
	}
	return out, nil
}

func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix := stumpBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return err
	}

	// Delete the keys as found rather than recomputing them: rows written
	// before content addressing carry the old two-part key, and a block's
	// rows must all go regardless of which layout wrote them.
	var toDelete [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		toDelete = append(toDelete, append([]byte(nil), iter.Key()...))
	}
	if cerr := iter.Close(); cerr != nil {
		return cerr
	}
	idxPrefix := idxStumpBlockPrefix(blockHash)
	idxIter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: idxPrefix,
		UpperBound: endOfPrefix(idxPrefix),
	})
	if err != nil {
		return err
	}
	for idxIter.First(); idxIter.Valid(); idxIter.Next() {
		toDelete = append(toDelete, append([]byte(nil), idxIter.Key()...))
	}
	if cerr := idxIter.Close(); cerr != nil {
		return cerr
	}

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	for _, key := range toDelete {
		_ = b.Delete(key, nil)
	}
	return b.Commit(s.writeOpts)
}

// --- Submissions ---

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stored := storedSubmission{
		SubmissionID:      sub.SubmissionID,
		TxID:              sub.TxID,
		CallbackURL:       sub.CallbackURL,
		CallbackToken:     sub.CallbackToken,
		FullStatusUpdates: sub.FullStatusUpdates,
		RetryCount:        sub.RetryCount,
	}
	if !sub.CreatedAt.IsZero() {
		stored.CreatedUnixNs = sub.CreatedAt.UnixNano()
	}
	if sub.LastDeliveredStatus != "" {
		stored.LastDeliveredStatus = string(sub.LastDeliveredStatus)
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(subKey(sub.SubmissionID), payload, nil); err != nil {
		return err
	}
	if sub.TxID != "" {
		_ = b.Set(idxSubTxIDKey(sub.TxID, sub.SubmissionID), nil, nil)
	}
	if sub.CallbackToken != "" {
		_ = b.Set(idxSubTokenKey(sub.CallbackToken, sub.SubmissionID), nil, nil)
	}
	return b.Commit(s.writeOpts)
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	return s.submissionsByIndex(ctx, idxSubTxIDPrefix(txid))
}

func (s *Store) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
	return s.submissionsByIndex(ctx, idxSubTokenPrefix(callbackToken))
}

// TokensForTxIDs resolves via the by-txid index (a txid has a handful of
// submissions) — never the token index, which can hold millions. Pebble has
// no set-returning read, so a batch is one local prefix scan per txid; the
// win over the probe this replaced is that fan-out asks once per txid no
// matter how many clients are connected.
func (s *Store) TokensForTxIDs(ctx context.Context, txids []string) (map[string][]string, error) {
	return store.TokensForTxIDsViaPerTxID(ctx, txids, s.GetSubmissionsByTxID)
}

// maxTokenReplayScan bounds how many of a token's submissions this backend
// will walk for one catchup replay.
//
// Unlike Postgres, Pebble has no index over (callback_token, timestamp_at,
// txid), so this implementation has to load the token's submission list, do a
// point read per txid, and sort the survivors in memory. That is O(token
// cardinality) per connect, and a token can hold millions of submissions —
// the shape that OOM-killed the SSE pod (#237/#238). Past this bound, refusing
// is strictly better than trying: the caller degrades to an explicit gap and
// the client reconciles over REST, instead of the pod dying and taking every
// other connected client with it.
//
// The bound is enforced while the index is being walked, so peak memory is
// capped at maxTokenReplayScan submissions. Checking the length of an
// already-materialized list would be no protection at all — the allocation
// that causes the OOM happens before such a check could run.
const maxTokenReplayScan = 250_000

func (s *Store) IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error {
	subs, err := s.submissionsByIndexBounded(ctx, idxSubTokenPrefix(callbackToken), maxTokenReplayScan)
	if err != nil {
		return err
	}
	only := make(map[models.Status]struct{}, len(onlyStatuses))
	for _, st := range onlyStatuses {
		only[st] = struct{}{}
	}
	seen := make(map[string]struct{}, len(subs))
	// Projection only — readStatus without enrichMerklePath, RawTx and
	// MerklePath stripped. Filter BEFORE collecting so the sort buffer holds
	// the (small) matching set, not the token's full history.
	matched := make([]*models.TransactionStatus, 0, 64)
	for _, sub := range subs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, dup := seen[sub.TxID]; dup {
			continue
		}
		seen[sub.TxID] = struct{}{}
		st, rErr := s.readStatus(sub.TxID)
		if rErr != nil || st == nil {
			continue
		}
		if !since.IsZero() && !st.Timestamp.After(since) {
			continue
		}
		if len(only) > 0 {
			if _, ok := only[st.Status]; !ok {
				continue
			}
		}
		matched = append(matched, &models.TransactionStatus{
			TxID:        st.TxID,
			Status:      st.Status,
			Timestamp:   st.Timestamp,
			BlockHash:   st.BlockHash,
			BlockHeight: st.BlockHeight,
		})
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Timestamp.Before(matched[j].Timestamp) })
	for _, st := range matched {
		if err := fn(st); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) submissionsByIndex(ctx context.Context, prefix []byte) ([]*models.Submission, error) {
	return s.submissionsByIndexBounded(ctx, prefix, 0)
}

// submissionsByIndexBounded is submissionsByIndex with an optional ceiling on
// how many submissions it will accumulate. A limit of 0 means unbounded.
//
// The bound is enforced DURING iteration, not after: checking a returned
// slice's length is no protection, because by then the memory has already been
// allocated and the OOM it was meant to prevent has already happened. Exceeding
// the limit abandons the scan and returns store.ErrReplayUnavailable, so peak
// memory is capped at limit entries regardless of the token's real size.
func (s *Store) submissionsByIndexBounded(ctx context.Context, prefix []byte, limit int) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var subs []*models.Submission
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return subs, err
		}
		id := lastSegment(iter.Key())
		v, closer, err := s.db.Get(subKey(id))
		if errors.Is(err, pebbledb.ErrNotFound) {
			continue
		}
		if err != nil {
			return subs, err
		}
		var ss storedSubmission
		if err := json.Unmarshal(v, &ss); err != nil {
			_ = closer.Close()
			continue
		}
		_ = closer.Close()
		if limit > 0 && len(subs) >= limit {
			return nil, fmt.Errorf("%w: token has more than %d submissions", store.ErrReplayUnavailable, limit)
		}
		subs = append(subs, ss.toModel())
	}
	return subs, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v, closer, err := s.db.Get(subKey(submissionID))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var ss storedSubmission
	if uErr := json.Unmarshal(v, &ss); uErr != nil {
		_ = closer.Close()
		return uErr
	}
	_ = closer.Close()

	ss.LastDeliveredStatus = string(lastStatus)
	ss.RetryCount = retryCount
	if nextRetry != nil {
		ss.NextRetryUnixNs = nextRetry.UnixNano()
	} else {
		ss.NextRetryUnixNs = 0
	}

	payload, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return s.db.Set(subKey(submissionID), payload, s.writeOpts)
}

// RecordDeliveryAttempt implements store.Store. The read-modify-write is
// serialized via submissionMu so a concurrent CAS or retry-state write can't
// lose the attempts increment.
func (s *Store) RecordDeliveryAttempt(ctx context.Context, submissionID string, at time.Time, result string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.submissionMu.Lock()
	defer s.submissionMu.Unlock()

	v, closer, err := s.db.Get(subKey(submissionID))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var ss storedSubmission
	if uErr := json.Unmarshal(v, &ss); uErr != nil {
		_ = closer.Close()
		return uErr
	}
	_ = closer.Close()

	ss.Attempts++
	ss.LastAttemptUnixNs = at.UnixNano()
	ss.LastResult = result

	payload, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return s.db.Set(subKey(submissionID), payload, s.writeOpts)
}

// UpdateDeliveryStatusCAS implements store.Store. The read-modify-write is
// serialized via submissionMu so a concurrent caller can't observe a stale
// LastDeliveredStatus between our Get and Set. Pebble has no native CAS;
// process-local serialization is sufficient because the database is
// embedded and webhook-delivery contention is exclusively cross-replica in
// the Postgres deployment shape, not within one Pebble process.
func (s *Store) UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.submissionMu.Lock()
	defer s.submissionMu.Unlock()

	v, closer, err := s.db.Get(subKey(submissionID))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var ss storedSubmission
	if uErr := json.Unmarshal(v, &ss); uErr != nil {
		_ = closer.Close()
		return false, uErr
	}
	_ = closer.Close()

	if models.Status(ss.LastDeliveredStatus) != expected {
		return false, nil
	}

	ss.LastDeliveredStatus = string(next)
	ss.RetryCount = 0
	ss.NextRetryUnixNs = 0

	payload, err := json.Marshal(ss)
	if err != nil {
		return false, err
	}
	if err := s.db.Set(subKey(submissionID), payload, s.writeOpts); err != nil {
		return false, err
	}
	return true, nil
}

// ListSubmissionsReadyForRetry walks the sub: prefix and returns rows where
// RetryCount > 0 and NextRetryUnixNs has elapsed. There is no native ordered
// retry index on submissions (the in-retry working set is small enough that
// the cost of building one isn't justified), so the iteration is followed by
// an in-code sort.
func (s *Store) ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := []byte(prefixSub)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	nowNs := now.UnixNano()
	candidates := make([]storedSubmission, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var ss storedSubmission
		if err := json.Unmarshal(iter.Value(), &ss); err != nil {
			continue
		}
		if ss.RetryCount <= 0 || ss.NextRetryUnixNs <= 0 || ss.NextRetryUnixNs > nowNs {
			continue
		}
		candidates = append(candidates, ss)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].NextRetryUnixNs < candidates[j].NextRetryUnixNs
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]*models.Submission, len(candidates))
	for i, ss := range candidates {
		out[i] = ss.toModel()
	}
	return out, nil
}

// --- Leaser ---

// TryAcquireOrRenew uses a single mutex across all lease names — standalone
// mode is single-process so contention is negligible, and a process-wide
// mutex is easier to reason about than striping across names. The TTL is
// enforced on read: a lease whose ExpiresAt has elapsed is treated as vacant.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	now := time.Now()
	expires := now.Add(ttl)

	v, closer, err := s.db.Get(leaseKey(name))
	if err != nil && !errors.Is(err, pebbledb.ErrNotFound) {
		return time.Time{}, fmt.Errorf("read lease %s: %w", name, err)
	}
	if err == nil {
		var cur storedLease
		if jerr := json.Unmarshal(v, &cur); jerr == nil {
			_ = closer.Close()
			if cur.Holder != holder && cur.ExpiresUnixNs > now.UnixNano() {
				return time.Time{}, nil
			}
		} else {
			_ = closer.Close()
		}
	}

	payload, err := json.Marshal(storedLease{Holder: holder, ExpiresUnixNs: expires.UnixNano()})
	if err != nil {
		return time.Time{}, err
	}
	if err := s.db.Set(leaseKey(name), payload, s.writeOpts); err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

func (s *Store) Release(ctx context.Context, name, holder string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	v, closer, err := s.db.Get(leaseKey(name))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var cur storedLease
	if jerr := json.Unmarshal(v, &cur); jerr == nil {
		_ = closer.Close()
		if cur.Holder != holder {
			return nil
		}
	} else {
		_ = closer.Close()
	}
	return s.db.Delete(leaseKey(name), s.writeOpts)
}

// --- Datahub endpoint registry ---

func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ep.URL == "" {
		return fmt.Errorf("upsert datahub endpoint: empty url")
	}
	stored := storedDatahubEndpoint{
		URL:     ep.URL,
		Network: ep.Network,
		Source:  ep.Source,
	}
	if !ep.LastSeen.IsZero() {
		stored.LastSeenUnixNs = ep.LastSeen.UnixNano()
	}

	switch {
	case ep.Policy != nil:
		if err := ep.Policy.Validate(); err != nil {
			return err
		}
		stored.Policy = &storedEndpointPolicy{
			MiningFeeSatoshis:       ep.Policy.MiningFeeSatoshis,
			MiningFeeBytes:          ep.Policy.MiningFeeBytes,
			MaxTxSizePolicy:         ep.Policy.MaxTxSizePolicy,
			MaxScriptSizePolicy:     ep.Policy.MaxScriptSizePolicy,
			MaxTxSigopsCountsPolicy: ep.Policy.MaxTxSigopsCountsPolicy,
		}
	default:
		// A write carrying no policy must not erase one already recorded.
		// Set is a blind overwrite here — unlike Postgres, which COALESCEs, and
		// Aerospike, which simply omits the bins — so this backend has to read
		// the current row and carry its policy forward.
		stored.Policy = s.existingEndpointPolicy(ep.URL)
	}

	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	return s.db.Set(datahubEndpointKey(ep.URL), payload, s.writeOpts)
}

// existingEndpointPolicy returns the policy already stored for url, or nil when
// there is no row, no policy, or the row cannot be read. A failure here costs
// only the carried-forward policy, which the peer's next announcement restores;
// it must never fail the URL registration itself.
func (s *Store) existingEndpointPolicy(url string) *storedEndpointPolicy {
	val, closer, err := s.db.Get(datahubEndpointKey(url))
	if err != nil {
		return nil
	}
	defer func() { _ = closer.Close() }()
	var row storedDatahubEndpoint
	if err := json.Unmarshal(val, &row); err != nil {
		return nil
	}
	return row.Policy
}

func (s *Store) ListDatahubEndpoints(ctx context.Context, network string) ([]store.DatahubEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := datahubEndpointPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var out []store.DatahubEndpoint
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		var row storedDatahubEndpoint
		if err := json.Unmarshal(iter.Value(), &row); err != nil {
			continue
		}
		if row.Network != network {
			continue
		}
		ep := store.DatahubEndpoint{
			URL:     row.URL,
			Network: row.Network,
			Source:  row.Source,
		}
		if p := row.Policy; p != nil {
			ep.Policy = &store.EndpointPolicy{
				MiningFeeSatoshis:       p.MiningFeeSatoshis,
				MiningFeeBytes:          p.MiningFeeBytes,
				MaxTxSizePolicy:         p.MaxTxSizePolicy,
				MaxScriptSizePolicy:     p.MaxScriptSizePolicy,
				MaxTxSigopsCountsPolicy: p.MaxTxSigopsCountsPolicy,
			}
		}
		if row.LastSeenUnixNs != 0 {
			ep.LastSeen = time.Unix(0, row.LastSeenUnixNs)
		}
		out = append(out, ep)
	}
	return out, nil
}

func (s *Store) UpsertPeerPolicy(ctx context.Context, pp store.PeerPolicy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Pebble stores the fees as JSON uint64 and so cannot lose precision, but it
	// validates on the same terms as the other backends: a zero byte basis is
	// meaningless everywhere, and one backend silently accepting what the others
	// reject would make the store's contract depend on its deployment.
	if err := pp.Validate(); err != nil {
		return err
	}
	stored := storedPeerPolicy{
		PeerID:              pp.PeerID,
		Network:             pp.Network,
		MiningFeeSatoshis:   pp.MiningFeeSatoshis,
		MiningFeeBytes:      pp.MiningFeeBytes,
		MaxTxSizePolicy:     pp.MaxTxSizePolicy,
		MaxScriptSizePolicy: pp.MaxScriptSizePolicy,
	}
	if !pp.LastSeen.IsZero() {
		stored.LastSeenUnixNs = pp.LastSeen.UnixNano()
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	return s.db.Set(peerPolicyKey(pp.PeerID), payload, s.writeOpts)
}

func (s *Store) ListPeerPolicies(ctx context.Context, network string) ([]store.PeerPolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := peerPolicyPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var out []store.PeerPolicy
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		var row storedPeerPolicy
		if err := json.Unmarshal(iter.Value(), &row); err != nil {
			continue
		}
		if row.Network != network {
			continue
		}
		pp := store.PeerPolicy{
			PeerID:              row.PeerID,
			Network:             row.Network,
			MiningFeeSatoshis:   row.MiningFeeSatoshis,
			MiningFeeBytes:      row.MiningFeeBytes,
			MaxTxSizePolicy:     row.MaxTxSizePolicy,
			MaxScriptSizePolicy: row.MaxScriptSizePolicy,
		}
		if row.LastSeenUnixNs != 0 {
			pp.LastSeen = time.Unix(0, row.LastSeenUnixNs)
		}
		out = append(out, pp)
	}
	return out, nil
}

// --- Helpers ---

// EnrichMerklePath implements store.Store: it populates status.MerklePath in
// place for a mined/immutable status carrying a BlockHash, extracting the tx's
// minimal path from the block's compound BUMP. Best-effort — see the interface
// doc. Delegates to the same enrichMerklePath used by GetStatus.
func (s *Store) EnrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	s.enrichMerklePath(ctx, status)
}

// enrichMerklePath attaches the per-tx minimal merkle path for mined/immutable
// rows, extracting it from the block's cached, indexed compound BUMP.
func (s *Store) enrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	s.bumpCache.Enrich(status, func() ([]byte, error) {
		_, bumpData, err := s.GetBUMP(ctx, status.BlockHash)
		return bumpData, err
	})
}

// enrichOrphanedProofs resolves each historical anchor's merkle path from
// the orphaned block's retained compound BUMP (issue #279). Best-effort:
// an entry whose BUMP is gone or unparseable is served without a path.
func (s *Store) enrichOrphanedProofs(ctx context.Context, status *models.TransactionStatus) {
	if status == nil {
		return
	}
	for i := range status.OrphanedProofs {
		entry := &status.OrphanedProofs[i]
		if len(entry.MerklePath) > 0 || entry.BlockHash == "" {
			continue
		}
		hash := entry.BlockHash
		entry.MerklePath = s.bumpCache.MinimalPath(hash, status.TxID, func() ([]byte, error) {
			_, bumpData, err := s.GetBUMP(ctx, hash)
			return bumpData, err
		})
	}
}

// LookupPartitionKeys implements store.Store. Pebble reads the row whole
// either way, so this is one point read per txid.
func (s *Store) LookupPartitionKeys(ctx context.Context, txids []string) (map[string]store.ParentKey, error) {
	out := make(map[string]store.ParentKey, len(txids))
	for _, txid := range txids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pk, found, err := s.readPartitionKey(txid)
		if err != nil {
			return nil, err
		}
		if found {
			out[txid] = pk
		}
	}
	return out, nil
}

// readPartitionKey decodes only the status and partition key of txid's row,
// so the intake-path lookup never base64-decodes raw_tx or merkle_path.
func (s *Store) readPartitionKey(txid string) (store.ParentKey, bool, error) {
	v, closer, err := s.db.Get(txKey(txid))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return store.ParentKey{}, false, nil
	}
	if err != nil {
		return store.ParentKey{}, false, fmt.Errorf("get tx %s: %w", txid, err)
	}
	defer func() { _ = closer.Close() }()
	var row struct {
		Status       string `json:"status"`
		PartitionKey string `json:"partition_key,omitempty"`
	}
	if err := json.Unmarshal(v, &row); err != nil {
		return store.ParentKey{}, false, fmt.Errorf("unmarshal tx %s: %w", txid, err)
	}
	return store.ParentKey{Status: models.Status(row.Status), PartitionKey: row.PartitionKey}, true, nil
}

// LookupRawTxs implements store.Store. Pebble reads the row whole either
// way, so this is one point read per txid.
func (s *Store) LookupRawTxs(ctx context.Context, txids []string) (map[string]store.RawTxRow, error) {
	out := make(map[string]store.RawTxRow, len(txids))
	for _, txid := range txids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, err := s.readStoredStatus(txid)
		if err != nil {
			return nil, err
		}
		if row != nil {
			out[txid] = store.RawTxRow{Status: models.Status(row.Status), RawTx: row.RawTx}
		}
	}
	return out, nil
}

// SetRawTx implements store.Store. raw_tx is not indexed, so only the row
// itself is rewritten.
func (s *Store) SetRawTx(_ context.Context, txid string, rawTx []byte) error {
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("set raw tx %s: %w", txid, store.ErrNotFound)
	}
	updated := *existing
	updated.RawTx = rawTx
	payload, err := json.Marshal(updated)
	if err != nil {
		return err
	}
	return s.db.Set(txKey(txid), payload, s.writeOpts)
}
