package aerospike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/bumpcache"
)

func isKeyNotFound(err error) bool {
	return errors.Is(err, aero.ErrKeyNotFound)
}

// isGenerationErr is true when an Aerospike write fails because the record was
// modified between our read and our CAS write (EXPECT_GEN_EQUAL mismatch).
func isGenerationErr(err error) bool {
	var aerr aero.Error
	if errors.As(err, &aerr) {
		return aerr.Matches(types.GENERATION_ERROR)
	}
	return false
}

// Set and secondary-index names keep the legacy "arcade_" prefix from before
// the BSV Broadcaster rebrand: they name persisted data, so renaming them would
// orphan every existing record and index.
const (
	setTransactions     = "arcade_transactions"
	setBumps            = "arcade_bumps"
	setStumps           = "arcade_stumps"
	setSubmissions      = "arcade_submissions"
	setBlockProcessing  = "arcade_block_processing"
	setLeases           = "arcade_leases"
	setDatahubEndpoints = "arcade_datahub_endpoints"
	setPeerPolicies     = "arcade_peer_policies"
)

// BUMP chunking — large compound BUMPs (scaling networks with millions of
// txs/block produce tens of MiB of merkle path data) cannot fit in a single
// Aerospike record because record size is bounded by the namespace's
// write-block-size (default 1 MiB, max 8 MiB). InsertBUMP/GetBUMP split the
// payload across N+1 records: a manifest at <blockHash> with chunk_count, and
// chunk records at <blockHash>:c:<index>.
const (
	bumpFormatVersion         = 1
	bumpDefaultChunkSizeBytes = 768 * 1024
	bumpChunkKeyFormat        = "%s:c:%08d"
)

// STUMP chunking mirrors BUMP chunking above — a single subtree's STUMP on a
// busy block also overruns Aerospike's per-record ceiling, and an unchunked
// InsertStump then fails with RECORD_TOO_BIG, losing the block's STUMPs so the
// bump builder never marks its txs MINED. See the STUMP Operations section for
// the manifest/chunk layout. The chunk size is shared with BUMP (bumpChunkSize)
// since the same namespace write-block-size bounds both record types.
const (
	stumpFormatVersion = 1
	// stumpChunkKeyFormat is the chunk key of a content-addressed STUMP
	// (block:subtree:content_hash:c:index); stumpChunkKeyFormatLegacy is the
	// pre-content-addressing layout, still readable so a STUMP written before
	// the upgrade can be assembled and deleted.
	stumpChunkKeyFormat       = "%s:%d:%s:c:%08d"
	stumpChunkKeyFormatLegacy = "%s:%d:c:%08d"
)

// Ensure Store implements the store interfaces.
var (
	_ store.Store  = (*Store)(nil)
	_ store.Leaser = (*Store)(nil)
)

// Store is the Aerospike-backed implementation of store.Store and store.Leaser.
type Store struct {
	client        *aero.Client
	namespace     string
	batchSize     int
	queryTimeout  time.Duration
	opTimeout     time.Duration
	socketTimeout time.Duration
	bumpChunkSize int
	// bumpCache holds parsed+indexed compound BUMPs for merkle-path
	// enrichment — sizing, budget, and singleflight live in bumpcache.
	// Especially valuable here: a miss reassembles the chunked BUMP from
	// Aerospike before parsing.
	bumpCache *bumpcache.Cache
}

// New creates an Aerospike-backed Store connected to the configured cluster.
func New(ctx context.Context, cfg config.Aero) (*Store, error) {
	hosts := make([]*aero.Host, 0, len(cfg.Hosts))
	for _, h := range cfg.Hosts {
		hostname, portStr, err := net.SplitHostPort(h)
		if err != nil {
			// No port specified, use default
			hosts = append(hosts, aero.NewHost(h, 3000))
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port in host %q: %w", h, err)
		}
		hosts = append(hosts, aero.NewHost(hostname, port))
	}

	policy := aero.NewClientPolicy()
	policy.ConnectionQueueSize = cfg.PoolSize
	// IdleTimeout reaps idle pooled connections client-side. Required when the
	// Aerospike server runs with proto-fd-idle-ms=0 (no server-side idle
	// reap) — without this, connections pile up forever and starve the pool
	// (we observed 35k zombie conns on a single node before adding this).
	// Should be set a few seconds below the server's proto-fd-idle-ms when
	// nonzero. cfg.IdleTimeoutSec=0 falls back to the safe default.
	idleSec := cfg.IdleTimeoutSec
	if idleSec <= 0 {
		idleSec = 55
	}
	policy.IdleTimeout = time.Duration(idleSec) * time.Second

	// Aggressive node-level circuit breaker. In a multi-tenant Aerospike
	// cluster a single slow node — squeezed by another namespace's traffic
	// — will otherwise drain the per-node pool because every retry just
	// piles another timeout on the unhealthy peer. The default
	// MaxErrorRate=100 is too lenient; we trip the breaker much sooner so
	// the client returns MAX_ERROR_RATE for affected partitions instead of
	// holding pool slots open while waiting for timeouts.
	if cfg.MaxErrorRate > 0 {
		policy.MaxErrorRate = cfg.MaxErrorRate
	} else {
		policy.MaxErrorRate = 5
	}
	if cfg.ErrorRateWindow > 0 {
		policy.ErrorRateWindow = cfg.ErrorRateWindow
	} else {
		policy.ErrorRateWindow = 1
	}

	client, err := aero.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to aerospike: %w", err)
	}

	bumpChunkSize := cfg.BumpChunkSizeBytes
	if bumpChunkSize <= 0 {
		bumpChunkSize = bumpDefaultChunkSizeBytes
	}

	s := &Store{
		client:        client,
		namespace:     cfg.Namespace,
		batchSize:     cfg.BatchSize,
		queryTimeout:  time.Duration(cfg.QueryTimeoutMs) * time.Millisecond,
		opTimeout:     time.Duration(cfg.OpTimeoutMs) * time.Millisecond,
		socketTimeout: time.Duration(cfg.SocketTimeoutMs) * time.Millisecond,
		bumpChunkSize: bumpChunkSize,
		bumpCache:     bumpcache.New(),
	}

	if err := s.EnsureIndexes(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("creating indexes: %w", err)
	}

	return s, nil
}

func (s *Store) Healthy() bool {
	return s.client.IsConnected()
}

func (s *Store) Close() error {
	s.client.Close()
	return nil
}

func (s *Store) EnsureIndexes(ctx context.Context) error {
	indexes := []struct {
		set, bin, name string
		indexType      aero.IndexType
	}{
		{setStumps, "block_hash", "arcade_idx_stumps_block_hash", aero.STRING},
		{setTransactions, "block_hash", "arcade_idx_tx_block_hash", aero.STRING},
		{setSubmissions, "txid", "arcade_idx_sub_txid", aero.STRING},
		{setSubmissions, "callback_token", "arcade_idx_sub_callback_token", aero.STRING},
		{setBlockProcessing, "block_height", "arcade_idx_bp_block_height", aero.NUMERIC},
		{setBlockProcessing, "status", "arcade_idx_bp_status", aero.STRING},
		{setTransactions, "status", "arcade_idx_tx_status", aero.STRING},
		{setDatahubEndpoints, "last_seen", "arcade_idx_dh_last_seen", aero.NUMERIC},
	}
	// CreateIndex returns an IndexTask that must be polled — the server call
	// is asynchronous and the index won't be queryable on every node until
	// the task is done. Skipping this wait was the proximate cause of the
	// INDEX_NOTFOUND storm we saw in production: the service started
	// consuming Kafka messages before propagation finished, every query
	// returned an error, the client retried, and the connection pool drained.
	for _, idx := range indexes {
		task, err := s.client.CreateIndex(nil, s.namespace, idx.set, idx.name, idx.bin, idx.indexType)
		if err != nil {
			if strings.Contains(err.Error(), "Index already exists") {
				continue
			}
			return fmt.Errorf("creating index %s: %w", idx.name, err)
		}
		if task == nil {
			continue
		}
		select {
		case taskErr := <-task.OnComplete():
			if taskErr != nil {
				return fmt.Errorf("waiting for index %s: %w", idx.name, taskErr)
			}
		case <-ctx.Done():
			return fmt.Errorf("waiting for index %s: %w", idx.name, ctx.Err())
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timed out waiting for index %s to build", idx.name)
		}
	}
	return nil
}

func (s *Store) key(set, pk string) (*aero.Key, error) {
	return aero.NewKey(s.namespace, set, pk)
}

// remaining returns the time budget for a single Aerospike operation under ctx.
// If ctx is already done, returns 0 so the client fails fast without borrowing
// a pool slot. If ctx has no deadline, falls back to the configured default.
// Otherwise returns the shorter of (deadline-remaining, default).
func remaining(ctx context.Context, def time.Duration) time.Duration {
	if err := ctx.Err(); err != nil {
		return 0
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return def
	}
	rem := time.Until(dl)
	if rem <= 0 {
		return 0
	}
	if def > 0 && rem > def {
		return def
	}
	return rem
}

// queryPolicy builds a QueryPolicy whose TotalTimeout tracks ctx deadline,
// capped by the configured query timeout. MaxRetries is kept low so a
// failing query can't hog a connection across many server-side retries.
func (s *Store) queryPolicy(ctx context.Context) *aero.QueryPolicy {
	p := aero.NewQueryPolicy()
	p.TotalTimeout = remaining(ctx, s.queryTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 1
	return p
}

// readPolicy builds a BasePolicy for single-record reads.
func (s *Store) readPolicy(ctx context.Context) *aero.BasePolicy {
	p := aero.NewPolicy()
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 2
	return p
}

// writePolicy builds a WritePolicy for single-record writes. Callers can
// mutate RecordExistsAction, Expiration, GenerationPolicy, etc. on the
// returned struct.
func (s *Store) writePolicy(ctx context.Context) *aero.WritePolicy {
	p := aero.NewWritePolicy(0, 0)
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 2
	return p
}

// batchPolicy builds a BatchPolicy for multi-record operations. Retries are
// disabled at the client layer because batch failures amplify connection
// pressure (every retry re-fans-out across every node, including the sick
// one), and Kafka-driven application retries handle real recovery.
func (s *Store) batchPolicy(ctx context.Context) *aero.BatchPolicy {
	p := aero.NewBatchPolicy()
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 0
	return p
}

// --- Transaction Status Operations ---

func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	key, err := s.key(setTransactions, status.TxID)
	if err != nil {
		return nil, false, err
	}

	// Try to read existing
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return nil, false, fmt.Errorf("get status: %w", err)
	}
	if rec != nil {
		existing := recordToStatus(rec, status.TxID)
		return existing, false, nil
	}

	// Insert new
	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	st := string(models.StatusReceived)
	if status.Status != "" {
		st = string(status.Status)
	}

	bins := aero.BinMap{
		"txid":       status.TxID,
		"status":     st,
		"timestamp":  status.Timestamp.UnixMilli(),
		"created_at": now.UnixMilli(),
	}
	// Rows can be born terminal (intake rejection): carry the machine-
	// readable cause and code on the insert, matching the other backends.
	if status.StatusCode != 0 {
		bins["status_code"] = status.StatusCode
	}
	if status.ExtraInfo != "" {
		bins["extra_info"] = status.ExtraInfo
	}
	if status.PartitionKey != "" {
		bins["partition_key"] = status.PartitionKey
	}

	policy := s.writePolicy(ctx)
	policy.RecordExistsAction = aero.CREATE_ONLY
	if err := s.client.Put(policy, key, bins); err != nil {
		// Race condition: someone else inserted — re-read
		rec, getErr := s.client.Get(s.readPolicy(ctx), key)
		if getErr != nil && !isKeyNotFound(getErr) {
			return nil, false, fmt.Errorf("re-read after conflict: %w", getErr)
		}
		if rec != nil {
			return recordToStatus(rec, status.TxID), false, nil
		}
		return nil, false, fmt.Errorf("insert status: %w", err)
	}

	status.Status = models.Status(st)
	status.CreatedAt = now
	return status, true, nil
}

// BatchGetOrInsertStatus runs GetOrInsertStatus concurrently for each row.
// Aerospike's BatchOperate API doesn't support per-record CREATE_ONLY with
// the existing-row read-back semantics this method needs, so the parallel-
// loop fallback is the simplest correct path. Each Aerospike op is already
// fast (~1-3ms) so 16-way parallelism easily saturates throughput.
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return store.BatchGetOrInsertStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatus runs UpdateStatus concurrently for each row.
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	return store.BatchUpdateStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatusReturning applies each row with UpdateStatusReturning.
// The generation-checked put is the applied-result: a non-nil previous row
// is the record this call's CAS wrote over.
func (s *Store) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]store.StatusUpdate, error) {
	return store.BatchUpdateStatusReturningParallel(ctx, s, statuses)
}

// UpdateStatus updates an existing transaction record. If no record exists for
// status.TxID the call returns store.ErrNotFound without writing — callers
// must use GetOrInsertStatus to create new rows. A lattice skip is a nil
// error and writes nothing. See UpdateStatusReturning.
func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	_, err := s.UpdateStatusReturning(ctx, status)
	return err
}

// statusUpdateReadBins are the only bins the CAS read fetches. The
// applied-result needs the lifecycle status, its timestamp, and the block
// anchor / extra info the contract returns. raw_tx, merkle_path, and
// competing_txs stay unread.
var statusUpdateReadBins = []string{
	"status",
	"timestamp",
	"block_hash",
	"block_height",
	"extra_info",
}

// statusFromUpdateBins decodes only statusUpdateReadBins.
func statusFromUpdateBins(rec *aero.Record, txid string) *models.TransactionStatus {
	st := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.Status(getString(rec, "status")),
		BlockHash: getString(rec, "block_hash"),
		ExtraInfo: getString(rec, "extra_info"),
	}
	if ms := getInt(rec, "timestamp"); ms != 0 {
		st.Timestamp = time.UnixMilli(int64(ms))
	}
	if h := getInt(rec, "block_height"); h > 0 {
		st.BlockHeight = uint64(h)
	}
	return st
}

// UpdateStatusReturning applies status and returns the applied-result.
// Prev is the record this call's generation-checked put wrote over, decoded
// from statusUpdateReadBins only. Current is that same narrow record when
// the lattice skips the write. A generation mismatch retries the read so a
// concurrent MINED writer cannot leave this call holding a stale
// ACCEPTED_BY_NETWORK pre-image.
func (s *Store) UpdateStatusReturning(ctx context.Context, status *models.TransactionStatus) (store.StatusUpdate, error) {
	key, err := s.key(setTransactions, status.TxID)
	if err != nil {
		return store.StatusUpdate{}, err
	}

	bins := aero.BinMap{
		"status":    string(status.Status),
		"timestamp": status.Timestamp.UnixMilli(),
	}
	if status.StatusCode != 0 {
		bins["status_code"] = status.StatusCode
	}
	if status.BlockHash != "" {
		bins["block_hash"] = status.BlockHash
	}
	if status.BlockHeight > 0 {
		bins["block_height"] = int(status.BlockHeight) //nolint:gosec // block height fits in int on 64-bit platforms (max ~10^9 << math.MaxInt32)
	}
	if status.ExtraInfo != "" {
		bins["extra_info"] = status.ExtraInfo
	}
	if status.Status != "" && status.Status.ClearsRetryState() {
		// A nil bin value deletes the bin. A positive verdict retires the
		// retry bookkeeping; extra_info keeps only what this write carries.
		// See models.Status.ClearsRetryState.
		bins["next_retry_at"] = nil
		bins["retry_reason"] = nil
		if status.ExtraInfo == "" {
			bins["extra_info"] = nil
		}
	}
	if len(status.MerklePath) > 0 {
		bins["merkle_path"] = []byte(status.MerklePath)
	}
	if !status.MerkleRegisteredAt.IsZero() {
		bins["merkle_reg_at"] = status.MerkleRegisteredAt.UnixMilli()
	}
	if len(status.CompetingTxs) > 0 {
		// JSON bytes, the encoding the GetStatus decoder already reads.
		ct, err := json.Marshal(status.CompetingTxs)
		if err != nil {
			return store.StatusUpdate{}, fmt.Errorf("marshal competing_txs for %s: %w", status.TxID, err)
		}
		bins["competing_txs"] = ct
	}

	// Read-then-CAS-write using the record's generation. The pre-write
	// lattice check and the put are atomic with respect to other writers:
	// a generation mismatch retries rather than clobbering their write.
	// UPDATE_ONLY refuses to create a record (F-033 / #91).
	for {
		rec, gerr := s.client.Get(s.readPolicy(ctx), key, statusUpdateReadBins...)
		if gerr != nil && !isKeyNotFound(gerr) {
			return store.StatusUpdate{}, fmt.Errorf("read status for lattice check %s: %w", status.TxID, gerr)
		}
		if rec == nil {
			return store.StatusUpdate{}, store.ErrNotFound
		}
		observed := statusFromUpdateBins(rec, status.TxID)
		if status.Status != "" && !status.Status.CanTransitionFrom(observed.Status) {
			return store.StatusUpdate{Current: observed}, nil
		}
		policy := s.writePolicy(ctx)
		policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		policy.Generation = rec.Generation
		policy.RecordExistsAction = aero.UPDATE_ONLY
		if err := s.client.Put(policy, key, bins); err != nil {
			if isGenerationErr(err) {
				continue
			}
			if isKeyNotFound(err) {
				return store.StatusUpdate{}, store.ErrNotFound
			}
			return store.StatusUpdate{}, fmt.Errorf("update tx %s: %w", status.TxID, err)
		}
		return store.StatusUpdate{Prev: observed}, nil
	}
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return nil, err
	}

	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return nil, fmt.Errorf("get status %s: %w", txid, err)
	}
	if rec == nil {
		return nil, nil
	}

	status := recordToStatus(rec, txid)
	s.enrichMerklePath(ctx, status)
	s.enrichOrphanedProofs(ctx, status)
	return status, nil
}

func (s *Store) GetStatusesSince(ctx context.Context, _ time.Time) ([]*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Scan all transactions — for TxTracker loading
	stmt := aero.NewStatement(s.namespace, setTransactions)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query statuses: %w", err)
	}

	var (
		results []*models.TransactionStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			txid := getString(rec.Record, "txid")
			results = append(results, recordToStatus(rec.Record, txid))
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return results, nil
}

// IterateStatusesSince streams scan results to fn one record at a time. The
// Aerospike client already produces records over a channel — we just hand
// them off without buffering, so peak memory is O(1) plus whatever fn keeps.
func (s *Store) IterateStatusesSince(ctx context.Context, _ time.Time, fn func(*models.TransactionStatus) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return fmt.Errorf("query statuses: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, ok := <-rs.Results():
			if !ok {
				return nil
			}
			if rec.Err != nil {
				return rec.Err
			}
			txid := getString(rec.Record, "txid")
			if err := fn(recordToStatus(rec.Record, txid)); err != nil {
				return err
			}
		}
	}
}

// IterateTrackerRows runs one secondary-index query per tracked status
// (arcade_idx_tx_status, the same index CensusStatusesSince and GetReadyRetries
// use) projecting only the three bins hydration reads.
//
// This is the largest of the three backend wins: IterateStatusesSince above is
// a full-namespace scan that ignores `since` entirely and transfers whole
// records, so hydration used to pull every row in the set — raw_tx included —
// across the wire on every api-server boot (issue #276).
//
// The MINED height cutoff is applied client-side via scan.Keep: Aerospike's
// equality filter cannot express it, and the store contract explicitly permits
// a backend to over-emit rather than requiring an exact server-side match.
func (s *Store) IterateTrackerRows(ctx context.Context, scan store.TrackerScan, fn func(store.TrackerRow) error) error {
	for _, status := range scan.Statuses() {
		if err := ctx.Err(); err != nil {
			return err
		}
		stmt := aero.NewStatement(s.namespace, setTransactions, "txid", "status", "block_height")
		_ = stmt.SetFilter(aero.NewEqualFilter("status", string(status)))

		rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
		if err != nil {
			if rs != nil {
				_ = rs.Close()
			}
			return fmt.Errorf("tracker rows query status %s: %w", status, err)
		}

		var loopErr error
	loop:
		for {
			select {
			case <-ctx.Done():
				loopErr = ctx.Err()
				break loop
			case rec, ok := <-rs.Results():
				if !ok {
					break loop
				}
				if rec.Err != nil {
					loopErr = rec.Err
					break loop
				}
				txid := getString(rec.Record, "txid")
				if txid == "" {
					continue
				}
				// Trust the record's own status bin rather than the filter we
				// asked for, so a stale index entry cannot hand the tracker a
				// status the row no longer holds.
				rowStatus := models.Status(getString(rec.Record, "status"))
				height := getInt(rec.Record, "block_height")
				if height < 0 {
					height = 0
				}
				blockHeight := uint64(height)
				if !scan.Keep(rowStatus, blockHeight) {
					continue
				}
				if err := fn(store.TrackerRow{
					TxID:        txid,
					Status:      rowStatus,
					BlockHeight: blockHeight,
				}); err != nil {
					loopErr = err
					break loop
				}
			}
		}
		_ = rs.Close()
		if loopErr != nil {
			return loopErr
		}
	}
	return nil
}

// CensusStatusesSince pushes the census as far into Aerospike as the client
// allows: one secondary-index query per requested status (arcade_idx_tx_status,
// same index GetReadyRetries uses) projecting ONLY the timestamp bin, with the
// count/min aggregation done over that stream. No raw_tx or full-record
// transfer, and rows outside the requested statuses are never visited — the
// full-namespace IterateStatusesSince walk this replaces is what pinned the
// reaper's cadence to the scan time (issue #290).
func (s *Store) CensusStatusesSince(ctx context.Context, since, stuckDeadline time.Time, statuses []models.Status) (map[models.Status]store.StatusCensus, error) {
	out := make(map[models.Status]store.StatusCensus, len(statuses))
	for _, st := range statuses {
		out[st] = store.StatusCensus{}
	}
	sinceMs := since.UnixMilli()
	deadlineMs := stuckDeadline.UnixMilli()

	for _, status := range statuses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stmt := aero.NewStatement(s.namespace, setTransactions, "timestamp")
		_ = stmt.SetFilter(aero.NewEqualFilter("status", string(status)))

		rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
		if err != nil {
			if rs != nil {
				_ = rs.Close()
			}
			return nil, fmt.Errorf("census query status %s: %w", status, err)
		}

		census := out[status]
		var loopErr error
	loop:
		for {
			select {
			case <-ctx.Done():
				loopErr = ctx.Err()
				break loop
			case rec, ok := <-rs.Results():
				if !ok {
					break loop
				}
				if rec.Err != nil {
					loopErr = rec.Err
					break loop
				}
				tsMs := getInt64(rec.Record, "timestamp")
				if tsMs < sinceMs || tsMs >= deadlineMs {
					continue
				}
				census.Count++
				ts := time.UnixMilli(tsMs)
				if census.Oldest.IsZero() || ts.Before(census.Oldest) {
					census.Oldest = ts
				}
			}
		}
		_ = rs.Close()
		if loopErr != nil {
			return nil, loopErr
		}
		out[status] = census
	}
	return out, nil
}

// binOrphAnchors is the transactions-set bin holding the row's orphaned-anchor
// history as JSON-encoded []orphanedAnchorRecord (Aerospike bin names are
// capped at 15 chars, hence the abbreviation). Written by SetMinedByTxIDs
// (re-anchor to a different block) and SetStatusByBlockHash (reorg revert),
// decoded back into models.TransactionStatus.OrphanedProofs by recordToStatus.
const binOrphAnchors = "orph_anchors"

// orphanedAnchorRecord is the persisted form of models.OrphanedAnchor — block
// identity plus when the anchor was superseded, in unix milliseconds like the
// other transactions-set timestamps. The proof itself is never persisted; it
// is re-derived from the orphaned block's retained compound BUMP at read time.
type orphanedAnchorRecord struct {
	BlockHash    string `json:"block_hash"`
	BlockHeight  uint64 `json:"block_height,omitempty"`
	OrphanedAtMs int64  `json:"orphaned_at"`
}

// orphanedAnchorsFromRecord decodes the orph_anchors bin. A missing or
// unparseable history reads as empty — the bin is best-effort bookkeeping,
// never a gate.
func orphanedAnchorsFromRecord(rec *aero.Record) []orphanedAnchorRecord {
	var out []orphanedAnchorRecord
	switch v := rec.Bins[binOrphAnchors].(type) {
	case []byte:
		_ = json.Unmarshal(v, &out)
	case string:
		_ = json.Unmarshal([]byte(v), &out)
	}
	return out
}

// appendOrphanedAnchor records that the blockHash/blockHeight anchor is being
// superseded (re-anchor to a different block, or reorg revert). Collapses
// consecutive duplicates and caps at models.MaxOrphanedAnchors, mirroring
// models.AppendOrphanedAnchor.
func appendOrphanedAnchor(history []orphanedAnchorRecord, blockHash string, blockHeight uint64, now time.Time) []orphanedAnchorRecord {
	if blockHash == "" {
		return history
	}
	if n := len(history); n > 0 && history[n-1].BlockHash == blockHash {
		return history
	}
	history = append(history, orphanedAnchorRecord{
		BlockHash:    blockHash,
		BlockHeight:  blockHeight,
		OrphanedAtMs: now.UnixMilli(),
	})
	if len(history) > models.MaxOrphanedAnchors {
		history = history[len(history)-models.MaxOrphanedAnchors:]
	}
	return history
}

// SetStatusByBlockHash rewrites every transaction currently anchored to
// blockHash. For SEEN_ON_NETWORK transitions block fields are cleared and the
// cleared anchor is appended to the row's orph_anchors history (reorg revert,
// issue #279); for IMMUTABLE they are kept. The secondary-index query is a
// snapshot: each row is then re-read and CAS-written under a generation match
// (the same read-check-write UpdateStatus uses for the lattice), so IMMUTABLE
// rows are never touched and rows whose block_hash no longer matches at write
// time — concurrently re-anchored to the canonical block — are skipped.
// Returns only the txids actually rewritten.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions, "txid")
	_ = stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query by block hash: %w", err)
	}

	var (
		candidates []string
		loopErr    error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				// A partial affected set would silently under-reconcile —
				// surface the error instead of skipping the record.
				loopErr = rec.Err
				break loop
			}
			if txid := getString(rec.Record, "txid"); txid != "" {
				candidates = append(candidates, txid)
			}
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}

	clearBlock := newStatus == models.StatusSeenOnNetwork
	now := time.Now()
	var txids []string
	for _, txid := range candidates {
		if err := ctx.Err(); err != nil {
			return txids, err
		}
		key, err := s.key(setTransactions, txid)
		if err != nil {
			continue
		}
		for {
			rec, gerr := s.client.Get(s.readPolicy(ctx), key, "status", "block_hash", "block_height", binOrphAnchors)
			if gerr != nil || rec == nil {
				break
			}
			// Stale-index guard: the row moved to a different anchor between
			// the index snapshot and this write — reverting it now would undo
			// a concurrent re-anchor to the canonical block (issue #279).
			if getString(rec, "block_hash") != blockHash {
				break
			}
			// IMMUTABLE rows are never touched by block-scoped rewrites.
			if getString(rec, "status") == string(models.StatusImmutable) {
				break
			}

			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("status", string(newStatus))),
				aero.PutOp(aero.NewBin("timestamp", now.UnixMilli())),
			}
			if clearBlock {
				// Preserve the anchor being cleared so the orphaned block's
				// proof stays auditable (issue #279), then drop the block bins
				// (Aerospike: writing a nil bin value deletes the bin).
				height := uint64(getInt(rec, "block_height")) //nolint:gosec // height non-negative
				history := appendOrphanedAnchor(orphanedAnchorsFromRecord(rec), blockHash, height, now)
				if payload, mErr := json.Marshal(history); mErr == nil {
					ops = append(ops, aero.PutOp(aero.NewBin(binOrphAnchors, payload)))
				}
				ops = append(
					ops,
					aero.PutOp(aero.NewBin("block_hash", nil)),
					aero.PutOp(aero.NewBin("block_height", nil)),
				)
			}

			wp := s.writePolicy(ctx)
			wp.GenerationPolicy = aero.EXPECT_GEN_EQUAL
			wp.Generation = rec.Generation
			wp.RecordExistsAction = aero.UPDATE_ONLY
			if _, werr := s.client.Operate(wp, key, ops...); werr != nil {
				// Generation mismatch means another writer landed between our
				// read and our put — re-read and re-evaluate the guards rather
				// than clobbering their write. Any other error skips the row,
				// matching the per-record error tolerance of the old scan loop.
				if isGenerationErr(werr) {
					continue
				}
				break
			}
			txids = append(txids, txid)
			break
		}
	}
	return txids, nil
}

// GetTxIDsByBlockHash returns every txid currently anchored to blockHash via
// the arcade_idx_tx_block_hash secondary index — the reorg reconciler's
// affected set for an orphaned block. Rows already re-anchored or reverted no
// longer match the index and don't appear. Only the txid bin is projected;
// the reconciler wants identities, not rows.
func (s *Store) GetTxIDsByBlockHash(ctx context.Context, blockHash string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions, "txid")
	_ = stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query txids by block hash: %w", err)
	}

	var (
		txids   []string
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				// A partial affected set would silently under-reconcile —
				// surface the error instead of skipping the record.
				loopErr = rec.Err
				break loop
			}
			if txid := getString(rec.Record, "txid"); txid != "" {
				txids = append(txids, txid)
			}
		}
	}
	if loopErr != nil {
		return txids, loopErr
	}
	return txids, nil
}

// BumpRetryCount atomically increments retry_count and returns the new value.
// Bin writes for status / raw_tx / next_retry_at are handled separately by
// SetPendingRetryFields so callers can compute the correct backoff from the
// post-increment count.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return 0, err
	}
	// AddOp is non-idempotent — set MaxRetries=0 so a timed-out attempt is not
	// retried and double-counted.
	wp := s.writePolicy(ctx)
	wp.MaxRetries = 0
	rec, err := s.client.Operate(wp, key, aero.AddOp(aero.NewBin("retry_count", 1)), aero.GetOp())
	if err != nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, err)
	}
	if v, ok := rec.Bins["retry_count"]; ok {
		if n, ok := v.(int); ok {
			return n, nil
		}
	}
	return 1, nil
}

// SetPendingRetryFields writes the durable retry bins in one atomic call. The
// caller is responsible for having already bumped retry_count and computed
// next_retry_at from that post-increment value.
func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return err
	}
	// Lattice guard: the park path writes twice (a guarded status update, then
	// this), so skipping it here would silently undo the first write's
	// protection and drag a MINED / IMMUTABLE / SEEN / REJECTED row back to
	// PENDING_RETRY. Silent skip matches the guarded update's semantics.
	rec, gerr := s.client.Get(s.readPolicy(ctx), key, "status")
	if gerr != nil && !isKeyNotFound(gerr) {
		return fmt.Errorf("read status for lattice check %s: %w", txid, gerr)
	}
	if rec == nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, store.ErrNotFound)
	}
	if !models.StatusPendingRetry.CanTransitionFrom(models.Status(getString(rec, "status"))) {
		return nil
	}
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin("status", string(models.StatusPendingRetry))),
		aero.PutOp(aero.NewBin("raw_tx", rawTx)),
		aero.PutOp(aero.NewBin("next_retry_at", nextRetryAt.UnixMilli())),
		aero.PutOp(aero.NewBin("timestamp", time.Now().UnixMilli())),
	}
	if lastReason != "" {
		ops = append(ops, aero.PutOp(aero.NewBin("retry_reason", lastReason)))
	} else {
		// Aerospike: writing a nil bin value deletes the bin.
		ops = append(ops, aero.PutOp(aero.NewBin("retry_reason", nil)))
	}
	if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, err)
	}
	return nil
}

// GetReadyRetries uses the existing arcade_idx_tx_status index to find PENDING_RETRY
// rows and filters by next_retry_at in code. At expected cardinality
// (thousands, not millions) this is cheap.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions)
	_ = stmt.SetFilter(aero.NewEqualFilter("status", string(models.StatusPendingRetry)))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query pending retry txs: %w", err)
	}

	nowMs := now.UnixMilli()
	results := make([]*store.PendingRetry, 0, limit)
	var loopErr error
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			nextMs := getInt64(rec.Record, "next_retry_at")
			if nextMs > nowMs {
				continue
			}
			rawTx, _ := rec.Record.Bins["raw_tx"].([]byte)
			if len(rawTx) == 0 {
				// Legacy PENDING_RETRY rows from before durable retries — skip.
				continue
			}
			results = append(results, &store.PendingRetry{
				TxID:        getString(rec.Record, "txid"),
				RawTx:       rawTx,
				RetryCount:  getInt(rec.Record, "retry_count"),
				NextRetryAt: time.UnixMilli(nextMs),
				LastReason:  getString(rec.Record, "retry_reason"),
			})
			if len(results) >= limit {
				break loop
			}
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return results, nil
}

// ClearRetryState transitions a tx out of PENDING_RETRY and deletes the retry
// bins so the row stops matching the reaper's query. retry_count is retained
// for observability.
func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return err
	}
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin("status", string(finalStatus))),
		aero.PutOp(aero.NewBin("timestamp", time.Now().UnixMilli())),
		// Aerospike: writing a nil bin value deletes the bin.
		aero.PutOp(aero.NewBin("raw_tx", nil)),
		aero.PutOp(aero.NewBin("next_retry_at", nil)),
		aero.PutOp(aero.NewBin("retry_reason", nil)),
	}
	if extraInfo != "" {
		ops = append(ops, aero.PutOp(aero.NewBin("extra_info", extraInfo)))
	}
	if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
		return fmt.Errorf("clear retry state %s: %w", txid, err)
	}
	return nil
}

// minedWrite computes the per-record MINED write from a snapshot of an
// existing row: the ops to apply, the pre-write model for the prevs slice,
// and whether the row is eligible. IMMUTABLE rows are ineligible — a replayed
// BLOCK_PROCESSED must never demote a confirmation-depth promotion (issue
// #279 hardening). A MINED row re-anchored to a DIFFERENT block appends the
// anchor being superseded to its orph_anchors history first, so the orphaned
// block's proof stays auditable; first-time MINED and idempotent same-block
// replays leave history untouched.
func minedWrite(rec *aero.Record, txid, blockHash string, blockHeight uint64, now time.Time) (ops []*aero.Operation, prev *models.TransactionStatus, ok bool) {
	prevStatus := models.Status(getString(rec, "status"))
	// Respect the status lattice: of all statuses only IMMUTABLE may not
	// regress to MINED. See models.Status.CanTransitionFrom.
	if !models.StatusMined.CanTransitionFrom(prevStatus) {
		return nil, nil, false
	}
	prev = recordToStatus(rec, txid)
	ops = []*aero.Operation{
		aero.PutOp(aero.NewBin("status", string(models.StatusMined))),
		aero.PutOp(aero.NewBin("block_hash", blockHash)),
		aero.PutOp(aero.NewBin("block_height", int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
		aero.PutOp(aero.NewBin("timestamp", now.UnixMilli())),
		// MINED is a positive verdict: retire the retry bookkeeping (nil
		// deletes the bin). See models.Status.ClearsRetryState.
		aero.PutOp(aero.NewBin("extra_info", nil)),
		aero.PutOp(aero.NewBin("next_retry_at", nil)),
		aero.PutOp(aero.NewBin("retry_reason", nil)),
	}
	if prevStatus == models.StatusMined && prev.BlockHash != "" && prev.BlockHash != blockHash {
		history := appendOrphanedAnchor(orphanedAnchorsFromRecord(rec), prev.BlockHash, prev.BlockHeight, now)
		if payload, err := json.Marshal(history); err == nil {
			ops = append(ops, aero.PutOp(aero.NewBin(binOrphAnchors, payload)))
		}
	}
	return ops, prev, true
}

// setMinedCAS is the per-record fallback for a batch MINED write that lost a
// generation race: re-read, re-run the IMMUTABLE guard and anchor bookkeeping
// against the fresh snapshot, and CAS-write — the same read-check-write loop
// UpdateStatus uses for the lattice. Returns the pre-write row and whether
// the write was applied (false = row missing or ineligible).
func (s *Store) setMinedCAS(ctx context.Context, txid, blockHash string, blockHeight uint64, now time.Time) (*models.TransactionStatus, bool, error) {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return nil, false, err
	}
	for {
		rec, gerr := s.client.Get(s.readPolicy(ctx), key)
		if gerr != nil && !isKeyNotFound(gerr) {
			return nil, false, fmt.Errorf("read status for set mined %s: %w", txid, gerr)
		}
		if rec == nil {
			return nil, false, nil
		}
		ops, prev, ok := minedWrite(rec, txid, blockHash, blockHeight, now)
		if !ok {
			return nil, false, nil
		}
		wp := s.writePolicy(ctx)
		wp.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		wp.Generation = rec.Generation
		wp.RecordExistsAction = aero.UPDATE_ONLY
		if _, werr := s.client.Operate(wp, key, ops...); werr != nil {
			if isGenerationErr(werr) {
				continue
			}
			if isKeyNotFound(werr) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("set mined %s: %w", txid, werr)
		}
		return prev, true, nil
	}
}

// SetMinedByTxIDs writes a MINED status batch keyed by blockHash + blockHeight.
// blockHeight is persisted as the block_height bin and echoed back on each
// returned TransactionStatus so SSE/webhook consumers always see the height
// alongside the hash (issue #87 / F-029).
//
// Hardening (issue #279): rows are snapshot via BatchGet first. The snapshot
// feeds the returned prevs, skips IMMUTABLE rows (never demote a
// confirmation-depth promotion), and appends the previous anchor to
// orph_anchors when a MINED row is re-anchored to a DIFFERENT block. Each
// batch write carries a generation CAS pinned to its snapshot, so a record
// that moved in between falls back to the per-record read-check-write
// (setMinedCAS) instead of clobbering the concurrent write with stale
// bookkeeping.
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	now := time.Now()
	var prevs, statuses []*models.TransactionStatus

	for i := 0; i < len(txids); i += s.batchSize {
		if err := ctx.Err(); err != nil {
			return prevs, statuses, err
		}
		end := i + s.batchSize
		if end > len(txids) {
			end = len(txids)
		}
		batch := txids[i:end]

		// Snapshot pre-update rows. BatchGet is a second round-trip but
		// matches the access pattern Pebble does atomically via per-shard
		// locks; the generation CAS on each write closes the gap in between.
		keys := make([]*aero.Key, 0, len(batch))
		keyByTxID := make(map[string]int, len(batch))
		for _, txid := range batch {
			key, err := s.key(setTransactions, txid)
			if err != nil {
				continue
			}
			keyByTxID[txid] = len(keys)
			keys = append(keys, key)
		}
		recByTxID := make(map[string]*aero.Record, len(batch))
		if len(keys) > 0 {
			recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys)
			if err != nil {
				return prevs, statuses, fmt.Errorf("batch get for set mined prev: %w", err)
			}
			for txid, idx := range keyByTxID {
				if idx >= len(recs) || recs[idx] == nil {
					continue
				}
				recByTxID[txid] = recs[idx]
			}
		}

		records := make([]aero.BatchRecordIfc, 0, len(batch))
		written := make([]string, 0, len(batch))
		prevForSlot := make([]*models.TransactionStatus, 0, len(batch))
		for _, txid := range batch {
			rec := recByTxID[txid]
			if rec == nil {
				// Unknown txid — silently skipped, never created.
				continue
			}
			ops, prev, ok := minedWrite(rec, txid, blockHash, blockHeight, now)
			if !ok {
				// IMMUTABLE — never demoted; no entry in either slice.
				continue
			}
			bwp := aero.NewBatchWritePolicy()
			bwp.RecordExistsAction = aero.UPDATE_ONLY
			bwp.GenerationPolicy = aero.EXPECT_GEN_EQUAL
			bwp.Generation = rec.Generation
			records = append(records, aero.NewBatchWrite(bwp, keys[keyByTxID[txid]], ops...))
			written = append(written, txid)
			prevForSlot = append(prevForSlot, prev)
		}
		if len(records) == 0 {
			continue
		}

		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return prevs, statuses, fmt.Errorf("batch set mined: %w", err)
		}

		for j, txid := range written {
			switch br := records[j].BatchRec(); {
			case br.Err == nil:
				prevs = append(prevs, prevForSlot[j])
			case isGenerationErr(br.Err):
				// Another writer landed between snapshot and CAS write —
				// redo this record alone against a fresh read rather than
				// silently dropping the MINED transition.
				prev, applied, err := s.setMinedCAS(ctx, txid, blockHash, blockHeight, now)
				if err != nil {
					return prevs, statuses, err
				}
				if !applied {
					continue
				}
				prevs = append(prevs, prev)
			default:
				// UPDATE_ONLY on a row deleted since the snapshot, or another
				// per-record error — skipped, matching the unknown-txid
				// contract.
				continue
			}
			statuses = append(statuses, &models.TransactionStatus{
				TxID:        txid,
				Status:      models.StatusMined,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Timestamp:   now,
			})
		}
	}

	return prevs, statuses, nil
}

// MarkMerkleRegisteredByTxIDs writes merkle_reg_at = ts.UnixMilli() on
// every existing transaction record in the txid list. Unknown txids are
// silently skipped via UPDATE_ONLY (matching SetMinedByTxIDs). Used by the
// startup replay loop to skip rows registered recently (issue #145).
func (s *Store) MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error {
	if len(txids) == 0 {
		return nil
	}
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.UPDATE_ONLY

	for i := 0; i < len(txids); i += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := i + s.batchSize
		if end > len(txids) {
			end = len(txids)
		}
		batch := txids[i:end]

		records := make([]aero.BatchRecordIfc, len(batch))
		for j, txid := range batch {
			key, err := s.key(setTransactions, txid)
			if err != nil {
				continue
			}
			records[j] = aero.NewBatchWrite(
				bwp, key,
				aero.PutOp(aero.NewBin("merkle_reg_at", ts.UnixMilli())),
			)
		}

		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch mark merkle registered: %w", err)
		}
	}
	return nil
}

// --- BUMP Operations ---
//
// BUMPs are stored as a manifest record at primary key <blockHash> plus N
// chunk records at <blockHash>:c:<index>. Chunking is unconditional — every
// BUMP, however small, takes one manifest + at least one chunk record. This
// is required because compound BUMPs on scaling networks routinely exceed
// Aerospike's per-record ceiling (write-block-size, default 1 MiB, max 8 MiB).
//
// Manifest bins: block_hash, block_height, chunk_count, total_size,
// format_version. The manifest carries no bump_data — it points exclusively
// at chunk records.
//
// Chunk bins: block_hash, chunk_index, chunk_data.
//
// Atomicity: chunks are written first (BatchOperate), then the manifest. The
// manifest write is the linearization point — readers see the new BUMP only
// after it lands. A crash between chunk write and manifest write leaves
// orphan chunks but the previous manifest still references the previous
// chunks, so readers continue to see the old BUMP intact.

// InsertBUMP stores a compound BUMP as a manifest plus chunks. Chunks are
// written before the manifest so a concurrent reader observing the new
// manifest is guaranteed to find every chunk it references.
func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if blockHash == "" {
		return fmt.Errorf("insert bump: empty block hash")
	}

	// Read the previous manifest so we know whether to clean up orphan chunks
	// from a prior larger BUMP for the same block. Failure here is non-fatal
	// for the write itself — if we can't read it, we skip cleanup; the write
	// still succeeds and orphans are unreferenced.
	oldChunkCount := 0
	if oldManifestKey, err := s.key(setBumps, blockHash); err == nil {
		if rec, err := s.client.Get(s.readPolicy(ctx), oldManifestKey, "chunk_count"); err == nil && rec != nil {
			if v, ok := rec.Bins["chunk_count"].(int); ok && v > 0 {
				oldChunkCount = v
			}
		}
	}

	newChunkCount := 1
	if len(bumpData) > s.bumpChunkSize {
		newChunkCount = (len(bumpData) + s.bumpChunkSize - 1) / s.bumpChunkSize
	}

	if err := s.writeBumpChunks(ctx, blockHash, bumpData, newChunkCount); err != nil {
		return fmt.Errorf("write bump chunks for %s: %w", blockHash, err)
	}

	manifestKey, err := s.key(setBumps, blockHash)
	if err != nil {
		return err
	}
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.REPLACE
	bins := aero.BinMap{
		"block_hash":     blockHash,
		"block_height":   int(blockHeight), //nolint:gosec // block height fits in int on 64-bit platforms
		"chunk_count":    newChunkCount,
		"total_size":     len(bumpData),
		"format_version": bumpFormatVersion,
	}
	if err := s.client.Put(wp, manifestKey, bins); err != nil {
		return fmt.Errorf("write bump manifest for %s: %w", blockHash, err)
	}

	// Best-effort cleanup of orphan chunks from a prior write that had more
	// chunks than this one. Orphans are unreferenced (the manifest now caps at
	// newChunkCount) so a failure here only wastes disk.
	if oldChunkCount > newChunkCount {
		if err := s.deleteBumpChunkRange(ctx, blockHash, newChunkCount, oldChunkCount); err != nil {
			// Don't fail the write — orphans are harmless aside from disk.
			// The next InsertBUMP for this block retries the cleanup.
			_ = err
		}
	}
	// A rebuild can overwrite the compound for an existing block (e.g. late
	// STUMP callbacks). Drop any cached parse so the next enrichment reassembles
	// and re-parses the current stored BUMP.
	s.bumpCache.Remove(blockHash)
	return nil
}

// GetBUMP fetches the manifest, then batch-reads every chunk and reassembles
// the original bumpData. Returns store.ErrNotFound if no manifest exists.
// Any chunk error or assembly mismatch returns a wrapped error and no partial
// data — callers (enrichMerklePath) treat errors as "skip enrichment", which
// is correct: a truncated BUMP would silently produce wrong per-tx proofs.
func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	manifestKey, err := s.key(setBumps, blockHash)
	if err != nil {
		return 0, nil, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), manifestKey)
	if err != nil && !isKeyNotFound(err) {
		return 0, nil, fmt.Errorf("get bump manifest %s: %w", blockHash, err)
	}
	if rec == nil {
		return 0, nil, store.ErrNotFound
	}

	chunkCount, _ := rec.Bins["chunk_count"].(int)
	totalSize, _ := rec.Bins["total_size"].(int)
	formatVersion, _ := rec.Bins["format_version"].(int)
	if chunkCount <= 0 || totalSize < 0 || formatVersion != bumpFormatVersion {
		return 0, nil, fmt.Errorf(
			"get bump %s: invalid manifest (chunk_count=%d total_size=%d format_version=%d)",
			blockHash, chunkCount, totalSize, formatVersion,
		)
	}

	var height uint64
	if v, ok := rec.Bins["block_height"].(int); ok {
		height = uint64(v) //nolint:gosec // round-trip of a value we wrote as int from a uint64 fitting block height
	}

	data, err := s.readBumpChunks(ctx, blockHash, chunkCount, totalSize)
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	return height, data, nil
}

// bumpChunkKey constructs the primary key for a chunk record. Index is
// zero-padded to a fixed width so chunk keys sort lexicographically and don't
// collide with future schema additions on the same set.
func (s *Store) bumpChunkKey(blockHash string, idx int) (*aero.Key, error) {
	return s.key(setBumps, fmt.Sprintf(bumpChunkKeyFormat, blockHash, idx))
}

// writeBumpChunks slices bumpData into chunkCount records and writes them in
// one BatchOperate. Uses RecordExistsAction=REPLACE so any stale state from a
// previously failed write at the same key is dropped.
func (s *Store) writeBumpChunks(ctx context.Context, blockHash string, bumpData []byte, chunkCount int) error {
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.REPLACE

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			off := i * s.bumpChunkSize
			tail := off + s.bumpChunkSize
			if tail > len(bumpData) {
				tail = len(bumpData)
			}
			key, err := s.bumpChunkKey(blockHash, i)
			if err != nil {
				return err
			}
			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("block_hash", blockHash)),
				aero.PutOp(aero.NewBin("chunk_index", i)),
				aero.PutOp(aero.NewBin("chunk_data", bumpData[off:tail])),
			}
			records = append(records, aero.NewBatchWrite(bwp, key, ops...))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch write bump chunks: %w", err)
		}
		for _, r := range records {
			if br := r.BatchRec(); br != nil && br.Err != nil {
				return fmt.Errorf("write bump chunk: %w", br.Err)
			}
		}
	}
	return nil
}

// readBumpChunks fetches chunkCount chunk records via BatchGet (one round
// trip per batch slice) and assembles them into a single byte slice of length
// totalSize. Validates each chunk's index matches the slot it occupies and
// rejects any missing chunk or length mismatch.
func (s *Store) readBumpChunks(ctx context.Context, blockHash string, chunkCount, totalSize int) ([]byte, error) {
	out := make([]byte, totalSize)
	written := 0

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		keys := make([]*aero.Key, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.bumpChunkKey(blockHash, i)
			if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}

		recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys, "chunk_index", "chunk_data")
		if err != nil {
			return nil, fmt.Errorf("batch get bump chunks: %w", err)
		}
		if len(recs) != len(keys) {
			return nil, fmt.Errorf("batch get bump chunks: got %d records, want %d", len(recs), len(keys))
		}

		for j, r := range recs {
			idx := start + j
			if r == nil {
				return nil, fmt.Errorf("missing bump chunk %d", idx)
			}
			gotIdx, ok := r.Bins["chunk_index"].(int)
			if !ok || gotIdx != idx {
				return nil, fmt.Errorf("bump chunk %d: chunk_index mismatch (got %v)", idx, r.Bins["chunk_index"])
			}
			data, ok := r.Bins["chunk_data"].([]byte)
			if !ok {
				return nil, fmt.Errorf("bump chunk %d: chunk_data missing or wrong type", idx)
			}
			off := idx * s.bumpChunkSize
			if off+len(data) > totalSize {
				return nil, fmt.Errorf("bump chunk %d: would overflow assembled buffer (off=%d len=%d total=%d)", idx, off, len(data), totalSize)
			}
			copy(out[off:], data)
			written += len(data)
		}
	}

	if written != totalSize {
		return nil, fmt.Errorf("assembled %d bytes, manifest says %d", written, totalSize)
	}
	return out, nil
}

// deleteBumpChunkRange batch-deletes chunk records at indices [fromIdx,
// toExcl). Used by InsertBUMP to clean up orphans when a rewrite shrinks the
// chunk count for a block.
func (s *Store) deleteBumpChunkRange(ctx context.Context, blockHash string, fromIdx, toExcl int) error {
	if toExcl <= fromIdx {
		return nil
	}
	for start := fromIdx; start < toExcl; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > toExcl {
			end = toExcl
		}
		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.bumpChunkKey(blockHash, i)
			if err != nil {
				return err
			}
			records = append(records, aero.NewBatchDelete(nil, key))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch delete bump chunks: %w", err)
		}
	}
	return nil
}

// DeleteBUMPByBlockHash removes the stored compound BUMP — the manifest plus
// every chunk record it references — and drops any cached parse. Idempotent:
// a missing manifest is a no-op. The manifest is deleted before its chunks
// (mirroring DeleteStumpsByBlockHash): a crash mid-delete then leaves only
// unreferenced orphan chunks (harmless disk waste) rather than a manifest
// pointing at missing chunks (a hard read error).
func (s *Store) DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error {
	manifestKey, err := s.key(setBumps, blockHash)
	if err != nil {
		return err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), manifestKey, "chunk_count")
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("read bump manifest %s: %w", blockHash, err)
	}
	if rec == nil {
		// Nothing stored — still drop any cached parse so a stale in-memory
		// compound can't outlive the delete.
		s.bumpCache.Remove(blockHash)
		return nil
	}
	chunkCount, _ := rec.Bins["chunk_count"].(int)

	if _, err := s.client.Delete(s.writePolicy(ctx), manifestKey); err != nil {
		return fmt.Errorf("delete bump manifest %s: %w", blockHash, err)
	}
	if err := s.deleteBumpChunkRange(ctx, blockHash, 0, chunkCount); err != nil {
		return fmt.Errorf("delete bump chunks %s: %w", blockHash, err)
	}
	s.bumpCache.Remove(blockHash)
	return nil
}

// --- STUMP Operations ---
//
// A STUMP is stored exactly like a BUMP (see above): a manifest record plus N
// chunk records, because a single subtree's STUMP on a busy block routinely
// exceeds Aerospike's per-record ceiling (write-block-size, default 1 MiB, max
// 8 MiB). An unchunked write fails with RECORD_TOO_BIG.
//
// Manifest record — primary key <blockHash>:<subtreeIndex>, bins: block_hash,
// subtree_index, chunk_count, total_size, format_version. It carries no
// stump_data; it points at chunk records. Only the manifest carries the
// block_hash bin, so the arcade_idx_stumps_block_hash secondary index — and
// therefore GetStumpsByBlockHash — sees manifests only, never chunks.
//
// Chunk record — primary key <blockHash>:<subtreeIndex>:c:<index>, bins:
// chunk_index, chunk_data. Deliberately no block_hash bin (see above).
//
// Atomicity matches InsertBUMP: chunks are written first, then the manifest,
// which is the linearization point a concurrent reader keys off.

// stumpManifestKey builds the primary key for a STUMP manifest record.
// stumpManifestKey is content-addressed (see models.Stump); an empty
// contentHash selects the legacy (block:subtree) key for rows written before
// the upgrade.
func (s *Store) stumpManifestKey(blockHash string, subtreeIndex int, contentHash string) (*aero.Key, error) {
	if contentHash == "" {
		return s.key(setStumps, fmt.Sprintf("%s:%d", blockHash, subtreeIndex))
	}
	return s.key(setStumps, fmt.Sprintf("%s:%d:%s", blockHash, subtreeIndex, contentHash))
}

// stumpChunkKey builds the primary key for one chunk of a STUMP. The index is
// zero-padded so chunk keys sort lexicographically and never collide with a
// manifest key (which has no ":c:" segment).
func (s *Store) stumpChunkKey(blockHash string, subtreeIndex int, contentHash string, idx int) (*aero.Key, error) {
	if contentHash == "" {
		return s.key(setStumps, fmt.Sprintf(stumpChunkKeyFormatLegacy, blockHash, subtreeIndex, idx))
	}
	return s.key(setStumps, fmt.Sprintf(stumpChunkKeyFormat, blockHash, subtreeIndex, contentHash, idx))
}

// InsertStump stores a STUMP as a manifest plus chunk records. Chunks are
// written before the manifest so a concurrent reader observing the new
// manifest is guaranteed to find every chunk it references.
func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	if stump.BlockHash == "" {
		return fmt.Errorf("insert stump: empty block hash")
	}

	hash := stump.EnsureContentHash()
	manifestKey, err := s.stumpManifestKey(stump.BlockHash, stump.SubtreeIndex, hash)
	if err != nil {
		return err
	}
	// Content-addressed: a manifest under this key already references
	// exactly these bytes, so a redelivery from another merkle-service is
	// one read instead of a chunk rewrite. A key's content never changes,
	// which is also why no orphan-chunk cleanup is needed here any more.
	if exists, err := s.client.Exists(s.readPolicy(ctx), manifestKey); err == nil && exists {
		return nil
	}

	chunkCount := 1
	if len(stump.StumpData) > s.bumpChunkSize {
		chunkCount = (len(stump.StumpData) + s.bumpChunkSize - 1) / s.bumpChunkSize
	}

	if err := s.writeStumpChunks(ctx, stump.BlockHash, stump.SubtreeIndex, hash, stump.StumpData, chunkCount); err != nil {
		return fmt.Errorf("write stump chunks for %s:%d: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}

	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.REPLACE
	bins := aero.BinMap{
		"block_hash":     stump.BlockHash,
		"subtree_index":  stump.SubtreeIndex,
		"content_hash":   hash,
		"chunk_count":    chunkCount,
		"total_size":     len(stump.StumpData),
		"format_version": stumpFormatVersion,
	}
	if err := s.client.Put(wp, manifestKey, bins); err != nil {
		return fmt.Errorf("write stump manifest for %s:%d: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}
	return nil
}

// stumpManifest is the metadata read from a manifest record before its chunks
// are fetched. It is internal scaffolding for GetStumpsByBlockHash and
// DeleteStumpsByBlockHash.
type stumpManifest struct {
	subtreeIndex int
	contentHash  string // empty on manifests written before content addressing
	chunkCount   int
	totalSize    int
}

// queryStumpManifests runs the block_hash secondary-index query and returns
// every manifest for the block. Chunk records carry no block_hash bin, so the
// index never surfaces them here.
func (s *Store) queryStumpManifests(ctx context.Context, blockHash string) ([]stumpManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setStumps)
	_ = stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query stumps: %w", err)
	}

	var (
		manifests []stumpManifest
		loopErr   error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			m := stumpManifest{}
			if v, ok := rec.Record.Bins["subtree_index"].(int); ok {
				m.subtreeIndex = v
			}
			if v, ok := rec.Record.Bins["content_hash"].(string); ok {
				m.contentHash = v
			}
			if v, ok := rec.Record.Bins["chunk_count"].(int); ok {
				m.chunkCount = v
			}
			if v, ok := rec.Record.Bins["total_size"].(int); ok {
				m.totalSize = v
			}
			fv, _ := rec.Record.Bins["format_version"].(int)
			if m.chunkCount <= 0 || m.totalSize < 0 || fv != stumpFormatVersion {
				loopErr = fmt.Errorf(
					"stumps %s: invalid manifest for subtree %d (chunk_count=%d total_size=%d format_version=%d)",
					blockHash, m.subtreeIndex, m.chunkCount, m.totalSize, fv,
				)
				break loop
			}
			manifests = append(manifests, m)
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return manifests, nil
}

// GetStumpsByBlockHash retrieves every STUMP for a block. The secondary index
// returns manifest records only; each manifest's chunks are then batch-read
// and reassembled into the original stump_data.
func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	manifests, err := s.queryStumpManifests(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	// A secondary-index query returns records in no particular order; the
	// store contract is subtree-index order, with variants of one subtree
	// ordered by content hash so repeated reads agree.
	sort.Slice(manifests, func(i, j int) bool {
		if manifests[i].subtreeIndex != manifests[j].subtreeIndex {
			return manifests[i].subtreeIndex < manifests[j].subtreeIndex
		}
		return manifests[i].contentHash < manifests[j].contentHash
	})
	stumps := make([]*models.Stump, 0, len(manifests))
	for _, m := range manifests {
		data, err := s.readStumpChunks(ctx, blockHash, m.subtreeIndex, m.contentHash, m.chunkCount, m.totalSize)
		if err != nil {
			return nil, fmt.Errorf("get stumps %s: subtree %d: %w", blockHash, m.subtreeIndex, err)
		}
		st := &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: m.subtreeIndex,
			StumpData:    data,
			ContentHash:  m.contentHash,
		}
		st.EnsureContentHash()
		stumps = append(stumps, st)
	}
	return stumps, nil
}

// writeStumpChunks slices stumpData into chunkCount chunk records under the
// (blockHash, subtreeIndex) namespace and writes them with REPLACE semantics
// so stale state from a previously failed write at the same key is dropped.
func (s *Store) writeStumpChunks(ctx context.Context, blockHash string, subtreeIndex int, contentHash string, stumpData []byte, chunkCount int) error {
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.REPLACE

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			off := i * s.bumpChunkSize
			tail := off + s.bumpChunkSize
			if tail > len(stumpData) {
				tail = len(stumpData)
			}
			key, err := s.stumpChunkKey(blockHash, subtreeIndex, contentHash, i)
			if err != nil {
				return err
			}
			// No block_hash bin: chunk records must stay out of the
			// arcade_idx_stumps_block_hash index so manifest queries don't
			// surface them.
			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("chunk_index", i)),
				aero.PutOp(aero.NewBin("chunk_data", stumpData[off:tail])),
			}
			records = append(records, aero.NewBatchWrite(bwp, key, ops...))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch write stump chunks: %w", err)
		}
		for _, r := range records {
			if br := r.BatchRec(); br != nil && br.Err != nil {
				return fmt.Errorf("write stump chunk: %w", br.Err)
			}
		}
	}
	return nil
}

// readStumpChunks batch-reads chunkCount chunk records and assembles them into
// a single byte slice of length totalSize, rejecting any missing chunk, index
// mismatch, or length overflow.
func (s *Store) readStumpChunks(ctx context.Context, blockHash string, subtreeIndex int, contentHash string, chunkCount, totalSize int) ([]byte, error) {
	out := make([]byte, totalSize)
	written := 0

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		keys := make([]*aero.Key, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.stumpChunkKey(blockHash, subtreeIndex, contentHash, i)
			if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}

		recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys, "chunk_index", "chunk_data")
		if err != nil {
			return nil, fmt.Errorf("batch get stump chunks: %w", err)
		}
		if len(recs) != len(keys) {
			return nil, fmt.Errorf("batch get stump chunks: got %d records, want %d", len(recs), len(keys))
		}

		for j, r := range recs {
			idx := start + j
			if r == nil {
				return nil, fmt.Errorf("missing stump chunk %d", idx)
			}
			gotIdx, ok := r.Bins["chunk_index"].(int)
			if !ok || gotIdx != idx {
				return nil, fmt.Errorf("stump chunk %d: chunk_index mismatch (got %v)", idx, r.Bins["chunk_index"])
			}
			data, ok := r.Bins["chunk_data"].([]byte)
			if !ok {
				return nil, fmt.Errorf("stump chunk %d: chunk_data missing or wrong type", idx)
			}
			off := idx * s.bumpChunkSize
			if off+len(data) > totalSize {
				return nil, fmt.Errorf("stump chunk %d: would overflow assembled buffer (off=%d len=%d total=%d)", idx, off, len(data), totalSize)
			}
			copy(out[off:], data)
			written += len(data)
		}
	}

	if written != totalSize {
		return nil, fmt.Errorf("assembled %d bytes, manifest says %d", written, totalSize)
	}
	return out, nil
}

// deleteStumpChunkRange batch-deletes chunk records at indices [fromIdx,
// toExcl) for one (blockHash, subtreeIndex) STUMP.
func (s *Store) deleteStumpChunkRange(ctx context.Context, blockHash string, subtreeIndex int, contentHash string, fromIdx, toExcl int) error {
	if toExcl <= fromIdx {
		return nil
	}
	for start := fromIdx; start < toExcl; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > toExcl {
			end = toExcl
		}
		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.stumpChunkKey(blockHash, subtreeIndex, contentHash, i)
			if err != nil {
				return err
			}
			records = append(records, aero.NewBatchDelete(nil, key))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch delete stump chunks: %w", err)
		}
	}
	return nil
}

// DeleteStumpsByBlockHash removes every STUMP (manifests and chunks) for a
// block. Used during reorg cleanup. For each STUMP the manifest is deleted
// before its chunks: a crash mid-delete then leaves only unreferenced orphan
// chunks (harmless disk waste) rather than a manifest pointing at missing
// chunks (a hard read error).
func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	manifests, err := s.queryStumpManifests(ctx, blockHash)
	if err != nil {
		return err
	}

	for _, m := range manifests {
		manifestKey, err := s.stumpManifestKey(blockHash, m.subtreeIndex, m.contentHash)
		if err != nil {
			return err
		}
		if _, err := s.client.Delete(s.writePolicy(ctx), manifestKey); err != nil {
			return fmt.Errorf("delete stump manifest %s:%d: %w", blockHash, m.subtreeIndex, err)
		}
		if err := s.deleteStumpChunkRange(ctx, blockHash, m.subtreeIndex, m.contentHash, 0, m.chunkCount); err != nil {
			return fmt.Errorf("delete stump chunks %s:%d: %w", blockHash, m.subtreeIndex, err)
		}
	}
	return nil
}

// --- Submission Operations ---

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	key, err := s.key(setSubmissions, sub.SubmissionID)
	if err != nil {
		return err
	}
	// Bin names must be <=15 chars (Aerospike limit) — a longer name
	// fails the whole Put with BIN_NAME_TOO_LONG.
	bins := aero.BinMap{
		"submission_id":  sub.SubmissionID,
		"txid":           sub.TxID,
		"callback_url":   sub.CallbackURL,
		"callback_token": sub.CallbackToken,
		"full_updates":   sub.FullStatusUpdates,
		"created_at":     sub.CreatedAt.UnixMilli(),
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	_ = stmt.SetFilter(aero.NewEqualFilter("txid", txid))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, err
	}

	var (
		subs    []*models.Submission
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			subs = append(subs, recordToSubmission(rec.Record))
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return subs, nil
}

// TokensForTxIDs resolves via the txid secondary index (a txid has a handful
// of submissions) — never the token side, which can hold millions. Aerospike
// secondary-index queries take a single equality filter, so a batch is one
// query per txid; the win over the probe this replaced is that fan-out asks
// once per txid no matter how many clients are connected.
func (s *Store) TokensForTxIDs(ctx context.Context, txids []string) (map[string][]string, error) {
	return store.TokensForTxIDsViaPerTxID(ctx, txids, s.GetSubmissionsByTxID)
}

// maxTokenReplayScan bounds how many of a token's submissions this backend
// will walk for one catchup replay.
//
// Unlike Postgres, Aerospike has no index over (callback_token, timestamp_at,
// txid) here, so this implementation loads the token's submission list, does a
// point Get per txid, and sorts the survivors in memory — O(token cardinality)
// per connect, against tokens that can hold millions of submissions. That is
// the shape that OOM-killed the SSE pod (#237/#238). Past this bound, refusing
// is strictly better than trying: the caller degrades to an explicit gap and
// the client reconciles over REST, instead of the pod dying and taking every
// other connected client with it.
//
// The bound is enforced while the query result set is being drained, so peak
// memory is capped at maxTokenReplayScan submissions. Checking the length of an
// already-materialized list would be no protection at all — the allocation that
// causes the OOM happens before such a check could run.
const maxTokenReplayScan = 250_000

func (s *Store) IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error {
	subs, err := s.submissionsByTokenBounded(ctx, callbackToken, maxTokenReplayScan)
	if err != nil {
		return err
	}
	only := make(map[models.Status]struct{}, len(onlyStatuses))
	for _, st := range onlyStatuses {
		only[st] = struct{}{}
	}
	seen := make(map[string]struct{}, len(subs))
	// Projection only — per-key Get with just the status/timestamp/block
	// bins (never raw_tx or merkle_path) and no enrichMerklePath. Filter
	// BEFORE collecting so the sort buffer holds the matching set only.
	matched := make([]*models.TransactionStatus, 0, 64)
	for _, sub := range subs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, dup := seen[sub.TxID]; dup {
			continue
		}
		seen[sub.TxID] = struct{}{}
		key, kErr := s.key(setTransactions, sub.TxID)
		if kErr != nil {
			continue
		}
		rec, gErr := s.client.Get(s.readPolicy(ctx), key, "status", "timestamp", "block_hash", "block_height")
		if gErr != nil || rec == nil {
			continue
		}
		st := recordToStatus(rec, sub.TxID)
		if st == nil {
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

func (s *Store) GetSubmissionsByToken(ctx context.Context, token string) ([]*models.Submission, error) {
	return s.submissionsByTokenBounded(ctx, token, 0)
}

// submissionsByTokenBounded is GetSubmissionsByToken with an optional ceiling
// on how many submissions it will accumulate. A limit of 0 means unbounded.
//
// The bound is enforced while the result set is being drained, not after:
// checking a returned slice's length is no protection, because by then the
// memory has already been allocated and the OOM it was meant to prevent has
// already happened. Exceeding the limit abandons the query and returns
// store.ErrReplayUnavailable, so peak memory is capped at limit entries
// regardless of the token's real size.
func (s *Store) submissionsByTokenBounded(ctx context.Context, token string, limit int) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	_ = stmt.SetFilter(aero.NewEqualFilter("callback_token", token))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, err
	}

	var (
		subs    []*models.Submission
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			if limit > 0 && len(subs) >= limit {
				loopErr = fmt.Errorf("%w: token has more than %d submissions", store.ErrReplayUnavailable, limit)
				break loop
			}
			subs = append(subs, recordToSubmission(rec.Record))
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return subs, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	key, err := s.key(setSubmissions, submissionID)
	if err != nil {
		return err
	}
	// Bin names must be <=15 chars (Aerospike limit).
	bins := aero.BinMap{
		"last_status": string(lastStatus),
		"retry_count": retryCount,
	}
	if nextRetry != nil {
		bins["next_retry_at"] = nextRetry.UnixMilli()
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// RecordDeliveryAttempt implements store.Store. One Operate call: AddOp gives
// a server-side atomic increment for the lifetime attempts counter (no
// read-modify-write race across delivery workers); the puts overwrite the
// last-attempt bookkeeping. Bin names must be <=15 chars ("last_attempt_at"
// is exactly 15).
func (s *Store) RecordDeliveryAttempt(ctx context.Context, submissionID string, at time.Time, result string) error {
	key, err := s.key(setSubmissions, submissionID)
	if err != nil {
		return err
	}
	policy := s.writePolicy(ctx)
	policy.RecordExistsAction = aero.UPDATE_ONLY // never create a phantom submission
	_, err = s.client.Operate(
		policy, key,
		aero.AddOp(aero.NewBin("attempts", 1)),
		aero.PutOp(aero.NewBin("last_attempt_at", at.UnixMilli())),
		aero.PutOp(aero.NewBin("last_result", result)),
	)
	if isKeyNotFound(err) {
		return nil
	}
	return err
}

// UpdateDeliveryStatusCAS implements store.Store using a generation-match
// CAS write — the same pattern TryAcquireOrRenew uses for the lease record.
// Reading then writing with EXPECT_GEN_EQUAL guarantees we lose to any
// concurrent writer that mutated the row between our Get and Put.
func (s *Store) UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (bool, error) {
	key, err := s.key(setSubmissions, submissionID)
	if err != nil {
		return false, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil {
		if isKeyNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read submission for cas %s: %w", submissionID, err)
	}
	if rec == nil {
		return false, nil
	}
	if models.Status(getString(rec, "last_status")) != expected {
		return false, nil
	}
	bins := aero.BinMap{
		"last_status":   string(next),
		"retry_count":   0,
		"next_retry_at": int64(0),
	}
	policy := s.writePolicy(ctx)
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	if err := s.client.Put(policy, key, bins); err != nil {
		// Generation mismatch is the canonical "another writer won" outcome
		// and is treated as a normal claim-lost — the caller silently skips
		// its POST. A non-gen error (network blip, namespace unhealthy, etc.)
		// is also behaviorally safe to surface as claim-lost (the next event
		// or webhook-reaper tick retries), but it should NOT be invisible:
		// without a counter, a flat WebhookCASLostTotal in production could
		// hide a backend that's failing every CAS write. Emit a distinct
		// metric so the two are distinguishable on dashboards.
		if !isGenerationErr(err) {
			metrics.WebhookCASErrorTotal.Inc()
		}
		return false, nil
	}
	return true, nil
}

// ListSubmissionsReadyForRetry returns up to limit submissions whose POST
// previously failed (retry_count > 0) and whose next_retry_at has elapsed.
// There is no secondary index on submission retry bins (the in-retry working
// set is small enough that the cost of an index isn't justified), so this
// scans the set and filters in code — the same approach GetReadyRetries uses
// against the transactions set.
func (s *Store) ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query submissions ready for retry: %w", err)
	}

	nowMs := now.UnixMilli()
	type candidate struct {
		sub    *models.Submission
		nextMs int64
	}
	candidates := make([]candidate, 0, limit)
	var loopErr error
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			retry := getInt(rec.Record, "retry_count")
			nextMs := getInt64(rec.Record, "next_retry_at")
			if retry <= 0 || nextMs <= 0 || nextMs > nowMs {
				continue
			}
			sub := recordToSubmission(rec.Record)
			if sub == nil {
				continue
			}
			candidates = append(candidates, candidate{sub: sub, nextMs: nextMs})
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}

	// Sort by next_retry_at ASC so the oldest backlog drains first, then
	// truncate. Sorting after the scan avoids reading the whole set into a
	// heap; backlog is bounded by failed-callback cardinality.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].nextMs < candidates[j].nextMs
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]*models.Submission, len(candidates))
	for i, c := range candidates {
		out[i] = c.sub
	}
	return out, nil
}

// --- Block processing status ---
//
// Bins:
//   block_hash      string  (also primary key)
//   block_height    int
//   header_seen_at  int64 (unix nanos; 0 = unset)
//   processed_at    int64 (unix nanos; 0 = unset)
//   bump_built_at   int64 (unix nanos; 0 = unset)
//   status          string ("active" | "orphaned" | "parked")
//   orphaned_at     int64 (unix nanos; 0 = unset)
//   reconciled_at   int64 (unix nanos; 0 = unset)
//
// Writers must use Operate with per-bin PutOps so concurrent header /
// processed / bump-built paths only touch their own bins. A full-record
// Put would clobber whichever bins the other writer recently set.

const (
	binBlockHash    = "block_hash"
	binBlockHeight  = "block_height"
	binHeaderSeenAt = "header_seen_at"
	binProcessedAt  = "processed_at"
	binBUMPBuiltAt  = "bump_built_at"
	binStatus       = "status"
	binOrphanedAt   = "orphaned_at"
	binReconciledAt = "reconciled_at"
	// binExpectedSubtrees holds the retained expected-subtree set
	// (store.Store.AddBlockExpectedSubtrees) as an ordered unique list.
	// Bin names are capped at 15 bytes.
	binExpectedSubtrees = "exp_subtrees"
)

func (s *Store) UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	// On insert we want every bin populated (the row may be brand new). On
	// update we must NOT clobber processed_at or bump_built_at, which is
	// what per-bin Operate gives us — bins not named here are left alone.
	// We do overwrite block_height (chaintracks is authoritative) and reset
	// status/reconciled_at so a returning orphan re-joins active and a later
	// re-orphaning reconciles again (issue #279). orphaned_at is left as the
	// row's orphan-generation high-water mark (blockProcessingFromRecord
	// hides it on an active row); MarkBlocksOrphaned mints above it.
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin(binBlockHash, blockHash)),
		aero.PutOp(aero.NewBin(binBlockHeight, int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
		aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusActive))),
		aero.PutOp(aero.NewBin(binReconciledAt, nil)),
		// header_seen_at: only set when the bin is currently absent. Aerospike
		// has no client-side conditional bin write that's race-free, so we do
		// a generation-CAS read+write loop to set it on first observation
		// only.
	}
	if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
		return fmt.Errorf("upsert block header seen %s: %w", blockHash, err)
	}
	// Set header_seen_at only if absent. Generation CAS isn't needed because
	// repeat writes are idempotent (same bin name, same value would just be
	// re-written but not harmfully).
	rec, err := s.client.Get(s.readPolicy(ctx), key, binHeaderSeenAt)
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("read header_seen_at %s: %w", blockHash, err)
	}
	if rec == nil || getInt64(rec, binHeaderSeenAt) == 0 {
		setOp := aero.PutOp(aero.NewBin(binHeaderSeenAt, seenAt.UnixNano()))
		if _, err := s.client.Operate(s.writePolicy(ctx), key, setOp); err != nil {
			return fmt.Errorf("set header_seen_at %s: %w", blockHash, err)
		}
	}
	return nil
}

// AddBlockExpectedSubtrees implements store.Store with an ordered, unique
// list append, so the union is applied atomically on the server. UPDATE_ONLY
// keeps a missing row a no-op.
func (s *Store) AddBlockExpectedSubtrees(ctx context.Context, blockHash string, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	vals := make([]interface{}, len(indices))
	for i, v := range indices {
		vals[i] = v
	}
	policy := aero.NewListPolicy(aero.ListOrderOrdered, aero.ListWriteFlagsAddUnique|aero.ListWriteFlagsNoFail|aero.ListWriteFlagsPartial)
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.UPDATE_ONLY
	_, err = s.client.Operate(wp, key, aero.ListAppendWithPolicyOp(policy, binExpectedSubtrees, vals...))
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("add block expected subtrees %s: %w", blockHash, err)
	}
	return nil
}

// GetBlockExpectedSubtrees implements store.Store.
func (s *Store) GetBlockExpectedSubtrees(ctx context.Context, blockHash string) ([]int, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key, binExpectedSubtrees)
	if err != nil {
		if isKeyNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get block expected subtrees %s: %w", blockHash, err)
	}
	raw, _ := rec.Bins[binExpectedSubtrees].([]interface{})
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]int, 0, len(raw))
	for _, v := range raw {
		switch n := v.(type) {
		case int:
			out = append(out, n)
		case int64:
			out = append(out, int(n))
		}
	}
	return out, nil
}

// ClearBlockExpectedSubtrees implements store.Store.
func (s *Store) ClearBlockExpectedSubtrees(ctx context.Context, blockHash string) error {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.UPDATE_ONLY
	_, err = s.client.Operate(wp, key, aero.PutOp(aero.NewBin(binExpectedSubtrees, nil)))
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("clear block expected subtrees %s: %w", blockHash, err)
	}
	return nil
}

func (s *Store) MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error {
	return s.markBlockMilestone(ctx, blockHash, blockHeight, processedAt, binProcessedAt)
}

func (s *Store) MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error {
	return s.markBlockMilestone(ctx, blockHash, blockHeight, builtAt, binBUMPBuiltAt)
}

// ClearBlockProcessed drops the processed_at bin on an existing row (a nil
// bin value deletes the bin); a missing row is a no-op, never an upsert.
func (s *Store) ClearBlockProcessed(ctx context.Context, blockHash string) error {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.UPDATE_ONLY
	_, err = s.client.Operate(wp, key, aero.PutOp(aero.NewBin(binProcessedAt, nil)))
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("clear block processed %s: %w", blockHash, err)
	}
	return nil
}

// markBlockMilestone is the shared upsert path for processed_at and
// bump_built_at. When the row exists, only the milestone bin is written
// (block_height and other bins are preserved). When it doesn't, the row
// is created with header_seen_at synthesized to the milestone time so
// observability still has a record. The synthesized header_seen_at will
// be preserved by a later UpsertBlockHeaderSeen.
func (s *Store) markBlockMilestone(ctx context.Context, blockHash string, blockHeight uint64, at time.Time, milestoneBin string) error {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key, binBlockHash)
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("read block_processing %s: %w", blockHash, err)
	}
	if rec == nil {
		// New row: write all bins so block_height and header_seen_at land.
		ops := []*aero.Operation{
			aero.PutOp(aero.NewBin(binBlockHash, blockHash)),
			aero.PutOp(aero.NewBin(binBlockHeight, int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
			aero.PutOp(aero.NewBin(binHeaderSeenAt, at.UnixNano())),
			aero.PutOp(aero.NewBin(milestoneBin, at.UnixNano())),
			aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusActive))),
		}
		if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
			return fmt.Errorf("create block_processing %s: %w", blockHash, err)
		}
		return nil
	}
	// Existing row: touch only the milestone bin.
	op := aero.PutOp(aero.NewBin(milestoneBin, at.UnixNano()))
	if _, err := s.client.Operate(s.writePolicy(ctx), key, op); err != nil {
		return fmt.Errorf("update %s on block_processing %s: %w", milestoneBin, blockHash, err)
	}
	return nil
}

// casAttempts bounds the read-then-EXPECT_GEN_EQUAL-write loops the
// block_processing status writes use. A block row has a handful of writers
// at most (tracker, tie-scan, anchor guard, reconciler), so the loop settles
// in one or two rounds; the cap only stops a pathological hot spot from
// spinning.
const casAttempts = 8

func (s *Store) MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) (int, error) {
	if len(blockHashes) == 0 {
		return 0, nil
	}
	transitions := 0
	for _, h := range blockHashes {
		// Re-checked per row: the reconciler's lease heartbeat cancels the
		// context mid-batch when the lease is lost, and every row written
		// after that overlaps the new holder's pass.
		if err := ctx.Err(); err != nil {
			return transitions, err
		}
		transitioned, err := s.markBlockOrphanedCAS(ctx, h, orphanedAt)
		if err != nil {
			return transitions, err
		}
		if transitioned {
			transitions++
		}
	}
	return transitions, nil
}

// markBlockOrphanedCAS orphans one row and reports whether that changed its
// status. The pre-read's status is only trustworthy if the write that follows
// is conditioned on the record not having moved in between: with an
// unconditional Operate, two replicas orphaning the same row could both read
// 'active' and both report a transition although only one write changed it.
// So the write carries EXPECT_GEN_EQUAL on the generation the read saw, and a
// generation mismatch re-reads and retries — the loser of the race then reads
// 'orphaned' and reports no transition. A hash with no record is skipped
// (chaintracks may emit OrphanedHashes for blocks observed before this
// service started recording), as is a record deleted between read and write.
//
// On a row that is already orphaned the write is a generation refresh, and
// it is forward-only-inclusive: a stored orphan generation NEWER than this
// call's is kept, stamp state and all — the record CAS stops lost writes,
// not a delayed call carrying an older timestamp, which would otherwise move
// the generation backwards and let a reconciler holding that older token
// pass its CAS. An EQUAL generation still refreshes (clears the stamp): the
// contract does not require a strictly increasing time, and a re-orphaning
// that reuses the timestamp must still requeue the row.
func (s *Store) markBlockOrphanedCAS(ctx context.Context, blockHash string, orphanedAt time.Time) (bool, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return false, err
	}
	gen := orphanedAt.UnixNano()
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, err := s.client.Get(s.readPolicy(ctx), key, binStatus, binOrphanedAt)
		if err != nil {
			if isKeyNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("read block_processing %s: %w", blockHash, err)
		}
		if rec == nil {
			return false, nil
		}
		wasOrphaned := getString(rec, binStatus) == string(models.BlockStatusOrphaned)
		prev := getInt64(rec, binOrphanedAt) // the high-water mark, whatever the status
		if wasOrphaned && prev > gen {
			return false, nil // a newer generation is in place; nothing to write
		}
		// Mint the generation strictly above any this row has had, with the
		// caller's timestamp as the lower bound: two orphanings never share
		// one, so a token read before this write cannot pass the CAS after
		// it. Computed per attempt, from the record the CAS is pinned to.
		minted := gen
		if prev >= gen {
			minted = prev + 1
		}
		ops := []*aero.Operation{
			aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusOrphaned))),
			aero.PutOp(aero.NewBin(binOrphanedAt, minted)),
			// Clear the previous generation's stamp so the row re-enters the
			// reconciler queue for this orphaning.
			aero.PutOp(aero.NewBin(binReconciledAt, nil)),
		}
		policy := s.writePolicy(ctx)
		policy.RecordExistsAction = aero.UPDATE_ONLY
		policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		policy.Generation = rec.Generation
		_, opErr := s.client.Operate(policy, key, ops...)
		switch {
		case opErr == nil:
			return !wasOrphaned, nil
		case isKeyNotFound(opErr):
			return false, nil // deleted between read and write
		case isGenerationErr(opErr):
			continue // another writer moved the row: re-read, re-judge, retry
		default:
			return false, fmt.Errorf("mark orphaned %s: %w", blockHash, opErr)
		}
	}
	return false, fmt.Errorf("mark orphaned %s: record kept changing across %d CAS attempts", blockHash, casAttempts)
}

// MarkBlockReconciled stamps reconciled_at on an orphaned block's row — the
// anchor reconciler finished re-anchoring/reverting its transactions — as a
// compare-and-set on the orphan generation: the row must still be orphaned
// with the orphaned_at the caller observed (a zero orphanedAt checks status
// only). Missing, resurrected and re-orphaned rows are silently skipped and
// reported as not stamped (issue #339). Read-then-write with
// EXPECT_GEN_EQUAL (the UpdateDeliveryStatusCAS pattern) so a concurrent
// writer that moves the row between our Get and Operate wins; UPDATE_ONLY
// never creates a phantom block row.
func (s *Store) MarkBlockReconciled(ctx context.Context, blockHash string, orphanedAt, at time.Time) (bool, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return false, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key, binStatus, binOrphanedAt)
	if err != nil {
		if isKeyNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read block for reconcile cas %s: %w", blockHash, err)
	}
	if rec == nil || getString(rec, binStatus) != string(models.BlockStatusOrphaned) {
		return false, nil
	}
	if !orphanedAt.IsZero() && getInt64(rec, binOrphanedAt) != orphanedAt.UnixNano() {
		return false, nil
	}
	policy := s.writePolicy(ctx)
	policy.RecordExistsAction = aero.UPDATE_ONLY
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	_, opErr := s.client.Operate(policy, key,
		aero.PutOp(aero.NewBin(binReconciledAt, at.UnixNano())))
	if opErr != nil {
		if isKeyNotFound(opErr) || isGenerationErr(opErr) {
			return false, nil // the row moved on between read and write: that writer's generation wins
		}
		return false, fmt.Errorf("mark block reconciled %s: %w", blockHash, opErr)
	}
	return true, nil
}

// RequeueOrphanedBlock clears reconciled_at on a row that is still orphaned
// with the generation the caller judged (a zero orphanedAt checks status
// only), putting it back on the reconciler's queue without changing the
// generation. Missing, active, parked and re-orphaned rows are left
// untouched and reported as not applied; it never transitions a row.
// Read-then-EXPECT_GEN_EQUAL with a bounded re-read on a record-generation
// conflict, like ReactivateBlock; UPDATE_ONLY never creates a phantom row.
func (s *Store) RequeueOrphanedBlock(ctx context.Context, blockHash string, orphanedAt time.Time) (bool, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return false, err
	}
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, err := s.client.Get(s.readPolicy(ctx), key, binStatus, binOrphanedAt, binReconciledAt)
		if err != nil {
			if isKeyNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("read block for requeue cas %s: %w", blockHash, err)
		}
		if rec == nil || getString(rec, binStatus) != string(models.BlockStatusOrphaned) {
			return false, nil
		}
		if !orphanedAt.IsZero() && getInt64(rec, binOrphanedAt) != orphanedAt.UnixNano() {
			return false, nil
		}
		if getInt64(rec, binReconciledAt) == 0 {
			return true, nil // already queued; nothing to write
		}
		policy := s.writePolicy(ctx)
		policy.RecordExistsAction = aero.UPDATE_ONLY
		policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		policy.Generation = rec.Generation
		_, opErr := s.client.Operate(policy, key, aero.PutOp(aero.NewBin(binReconciledAt, nil)))
		switch {
		case opErr == nil:
			return true, nil
		case isKeyNotFound(opErr):
			return false, nil
		case isGenerationErr(opErr):
			continue
		default:
			return false, fmt.Errorf("requeue orphaned block %s: %w", blockHash, opErr)
		}
	}
	return false, fmt.Errorf("requeue orphaned block %s: record kept changing across %d CAS attempts", blockHash, casAttempts)
}

// ReactivateBlock returns an orphaned row to active as a compare-and-set on
// the orphan generation (issue #339): the row must still be orphaned with the
// orphaned_at the caller judged (a zero orphanedAt checks status only).
// Read-then-EXPECT_GEN_EQUAL like MarkBlockReconciled, but a generation
// mismatch re-reads and re-judges rather than giving up: a concurrent
// milestone write (processed_at, bump_built_at) bumps the generation without
// touching status or orphaned_at and must not turn a valid reactivation into
// a spurious "not applied". A row that fails the check on re-read —
// resurrected, parked, re-orphaned or gone — is left untouched and reported
// as not applied; UPDATE_ONLY never creates a phantom row.
func (s *Store) ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return false, err
	}
	// orphaned_at is kept as the high-water mark the next orphaning mints
	// above; blockProcessingFromRecord hides it on an active row.
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin(binBlockHeight, int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
		aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusActive))),
		aero.PutOp(aero.NewBin(binReconciledAt, nil)),
	}
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, err := s.client.Get(s.readPolicy(ctx), key, binStatus, binOrphanedAt)
		if err != nil {
			if isKeyNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("read block for reactivate cas %s: %w", blockHash, err)
		}
		if rec == nil || getString(rec, binStatus) != string(models.BlockStatusOrphaned) {
			return false, nil
		}
		if !orphanedAt.IsZero() && getInt64(rec, binOrphanedAt) != orphanedAt.UnixNano() {
			return false, nil
		}
		policy := s.writePolicy(ctx)
		policy.RecordExistsAction = aero.UPDATE_ONLY
		policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		policy.Generation = rec.Generation
		_, opErr := s.client.Operate(policy, key, ops...)
		switch {
		case opErr == nil:
			return true, nil
		case isKeyNotFound(opErr):
			return false, nil
		case isGenerationErr(opErr):
			continue
		default:
			return false, fmt.Errorf("reactivate block %s: %w", blockHash, opErr)
		}
	}
	return false, fmt.Errorf("reactivate block %s: record kept changing across %d CAS attempts", blockHash, casAttempts)
}

// ListOrphanedBlocksToReconcile narrows by status='orphaned' via the secondary
// index, then in-memory filters to rows with no reconciled_at — the anchor
// reconciler's durable work queue. Rows that can be re-anchored RIGHT NOW —
// the active (canonical) block at the same height already has a stored BUMP —
// sort ahead of rows that cannot, with orphaned_at ascending WITHIN each group,
// before truncating to limit. This mirrors the Postgres store: the reconciler
// consumes a small LIMIT per tick, so a large backlog of orphans whose
// canonical block has no stored BUMP (only parkable, never re-anchorable now)
// must not starve a genuinely re-anchorable recent incident behind them.
//
// The re-anchorable signal mirrors reconcileBlock, which resolves the canonical
// block at the orphan's height and re-anchors from that block's stored compound
// BUMP; the active row at a height is the store's record of that canonical
// block. Checking specifically the ACTIVE block's BUMP (not any BUMP at the
// height) matters: an orphan retains its own compound BUMP, which must not
// count. Cardinality is bounded by unreconciled orphans (near-zero in steady
// state); the re-anchorable probe is memoized per distinct height.
func (s *Store) ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	_ = stmt.SetFilter(aero.NewEqualFilter(binStatus, string(models.BlockStatusOrphaned)))
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query orphaned block_processing: %w", err)
	}
	var (
		all     []*models.BlockProcessingStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			// Already reconciled rows are done — not work-queue members.
			if getInt64(rec.Record, binReconciledAt) != 0 {
				continue
			}
			all = append(all, blockProcessingFromRecord(rec.Record))
		}
	}
	if loopErr != nil {
		return all, loopErr
	}

	// Memoize the re-anchorable probe per distinct height: a backlog is mostly
	// same-height competitions, so distinct heights are far fewer than rows.
	reanchorableAt := make(map[uint64]bool)
	for _, row := range all {
		if _, done := reanchorableAt[row.BlockHeight]; done {
			continue
		}
		ok, hErr := s.heightHasActiveBUMP(ctx, row.BlockHeight)
		if hErr != nil {
			return all, hErr
		}
		reanchorableAt[row.BlockHeight] = ok
	}

	sortReanchorableThenOrphanedAtAsc(all, reanchorableAt)
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// heightHasActiveBUMP reports whether the active (canonical) block at height
// has a stored compound BUMP — the store-expressible proxy for "reconcileBlock
// can re-anchor this orphan's txs right now". It queries the block_processing
// rows at the height via the numeric secondary index and returns true on the
// first active row whose block hash has a BUMP manifest.
func (s *Store) heightHasActiveBUMP(ctx context.Context, height uint64) (bool, error) {
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	_ = stmt.SetFilter(aero.NewRangeFilter(binBlockHeight, int64(height), int64(height))) //nolint:gosec // block height fits in int64
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return false, fmt.Errorf("query block_processing at height %d: %w", height, err)
	}
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case rec, ok := <-rs.Results():
			if !ok {
				return false, nil
			}
			if rec.Err != nil {
				continue
			}
			if getString(rec.Record, binStatus) != string(models.BlockStatusActive) {
				continue
			}
			if has, hErr := s.hasBUMP(ctx, getString(rec.Record, binBlockHash)); hErr != nil {
				return false, hErr
			} else if has {
				return true, nil
			}
		}
	}
}

// hasBUMP reports whether a compound BUMP manifest is stored for blockHash. It
// reads only the manifest's presence (one bin) — the ordering signal mirrors
// the Postgres EXISTS(... JOIN bumps ...) check, which is presence, not content.
func (s *Store) hasBUMP(ctx context.Context, blockHash string) (bool, error) {
	key, err := s.key(setBumps, blockHash)
	if err != nil {
		return false, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key, "chunk_count")
	if err != nil {
		if isKeyNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("probe bump manifest %s: %w", blockHash, err)
	}
	return rec != nil, nil
}

// sortReanchorableThenOrphanedAtAsc orders rows re-anchorable-first (per the
// reanchorableAt map keyed by height), then oldest orphaned_at within each
// group, matching the Postgres ORDER BY. A nil orphaned_at sorts last.
func sortReanchorableThenOrphanedAtAsc(rows []*models.BlockProcessingStatus, reanchorableAt map[uint64]bool) {
	before := func(a, b *models.BlockProcessingStatus) bool {
		ra, rb := reanchorableAt[a.BlockHeight], reanchorableAt[b.BlockHeight]
		if ra != rb {
			return ra // re-anchorable first
		}
		switch {
		case a.OrphanedAt == nil:
			return false
		case b.OrphanedAt == nil:
			return true
		default:
			return a.OrphanedAt.Before(*b.OrphanedAt)
		}
	}
	// Insertion sort matches the other block_processing sorts — the orphan
	// backlog is at most a handful of rows.
	for i := 1; i < len(rows); i++ {
		j := i
		for j > 0 && before(rows[j], rows[j-1]) {
			rows[j-1], rows[j] = rows[j], rows[j-1]
			j--
		}
	}
}

func (s *Store) MarkBlocksParked(ctx context.Context, blockHashes []string) error {
	if len(blockHashes) == 0 {
		return nil
	}
	for _, h := range blockHashes {
		key, err := s.key(setBlockProcessing, h)
		if err != nil {
			return err
		}
		rec, err := s.client.Get(s.readPolicy(ctx), key, binStatus)
		if err != nil {
			if isKeyNotFound(err) {
				continue
			}
			return fmt.Errorf("read block_processing %s: %w", h, err)
		}
		// Only 'active' rows park: missing rows are skipped, and an orphaned
		// row is off-chain (a more specific terminal state) that must not be
		// relabeled as a missing-BUMP backlog.
		if rec == nil {
			continue
		}
		if status, _ := rec.Bins[binStatus].(string); status != string(models.BlockStatusActive) {
			continue
		}
		if _, err := s.client.Operate(s.writePolicy(ctx), key,
			aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusParked)))); err != nil {
			return fmt.Errorf("mark parked %s: %w", h, err)
		}
	}
	return nil
}

func (s *Store) GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil {
		if isKeyNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get block_processing %s: %w", blockHash, err)
	}
	return blockProcessingFromRecord(rec), nil
}

func (s *Store) ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	if beforeHeight > 0 {
		// Aerospike's NUMERIC range filter is inclusive on the upper bound,
		// so subtract one to express "block_height < beforeHeight".
		_ = stmt.SetFilter(aero.NewRangeFilter(binBlockHeight, 0, int64(beforeHeight-1))) //nolint:gosec // height fits in int64
	}
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query block_processing: %w", err)
	}
	var (
		all     []*models.BlockProcessingStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			all = append(all, blockProcessingFromRecord(rec.Record))
		}
	}
	if loopErr != nil {
		return all, loopErr
	}
	// Aerospike secondary-index queries don't sort. Sort in memory by
	// height descending and truncate. Cardinality is ~144 rows/day, so
	// even unbounded scans here cost milliseconds.
	sortByHeightDesc(all)
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func blockProcessingFromRecord(rec *aero.Record) *models.BlockProcessingStatus {
	if rec == nil {
		return nil
	}
	bp := &models.BlockProcessingStatus{
		BlockHash:   getString(rec, binBlockHash),
		BlockHeight: uint64(getInt(rec, binBlockHeight)), //nolint:gosec // height non-negative
		Status:      models.BlockProcessingStatusValue(getString(rec, binStatus)),
	}
	if v := getInt64(rec, binHeaderSeenAt); v != 0 {
		bp.HeaderSeenAt = time.Unix(0, v).UTC()
	}
	if v := getInt64(rec, binProcessedAt); v != 0 {
		t := time.Unix(0, v).UTC()
		bp.ProcessedAt = &t
	}
	if v := getInt64(rec, binBUMPBuiltAt); v != 0 {
		t := time.Unix(0, v).UTC()
		bp.BUMPBuiltAt = &t
	}
	// orphaned_at survives a reactivation as the generation high-water mark;
	// it is current — and surfaced — only while the row is orphaned.
	if v := getInt64(rec, binOrphanedAt); v != 0 && bp.Status == models.BlockStatusOrphaned {
		t := time.Unix(0, v).UTC()
		bp.OrphanedAt = &t
	}
	if v := getInt64(rec, binReconciledAt); v != 0 {
		t := time.Unix(0, v).UTC()
		bp.ReconciledAt = &t
	}
	return bp
}

// GetActiveTipBlockHeight scans every block_processing row filtered to
// status='active' via the secondary index and returns the highest height.
// Aerospike has no MAX aggregation, so we walk the matching rows. The set
// holds ~one row per BSV block (~144/day) so even unbounded retention is
// cheap.
func (s *Store) GetActiveTipBlockHeight(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	_ = stmt.SetFilter(aero.NewEqualFilter(binStatus, string(models.BlockStatusActive)))
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return 0, fmt.Errorf("query active tip: %w", err)
	}
	var tip uint64
	for {
		select {
		case <-ctx.Done():
			return tip, ctx.Err()
		case rec, ok := <-rs.Results():
			if !ok {
				return tip, nil
			}
			if rec.Err != nil {
				continue
			}
			if h := uint64(getInt(rec.Record, binBlockHeight)); h > tip { //nolint:gosec // height non-negative
				tip = h
			}
		}
	}
}

// ListStaleBlockProcessingStatus narrows by status='active' via the secondary
// index, then in-memory filters on processed_at (absent / zero) +
// header_seen_at + block_height, sorts by header_seen_at ascending, and
// truncates to limit. Cardinality bound is the count of active rows which
// matches the recency window in the common case (the watchdog never asks
// for ancient blocks).
func (s *Store) ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	threshold := olderThan.UnixNano()
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	_ = stmt.SetFilter(aero.NewEqualFilter(binStatus, string(models.BlockStatusActive)))
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query stale block_processing: %w", err)
	}
	var (
		all     []*models.BlockProcessingStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			// Filter in-memory. processed_at absent or zero counts as "not yet";
			// header_seen_at < threshold; block_height >= minHeight.
			if v := getInt64(rec.Record, binProcessedAt); v != 0 {
				continue
			}
			seen := getInt64(rec.Record, binHeaderSeenAt)
			if seen == 0 || seen >= threshold {
				continue
			}
			h := uint64(getInt(rec.Record, binBlockHeight)) //nolint:gosec // height non-negative
			if h < minHeight {
				continue
			}
			all = append(all, blockProcessingFromRecord(rec.Record))
		}
	}
	if loopErr != nil {
		return all, loopErr
	}
	sortByHeaderSeenAsc(all)
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func sortByHeaderSeenAsc(rows []*models.BlockProcessingStatus) {
	for i := 1; i < len(rows); i++ {
		j := i
		for j > 0 && rows[j-1].HeaderSeenAt.After(rows[j].HeaderSeenAt) {
			rows[j-1], rows[j] = rows[j], rows[j-1]
			j--
		}
	}
}

func sortByHeightDesc(rows []*models.BlockProcessingStatus) {
	// Insertion sort is fine — the result set is at most a couple hundred rows.
	for i := 1; i < len(rows); i++ {
		j := i
		for j > 0 && rows[j-1].BlockHeight < rows[j].BlockHeight {
			rows[j-1], rows[j] = rows[j], rows[j-1]
			j--
		}
	}
}

// --- Datahub endpoint registry ---

// UpsertDatahubEndpoint writes the endpoint with the URL as primary key.
// Existing rows are overwritten so LastSeen advances and a flap from
// "discovered" to "configured" (or vice versa) is reflected.
func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if ep.URL == "" {
		return fmt.Errorf("upsert datahub endpoint: empty url")
	}
	key, err := s.key(setDatahubEndpoints, ep.URL)
	if err != nil {
		return err
	}
	bins := aero.BinMap{
		"url":       ep.URL,
		"network":   ep.Network,
		"source":    ep.Source,
		"last_seen": ep.LastSeen.UnixMilli(),
	}

	// The advertised-policy bins are written only when the caller has one. A
	// Put updates just the bins it names, so omitting them is exactly the
	// "a write carrying no policy must not erase one already recorded"
	// rule the other backends implement with COALESCE (Postgres) and a
	// read-modify-write (Pebble). Bin names are abbreviated to stay inside
	// Aerospike's limit, matching fee_sats/fee_bytes on the peer-policy set.
	if p := ep.Policy; p != nil {
		if err := p.Validate(); err != nil {
			return err
		}
		// Every narrowing is bounded by Validate above.
		bins["fee_sats"] = int64(p.MiningFeeSatoshis)         //nolint:gosec // bounded by EndpointPolicy.Validate
		bins["fee_bytes"] = int64(p.MiningFeeBytes)           //nolint:gosec // bounded by EndpointPolicy.Validate
		bins["max_tx_sz"] = int64(p.MaxTxSizePolicy)          //nolint:gosec // bounded by EndpointPolicy.Validate
		bins["max_scr_sz"] = int64(p.MaxScriptSizePolicy)     //nolint:gosec // bounded by EndpointPolicy.Validate
		bins["max_sigops"] = int64(p.MaxTxSigopsCountsPolicy) //nolint:gosec // bounded by EndpointPolicy.Validate
		bins["has_policy"] = 1
	}

	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// ListDatahubEndpoints scans every row in the registry, filtering to entries
// scoped to the given network. Cardinality is expected in the dozens to low
// hundreds — no pagination needed. Records written before the network bin
// existed return an empty Network and are filtered out.
func (s *Store) ListDatahubEndpoints(ctx context.Context, network string) ([]store.DatahubEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setDatahubEndpoints)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query datahub endpoints: %w", err)
	}

	var (
		out     []store.DatahubEndpoint
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			ep := store.DatahubEndpoint{
				URL:     getString(rec.Record, "url"),
				Network: getString(rec.Record, "network"),
				Source:  getString(rec.Record, "source"),
			}
			// has_policy distinguishes "advertised none" from "advertised all
			// zeros": getInt64 returns 0 for a missing bin, so without the flag
			// a legacy record and a genuinely zero policy would look alike.
			if getInt64(rec.Record, "has_policy") == 1 {
				ep.Policy = &store.EndpointPolicy{
					MiningFeeSatoshis:       uint64(getInt64(rec.Record, "fee_sats")),   //nolint:gosec // stored non-negative
					MiningFeeBytes:          uint64(getInt64(rec.Record, "fee_bytes")),  //nolint:gosec // stored non-negative
					MaxTxSizePolicy:         uint64(getInt64(rec.Record, "max_tx_sz")),  //nolint:gosec // stored non-negative
					MaxScriptSizePolicy:     uint64(getInt64(rec.Record, "max_scr_sz")), //nolint:gosec // stored non-negative
					MaxTxSigopsCountsPolicy: uint64(getInt64(rec.Record, "max_sigops")), //nolint:gosec // stored non-negative
				}
			}
			if ms := getInt64(rec.Record, "last_seen"); ms > 0 {
				ep.LastSeen = time.UnixMilli(ms)
			}
			if ep.URL != "" && ep.Network == network {
				out = append(out, ep)
			}
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return out, nil
}

func (s *Store) UpsertPeerPolicy(ctx context.Context, pp store.PeerPolicy) error {
	if err := pp.Validate(); err != nil {
		return err
	}
	key, err := s.key(setPeerPolicies, pp.PeerID)
	if err != nil {
		return err
	}
	bins := aero.BinMap{
		"peer_id": pp.PeerID,
		"network": pp.Network,
		// Every narrowing here is bounded by PeerPolicy.Validate above; without
		// it a peer-supplied value over MaxInt64 would wrap negative into the
		// bin and read back as an astronomical fee or size limit.
		"fee_sats":   int64(pp.MiningFeeSatoshis),   //nolint:gosec // bounded by PeerPolicy.Validate
		"fee_bytes":  int64(pp.MiningFeeBytes),      //nolint:gosec // bounded by PeerPolicy.Validate
		"max_tx_sz":  int64(pp.MaxTxSizePolicy),     //nolint:gosec // bounded by PeerPolicy.Validate
		"max_scr_sz": int64(pp.MaxScriptSizePolicy), //nolint:gosec // bounded by PeerPolicy.Validate
		"last_seen":  pp.LastSeen.UnixMilli(),
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// ListPeerPolicies scans every recorded peer policy, filtering to entries
// scoped to the given network. Cardinality tracks the peer count (dozens to
// low hundreds) — no pagination needed.
func (s *Store) ListPeerPolicies(ctx context.Context, network string) ([]store.PeerPolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setPeerPolicies)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query peer policies: %w", err)
	}

	var (
		out     []store.PeerPolicy
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			// max_tx_sz / max_scr_sz are absent on records written before the
			// size limits were observed; getInt64 returns 0 for a missing bin,
			// which is the "peer did not advertise" sentinel, so old records
			// need no migration.
			pp := store.PeerPolicy{
				PeerID:              getString(rec.Record, "peer_id"),
				Network:             getString(rec.Record, "network"),
				MiningFeeSatoshis:   uint64(getInt64(rec.Record, "fee_sats")),   //nolint:gosec // stored non-negative
				MiningFeeBytes:      uint64(getInt64(rec.Record, "fee_bytes")),  //nolint:gosec // stored non-negative
				MaxTxSizePolicy:     uint64(getInt64(rec.Record, "max_tx_sz")),  //nolint:gosec // stored non-negative
				MaxScriptSizePolicy: uint64(getInt64(rec.Record, "max_scr_sz")), //nolint:gosec // stored non-negative
			}
			if ms := getInt64(rec.Record, "last_seen"); ms > 0 {
				pp.LastSeen = time.UnixMilli(ms)
			}
			if pp.PeerID != "" && pp.Network == network {
				out = append(out, pp)
			}
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return out, nil
}

// --- Helpers ---

func recordToStatus(rec *aero.Record, txid string) *models.TransactionStatus {
	status := &models.TransactionStatus{TxID: txid}
	if v, ok := rec.Bins["status"]; ok {
		if s, ok := v.(string); ok {
			status.Status = models.Status(s)
		}
	}
	if v, ok := rec.Bins["status_code"]; ok {
		if n, ok := v.(int); ok {
			status.StatusCode = n
		}
	}
	if v, ok := rec.Bins["block_hash"]; ok {
		if s, ok := v.(string); ok {
			status.BlockHash = s
		}
	}
	if v, ok := rec.Bins["block_height"]; ok {
		if n, ok := v.(int); ok {
			status.BlockHeight = uint64(n) //nolint:gosec // round-trip of a value we wrote as int from a uint64 fitting block height
		}
	}
	if v, ok := rec.Bins["extra_info"]; ok {
		if s, ok := v.(string); ok {
			status.ExtraInfo = s
		}
	}
	if v, ok := rec.Bins["merkle_path"]; ok {
		if b, ok := v.([]byte); ok {
			status.MerklePath = b
		}
	}
	if v, ok := rec.Bins["competing_txs"]; ok {
		switch ct := v.(type) {
		case []byte:
			_ = json.Unmarshal(ct, &status.CompetingTxs)
		case string:
			_ = json.Unmarshal([]byte(ct), &status.CompetingTxs)
		}
	}
	if v, ok := rec.Bins["timestamp"]; ok {
		if ms, ok := v.(int); ok {
			status.Timestamp = time.UnixMilli(int64(ms))
		}
	}
	if v, ok := rec.Bins["created_at"]; ok {
		if ms, ok := v.(int); ok {
			status.CreatedAt = time.UnixMilli(int64(ms))
		}
	}
	if v, ok := rec.Bins["merkle_reg_at"]; ok {
		if ms, ok := v.(int); ok {
			status.MerkleRegisteredAt = time.UnixMilli(int64(ms))
		}
	}
	// The durable-retry bookkeeping, surfaced on reads as the other backends
	// do. A cleared bin is absent, so NextRetryAt stays zero.
	status.RetryCount = int(getInt64(rec, "retry_count"))
	if ms := getInt64(rec, "next_retry_at"); ms > 0 {
		status.NextRetryAt = time.UnixMilli(ms)
	}
	status.PartitionKey = getString(rec, "partition_key")
	for _, a := range orphanedAnchorsFromRecord(rec) {
		status.OrphanedProofs = append(status.OrphanedProofs, models.OrphanedAnchor{
			BlockHash:   a.BlockHash,
			BlockHeight: a.BlockHeight,
			OrphanedAt:  time.UnixMilli(a.OrphanedAtMs).UTC(),
		})
	}
	return status
}

// EnrichMerklePath implements store.Store: it populates status.MerklePath in
// place for a mined/immutable status carrying a BlockHash, extracting the tx's
// minimal path from the block's compound BUMP. Best-effort — see the interface
// doc. Delegates to the same enrichMerklePath used by GetStatus.
func (s *Store) EnrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	s.enrichMerklePath(ctx, status)
}

// enrichMerklePath fetches the compound BUMP for a mined/immutable transaction
// and extracts the per-tx minimal merkle path if not already present, reusing
// the block's cached, indexed compound.
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

func recordToSubmission(rec *aero.Record) *models.Submission {
	sub := &models.Submission{}
	if v, ok := rec.Bins["submission_id"]; ok {
		if s, ok := v.(string); ok {
			sub.SubmissionID = s
		}
	}
	if v, ok := rec.Bins["txid"]; ok {
		if s, ok := v.(string); ok {
			sub.TxID = s
		}
	}
	if v, ok := rec.Bins["callback_url"]; ok {
		if s, ok := v.(string); ok {
			sub.CallbackURL = s
		}
	}
	if v, ok := rec.Bins["callback_token"]; ok {
		if s, ok := v.(string); ok {
			sub.CallbackToken = s
		}
	}
	if v, ok := rec.Bins["full_updates"]; ok {
		if b, ok := v.(bool); ok {
			sub.FullStatusUpdates = b
		}
	}
	if v, ok := rec.Bins["created_at"]; ok {
		if ms, ok := v.(int); ok {
			sub.CreatedAt = time.UnixMilli(int64(ms))
		}
	}
	if v, ok := rec.Bins["retry_count"]; ok {
		if n, ok := v.(int); ok {
			sub.RetryCount = n
		}
	}
	// last_status is written by UpdateDeliveryStatus / UpdateDeliveryStatusCAS.
	// Reading it back is essential for the webhook dedup path — without it
	// the in-memory `sub.LastDeliveredStatus` would always be the zero value
	// and the CAS expected-state predicate would never match after the first
	// delivery.
	if v, ok := rec.Bins["last_status"]; ok {
		if s, ok := v.(string); ok {
			sub.LastDeliveredStatus = models.Status(s)
		}
	}
	if v, ok := rec.Bins["next_retry_at"]; ok {
		if ms, ok := v.(int); ok {
			t := time.UnixMilli(int64(ms))
			sub.NextRetryAt = &t
		}
	}
	if v, ok := rec.Bins["attempts"]; ok {
		if n, ok := v.(int); ok {
			sub.Attempts = n
		}
	}
	if v, ok := rec.Bins["last_attempt_at"]; ok {
		if ms, ok := v.(int); ok {
			t := time.UnixMilli(int64(ms))
			sub.LastAttemptAt = &t
		}
	}
	if v, ok := rec.Bins["last_result"]; ok {
		if s, ok := v.(string); ok {
			sub.LastResult = s
		}
	}
	return sub
}

// --- Lease Operations ---

// TryAcquireOrRenew implements store.Leaser. Uses Aerospike generation-match
// CAS to serialize acquire / renew across concurrent writers, with the record's
// native TTL as the authoritative expiration (no client clock dependency). The
// expires_at bin is a belt-and-braces hint for the narrow window between TTL
// lapse and next client scan.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	key, err := s.key(setLeases, name)
	if err != nil {
		return time.Time{}, err
	}
	now := time.Now()
	expiresAt := now.Add(ttl)

	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return time.Time{}, fmt.Errorf("read lease %s: %w", name, err)
	}

	bins := aero.BinMap{
		"holder":     holder,
		"expires_at": expiresAt.UnixMilli(),
	}
	ttlSecs := uint32(ttl.Seconds())

	if rec == nil {
		// No lease record yet — try to create it. CREATE_ONLY fails if a
		// concurrent writer creates the row between our Get and Put; treat
		// that as a benign contention signal.
		policy := s.writePolicy(ctx)
		policy.Expiration = ttlSecs
		policy.RecordExistsAction = aero.CREATE_ONLY
		if err := s.client.Put(policy, key, bins); err != nil {
			return time.Time{}, nil
		}
		return expiresAt, nil
	}

	currentHolder := getString(rec, "holder")
	currentExpires := getInt64(rec, "expires_at")
	canTake := currentHolder == holder || currentExpires <= now.UnixMilli()
	if !canTake {
		return time.Time{}, nil
	}

	// CAS-write: succeeds only if the record hasn't changed since our read.
	// A gen mismatch means another pod won the race; not an error.
	policy := s.writePolicy(ctx)
	policy.Expiration = ttlSecs
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	if err := s.client.Put(policy, key, bins); err != nil {
		return time.Time{}, nil
	}
	return expiresAt, nil
}

// Release deletes a lease record if it's still held by this caller. Gen-match
// prevents us from stepping on a successor who has already acquired it after
// our TTL lapsed. Swallows benign races (not held / raced to delete) as nil.
func (s *Store) Release(ctx context.Context, name, holder string) error {
	key, err := s.key(setLeases, name)
	if err != nil {
		return err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil {
		if isKeyNotFound(err) {
			return nil
		}
		return fmt.Errorf("read lease for release %s: %w", name, err)
	}
	if getString(rec, "holder") != holder {
		return nil
	}
	policy := s.writePolicy(ctx)
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	if _, err := s.client.Delete(policy, key); err != nil {
		// Gen mismatch = successor already took over. Not an error.
		return nil
	}
	return nil
}

func getString(rec *aero.Record, bin string) string {
	if v, ok := rec.Bins[bin]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt(rec *aero.Record, bin string) int {
	if v, ok := rec.Bins[bin]; ok {
		if n, ok := v.(int); ok {
			return n
		}
	}
	return 0
}

// getInt64 reads a 64-bit integer bin. Aerospike returns integer bins as the
// language's native int, so we widen safely.
func getInt64(rec *aero.Record, bin string) int64 {
	if v, ok := rec.Bins[bin]; ok {
		switch n := v.(type) {
		case int:
			return int64(n)
		case int64:
			return n
		}
	}
	return 0
}

// LookupPartitionKeys implements store.Store with one batch read of two bins.
func (s *Store) LookupPartitionKeys(ctx context.Context, txids []string) (map[string]store.ParentKey, error) {
	out := make(map[string]store.ParentKey, len(txids))
	keys := make([]*aero.Key, 0, len(txids))
	ids := make([]string, 0, len(txids))
	for _, txid := range txids {
		key, err := s.key(setTransactions, txid)
		if err != nil {
			continue
		}
		keys = append(keys, key)
		ids = append(ids, txid)
	}
	if len(keys) == 0 {
		return out, nil
	}
	recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys, "status", "partition_key")
	if err != nil {
		return nil, fmt.Errorf("lookup partition keys: %w", err)
	}
	for i, rec := range recs {
		if rec == nil || i >= len(ids) {
			continue
		}
		out[ids[i]] = store.ParentKey{Status: models.Status(getString(rec, "status")), PartitionKey: getString(rec, "partition_key")}
	}
	return out, nil
}
