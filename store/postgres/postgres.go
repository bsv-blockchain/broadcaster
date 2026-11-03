// Package postgres implements store.Store and store.Leaser against a
// Postgres database via pgx/v5. It supports two modes:
//
//  1. External Postgres: provide a DSN in config.
//  2. Embedded: fergusstrange/embedded-postgres spins up a local Postgres
//     process, data-dir on disk. Intended for single-binary standalone
//     mode alongside the in-memory Kafka broker.
//
// For BSV Broadcaster's sustained hot-path throughput Pebble is the recommended
// standalone backend; embedded-postgres is here primarily for users who
// want a real SQL surface for testing migrations, joins, and queries.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/bumpcache"
)

var (
	_ store.Store  = (*Store)(nil)
	_ store.Leaser = (*Store)(nil)
)

// Store is the Postgres-backed implementation of the store interfaces.
type Store struct {
	pool    *pgxpool.Pool
	stopEmb func() error
	// bumpCache holds parsed+indexed compound BUMPs for merkle-path
	// enrichment — sizing, budget, and singleflight live in bumpcache.
	bumpCache *bumpcache.Cache
	// schemaApplyTimeout bounds EnsureIndexes' slow path; <=0 means
	// defaultSchemaApplyTimeout. See schema.go.
	schemaApplyTimeout time.Duration
	// quotaSweptAt (unix ns) rate-limits the opportunistic api_quota sweep
	// in ReserveQuota to one DELETE per minute per process.
	quotaSweptAt atomic.Int64
}

// New connects to Postgres (optionally starting the embedded-postgres process
// first) and applies the schema. The caller owns the returned Store and must
// call Close during shutdown.
func New(ctx context.Context, cfg config.Postgres) (*Store, error) {
	dsn := cfg.DSN
	var stopEmbedded func() error
	if cfg.Embedded {
		d, stop, err := startEmbedded(cfg) //nolint:contextcheck // bootstrap path; ctx is honored by the pgxpool dial below
		if err != nil {
			return nil, err
		}
		dsn = d
		stopEmbedded = stop
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		if stopEmbedded != nil {
			_ = stopEmbedded()
		}
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		if stopEmbedded != nil {
			_ = stopEmbedded()
		}
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	return &Store{
		pool:               pool,
		stopEmb:            stopEmbedded,
		bumpCache:          bumpcache.New(),
		schemaApplyTimeout: time.Duration(cfg.SchemaApplyTimeoutMs) * time.Millisecond,
	}, nil
}

// Close drains the pool and stops the embedded Postgres process (if any).
// Errors from stopping the embedded process are reported — leaving the
// process alive blocks the next start-up because it holds the data-dir lock.
func (s *Store) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.stopEmb != nil {
		return s.stopEmb()
	}
	return nil
}

// --- Transaction Status ---

// GetOrInsertStatus uses INSERT ... ON CONFLICT DO NOTHING for a single
// round-trip CAS. If no row is inserted we fall back to a SELECT to read
// whatever the winning writer persisted.
func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	if status.Status == "" {
		status.Status = models.StatusReceived
	}
	status.CreatedAt = now

	// competing_txs is a JSONB column; pass a string so pgx encodes it as JSON
	// rather than BYTEA. nil is allowed (column is nullable).
	var competing any
	if len(status.CompetingTxs) > 0 {
		b, err := json.Marshal(status.CompetingTxs)
		if err != nil {
			return nil, false, err
		}
		competing = string(b)
	}

	const q = `
INSERT INTO transactions (txid, status, status_code, block_hash, block_height,
    merkle_path, extra_info, competing_txs, raw_tx, retry_count,
    next_retry_at, timestamp_at, created_at, merkle_registered_at, partition_key)
VALUES ($1,$2,NULLIF($3,0),NULLIF($4,''),NULLIF($5,0),$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13,$14,NULLIF($15,''))
ON CONFLICT (txid) DO NOTHING`

	var nextRetry any
	if !status.NextRetryAt.IsZero() {
		nextRetry = status.NextRetryAt
	}
	var merkleRegisteredAt any
	if !status.MerkleRegisteredAt.IsZero() {
		merkleRegisteredAt = status.MerkleRegisteredAt
	}

	tag, err := s.pool.Exec(
		ctx, q,
		status.TxID, string(status.Status), status.StatusCode,
		status.BlockHash, int64(status.BlockHeight), /* #nosec G115 */
		[]byte(status.MerklePath), status.ExtraInfo, competing,
		[]byte(status.RawTx), status.RetryCount,
		nextRetry, status.Timestamp, status.CreatedAt, merkleRegisteredAt,
		status.PartitionKey,
	)
	if err != nil {
		return nil, false, fmt.Errorf("insert tx %s: %w", status.TxID, err)
	}
	if tag.RowsAffected() > 0 {
		return status, true, nil
	}

	existing, err := s.GetStatus(ctx, status.TxID)
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

// columnsPerInsertRow is how many placeholders one row in the multi-row
// INSERT VALUES list consumes. Matches the column list in the static SQL
// fragment built by BatchGetOrInsertStatus.
const columnsPerInsertRow = 15

// BatchGetOrInsertStatus is the multi-row form of GetOrInsertStatus. It uses
// the "xmax = 0" trick: ON CONFLICT DO UPDATE SET txid = excluded.txid is a
// no-op write that nonetheless triggers RETURNING for the existing row, and
// xmax is 0 for newly-inserted rows but non-zero for rows we re-touched. So
// one round-trip gives us per-row inserted/existing flag plus the existing
// row data when the insert lost the race.
//
// The single-row GetOrInsertStatus uses ON CONFLICT DO NOTHING + a fallback
// SELECT, which is two round-trips when the row exists. For batches of 100+
// txs the win from collapsing into one round-trip is large.
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	// Normalise inputs the same way GetOrInsertStatus does — empty Status
	// becomes RECEIVED, missing timestamps default to now, CreatedAt is set.
	// Defaults are computed into local variables, never written back to the
	// caller's struct: callers may share the same input slice across
	// goroutines, and mutating shared pointers would race.
	// Postgres rejects multi-row INSERT…ON CONFLICT DO UPDATE when the same
	// key appears twice in VALUES (SQLSTATE 21000), so dedupe by txid here
	// and stitch results back to every duplicate position after the round
	// trip. firstPos remembers the input slot of the surviving row per txid.
	now := time.Now()
	firstPos := make(map[string]int, len(statuses))
	uniqueIdx := make([]int, 0, len(statuses))
	args := make([]any, 0, len(statuses)*columnsPerInsertRow)
	for i, st := range statuses {
		if _, seen := firstPos[st.TxID]; seen {
			continue
		}
		firstPos[st.TxID] = i
		uniqueIdx = append(uniqueIdx, i)

		ts := st.Timestamp
		if ts.IsZero() {
			ts = now
		}
		statusVal := st.Status
		if statusVal == "" {
			statusVal = models.StatusReceived
		}

		var competing any
		if len(st.CompetingTxs) > 0 {
			b, err := json.Marshal(st.CompetingTxs)
			if err != nil {
				return nil, fmt.Errorf("marshal competing_txs for %s: %w", st.TxID, err)
			}
			competing = string(b)
		}
		var nextRetry any
		if !st.NextRetryAt.IsZero() {
			nextRetry = st.NextRetryAt
		}
		var merkleRegisteredAt any
		if !st.MerkleRegisteredAt.IsZero() {
			merkleRegisteredAt = st.MerkleRegisteredAt
		}
		args = append(
			args,
			st.TxID, string(statusVal), st.StatusCode,
			st.BlockHash, int64(st.BlockHeight), /* #nosec G115 */
			[]byte(st.MerklePath), st.ExtraInfo, competing,
			[]byte(st.RawTx), st.RetryCount,
			nextRetry, ts, now, merkleRegisteredAt,
			st.PartitionKey,
		)
	}

	// Build the VALUES clause: one (...) tuple per row, NULLIF guards mirror
	// the single-row insert exactly.
	var values strings.Builder
	for i := 0; i < len(uniqueIdx); i++ {
		if i > 0 {
			values.WriteString(", ")
		}
		base := i * columnsPerInsertRow
		fmt.Fprintf(
			&values,
			"($%d,$%d,NULLIF($%d,0),NULLIF($%d,''),NULLIF($%d,0),$%d,NULLIF($%d,''),$%d,$%d,$%d,$%d,$%d,$%d,$%d,NULLIF($%d,''))",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
			base+8, base+9, base+10, base+11, base+12, base+13, base+14, base+15,
		)
	}

	q := `
INSERT INTO transactions (txid, status, status_code, block_hash, block_height,
    merkle_path, extra_info, competing_txs, raw_tx, retry_count,
    next_retry_at, timestamp_at, created_at, merkle_registered_at, partition_key)
VALUES ` + values.String() + `
ON CONFLICT (txid) DO UPDATE SET txid = transactions.txid
RETURNING txid, status, status_code, block_hash, block_height, merkle_path,
          extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
          timestamp_at, created_at, merkle_registered_at, (xmax = 0) AS inserted`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("batch insert: %w", err)
	}
	defer rows.Close()

	// Postgres doesn't guarantee RETURNING order matches input order. Build a
	// txid → result map, then assemble the output in input order.
	byTxID := make(map[string]store.BatchInsertResult, len(statuses))
	for rows.Next() {
		st, inserted, err := scanStatusWithInserted(rows)
		if err != nil {
			return nil, fmt.Errorf("scan batch result: %w", err)
		}
		if inserted {
			byTxID[st.TxID] = store.BatchInsertResult{Inserted: true}
		} else {
			byTxID[st.TxID] = store.BatchInsertResult{Existing: st, Inserted: false}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	out := make([]store.BatchInsertResult, len(statuses))
	for i, st := range statuses {
		res := byTxID[st.TxID]
		// Only the first input slot for a given txid keeps Inserted=true;
		// later duplicates report as already-existing so callers don't
		// double-process the same tx.
		if i != firstPos[st.TxID] && res.Inserted {
			first := statuses[firstPos[st.TxID]]
			cp := *first
			if cp.Status == "" {
				cp.Status = models.StatusReceived
			}
			if cp.Timestamp.IsZero() {
				cp.Timestamp = now
			}
			cp.CreatedAt = now
			res = store.BatchInsertResult{Inserted: false, Existing: &cp}
		}
		out[i] = res
	}
	return out, nil
}

// BatchUpdateStatus is the multi-row form of UpdateStatus. Uses
// UPDATE … FROM (VALUES …) so all rows update in one round-trip. Same
// partial-update semantics as UpdateStatus: empty fields don't overwrite
// existing values (handled by NULLIF + COALESCE in the SET clause).
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	if len(statuses) == 0 {
		return nil
	}
	err := s.batchUpdateStatusSQL(ctx, statuses)
	return err
}

// BatchUpdateStatusReturning applies each row with UpdateStatusReturning.
// A non-nil previous row means that row's transition was durably applied.
func (s *Store) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]store.StatusUpdate, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	return store.BatchUpdateStatusReturningParallel(ctx, s, statuses)
}

// batchUpdateStatusSQL is the single-round-trip batch UPDATE. The
// returning variant cannot share it: a non-nil previous row has to be the
// pre-image of a write this call applied, which the per-row CTE in
// UpdateStatusReturning decides under FOR UPDATE.
func (s *Store) batchUpdateStatusSQL(ctx context.Context, statuses []*models.TransactionStatus) error {
	if len(statuses) == 0 {
		return nil
	}

	const colsPerRow = 10 // txid, status, status_code, block_hash, block_height, extra_info, merkle_path, competing_txs, timestamp_at, clears_retry, disallowed_prev

	args := make([]any, 0, len(statuses)*(colsPerRow+1))
	now := time.Now()
	for _, st := range statuses {
		ts := st.Timestamp
		if ts.IsZero() {
			ts = now
		}
		var mp any
		if len(st.MerklePath) > 0 {
			mp = []byte(st.MerklePath)
		}
		competing, err := competingTxsArg(st.CompetingTxs)
		if err != nil {
			return fmt.Errorf("marshal competing_txs for %s: %w", st.TxID, err)
		}
		// disallowed previous statuses for this row's lattice guard, shared
		// with UpdateStatusReturning so a same-status re-assert is applied on
		// both paths (CanTransitionFrom allows it). An empty array means "no
		// constraint" — the AND clause uses ALL() so it is satisfied
		// trivially (status <> ALL('{}'::text[]) is true for every row).
		disallowed := transitionGuardStatuses(st.Status)
		args = append(
			args,
			st.TxID,
			string(st.Status),
			st.StatusCode,
			st.BlockHash,
			int64(st.BlockHeight), /* #nosec G115 */
			st.ExtraInfo,
			mp,
			competing,
			ts,
			st.Status.ClearsRetryState(),
			disallowed,
		)
	}

	var values strings.Builder
	for i := 0; i < len(statuses); i++ {
		if i > 0 {
			values.WriteString(", ")
		}
		base := i * (colsPerRow + 1)
		// Cast text/bytea/bigint/timestamptz/text[] so Postgres can pick the
		// right types for the VALUES alias columns.
		fmt.Fprintf(
			&values,
			"($%d::text,$%d::text,$%d::int,$%d::text,$%d::bigint,$%d::text,$%d::bytea,$%d::jsonb,$%d::timestamptz,$%d::boolean,$%d::text[])",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11,
		)
	}

	// Lattice guard (status <> ALL(v.disallowed_prev)) is applied in the
	// WHERE clause: rows whose existing status appears in the per-row disallowed
	// list are silently skipped. See models.Status.DisallowedPreviousStatuses
	// and #61 / F-003.
	//
	// A positive verdict (v.clears_retry, models.Status.ClearsRetryState)
	// retires the retry bookkeeping: extra_info becomes exactly what the write
	// carries (so a park or rejection reason does not outlive its cause), and
	// next_retry_at / retry_reason are cleared. Every other write keeps the
	// row's existing values when it carries none.
	q := `
UPDATE transactions t SET
    status       = v.status,
    status_code  = COALESCE(NULLIF(v.status_code, 0),     t.status_code),
    block_hash   = COALESCE(NULLIF(v.block_hash, ''),     t.block_hash),
    block_height = COALESCE(NULLIF(v.block_height, 0),    t.block_height),
    extra_info   = CASE WHEN v.clears_retry THEN NULLIF(v.extra_info, '')
                        ELSE COALESCE(NULLIF(v.extra_info, ''), t.extra_info) END,
    next_retry_at = CASE WHEN v.clears_retry THEN NULL ELSE t.next_retry_at END,
    retry_reason  = CASE WHEN v.clears_retry THEN NULL ELSE t.retry_reason END,
    merkle_path  = COALESCE(v.merkle_path,                t.merkle_path),
    competing_txs = COALESCE(v.competing_txs,             t.competing_txs),
    timestamp_at = v.timestamp_at
FROM (VALUES ` + values.String() + `) AS v(txid, status, status_code, block_hash, block_height, extra_info, merkle_path, competing_txs, timestamp_at, clears_retry, disallowed_prev)
WHERE t.txid = v.txid AND t.status <> ALL(v.disallowed_prev)`

	if _, err := s.pool.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("batch update: %w", err)
	}
	return nil
}

// competingTxsArg renders CompetingTxs for the JSONB competing_txs column: a
// JSON string so pgx encodes it as JSON rather than BYTEA, or nil for "no
// competing txs" so COALESCE keeps whatever the row already holds.
func competingTxsArg(competing []string) (any, error) {
	if len(competing) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(competing)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// UpdateStatus updates an existing transaction. If no row exists for
// status.TxID the call returns store.ErrNotFound without writing — callers
// must use GetOrInsertStatus to create new rows. A lattice skip is a nil
// error and writes nothing. See UpdateStatusReturning.
func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	_, err := s.UpdateStatusReturning(ctx, status)
	return err
}

// UpdateStatusReturning applies status and returns the applied-result.
// Prev is the row as it stood immediately before this statement's write.
// Current is that row when the lattice skipped the write. The read, the
// lattice check, and the write are one statement: FOR UPDATE holds the row,
// and the UPDATE matches only when CanTransitionFrom allows the move
// (including an idempotent re-assert of the same status). A missing row
// returns ErrNotFound. Prev and Current carry status metadata only:
// raw_tx, merkle_path and competing_txs are not read, matching the
// StatusUpdate contract and the Mongo/Aerospike projections.
func (s *Store) UpdateStatusReturning(ctx context.Context, status *models.TransactionStatus) (store.StatusUpdate, error) {
	if status == nil || status.TxID == "" {
		return store.StatusUpdate{}, store.ErrNotFound
	}
	ts := status.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	sets := []string{"status = $2", "timestamp_at = $3"}
	args := []any{status.TxID, string(status.Status), ts}
	idx := 4
	if status.BlockHash != "" {
		sets = append(sets, fmt.Sprintf("block_hash = $%d", idx))
		args = append(args, status.BlockHash)
		idx++
	}
	if status.BlockHeight > 0 {
		sets = append(sets, fmt.Sprintf("block_height = $%d", idx))
		args = append(args, int64(status.BlockHeight) /* #nosec G115 */)
		idx++
	}
	if status.ExtraInfo != "" {
		sets = append(sets, fmt.Sprintf("extra_info = $%d", idx))
		args = append(args, status.ExtraInfo)
		idx++
	}
	if status.Status.ClearsRetryState() {
		// A positive verdict retires the retry bookkeeping; see
		// models.Status.ClearsRetryState and batchUpdateStatusSQL.
		sets = append(sets, "next_retry_at = NULL", "retry_reason = NULL")
		if status.ExtraInfo == "" {
			sets = append(sets, "extra_info = NULL")
		}
	}
	if status.StatusCode != 0 {
		sets = append(sets, fmt.Sprintf("status_code = $%d", idx))
		args = append(args, status.StatusCode)
		idx++
	}
	if len(status.MerklePath) > 0 {
		sets = append(sets, fmt.Sprintf("merkle_path = $%d", idx))
		args = append(args, []byte(status.MerklePath))
		idx++
	}
	if len(status.CompetingTxs) > 0 {
		competing, err := competingTxsArg(status.CompetingTxs)
		if err != nil {
			return store.StatusUpdate{}, fmt.Errorf("marshal competing_txs for %s: %w", status.TxID, err)
		}
		sets = append(sets, fmt.Sprintf("competing_txs = $%d::jsonb", idx))
		args = append(args, competing)
		idx++
	}
	disallowed := transitionGuardStatuses(status.Status)
	args = append(args, disallowed)
	guard := fmt.Sprintf("($2 = '' OR old.status = $2 OR NOT (old.status = ANY($%d::text[])))", idx)

	var setSQL strings.Builder
	for i, set := range sets {
		if i > 0 {
			setSQL.WriteString(", ")
		}
		setSQL.WriteString(set)
	}
	q := `
WITH old AS (
    SELECT txid, status, status_code, block_hash, block_height, extra_info,
           retry_count, next_retry_at, timestamp_at, created_at, merkle_registered_at
    FROM transactions
    WHERE txid = $1
    FOR UPDATE
), upd AS (
    UPDATE transactions t SET ` + setSQL.String() + `
    FROM old
    WHERE t.txid = old.txid
      AND ` + guard + `
    RETURNING t.txid
)
SELECT old.txid, old.status, old.status_code, old.block_hash, old.block_height, old.extra_info,
       old.retry_count, old.next_retry_at, old.timestamp_at, old.created_at, old.merkle_registered_at,
       EXISTS (SELECT 1 FROM upd) AS applied
FROM old`

	row := s.pool.QueryRow(ctx, q, args...)
	prev, applied, err := scanStatusApplied(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.StatusUpdate{}, store.ErrNotFound
	}
	if err != nil {
		return store.StatusUpdate{}, fmt.Errorf("update tx %s: %w", status.TxID, err)
	}
	if !applied {
		return store.StatusUpdate{Current: prev}, nil
	}
	return store.StatusUpdate{Prev: prev}, nil
}

// transitionGuardStatuses is DisallowedPreviousStatuses without the target
// itself, as a text[] for the status-update lattice guards
// (UpdateStatusReturning and batchUpdateStatusSQL). CanTransitionFrom
// allows re-asserting the same status, so the array holds only what
// cannot move to the target. Never nil: an empty array is "no constraint".
func transitionGuardStatuses(s models.Status) []string {
	if s == "" {
		return []string{}
	}
	prev := s.DisallowedPreviousStatuses()
	out := make([]string, 0, len(prev))
	for _, p := range prev {
		if p == s {
			continue
		}
		out = append(out, string(p))
	}
	return out
}

// scanStatusApplied decodes the UpdateStatusReturning row: the status
// metadata the StatusUpdate contract guarantees plus the trailing applied
// flag. raw_tx, merkle_path and competing_txs are deliberately not read —
// no caller of the returning path uses them and raw_tx can be megabytes
// per row on the callback hot path. applied is false when the row existed
// but the lattice guard skipped the write; the caller then reports that
// row as Current.
func scanStatusApplied(row rowScanner) (*models.TransactionStatus, bool, error) {
	var (
		st                 models.TransactionStatus
		statusCode         *int
		blockHash          *string
		blockHeight        *int64
		extraInfo          *string
		nextRetry          *time.Time
		merkleRegisteredAt *time.Time
		applied            bool
	)
	if err := row.Scan(
		&st.TxID, &st.Status, &statusCode,
		&blockHash, &blockHeight, &extraInfo,
		&st.RetryCount, &nextRetry,
		&st.Timestamp, &st.CreatedAt, &merkleRegisteredAt, &applied,
	); err != nil {
		return nil, false, err
	}
	if statusCode != nil {
		st.StatusCode = *statusCode
	}
	if blockHash != nil {
		st.BlockHash = *blockHash
	}
	if blockHeight != nil {
		st.BlockHeight = uint64(*blockHeight) //nolint:gosec // block height fits in either signed/unsigned 64-bit
	}
	if extraInfo != nil {
		st.ExtraInfo = *extraInfo
	}
	if nextRetry != nil {
		st.NextRetryAt = *nextRetry
	}
	if merkleRegisteredAt != nil {
		st.MerkleRegisteredAt = *merkleRegisteredAt
	}
	return &st, applied, nil
}

// disallowedPrevAsStrings drops the full DisallowedPreviousStatuses list,
// target included, into a parameterised text[] clause. The status-update
// paths use transitionGuardStatuses instead so a same-status re-assert is
// applied.
func disallowedPrevAsStrings(s models.Status) []string {
	if s == "" {
		return nil
	}
	prev := s.DisallowedPreviousStatuses()
	if len(prev) == 0 {
		return nil
	}
	out := make([]string, len(prev))
	for i, p := range prev {
		out[i] = string(p)
	}
	return out
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	// orphaned_anchors rides only on this single-tx read (the GET /tx path
	// where historical proofs matter) — the scanStatus-shared bulk queries
	// deliberately skip it.
	const q = `
SELECT txid, status, status_code, block_hash, block_height, merkle_path,
       extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
       timestamp_at, created_at, merkle_registered_at,
       COALESCE(orphaned_anchors, 'null'::jsonb)
FROM transactions WHERE txid = $1`
	row := s.pool.QueryRow(ctx, q, txid)
	st, anchorsJSON, err := scanStatusWithAnchors(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(anchorsJSON) > 0 && string(anchorsJSON) != "null" {
		_ = json.Unmarshal(anchorsJSON, &st.OrphanedProofs)
	}
	s.enrichMerklePath(ctx, st)
	s.enrichOrphanedProofs(ctx, st)
	return st, nil
}

// scanStatusWithAnchors is scanStatus + the trailing orphaned_anchors JSONB
// column GetStatus selects.
func scanStatusWithAnchors(row rowScanner) (*models.TransactionStatus, []byte, error) {
	var (
		st                 models.TransactionStatus
		statusCode         *int
		blockHash          *string
		blockHeight        *int64
		merklePath         []byte
		extraInfo          *string
		competingTxs       []byte
		rawTx              []byte
		nextRetry          *time.Time
		merkleRegisteredAt *time.Time
		anchors            []byte
	)
	if err := row.Scan(
		&st.TxID, &st.Status, &statusCode,
		&blockHash, &blockHeight, &merklePath,
		&extraInfo, &competingTxs, &rawTx,
		&st.RetryCount, &nextRetry,
		&st.Timestamp, &st.CreatedAt, &merkleRegisteredAt, &anchors,
	); err != nil {
		return nil, nil, err
	}
	if statusCode != nil {
		st.StatusCode = *statusCode
	}
	if blockHash != nil {
		st.BlockHash = *blockHash
	}
	if blockHeight != nil {
		st.BlockHeight = uint64(*blockHeight) //nolint:gosec // block height fits in either signed/unsigned 64-bit
	}
	if len(merklePath) > 0 {
		st.MerklePath = merklePath
	}
	if extraInfo != nil {
		st.ExtraInfo = *extraInfo
	}
	if len(competingTxs) > 0 {
		_ = json.Unmarshal(competingTxs, &st.CompetingTxs)
	}
	if len(rawTx) > 0 {
		st.RawTx = rawTx
	}
	if nextRetry != nil {
		st.NextRetryAt = *nextRetry
	}
	if merkleRegisteredAt != nil {
		st.MerkleRegisteredAt = *merkleRegisteredAt
	}
	return &st, anchors, nil
}

func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	const q = `
SELECT txid, status, status_code, block_hash, block_height, merkle_path,
       extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
       timestamp_at, created_at, merkle_registered_at
FROM transactions WHERE timestamp_at >= $1
ORDER BY timestamp_at DESC`
	rows, err := s.pool.Query(ctx, q, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*models.TransactionStatus
	for rows.Next() {
		st, err := scanStatus(rows)
		if err != nil {
			return results, err
		}
		results = append(results, st)
	}
	return results, rows.Err()
}

// IterateStatusesSince streams the same query as GetStatusesSince through fn
// without buffering the full result set. pgx's rows.Next() pulls rows from the
// server one at a time, so memory stays O(row) regardless of history depth.
func (s *Store) IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error {
	const q = `
SELECT txid, status, status_code, block_hash, block_height, merkle_path,
       extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
       timestamp_at, created_at, merkle_registered_at
FROM transactions WHERE timestamp_at >= $1
ORDER BY timestamp_at DESC`
	rows, err := s.pool.Query(ctx, q, since)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := scanStatus(rows)
		if err != nil {
			return err
		}
		if err := fn(st); err != nil {
			return err
		}
	}
	return rows.Err()
}

// IterateTrackerRows streams the three columns hydration needs, filtered
// server-side. Contrast with IterateStatusesSince above, which this replaced
// for hydration: that query selects 14 columns including raw_tx / merkle_path
// / competing_txs and sorts the whole table by timestamp_at. On a ~5M-row
// store the planner resolved it with a Seq Scan plus an external merge sort
// over detoasted wide rows, which drove both BSV Broadcaster and its Postgres out of
// memory on every api-server boot (issue #276).
//
// Three properties matter here and each is load-bearing:
//   - three narrow columns, so raw_tx is never detoasted;
//   - the status filter is server-side, so REJECTED (~44% of a mature store)
//     never leaves the database;
//   - NO ORDER BY — hydration builds a map, order is meaningless, and the
//     sort was the memory hazard.
func (s *Store) IterateTrackerRows(ctx context.Context, scan store.TrackerScan, fn func(store.TrackerRow) error) error {
	statuses := scan.Statuses()
	if len(statuses) == 0 {
		return nil
	}
	names := make([]string, len(statuses))
	for i, st := range statuses {
		names[i] = string(st)
	}

	// One static query text covering both regimes. When PruneMinedBelow is 0
	// (chain height unknown) `block_height >= 0` matches every non-null height,
	// so all MINED rows are kept — the documented degraded behavior. The
	// COALESCE disjunct keeps MINED rows with height 0 or NULL at any cutoff.
	const q = `
SELECT txid, status, COALESCE(block_height, 0)
FROM transactions
WHERE status = ANY($1)
  AND (status <> $2 OR COALESCE(block_height, 0) = 0 OR block_height >= $3)`

	//nolint:gosec // PruneMinedBelow is a block height; far below MaxInt64.
	rows, err := s.pool.Query(ctx, q, names, string(models.StatusMined), int64(scan.PruneMinedBelow))
	if err != nil {
		return fmt.Errorf("iterate tracker rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var (
			txid        string
			status      string
			blockHeight int64
		)
		if err := rows.Scan(&txid, &status, &blockHeight); err != nil {
			return err
		}
		if blockHeight < 0 {
			blockHeight = 0
		}
		if err := fn(store.TrackerRow{
			TxID:        txid,
			Status:      models.Status(status),
			BlockHeight: uint64(blockHeight),
		}); err != nil {
			return err
		}
	}
	return rows.Err()
}

// CensusStatusesSince is one aggregate round-trip: count + min(timestamp_at)
// per status over the census window, resolved by the idx_tx_status /
// idx_tx_updated indexes. The reaper's stuck census over a multi-million-row
// table must never stream rows through the client (issue #290) — this query
// is the entire point of the method.
func (s *Store) CensusStatusesSince(ctx context.Context, since, stuckDeadline time.Time, statuses []models.Status) (map[models.Status]store.StatusCensus, error) {
	out := make(map[models.Status]store.StatusCensus, len(statuses))
	if len(statuses) == 0 {
		return out, nil
	}
	names := make([]string, len(statuses))
	for i, st := range statuses {
		out[st] = store.StatusCensus{}
		names[i] = string(st)
	}

	const q = `
SELECT status, count(*), min(timestamp_at)
FROM transactions
WHERE timestamp_at >= $1 AND timestamp_at < $2 AND status = ANY($3)
GROUP BY status`
	rows, err := s.pool.Query(ctx, q, since, stuckDeadline, names)
	if err != nil {
		return nil, fmt.Errorf("census statuses since: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name   string
			count  int64
			oldest time.Time
		)
		if err := rows.Scan(&name, &count, &oldest); err != nil {
			return nil, err
		}
		out[models.Status(name)] = store.StatusCensus{Count: count, Oldest: oldest}
	}
	return out, rows.Err()
}

// SetStatusByBlockHash rewrites every row in the block. Block fields are
// cleared on SEEN_ON_NETWORK transitions (reorg path) and kept otherwise —
// matches the Aerospike / Pebble contract. IMMUTABLE rows are never touched,
// and the WHERE block_hash predicate is atomic per row, so a tx concurrently
// re-anchored to a different block simply falls out of the match (issue
// #279). Reverts append the cleared anchor to orphaned_anchors so the
// orphaned block's proof stays auditable.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	clearBlock := newStatus == models.StatusSeenOnNetwork

	var q string
	if clearBlock {
		q = `UPDATE transactions SET status=$2,
               orphaned_anchors = CASE
                 WHEN COALESCE(orphaned_anchors -> -1 ->> 'blockHash', '') <> block_hash
                 THEN (
                   SELECT jsonb_agg(elem ORDER BY ord)
                   FROM (
                     SELECT elem, ord
                     FROM jsonb_array_elements(
                            COALESCE(orphaned_anchors, '[]'::jsonb) ||
                            jsonb_build_array(jsonb_build_object(
                              'blockHash',   block_hash,
                              'blockHeight', COALESCE(block_height, 0),
                              'orphanedAt',  NOW()
                            ))
                          ) WITH ORDINALITY AS x(elem, ord)
                     ORDER BY ord DESC
                     LIMIT 5
                   ) tail
                 )
                 ELSE orphaned_anchors
               END,
               block_hash=NULL, block_height=NULL, timestamp_at=NOW()
             WHERE block_hash=$1 AND status <> 'IMMUTABLE' RETURNING txid`
	} else {
		q = `UPDATE transactions SET status=$2, timestamp_at=NOW()
             WHERE block_hash=$1 AND status <> 'IMMUTABLE' RETURNING txid`
	}

	rows, err := s.pool.Query(ctx, q, blockHash, string(newStatus))
	if err != nil {
		return nil, fmt.Errorf("update by block hash: %w", err)
	}
	defer rows.Close()

	var txids []string
	for rows.Next() {
		var txid string
		if err := rows.Scan(&txid); err != nil {
			return txids, err
		}
		txids = append(txids, txid)
	}
	return txids, rows.Err()
}

// GetTxIDsByBlockHash returns every txid currently anchored to blockHash —
// the reorg reconciler's affected set for an orphaned block. Resolved via
// idx_tx_block_hash.
func (s *Store) GetTxIDsByBlockHash(ctx context.Context, blockHash string) ([]string, error) {
	const q = `SELECT txid FROM transactions WHERE block_hash = $1`
	rows, err := s.pool.Query(ctx, q, blockHash)
	if err != nil {
		return nil, fmt.Errorf("get txids by block hash: %w", err)
	}
	defer rows.Close()

	var txids []string
	for rows.Next() {
		var txid string
		if err := rows.Scan(&txid); err != nil {
			return txids, err
		}
		txids = append(txids, txid)
	}
	return txids, rows.Err()
}

// BumpRetryCount is a single-statement atomic increment — no client-side
// mutex needed because Postgres handles the CAS.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	const q = `UPDATE transactions SET retry_count = retry_count + 1
	           WHERE txid = $1 RETURNING retry_count`
	var n int
	err := s.pool.QueryRow(ctx, q, txid).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, store.ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, err)
	}
	return n, nil
}

// SetPendingRetryFields records the durable-retry bins for a parked tx.
//
// The lattice guard in the WHERE clause is load-bearing, not decorative. The
// park path writes twice — a lattice-guarded BatchUpdateStatusReturning,
// then this — so without it the second write would silently undo the first
// one's protection and force a MINED / IMMUTABLE / SEEN / REJECTED row back
// to PENDING_RETRY, where the reaper would then rebroadcast it. Rows whose
// current status forbids PENDING_RETRY are skipped, matching
// BatchUpdateStatusReturning's silent-skip semantics.
func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time, lastReason string) error {
	const q = `
UPDATE transactions
SET status=$2, raw_tx=$3, next_retry_at=$4, timestamp_at=NOW(),
    retry_reason = NULLIF($6,'')
WHERE txid=$1 AND status <> ALL($5)`
	disallowed := disallowedPrevAsStrings(models.StatusPendingRetry)
	if disallowed == nil {
		disallowed = []string{}
	}
	_, err := s.pool.Exec(ctx, q, txid, string(models.StatusPendingRetry), rawTx, nextRetryAt, disallowed, lastReason)
	if err != nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, err)
	}
	return nil
}

// GetReadyRetries uses FOR UPDATE SKIP LOCKED so the reaper can run on
// multiple processes without delivering the same tx twice. Postgres holds
// the row locks until the enclosing transaction commits; for BSV Broadcaster's
// single-request reaper that happens as soon as this function returns.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const q = `
SELECT txid, raw_tx, retry_count, next_retry_at, COALESCE(retry_reason, '')
FROM transactions
WHERE status = 'PENDING_RETRY' AND next_retry_at <= $1
ORDER BY next_retry_at
LIMIT $2
FOR UPDATE SKIP LOCKED`
	rows, err := tx.Query(ctx, q, now, limit)
	if err != nil {
		return nil, err
	}
	var out []*store.PendingRetry
	for rows.Next() {
		r := &store.PendingRetry{}
		if err := rows.Scan(&r.TxID, &r.RawTx, &r.RetryCount, &r.NextRetryAt, &r.LastReason); err != nil {
			rows.Close()
			return out, err
		}
		if len(r.RawTx) == 0 {
			continue
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}

func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	const q = `
UPDATE transactions
SET status=$2, raw_tx=NULL, next_retry_at=NULL, retry_reason=NULL, timestamp_at=NOW(),
    extra_info = COALESCE(NULLIF($3,''), extra_info)
WHERE txid=$1`
	_, err := s.pool.Exec(ctx, q, txid, string(finalStatus), extraInfo)
	if err != nil {
		return fmt.Errorf("clear retry state %s: %w", txid, err)
	}
	return nil
}

// SetMinedByTxIDs updates only rows that already exist. A CTE snapshots the
// pre-update row state so callers can observe the MINED transition age via
// bsv_broadcaster_status_transition_age_seconds without a second round-trip.
// blockHeight is persisted alongside blockHash and echoed back on each
// returned status so downstream SSE/webhook consumers always see the height
// that anchors the MINED transition (issue #87 / F-029).
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	if len(txids) == 0 {
		return nil, nil, nil
	}
	now := time.Now()
	// Lattice hardening + orphaned-anchor history (issue #279):
	//   - IMMUTABLE rows are excluded from the join — a replayed
	//     BLOCK_PROCESSED must never demote a confirmation-depth promotion.
	//   - A MINED row re-anchored to a DIFFERENT block appends its previous
	//     anchor to orphaned_anchors (models.OrphanedAnchor JSON shape,
	//     capped at 5 = models.MaxOrphanedAnchors, consecutive duplicates
	//     collapsed) so the orphaned block's proof stays auditable.
	const q = `
WITH prev AS (
  SELECT txid, status, timestamp_at, block_hash, block_height, orphaned_anchors
  FROM transactions
  WHERE txid = ANY($5)
)
UPDATE transactions t
SET status=$1, block_hash=$2, block_height=$3, timestamp_at=$4,
    extra_info=NULL, next_retry_at=NULL, retry_reason=NULL,
    orphaned_anchors = CASE
      WHEN prev.status = 'MINED' AND prev.block_hash IS NOT NULL AND prev.block_hash <> $2
           AND COALESCE(prev.orphaned_anchors -> -1 ->> 'blockHash', '') <> prev.block_hash
      THEN (
        SELECT jsonb_agg(elem ORDER BY ord)
        FROM (
          SELECT elem, ord
          FROM jsonb_array_elements(
                 COALESCE(prev.orphaned_anchors, '[]'::jsonb) ||
                 jsonb_build_array(jsonb_build_object(
                   'blockHash',   prev.block_hash,
                   'blockHeight', COALESCE(prev.block_height, 0),
                   'orphanedAt',  $4::timestamptz
                 ))
               ) WITH ORDINALITY AS x(elem, ord)
          ORDER BY ord DESC
          LIMIT 5
        ) tail
      )
      ELSE prev.orphaned_anchors
    END
FROM prev
WHERE t.txid = prev.txid AND t.status <> 'IMMUTABLE'
RETURNING t.txid, prev.status, prev.timestamp_at, prev.block_hash, prev.block_height`
	rows, err := s.pool.Query(ctx, q, string(models.StatusMined), blockHash, int64(blockHeight), now, txids) //nolint:gosec // block height fits in int64
	if err != nil {
		return nil, nil, fmt.Errorf("set mined: %w", err)
	}
	defer rows.Close()
	var prevs, out []*models.TransactionStatus
	for rows.Next() {
		var (
			txid          string
			prevStatus    string
			prevTimestamp time.Time
			// transactions.block_hash / block_height are nullable for any
			// pre-MINED status (RECEIVED / SENT_TO_NETWORK / SEEN_*). The CTE
			// captures the pre-update row so these will be NULL whenever a tx
			// transitions to MINED for the first time. Scan via pointer so a
			// NULL doesn't fail the scan; an empty BlockHash on the returned
			// prev snapshot is the correct "no block yet" representation
			// (matches models.TransactionStatus zero values used elsewhere).
			prevBlockHash *string
			prevHeight    *int64
		)
		if err := rows.Scan(&txid, &prevStatus, &prevTimestamp, &prevBlockHash, &prevHeight); err != nil {
			return prevs, out, err
		}
		prev := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.Status(prevStatus),
			Timestamp: prevTimestamp,
		}
		if prevBlockHash != nil {
			prev.BlockHash = *prevBlockHash
		}
		if prevHeight != nil {
			prev.BlockHeight = uint64(*prevHeight) //nolint:gosec // value originated as uint64 in this column
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
	return prevs, out, rows.Err()
}

// MarkMerkleRegisteredByTxIDs stamps merkle_registered_at = $1 on every existing
// row whose txid is in $2. Unknown txids are silently no-ops, matching the
// SetMinedByTxIDs contract. The startup replay loop calls this after each
// successful /watch batch so future replays can skip recently-registered rows
// (issue #145).
func (s *Store) MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error {
	if len(txids) == 0 {
		return nil
	}
	const q = `UPDATE transactions SET merkle_registered_at = $1 WHERE txid = ANY($2)`
	if _, err := s.pool.Exec(ctx, q, ts, txids); err != nil {
		return fmt.Errorf("mark merkle registered: %w", err)
	}
	return nil
}

// --- BUMP / STUMP ---

func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	const q = `
INSERT INTO bumps (block_hash, block_height, bump_data) VALUES ($1,$2,$3)
ON CONFLICT (block_hash) DO UPDATE SET block_height=EXCLUDED.block_height, bump_data=EXCLUDED.bump_data`
	_, err := s.pool.Exec(ctx, q, blockHash, int64(blockHeight), bumpData) //nolint:gosec // block height fits in int64
	if err != nil {
		return fmt.Errorf("insert bump %s: %w", blockHash, err)
	}
	// ON CONFLICT DO UPDATE means a rebuild can overwrite bump_data for an
	// existing block hash (e.g. late STUMP callbacks produce a more complete
	// sparse compound). Drop any cached parse so the next status lookup
	// re-fetches the current stored BUMP instead of serving a stale compound.
	s.bumpCache.Remove(blockHash)
	return nil
}

func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	const q = `SELECT block_height, bump_data FROM bumps WHERE block_hash = $1`
	var h int64
	var data []byte
	err := s.pool.QueryRow(ctx, q, blockHash).Scan(&h, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, store.ErrNotFound
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	return uint64(h), data, nil //nolint:gosec // block height fits in uint64
}

func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	// Content-addressed: the same bytes from a second merkle-service hit the
	// conflict and do nothing; different bytes for the same subtree land as
	// a second row for the bump-builder to merge.
	const q = `
INSERT INTO stumps (block_hash, subtree_index, content_hash, stump_data) VALUES ($1,$2,$3,$4)
ON CONFLICT (block_hash, subtree_index, content_hash) DO NOTHING`
	_, err := s.pool.Exec(ctx, q, stump.BlockHash, stump.SubtreeIndex, stump.EnsureContentHash(), stump.StumpData)
	if err != nil {
		return fmt.Errorf("insert stump: %w", err)
	}
	return nil
}

func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	const q = `SELECT block_hash, subtree_index, content_hash, stump_data FROM stumps WHERE block_hash = $1 ORDER BY subtree_index, content_hash`
	rows, err := s.pool.Query(ctx, q, blockHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Stump
	for rows.Next() {
		st := &models.Stump{}
		if err := rows.Scan(&st.BlockHash, &st.SubtreeIndex, &st.ContentHash, &st.StumpData); err != nil {
			return out, err
		}
		st.EnsureContentHash()
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM stumps WHERE block_hash = $1`, blockHash)
	return err
}

// --- Block processing status ---

func (s *Store) UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error {
	// On conflict the existing milestone timestamps and header_seen_at must
	// be preserved. block_height is overwritten because chaintracks is the
	// authoritative source — earlier rows might have been created by
	// MarkBlockProcessed with height=0.
	const q = `
INSERT INTO block_processing (block_hash, block_height, header_seen_at, status)
VALUES ($1, $2, $3, 'active')
ON CONFLICT (block_hash) DO UPDATE SET
    block_height  = EXCLUDED.block_height,
    status        = 'active',
    reconciled_at = NULL`
	// orphaned_at is deliberately not reset: it stays as the row's
	// orphan-generation high-water mark, which MarkBlocksOrphaned mints
	// above; scanBlockProcessing hides it on an active row.
	_, err := s.pool.Exec(ctx, q, blockHash, int64(blockHeight), seenAt) //nolint:gosec // block height fits in int64
	if err != nil {
		return fmt.Errorf("upsert block header seen %s: %w", blockHash, err)
	}
	return nil
}

func (s *Store) MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error {
	const q = `
INSERT INTO block_processing (block_hash, block_height, header_seen_at, processed_at, status)
VALUES ($1, $2, $3, $3, 'active')
ON CONFLICT (block_hash) DO UPDATE SET
    processed_at = EXCLUDED.processed_at`
	_, err := s.pool.Exec(ctx, q, blockHash, int64(blockHeight), processedAt) //nolint:gosec // block height fits in int64
	if err != nil {
		return fmt.Errorf("mark block processed %s: %w", blockHash, err)
	}
	return nil
}

// ClearBlockProcessed unsets processed_at so the watchdog's stale scan
// (processed_at IS NULL) picks the block up again.
func (s *Store) ClearBlockProcessed(ctx context.Context, blockHash string) error {
	const q = `UPDATE block_processing SET processed_at = NULL WHERE block_hash = $1`
	if _, err := s.pool.Exec(ctx, q, blockHash); err != nil {
		return fmt.Errorf("clear block processed %s: %w", blockHash, err)
	}
	return nil
}

// AddBlockExpectedSubtrees implements store.Store in one UPDATE, so the
// union is computed against the row's current set server-side.
func (s *Store) AddBlockExpectedSubtrees(ctx context.Context, blockHash string, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	const q = `
UPDATE block_processing SET expected_subtrees = (
    SELECT array_agg(DISTINCT i ORDER BY i)
    FROM unnest(COALESCE(expected_subtrees, '{}'::int[]) || $2::int[]) AS i
)
WHERE block_hash = $1`
	vals := make([]int32, len(indices))
	for i, v := range indices {
		vals[i] = int32(v) //nolint:gosec // subtree indices are small
	}
	if _, err := s.pool.Exec(ctx, q, blockHash, vals); err != nil {
		return fmt.Errorf("add block expected subtrees %s: %w", blockHash, err)
	}
	return nil
}

// GetBlockExpectedSubtrees implements store.Store.
func (s *Store) GetBlockExpectedSubtrees(ctx context.Context, blockHash string) ([]int, error) {
	var vals []int32
	err := s.pool.QueryRow(ctx, `SELECT expected_subtrees FROM block_processing WHERE block_hash = $1`, blockHash).Scan(&vals)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get block expected subtrees %s: %w", blockHash, err)
	}
	if len(vals) == 0 {
		return nil, nil
	}
	out := make([]int, len(vals))
	for i, v := range vals {
		out[i] = int(v)
	}
	return out, nil
}

// ClearBlockExpectedSubtrees implements store.Store.
func (s *Store) ClearBlockExpectedSubtrees(ctx context.Context, blockHash string) error {
	if _, err := s.pool.Exec(ctx, `UPDATE block_processing SET expected_subtrees = NULL WHERE block_hash = $1`, blockHash); err != nil {
		return fmt.Errorf("clear block expected subtrees %s: %w", blockHash, err)
	}
	return nil
}

func (s *Store) MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error {
	const q = `
INSERT INTO block_processing (block_hash, block_height, header_seen_at, bump_built_at, status)
VALUES ($1, $2, $3, $3, 'active')
ON CONFLICT (block_hash) DO UPDATE SET
    bump_built_at = EXCLUDED.bump_built_at`
	_, err := s.pool.Exec(ctx, q, blockHash, int64(blockHeight), builtAt) //nolint:gosec // block height fits in int64
	if err != nil {
		return fmt.Errorf("mark block bump built %s: %w", blockHash, err)
	}
	return nil
}

func (s *Store) MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) (int, error) {
	if len(blockHashes) == 0 {
		return 0, nil
	}
	// Two data-modifying CTEs over one snapshot, so still one round-trip.
	// The transition count is the row count of the first: its WHERE
	// excludes rows already orphaned, and under READ COMMITTED a row a
	// concurrent writer orphans first is re-checked against its UPDATED
	// version once that writer's lock is released (EvalPlanQual) and drops
	// out — so two replicas racing on the same row report ONE transition
	// between them. A FROM-list self-join reading the "previous" status
	// cannot promise that: EvalPlanQual re-fetches only the target row while
	// the joined copy keeps the snapshot's value, so both racers would have
	// counted. The second CTE refreshes the generation and clears the stamp
	// on rows that were already orphaned; the snapshot decides which CTE a
	// row belongs to, so no row is updated twice in one statement.
	//
	// The refresh is forward-only-inclusive (orphaned_at <= $2): a stored
	// generation NEWER than this call's is kept, stamp state and all, so a
	// delayed call carrying an older timestamp cannot move the generation
	// backwards and let a reconciler holding that older token pass its CAS.
	// An EQUAL generation still refreshes and clears the stamp — the
	// contract does not require a strictly increasing time. Both sides of
	// the compare are microseconds: pgx encodes the $2 time.Time to
	// timestamptz by truncating to µs, the same encoding the stored value
	// went through, so a token read back from this table compares equal to
	// itself and sub-µs bits on a fresh time.Now() cannot exclude it.
	//
	// Both arms MINT the generation: GREATEST($2, orphaned_at + 1 µs) is
	// strictly above any generation the row has had — orphaned_at is kept
	// through reactivation as the high-water mark — with the caller's
	// timestamp as the lower bound, so two orphanings never share a
	// generation even when callers reuse a timestamp. GREATEST ignores a
	// NULL operand, so a row that never had one gets $2. One microsecond is
	// the step timestamptz can represent, and pgx truncates $2 to it too.
	const q = `
WITH transitioned AS (
    UPDATE block_processing
    SET status = 'orphaned',
        orphaned_at = GREATEST($2::timestamptz, orphaned_at + interval '1 microsecond'),
        reconciled_at = NULL
    WHERE block_hash = ANY($1) AND status <> 'orphaned'
    RETURNING 1
), refreshed AS (
    UPDATE block_processing
    SET orphaned_at = GREATEST($2::timestamptz, orphaned_at + interval '1 microsecond'),
        reconciled_at = NULL
    WHERE block_hash = ANY($1) AND status = 'orphaned'
      AND (orphaned_at IS NULL OR orphaned_at <= $2)
)
SELECT count(*) FROM transitioned`
	var transitions int64
	if err := s.pool.QueryRow(ctx, q, blockHashes, orphanedAt).Scan(&transitions); err != nil {
		return 0, fmt.Errorf("mark blocks orphaned: %w", err)
	}
	return int(transitions), nil
}

// MarkBlockReconciled stamps reconciled_at on an orphaned block's row — the
// anchor reconciler finished re-anchoring/reverting its transactions — as a
// compare-and-set on the orphan generation: the row must still be orphaned
// with the orphaned_at the caller observed (a zero orphanedAt checks status
// only). Missing, resurrected and re-orphaned rows are silently skipped and
// reported as not stamped (issue #339).
func (s *Store) MarkBlockReconciled(ctx context.Context, blockHash string, orphanedAt, at time.Time) (bool, error) {
	const q = `
UPDATE block_processing
SET reconciled_at = $2
WHERE block_hash = $1
  AND status = 'orphaned'
  AND ($3::timestamptz IS NULL OR orphaned_at = $3)`
	var generation *time.Time
	if !orphanedAt.IsZero() {
		generation = &orphanedAt
	}
	tag, err := s.pool.Exec(ctx, q, blockHash, at, generation)
	if err != nil {
		return false, fmt.Errorf("mark block reconciled %s: %w", blockHash, err)
	}
	return tag.RowsAffected() == 1, nil
}

// RequeueOrphanedBlock clears reconciled_at on a row that is still orphaned
// with the generation the caller judged (a zero orphanedAt checks status
// only), putting it back on the reconciler's queue without changing the
// generation. Missing, active, parked and re-orphaned rows are left
// untouched and reported as not applied; it never transitions a row. The
// WHERE is re-checked against the row's current version under READ
// COMMITTED, so the check and the write are one atomic step.
func (s *Store) RequeueOrphanedBlock(ctx context.Context, blockHash string, orphanedAt time.Time) (bool, error) {
	const q = `
UPDATE block_processing
SET reconciled_at = NULL
WHERE block_hash = $1
  AND status = 'orphaned'
  AND ($2::timestamptz IS NULL OR orphaned_at = $2)`
	var generation *time.Time
	if !orphanedAt.IsZero() {
		generation = &orphanedAt
	}
	tag, err := s.pool.Exec(ctx, q, blockHash, generation)
	if err != nil {
		return false, fmt.Errorf("requeue orphaned block %s: %w", blockHash, err)
	}
	return tag.RowsAffected() == 1, nil
}

// ReactivateBlock returns an orphaned row to active as a compare-and-set on
// the orphan generation (issue #339): the row must still be orphaned with the
// orphaned_at the caller judged (a zero orphanedAt checks status only), so a
// missing, active, parked or re-orphaned row is left untouched and reported
// as not applied. block_height is overwritten as the header-seen upsert does;
// the milestone timestamps are untouched. The WHERE is re-checked against the
// row's current version under READ COMMITTED, so the check and the write are
// one atomic step.
func (s *Store) ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error) {
	// orphaned_at is kept as the high-water mark the next orphaning mints
	// above; scanBlockProcessing hides it on an active row.
	const q = `
UPDATE block_processing
SET status = 'active', block_height = $2, reconciled_at = NULL
WHERE block_hash = $1
  AND status = 'orphaned'
  AND ($3::timestamptz IS NULL OR orphaned_at = $3)`
	var generation *time.Time
	if !orphanedAt.IsZero() {
		generation = &orphanedAt
	}
	tag, err := s.pool.Exec(ctx, q, blockHash, int64(blockHeight), generation) //nolint:gosec // block height fits in int64
	if err != nil {
		return false, fmt.Errorf("reactivate block %s: %w", blockHash, err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListOrphanedBlocksToReconcile returns the reconciler's work queue:
// orphaned rows not yet reconciled. Rows that can be re-anchored RIGHT NOW —
// the active (canonical) block at the same height already has a stored BUMP —
// sort ahead of rows that cannot, with oldest orphaned_at first WITHIN each
// group. Served by the idx_bp_orphaned_unreconciled partial index for the
// WHERE, with idx_bp_status_height + the bumps PK serving the re-anchorable
// probe.
//
// Why prioritize: the reconciler processes a small LIMIT per tick. A large
// backlog of orphans whose canonical block has NO stored BUMP (they can only
// be parked/deferred, never re-anchored right now) would, under a plain
// orphaned_at ASC ordering, starve a genuinely re-anchorable recent incident
// sitting behind them oldest-first. Ordering the re-anchorable ones first
// guarantees the tick spends its budget on work it can actually complete.
//
// The re-anchorable signal mirrors reconcileBlock: it resolves the canonical
// block at the orphan's height (chaintracks header) and re-anchors from that
// block's stored compound BUMP. The active block_processing row at a height is
// the store's record of that canonical block (the tie-scan/tracker keeps the
// competition winner 'active' and marks losers 'orphaned'), so "an active row
// at bp.block_height whose block_hash has a bumps row" is the in-SQL proxy for
// "the canonical block's BUMP is stored". The a.status='active' filter is what
// makes this correct: an orphan RETAINS its own compound BUMP, so a bare
// "any bump at this height" test would falsely flag every orphan re-anchorable.
func (s *Store) ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	const q = `
SELECT bp.block_hash, bp.block_height, bp.header_seen_at, bp.processed_at, bp.bump_built_at, bp.status, bp.orphaned_at, bp.reconciled_at
FROM block_processing bp
WHERE bp.status = 'orphaned' AND bp.reconciled_at IS NULL
ORDER BY
    EXISTS (
        SELECT 1 FROM block_processing a
        JOIN bumps b ON b.block_hash = a.block_hash
        WHERE a.block_height = bp.block_height AND a.status = 'active'
    ) DESC,
    bp.orphaned_at ASC NULLS LAST
LIMIT $1`
	rows, err := s.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list orphaned blocks to reconcile: %w", err)
	}
	defer rows.Close()
	var out []*models.BlockProcessingStatus
	for rows.Next() {
		bp, err := scanBlockProcessing(rows.Scan)
		if err != nil {
			return out, err
		}
		out = append(out, bp)
	}
	return out, rows.Err()
}

func (s *Store) MarkBlocksParked(ctx context.Context, blockHashes []string) error {
	if len(blockHashes) == 0 {
		return nil
	}
	// Only 'active' rows park: an orphaned row is off-chain (a more specific
	// terminal state) and must not be relabeled as a missing-BUMP backlog.
	const q = `
UPDATE block_processing
SET status = 'parked'
WHERE block_hash = ANY($1) AND status = 'active'`
	_, err := s.pool.Exec(ctx, q, blockHashes)
	if err != nil {
		return fmt.Errorf("mark blocks parked: %w", err)
	}
	return nil
}

func (s *Store) GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error) {
	const q = `
SELECT block_hash, block_height, header_seen_at, processed_at, bump_built_at, status, orphaned_at, reconciled_at
FROM block_processing
WHERE block_hash = $1`
	row := s.pool.QueryRow(ctx, q, blockHash)
	bp, err := scanBlockProcessing(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get block processing %s: %w", blockHash, err)
	}
	return bp, nil
}

func (s *Store) ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if limit <= 0 {
		return nil, nil
	}
	const q = `
SELECT block_hash, block_height, header_seen_at, processed_at, bump_built_at, status, orphaned_at, reconciled_at
FROM block_processing
WHERE ($1 = 0 OR block_height < $1)
ORDER BY block_height DESC
LIMIT $2`
	rows, err := s.pool.Query(ctx, q, int64(beforeHeight), limit) //nolint:gosec // block height fits in int64
	if err != nil {
		return nil, fmt.Errorf("list block processing: %w", err)
	}
	defer rows.Close()
	var out []*models.BlockProcessingStatus
	for rows.Next() {
		bp, err := scanBlockProcessing(rows.Scan)
		if err != nil {
			return out, err
		}
		out = append(out, bp)
	}
	return out, rows.Err()
}

func (s *Store) GetActiveTipBlockHeight(ctx context.Context) (uint64, error) {
	const q = `
SELECT COALESCE(MAX(block_height), 0)
FROM block_processing
WHERE status = 'active'`
	var height int64
	if err := s.pool.QueryRow(ctx, q).Scan(&height); err != nil {
		return 0, fmt.Errorf("get active tip height: %w", err)
	}
	if height < 0 {
		return 0, nil
	}
	return uint64(height), nil
}

func (s *Store) ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if limit <= 0 {
		return nil, nil
	}
	const q = `
SELECT block_hash, block_height, header_seen_at, processed_at, bump_built_at, status, orphaned_at, reconciled_at
FROM block_processing
WHERE processed_at IS NULL
  AND status = 'active'
  AND header_seen_at < $1
  AND block_height >= $2
ORDER BY header_seen_at ASC
LIMIT $3`
	rows, err := s.pool.Query(ctx, q, olderThan, int64(minHeight), limit) //nolint:gosec // height fits in int64
	if err != nil {
		return nil, fmt.Errorf("list stale block processing: %w", err)
	}
	defer rows.Close()
	var out []*models.BlockProcessingStatus
	for rows.Next() {
		bp, err := scanBlockProcessing(rows.Scan)
		if err != nil {
			return out, err
		}
		out = append(out, bp)
	}
	return out, rows.Err()
}

// scanBlockProcessing decodes one block_processing row. The scan callback
// shape lets us share this between QueryRow.Scan and Rows.Scan.
func scanBlockProcessing(scan func(...any) error) (*models.BlockProcessingStatus, error) {
	var (
		bp           models.BlockProcessingStatus
		height       int64
		processed    *time.Time
		bumpBuilt    *time.Time
		statusVal    string
		orphanedAt   *time.Time
		reconciledAt *time.Time
		seenAt       time.Time
	)
	if err := scan(&bp.BlockHash, &height, &seenAt, &processed, &bumpBuilt, &statusVal, &orphanedAt, &reconciledAt); err != nil {
		return nil, err
	}
	bp.BlockHeight = uint64(height) //nolint:gosec // height non-negative in storage
	bp.HeaderSeenAt = seenAt
	bp.ProcessedAt = processed
	bp.BUMPBuiltAt = bumpBuilt
	bp.Status = models.BlockProcessingStatusValue(statusVal)
	// orphaned_at survives a reactivation as the generation high-water mark;
	// it is current — and surfaced — only while the row is orphaned.
	if bp.Status == models.BlockStatusOrphaned {
		bp.OrphanedAt = orphanedAt
	}
	bp.ReconciledAt = reconciledAt
	return &bp, nil
}

// --- Submissions ---

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now()
	}
	const q = `
INSERT INTO submissions (submission_id, txid, callback_url, callback_token,
    full_status_updates, retry_count, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (submission_id) DO NOTHING`
	_, err := s.pool.Exec(
		ctx, q,
		sub.SubmissionID, sub.TxID, sub.CallbackURL, sub.CallbackToken,
		sub.FullStatusUpdates, sub.RetryCount, sub.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert submission: %w", err)
	}
	return nil
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	return s.submissions(ctx, "txid = $1", txid)
}

func (s *Store) GetSubmissionsByToken(ctx context.Context, token string) ([]*models.Submission, error) {
	return s.submissions(ctx, "callback_token = $1", token)
}

// tokensForTxIDsChunk bounds how many txids ride in one TokensForTxIDs
// query. A bulk MINED event carries up to the bump-builder's
// maxTxIDsPerBulkEvent (5000) txids, so a whole batch resolves in a handful
// of round-trips while each statement keeps a modest parameter array.
const tokensForTxIDsChunk = 1000

// TokensForTxIDs answers the SSE fan-out's membership question for a whole
// batch of txids in one statement per chunk. It replaces a per-(event,
// client) EXISTS probe that measured 0.583 ms under load against a 4.5M-row
// submissions table and capped fan-out at ~1.6k events/s.
//
// The plan is an Index Only Scan of idx_sub_txid_token: (txid,
// callback_token) covers both the predicate and the projection, so a match
// costs no heap fetch. That is also why schema.sql DROPS the narrower
// idx_sub_txid — verified by EXPLAIN, while it exists the planner costs it
// lower, picks it, and pays the heap fetch anyway. Access is exclusively from
// the txid side: a txid has a handful of submissions, a token can have
// millions (interface contract).
func (s *Store) TokensForTxIDs(ctx context.Context, txids []string) (map[string][]string, error) {
	const q = `
SELECT DISTINCT txid, callback_token
FROM submissions
WHERE txid = ANY($1) AND callback_token IS NOT NULL AND callback_token <> ''`
	out := make(map[string][]string, len(txids))
	for start := 0; start < len(txids); start += tokensForTxIDsChunk {
		end := min(start+tokensForTxIDsChunk, len(txids))
		if err := s.appendTokens(ctx, q, txids[start:end], out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// appendTokens runs one TokensForTxIDs chunk and folds its rows into out.
// Split out so the rows cursor is released per chunk instead of accumulating
// across the whole batch.
func (s *Store) appendTokens(ctx context.Context, q string, txids []string, out map[string][]string) error {
	rows, err := s.pool.Query(ctx, q, txids)
	if err != nil {
		return fmt.Errorf("tokens for txids: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var txid, token string
		if err := rows.Scan(&txid, &token); err != nil {
			return fmt.Errorf("scan submission token row: %w", err)
		}
		out[txid] = append(out[txid], token)
	}
	return rows.Err()
}

func (s *Store) IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error {
	// Projection only — no raw_tx, no merkle_path, no enrichment. This is
	// the SSE catchup hot path over potentially millions of submissions.
	q := `
SELECT t.txid, t.status, t.timestamp_at, COALESCE(t.block_hash, ''), COALESCE(t.block_height, 0)
FROM transactions t
WHERE t.txid IN (SELECT DISTINCT txid FROM submissions WHERE callback_token = $1)`
	args := []any{callbackToken}
	if !since.IsZero() {
		args = append(args, since)
		q += fmt.Sprintf(" AND t.timestamp_at > $%d", len(args))
	}
	if len(onlyStatuses) > 0 {
		vals := make([]string, len(onlyStatuses))
		for i, st := range onlyStatuses {
			vals[i] = string(st)
		}
		args = append(args, vals)
		q += fmt.Sprintf(" AND t.status = ANY($%d)", len(args))
	}
	// No LIMIT here, deliberately, and it is not an oversight.
	//
	// The caller's frame budget is SOFT at a timestamp boundary: timestamp_at
	// is microsecond-resolution and SetMinedByTxIDs stamps a whole block's rows
	// with one value, so the caller must be able to read past its budget to
	// finish an equal-timestamp run. A cursor that stops mid-run cannot resume
	// — the filter above is strictly-after — and the remainder is lost forever.
	// That was a real, silent data-loss bug on the Last-Event-ID path; a hard
	// SQL LIMIT would reimpose exactly the cut that caused it.
	//
	// Bounding this properly needs a keyset cursor on (timestamp_at, txid),
	// which makes the position a total order and therefore resumable mid-tie.
	// LIMIT becomes safe then, and lands with it. Until then the cost is a
	// planned sort over the token's matching set — idx_sub_token covers the
	// semi-join and idx_tx_updated the ordering — and rows stream, with the
	// caller stopping early once its budget and any trailing tie are satisfied.
	q += " ORDER BY t.timestamp_at ASC"

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("iterate statuses by token: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		st := &models.TransactionStatus{}
		var status string
		var height int64
		if err := rows.Scan(&st.TxID, &status, &st.Timestamp, &st.BlockHash, &height); err != nil {
			return fmt.Errorf("scan status row: %w", err)
		}
		st.Status = models.Status(status)
		st.BlockHeight = uint64(height) //nolint:gosec // heights are non-negative
		if err := fn(st); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) submissions(ctx context.Context, where string, arg any) ([]*models.Submission, error) {
	q := `
SELECT submission_id, txid, callback_url, callback_token, full_status_updates,
       last_delivered_status, retry_count, next_retry_at, attempts,
       last_attempt_at, last_result, created_at
FROM submissions WHERE ` + where
	rows, err := s.pool.Query(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*models.Submission
	for rows.Next() {
		sub, err := scanSubmission(rows)
		if err != nil {
			return out, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// scanSubmission reads one submissions row in the canonical column order
// (shared by submissions and ListSubmissionsReadyForRetry).
func scanSubmission(row rowScanner) (*models.Submission, error) {
	sub := &models.Submission{}
	var lastStatus, lastResult *string
	var nextRetry, lastAttempt *time.Time
	if err := row.Scan(
		&sub.SubmissionID, &sub.TxID, &sub.CallbackURL, &sub.CallbackToken,
		&sub.FullStatusUpdates, &lastStatus, &sub.RetryCount, &nextRetry,
		&sub.Attempts, &lastAttempt, &lastResult, &sub.CreatedAt,
	); err != nil {
		return nil, err
	}
	if lastStatus != nil {
		sub.LastDeliveredStatus = models.Status(*lastStatus)
	}
	if nextRetry != nil {
		t := *nextRetry
		sub.NextRetryAt = &t
	}
	if lastAttempt != nil {
		t := *lastAttempt
		sub.LastAttemptAt = &t
	}
	if lastResult != nil {
		sub.LastResult = *lastResult
	}
	return sub, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	const q = `
UPDATE submissions SET last_delivered_status=$2, retry_count=$3, next_retry_at=$4
WHERE submission_id=$1`
	_, err := s.pool.Exec(ctx, q, submissionID, string(lastStatus), retryCount, nextRetry)
	if err != nil {
		return fmt.Errorf("update delivery: %w", err)
	}
	return nil
}

// UpdateDeliveryStatusCAS implements store.Store. The predicate matches both
// SQL NULL (submission has never been delivered) and the empty string (some
// callers may persist Status("") explicitly) when expected is the zero value,
// so the very first delivery for a submission claims correctly.
func (s *Store) UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (bool, error) {
	const q = `
UPDATE submissions
SET last_delivered_status=$3, retry_count=0, next_retry_at=NULL
WHERE submission_id=$1
  AND (
    ($2 = '' AND (last_delivered_status IS NULL OR last_delivered_status = ''))
    OR last_delivered_status = $2
  )`
	tag, err := s.pool.Exec(ctx, q, submissionID, string(expected), string(next))
	if err != nil {
		return false, fmt.Errorf("update delivery cas: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListSubmissionsReadyForRetry returns up to limit submissions whose POST
// previously failed (retry_count > 0) and whose backoff has elapsed
// (next_retry_at <= now). Backed by the idx_sub_retry_ready partial index so
// the scan stays proportional to the in-retry backlog, not to the full
// submissions table.
func (s *Store) ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error) {
	if limit <= 0 {
		return nil, nil
	}
	const q = `
SELECT submission_id, txid, callback_url, callback_token, full_status_updates,
       last_delivered_status, retry_count, next_retry_at, attempts,
       last_attempt_at, last_result, created_at
FROM submissions
WHERE retry_count > 0
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= $1
ORDER BY next_retry_at ASC
LIMIT $2`
	rows, err := s.pool.Query(ctx, q, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list submissions ready for retry: %w", err)
	}
	defer rows.Close()

	var out []*models.Submission
	for rows.Next() {
		sub, err := scanSubmission(rows)
		if err != nil {
			return out, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// RecordDeliveryAttempt implements store.Store: one atomic in-place update —
// attempts increments server-side so concurrent workers can't lose counts.
func (s *Store) RecordDeliveryAttempt(ctx context.Context, submissionID string, at time.Time, result string) error {
	const q = `
UPDATE submissions SET attempts = attempts + 1, last_attempt_at=$2, last_result=$3
WHERE submission_id=$1`
	if _, err := s.pool.Exec(ctx, q, submissionID, at, result); err != nil {
		return fmt.Errorf("record delivery attempt: %w", err)
	}
	return nil
}

// --- Leaser ---

// TryAcquireOrRenew uses a single CTE to perform CAS-like acquire-or-renew:
// the INSERT ... ON CONFLICT UPDATE fires only if the caller is the current
// holder OR the existing lease has expired. Any other case leaves the row
// unchanged and returns (zero, nil) to signal contention.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	expires := time.Now().Add(ttl)
	const q = `
INSERT INTO leases (name, holder, expires_at) VALUES ($1,$2,$3)
ON CONFLICT (name) DO UPDATE
SET holder=EXCLUDED.holder, expires_at=EXCLUDED.expires_at
WHERE leases.holder=EXCLUDED.holder OR leases.expires_at < NOW()
RETURNING expires_at`
	var got time.Time
	err := s.pool.QueryRow(ctx, q, name, holder, expires).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("acquire lease %s: %w", name, err)
	}
	return got, nil
}

func (s *Store) Release(ctx context.Context, name, holder string) error {
	const q = `DELETE FROM leases WHERE name=$1 AND holder=$2`
	_, err := s.pool.Exec(ctx, q, name, holder)
	if err != nil {
		return fmt.Errorf("release lease %s: %w", name, err)
	}
	return nil
}

// --- Datahub endpoint registry ---

func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if ep.URL == "" {
		return fmt.Errorf("upsert datahub endpoint: empty url")
	}
	// COALESCE, not EXCLUDED, on the policy columns: a write that carries no
	// policy must leave a previously recorded one alone. The configured-URL
	// seed re-upserts on every process start with no policy attached, and
	// overwriting would blank an announced endpoint's policy on each restart.
	const q = `
INSERT INTO datahub_endpoints (
    url, network, source, last_seen,
    mining_fee_satoshis, mining_fee_bytes,
    max_tx_size_policy, max_script_size_policy, max_tx_sigops_counts_policy)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (url) DO UPDATE SET
    network = EXCLUDED.network,
    source = EXCLUDED.source,
    last_seen = EXCLUDED.last_seen,
    mining_fee_satoshis = COALESCE(EXCLUDED.mining_fee_satoshis, datahub_endpoints.mining_fee_satoshis),
    mining_fee_bytes = COALESCE(EXCLUDED.mining_fee_bytes, datahub_endpoints.mining_fee_bytes),
    max_tx_size_policy = COALESCE(EXCLUDED.max_tx_size_policy, datahub_endpoints.max_tx_size_policy),
    max_script_size_policy = COALESCE(EXCLUDED.max_script_size_policy, datahub_endpoints.max_script_size_policy),
    max_tx_sigops_counts_policy = COALESCE(EXCLUDED.max_tx_sigops_counts_policy, datahub_endpoints.max_tx_sigops_counts_policy)`

	var sats, byts, maxTx, maxScript, maxSigops *int64
	if p := ep.Policy; p != nil {
		if err := p.Validate(); err != nil {
			return err
		}
		// Narrowing is bounded by Validate above.
		sats = ptrInt64(p.MiningFeeSatoshis)
		byts = ptrInt64(p.MiningFeeBytes)
		maxTx = ptrInt64(p.MaxTxSizePolicy)
		maxScript = ptrInt64(p.MaxScriptSizePolicy)
		maxSigops = ptrInt64(p.MaxTxSigopsCountsPolicy)
	}

	if _, err := s.pool.Exec(ctx, q, ep.URL, ep.Network, ep.Source, ep.LastSeen,
		sats, byts, maxTx, maxScript, maxSigops); err != nil {
		return fmt.Errorf("upsert datahub endpoint %s: %w", ep.URL, err)
	}
	return nil
}

// ptrInt64 narrows a policy value bounded by EndpointPolicy.Validate and
// returns a pointer, so a nil policy writes SQL NULL rather than 0.
func ptrInt64(v uint64) *int64 {
	n := int64(v) //nolint:gosec // bounded by EndpointPolicy.Validate
	return &n
}

// endpointPolicyFromCols rebuilds an advertised policy from its five nullable
// columns. All-NULL means the node advertised none — including every row
// written before the columns existed — and yields a nil policy. The five are
// always written together, so a partial NULL is not expected; it is tolerated
// by reading the missing value as 0 rather than discarding the rest.
func endpointPolicyFromCols(sats, byts, maxTx, maxScript, maxSigops *int64) *store.EndpointPolicy {
	if sats == nil && byts == nil && maxTx == nil && maxScript == nil && maxSigops == nil {
		return nil
	}
	u := func(v *int64) uint64 {
		if v == nil || *v < 0 {
			return 0
		}
		return uint64(*v)
	}
	return &store.EndpointPolicy{
		MiningFeeSatoshis:       u(sats),
		MiningFeeBytes:          u(byts),
		MaxTxSizePolicy:         u(maxTx),
		MaxScriptSizePolicy:     u(maxScript),
		MaxTxSigopsCountsPolicy: u(maxSigops),
	}
}

func (s *Store) ListDatahubEndpoints(ctx context.Context, network string) ([]store.DatahubEndpoint, error) {
	const q = `SELECT url, network, source, last_seen,
       mining_fee_satoshis, mining_fee_bytes,
       max_tx_size_policy, max_script_size_policy, max_tx_sigops_counts_policy
FROM datahub_endpoints WHERE network = $1`
	rows, err := s.pool.Query(ctx, q, network)
	if err != nil {
		return nil, fmt.Errorf("list datahub endpoints: %w", err)
	}
	defer rows.Close()
	var out []store.DatahubEndpoint
	for rows.Next() {
		var (
			ep                                      store.DatahubEndpoint
			sats, byts, maxTx, maxScript, maxSigops *int64
		)
		if err := rows.Scan(&ep.URL, &ep.Network, &ep.Source, &ep.LastSeen,
			&sats, &byts, &maxTx, &maxScript, &maxSigops); err != nil {
			return nil, fmt.Errorf("scan datahub endpoint: %w", err)
		}
		ep.Policy = endpointPolicyFromCols(sats, byts, maxTx, maxScript, maxSigops)
		out = append(out, ep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter datahub endpoints: %w", err)
	}
	return out, nil
}

func (s *Store) UpsertPeerPolicy(ctx context.Context, pp store.PeerPolicy) error {
	if err := pp.Validate(); err != nil {
		return err
	}
	const q = `
INSERT INTO peer_policies (peer_id, network, mining_fee_satoshis, mining_fee_bytes, max_tx_size_policy, max_script_size_policy, last_seen)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (peer_id) DO UPDATE SET
    network = EXCLUDED.network,
    mining_fee_satoshis = EXCLUDED.mining_fee_satoshis,
    mining_fee_bytes = EXCLUDED.mining_fee_bytes,
    max_tx_size_policy = EXCLUDED.max_tx_size_policy,
    max_script_size_policy = EXCLUDED.max_script_size_policy,
    last_seen = EXCLUDED.last_seen`
	// Every narrowing is safe: Validate rejected anything above MaxInt64 above.
	var (
		sats      = int64(pp.MiningFeeSatoshis)   //nolint:gosec // bounded by PeerPolicy.Validate
		byts      = int64(pp.MiningFeeBytes)      //nolint:gosec // bounded by PeerPolicy.Validate
		maxTx     = int64(pp.MaxTxSizePolicy)     //nolint:gosec // bounded by PeerPolicy.Validate
		maxScript = int64(pp.MaxScriptSizePolicy) //nolint:gosec // bounded by PeerPolicy.Validate
	)
	if _, err := s.pool.Exec(ctx, q, pp.PeerID, pp.Network, sats, byts, maxTx, maxScript, pp.LastSeen); err != nil {
		return fmt.Errorf("upsert peer policy %s: %w", pp.PeerID, err)
	}
	return nil
}

func (s *Store) ListPeerPolicies(ctx context.Context, network string) ([]store.PeerPolicy, error) {
	const q = `SELECT peer_id, network, mining_fee_satoshis, mining_fee_bytes,
       max_tx_size_policy, max_script_size_policy, last_seen
FROM peer_policies WHERE network = $1`
	rows, err := s.pool.Query(ctx, q, network)
	if err != nil {
		return nil, fmt.Errorf("list peer policies: %w", err)
	}
	defer rows.Close()
	var out []store.PeerPolicy
	for rows.Next() {
		var (
			pp        store.PeerPolicy
			sats      int64
			byts      int64
			maxTx     int64
			maxScript int64
		)
		if err := rows.Scan(&pp.PeerID, &pp.Network, &sats, &byts, &maxTx, &maxScript, &pp.LastSeen); err != nil {
			return nil, fmt.Errorf("scan peer policy: %w", err)
		}
		pp.MiningFeeSatoshis = uint64(sats)        //nolint:gosec // stored non-negative
		pp.MiningFeeBytes = uint64(byts)           //nolint:gosec // stored non-negative
		pp.MaxTxSizePolicy = uint64(maxTx)         //nolint:gosec // stored non-negative
		pp.MaxScriptSizePolicy = uint64(maxScript) //nolint:gosec // stored non-negative
		out = append(out, pp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter peer policies: %w", err)
	}
	return out, nil
}

// --- scan helpers ---

type rowScanner interface {
	Scan(dest ...any) error
}

// scanStatusWithInserted is scanStatus + the boolean `inserted` column
// returned by BatchGetOrInsertStatus. Kept separate so the row layout for
// the single-row paths stays identical to the existing scanStatus.
func scanStatusWithInserted(row rowScanner) (*models.TransactionStatus, bool, error) {
	var (
		st                 models.TransactionStatus
		statusCode         *int
		blockHash          *string
		blockHeight        *int64
		merklePath         []byte
		extraInfo          *string
		competingTxs       []byte
		rawTx              []byte
		nextRetry          *time.Time
		merkleRegisteredAt *time.Time
		inserted           bool
	)
	if err := row.Scan(
		&st.TxID, &st.Status, &statusCode,
		&blockHash, &blockHeight, &merklePath,
		&extraInfo, &competingTxs, &rawTx,
		&st.RetryCount, &nextRetry,
		&st.Timestamp, &st.CreatedAt, &merkleRegisteredAt, &inserted,
	); err != nil {
		return nil, false, err
	}
	if statusCode != nil {
		st.StatusCode = *statusCode
	}
	if blockHash != nil {
		st.BlockHash = *blockHash
	}
	if blockHeight != nil {
		st.BlockHeight = uint64(*blockHeight) //nolint:gosec // block height fits in either signed/unsigned 64-bit
	}
	if len(merklePath) > 0 {
		st.MerklePath = merklePath
	}
	if extraInfo != nil {
		st.ExtraInfo = *extraInfo
	}
	if len(competingTxs) > 0 {
		_ = json.Unmarshal(competingTxs, &st.CompetingTxs)
	}
	if len(rawTx) > 0 {
		st.RawTx = rawTx
	}
	if nextRetry != nil {
		st.NextRetryAt = *nextRetry
	}
	if merkleRegisteredAt != nil {
		st.MerkleRegisteredAt = *merkleRegisteredAt
	}
	return &st, inserted, nil
}

func scanStatus(row rowScanner) (*models.TransactionStatus, error) {
	var (
		st                 models.TransactionStatus
		statusCode         *int
		blockHash          *string
		blockHeight        *int64
		merklePath         []byte
		extraInfo          *string
		competingTxs       []byte
		rawTx              []byte
		nextRetry          *time.Time
		merkleRegisteredAt *time.Time
	)
	if err := row.Scan(
		&st.TxID, &st.Status, &statusCode,
		&blockHash, &blockHeight, &merklePath,
		&extraInfo, &competingTxs, &rawTx,
		&st.RetryCount, &nextRetry,
		&st.Timestamp, &st.CreatedAt, &merkleRegisteredAt,
	); err != nil {
		return nil, err
	}
	if statusCode != nil {
		st.StatusCode = *statusCode
	}
	if blockHash != nil {
		st.BlockHash = *blockHash
	}
	if blockHeight != nil {
		st.BlockHeight = uint64(*blockHeight) //nolint:gosec // block height fits in either signed/unsigned 64-bit
	}
	if len(merklePath) > 0 {
		st.MerklePath = merklePath
	}
	if extraInfo != nil {
		st.ExtraInfo = *extraInfo
	}
	if len(competingTxs) > 0 {
		_ = json.Unmarshal(competingTxs, &st.CompetingTxs)
	}
	if len(rawTx) > 0 {
		st.RawTx = rawTx
	}
	if nextRetry != nil {
		st.NextRetryAt = *nextRetry
	}
	if merkleRegisteredAt != nil {
		st.MerkleRegisteredAt = *merkleRegisteredAt
	}
	return &st, nil
}

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

// DeleteBUMPByBlockHash removes the stored compound BUMP for a block and
// drops any cached parse. Idempotent.
func (s *Store) DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error {
	const q = `DELETE FROM bumps WHERE block_hash = $1`
	if _, err := s.pool.Exec(ctx, q, blockHash); err != nil {
		return fmt.Errorf("delete bump %s: %w", blockHash, err)
	}
	s.bumpCache.Remove(blockHash)
	return nil
}

// LookupPartitionKeys implements store.Store with one projected query.
func (s *Store) LookupPartitionKeys(ctx context.Context, txids []string) (map[string]store.ParentKey, error) {
	if len(txids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT txid, status, COALESCE(partition_key, '') FROM transactions WHERE txid = ANY($1)`, txids)
	if err != nil {
		return nil, fmt.Errorf("lookup partition keys: %w", err)
	}
	defer rows.Close()
	out := make(map[string]store.ParentKey, len(txids))
	for rows.Next() {
		var txid, status, key string
		if err := rows.Scan(&txid, &status, &key); err != nil {
			return nil, fmt.Errorf("lookup partition keys: %w", err)
		}
		out[txid] = store.ParentKey{Status: models.Status(status), PartitionKey: key}
	}
	return out, rows.Err()
}
