package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// clientBulkWriteWireVersion is the first wire version (MongoDB 8.0) with the
// client-level bulkWrite command.
const clientBulkWriteWireVersion = 25

// Values of Store.bulkWriteSupport.
const (
	bulkWriteUnknown int32 = iota
	bulkWriteSupported
	bulkWriteUnsupported
)

// NativeBatchStatusUpdate implements store.NativeBatchStatusUpdater: true when
// BatchUpdateStatusReturning will take the native path, false when it will
// run the per-row loop (server below 8.0 or 8.0 at a lower feature
// compatibility version, or the probe failed), so callers keep bounding the
// loop's per-row writes.
func (s *Store) NativeBatchStatusUpdate(ctx context.Context) bool {
	return s.clientBulkWriteSupported(ctx)
}

// BatchUpdateStatusReturning implements store.Store. On MongoDB 8.0+ a batch
// costs two round trips at any size, not one per row: one $in read of the
// current rows, then one client-level bulkWrite of version-guarded updates.
// Older servers use the per-row loop (store.BatchUpdateStatusReturningParallel
// over UpdateStatusReturning).
//
// Each row resolves exactly as UpdateStatusReturning would have at some point
// during the call:
//   - absent at the read: unknown txid, zero result (the read is the
//     linearization point; a row inserted afterwards is not written, as with
//     a per-row update that ran just before the insert);
//   - present but the lattice forbids the transition: skipped, Current = the
//     row as read (the read is the linearization point; the write would have
//     been a no-op);
//   - otherwise an update filtered on the row's _id, the version it was read
//     at and the lattice guard. If it matched, nothing wrote the row between
//     the read and the update, so the row as read is exactly the pre-image:
//     Prev. If it did not match, another writer got in between; that row is
//     re-run through UpdateStatusReturning, which resolves it afresh.
//
// The per-operation result of a verbose client bulkWrite is what makes this
// exact. An earlier bulk design for the block rewrites was rejected because a
// plain bulkWrite reports only aggregate counts, which cannot tell "this
// row's write landed and was superseded" from "it never landed". The version
// guard only works because every write to the transactions collection bumps
// version (see txDoc.Version).
//
// A txid repeated in one batch is written by its first occurrence in the bulk
// and its later occurrences one at a time afterwards, in input order, so each
// sees the previous one's write as its pre-image.
//
// On error the rows resolved so far are returned with it (zero for the rest),
// as the per-row loop does: a write-concern error or a failed chunk leaves
// that chunk's rows unknown and stops the rest.
//
// A server whose binary is 8.0 but whose feature compatibility version is
// still 7.0 (mid-upgrade) advertises wire version 25 yet refuses bulkWrite.
// When the first bulkWrite is refused as a command (refusedCommand), nothing
// was written, so the batch is re-run through the per-row loop and
// the store stops trying the native path.
//
// The version guard cannot see a row deleted (TTL expiry) and re-inserted at
// the same version between the read and the bulk; the window is the call's
// duration at the moment a row expires, and the re-inserted row's status is
// the RECEIVED an insert carries.
func (s *Store) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]store.StatusUpdate, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	for _, st := range statuses {
		if st == nil {
			return make([]store.StatusUpdate, len(statuses)), errors.New("mongodb: batch update status: nil status")
		}
	}
	if !s.clientBulkWriteSupported(ctx) {
		return store.BatchUpdateStatusReturningParallel(ctx, s, statuses)
	}
	b := statusBatch{s: s, statuses: statuses, out: make([]store.StatusUpdate, len(statuses)), start: time.Now()}
	err := b.run(ctx)
	if errors.Is(err, errBulkWriteRefused) {
		s.logger.Warn("mongodb: server refused client bulkWrite (feature compatibility version below 8.0?); using per-row status updates", zap.Error(err))
		atomic.StoreInt32(&s.bulkWriteSupport, bulkWriteUnsupported)
		return store.BatchUpdateStatusReturningParallel(ctx, s, statuses)
	}
	return b.out, err
}

// errBulkWriteRefused marks a first bulkWrite the server rejected as a
// command before running any operation: nothing was written.
var errBulkWriteRefused = errors.New("client bulkWrite refused")

// Server error codes for a bulkWrite the server rejects as a command, before
// running any operation: unknown command, command disabled at the current
// feature compatibility version, or a field the server's bulkWrite does not
// parse (MongoDB 7.0 ships an internal bulkWrite that rejects the client
// options).
const (
	codeCommandNotFound     = 59
	codeCommandNotSupported = 115
	codeIDLUnknownField     = 40415
)

// refusedCommand reports whether err is the server rejecting the bulkWrite
// command itself, so that no operation ran. The driver returns that as a
// plain CommandError, or as a ClientBulkWriteException carrying only a
// top-level error and no results.
func refusedCommand(err error) bool {
	refusal := func(code int) bool {
		return code == codeCommandNotFound || code == codeCommandNotSupported || code == codeIDLUnknownField
	}
	var bwe mongo.ClientBulkWriteException
	if errors.As(err, &bwe) {
		return bwe.WriteError != nil && bwe.PartialResult == nil && len(bwe.WriteErrors) == 0 &&
			len(bwe.WriteConcernErrors) == 0 && refusal(bwe.WriteError.Code)
	}
	var ce mongo.CommandError
	return errors.As(err, &ce) && refusal(int(ce.Code))
}

// clientBulkWriteSupported reports whether the server speaks client-level
// bulkWrite, asking it once. A failed probe is not cached, so a transient
// error only sends one batch down the per-row path.
func (s *Store) clientBulkWriteSupported(ctx context.Context) bool {
	switch atomic.LoadInt32(&s.bulkWriteSupport) {
	case bulkWriteSupported:
		return true
	case bulkWriteUnsupported:
		return false
	}
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var hello struct {
		MaxWireVersion int32 `bson:"maxWireVersion"`
	}
	if err := s.db.RunCommand(octx, doc(kv("hello", 1))).Decode(&hello); err != nil {
		s.logger.Warn("mongodb: could not read the server's wire version; using per-row status updates", zap.Error(err))
		return false
	}
	state := bulkWriteUnsupported
	if hello.MaxWireVersion >= clientBulkWriteWireVersion {
		state = bulkWriteSupported
	}
	atomic.StoreInt32(&s.bulkWriteSupport, state)
	return state == bulkWriteSupported
}

// statusBatch is one native BatchUpdateStatusReturning call.
type statusBatch struct {
	s        *Store
	statuses []*models.TransactionStatus
	out      []store.StatusUpdate
	start    time.Time
}

// pendingWrite is a row that the read found writable.
type pendingWrite struct {
	i      int // index into statuses
	before txDoc
}

func (b *statusBatch) run(ctx context.Context) error {
	// First occurrences go through the read and the bulk; later occurrences
	// of the same txid are re-run per row, after it.
	first := make(map[string]bool, len(b.statuses))
	var lead, again []int
	for i, st := range b.statuses {
		switch {
		case st.TxID == "":
			// Never names a row: unknown, as UpdateStatusReturning reports it.
		case first[st.TxID]:
			again = append(again, i)
		default:
			first[st.TxID] = true
			lead = append(lead, i)
		}
	}

	current, err := b.read(ctx, lead)
	if err != nil {
		return err
	}
	var writes []pendingWrite
	for _, i := range lead {
		st := b.statuses[i]
		d, ok := current[st.TxID]
		switch {
		case !ok:
			b.observe("", st.Status, outcomeNotFound)
		case !st.Status.CanTransitionFrom(models.Status(d.Status)):
			b.out[i] = store.StatusUpdate{Current: d.toStatus()}
			b.observe(d.Status, st.Status, outcomeSkippedLattice)
		default:
			writes = append(writes, pendingWrite{i: i, before: d})
		}
	}

	if b.s.afterStatusRead != nil {
		b.s.afterStatusRead()
	}
	contended, err := b.write(ctx, writes)
	if err != nil {
		return err
	}

	// Contended rows lost a race between the read and the bulk; repeats need
	// the previous occurrence's write in place. Both resolve per row: rows of
	// one txid in input order, different txids in parallel.
	groups := groupByTxID(b.statuses, mergeSorted(contended, again))
	return forEach(ctx, len(groups), b.s.rowConcurrency, func(g int) error {
		for _, i := range groups[g] {
			upd, err := b.s.UpdateStatusReturning(ctx, b.statuses[i])
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			b.out[i] = upd
		}
		return nil
	})
}

// groupByTxID splits idx (ascending) into one list per txid, each in input
// order, ordered by first appearance.
func groupByTxID(statuses []*models.TransactionStatus, idx []int) [][]int {
	pos := make(map[string]int, len(idx))
	var groups [][]int
	for _, i := range idx {
		g, ok := pos[statuses[i].TxID]
		if !ok {
			g = len(groups)
			pos[statuses[i].TxID] = g
			groups = append(groups, nil)
		}
		groups[g] = append(groups[g], i)
	}
	return groups
}

// read returns the current row (without raw_tx) of every txid at idx that
// exists. It reads the primary whatever the client's read preference: a
// lagging secondary would report a row absent or blocked, and those rows are
// not retried, so their updates would be lost. The per-row path never reads
// before writing, so it had no such exposure.
func (b *statusBatch) read(ctx context.Context, idx []int) (map[string]txDoc, error) {
	txids := make([]string, len(idx))
	for k, i := range idx {
		txids[k] = b.statuses[i].TxID
	}
	current := make(map[string]txDoc, len(idx))
	for _, chunk := range chunks(txids, inChunk) {
		qctx, cancel := b.s.queryCtx(ctx)
		cur, err := b.s.txPrimary.Find(qctx, doc(kv(fID, doc(kv(opIn, chunk)))), options.Find().SetProjection(projNoRawTx))
		if err != nil {
			cancel()
			return nil, fmt.Errorf("batch update status: read: %w", err)
		}
		var docs []txDoc
		err = cur.All(qctx, &docs)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("batch update status: read: %w", err)
		}
		for _, d := range docs {
			current[d.TxID] = d
		}
	}
	return current, nil
}

// write applies the version-guarded updates in chunks of batchSize, one
// client bulkWrite each. Rows whose update matched get Prev = the row as
// read; the indexes of rows whose update did not match are returned for a
// per-row retry. A chunk that fails leaves its rows unknown and stops the
// rest.
func (b *statusBatch) write(ctx context.Context, writes []pendingWrite) ([]int, error) {
	var contended []int
	now := time.Now()
	failChunk := func(chunk []pendingWrite) {
		for _, w := range chunk {
			b.observe(w.before.Status, b.statuses[w.i].Status, outcomeError)
		}
	}
	for start := 0; start < len(writes); start += b.s.batchSize {
		chunk := writes[start:min(start+b.s.batchSize, len(writes))]
		ops := make([]mongo.ClientBulkWrite, len(chunk))
		for k, w := range chunk {
			st := b.statuses[w.i]
			filter := append(doc(kv(fID, st.TxID), versionFilter(w.before.Version)), latticeFilter(st.Status)...)
			ops[k] = mongo.ClientBulkWrite{
				Database:   b.s.db.Name(),
				Collection: b.s.tx.Name(),
				Model:      mongo.NewClientUpdateOneModel().SetFilter(filter).SetUpdate(statusUpdate(st, now)),
			}
		}

		qctx, cancel := b.s.queryCtx(ctx)
		res, err := b.s.client.BulkWrite(qctx, ops, options.ClientBulkWrite().SetOrdered(false).SetVerboseResults(true))
		cancel()

		failed := map[int]mongo.WriteError{}
		if err != nil {
			if start == 0 && refusedCommand(err) {
				// The server refused the command itself before any operation
				// ran; the caller falls back to the per-row loop.
				return contended, fmt.Errorf("%w: %w", errBulkWriteRefused, err)
			}
			var bwe mongo.ClientBulkWriteException
			if !errors.As(err, &bwe) || bwe.WriteError != nil || len(bwe.WriteConcernErrors) > 0 || bwe.PartialResult == nil {
				// No operation's outcome is known: transport error, timeout,
				// top-level failure, or writes not majority committed.
				failChunk(chunk)
				return contended, fmt.Errorf("batch update status: %w", err)
			}
			failed = bwe.WriteErrors
			res = bwe.PartialResult
		}
		var firstErr error
		for k, w := range chunk {
			st := b.statuses[w.i]
			if we, ok := failed[k]; ok {
				b.observe(w.before.Status, st.Status, outcomeError)
				if firstErr == nil {
					firstErr = fmt.Errorf("batch update status of tx %s: %w", st.TxID, we)
				}
				continue
			}
			r, ok := res.UpdateResults[k]
			switch {
			case !ok:
				// No result for an operation that did not fail: its outcome is
				// unknown, so it must not be reported.
				b.observe(w.before.Status, st.Status, outcomeError)
				if firstErr == nil {
					firstErr = fmt.Errorf("batch update status of tx %s: no result returned", st.TxID)
				}
			case r.MatchedCount == 1:
				b.out[w.i] = store.StatusUpdate{Prev: w.before.toStatus()}
				b.observe(w.before.Status, st.Status, outcomeApplied)
			default:
				contended = append(contended, w.i)
			}
		}
		if firstErr != nil {
			return contended, firstErr
		}
	}
	return contended, nil
}

// observe records one row's StoreUpdateStatusDuration sample, as
// UpdateStatusReturning does per row. A row's duration is the batch's
// elapsed time when it resolved.
func (b *statusBatch) observe(from string, to models.Status, outcome string) {
	metrics.StoreUpdateStatusDuration.WithLabelValues(from, string(to), outcome).Observe(time.Since(b.start).Seconds())
}

// versionFilter matches a row still at the version it was read at. A row
// written before version existed decodes as 0 and has no field, which {$in:
// [0, null]} matches; versionBump treats a missing field as 0 the same way.
func versionFilter(v int64) bson.E {
	if v == 0 {
		return kv(fVersion, doc(kv(opIn, bson.A{0, nil})))
	}
	return kv(fVersion, v)
}

// mergeSorted merges two ascending index lists.
func mergeSorted(a, b []int) []int {
	out := make([]int, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0] < b[0] {
			out, a = append(out, a[0]), a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	return append(append(out, a...), b...)
}
