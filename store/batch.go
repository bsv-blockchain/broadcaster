package store

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/broadcaster/models"
)

// defaultBatchConcurrency caps how many parallel single-record store calls
// the helpers issue. Set to keep DB pool pressure modest on backends that
// don't have a native batch implementation.
//
// Exposed via SetBatchConcurrency so process bootstrap can align it with
// validator parallelism (which defaults to runtime.NumCPU). Stored as int32
// atomically so concurrent batch calls observe the latest value without a
// lock on every read.
var batchConcurrency = int32(runtime.NumCPU())

// SetBatchConcurrency overrides the parallel-loop helper concurrency at
// process start. Zero or negative values restore the runtime.NumCPU default.
// Safe to call at most once during bootstrap; concurrent batch calls observe
// the new value on their next iteration. Values above math.MaxInt32 clamp
// to math.MaxInt32 — far above any realistic DB pool size, but the bound
// satisfies gosec G115 without a per-call check on the read side.
func SetBatchConcurrency(n int) {
	if n <= 0 {
		n = runtime.NumCPU()
	}
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	atomic.StoreInt32(&batchConcurrency, int32(n))
}

func currentBatchConcurrency() int {
	return int(atomic.LoadInt32(&batchConcurrency))
}

// BatchConcurrency reports the parallel-loop concurrency configured via
// SetBatchConcurrency, for backends that run their own bounded per-row
// loops (e.g. the MongoDB block-scoped rewrites) and want to honour the
// same operator knob as the shared helpers.
func BatchConcurrency() int { return currentBatchConcurrency() }

// SingleStore is the narrow contract the parallel-loop helpers need: the
// single-record GetOrInsertStatus / UpdateStatus methods. Every Store
// satisfies this trivially. Defined as an interface so the helpers can be
// reused by any backend's BatchGetOrInsertStatus / BatchUpdateStatus
// implementation that wants the parallel-loop fallback.
type SingleStore interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error
}

// SingleStoreReturning extends SingleStore with the diagnostic-rich
// UpdateStatusReturning variant: one atomic applied-result per row. Prev
// is set only when the write landed; Current is set when the row is known
// and the lattice skipped it. Every Store backend implements it, and
// BatchUpdateStatusReturningParallel is the shared batch driver.
type SingleStoreReturning interface {
	UpdateStatusReturning(ctx context.Context, status *models.TransactionStatus) (StatusUpdate, error)
}

// ForEach runs fn(i) for every i in [0, n) with at most conc calls in flight.
// The first error is returned after every started call has finished; once
// ctx is done, or any call has failed, no further calls start.
//
// Stopping at the first failure is what keeps a per-row loop from turning an
// outage into a marathon: with the server gone, a million-row block would
// otherwise pay op_timeout_ms for every remaining row before returning, and
// the rows it did write meanwhile are exactly as partial as they would be
// after one failure. Callers get the same (partial results, first error)
// pair either way, just promptly.
func ForEach(ctx context.Context, n, conc int, fn func(i int) error) error {
	if conc < 1 {
		conc = 1
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	record := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}
	for i := 0; i < n; i++ {
		// Checked explicitly: a select with both a free slot and a done ctx
		// ready picks at random, which is how a cancelled loop still started
		// most of its calls.
		if err := ctx.Err(); err != nil {
			record(err)
			break
		}
		if failed() {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			record(ctx.Err())
			wg.Wait()
			return firstErr
		}
		// Re-checked after the slot, not only before it: the send blocks
		// while every worker is busy, which is exactly when one of them
		// fails, so the check above was made against a state that is stale by
		// the time the slot frees. Without this, a failing loop dispatches
		// one more operation — bounded, but the contract above says none.
		//
		// A cancellation is recorded, exactly as the pre-send check records
		// it. Breaking out silently would leave firstErr nil on a loop that
		// skipped work, so ForEach would report success for a block it never
		// finished — the one outcome this helper must never produce, since
		// its callers read a nil error as "every row was written".
		if err := ctx.Err(); err != nil {
			record(err)
			<-sem
			break
		}
		if failed() {
			<-sem
			break
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(i); err != nil {
				record(err)
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

// BatchGetOrInsertStatusParallel runs GetOrInsertStatus concurrently for each
// row, bounded by BatchConcurrency. Result order matches input order. Like
// ForEach it stops starting rows at the first failure (or once ctx is done)
// and returns that error; rows that failed or never started get a zero-value
// BatchInsertResult, so callers can still iterate the slice safely and tell
// the rows that were inserted from the rest.
func BatchGetOrInsertStatusParallel(ctx context.Context, s SingleStore, statuses []*models.TransactionStatus) ([]BatchInsertResult, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	results := make([]BatchInsertResult, len(statuses))
	err := ForEach(ctx, len(statuses), currentBatchConcurrency(), func(i int) error {
		existing, inserted, err := s.GetOrInsertStatus(ctx, statuses[i])
		if err != nil {
			return err
		}
		if inserted {
			results[i] = BatchInsertResult{Inserted: true}
		} else {
			results[i] = BatchInsertResult{Existing: existing}
		}
		return nil
	})
	return results, err
}

// BatchUpdateStatusReturningParallel is the diagnostic-rich form of
// BatchUpdateStatusParallel. Each row goes through UpdateStatusReturning so
// the caller can observe transition-age metrics without an extra read.
// result[i].Prev is the applied pre-image; result[i].Current is a known
// lattice skip; both nil means the txid is absent or the row errored.
func BatchUpdateStatusReturningParallel(ctx context.Context, s SingleStoreReturning, statuses []*models.TransactionStatus) ([]StatusUpdate, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	out := make([]StatusUpdate, len(statuses))
	sem := make(chan struct{}, currentBatchConcurrency())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, st := range statuses {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			upd, err := s.UpdateStatusReturning(ctx, st)
			if err != nil && !errors.Is(err, ErrNotFound) {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if err == nil {
				out[i] = upd
			}
		}()
	}
	wg.Wait()
	return out, firstErr
}

// BatchUpdateStatusParallel runs UpdateStatus concurrently for each row,
// bounded by defaultBatchConcurrency. Returns the first error encountered.
// Per-row ErrNotFound is treated as a silent no-op so the batch contract
// matches Postgres' WHERE-clause semantics: unknown txids are skipped, not
// turned into a fatal batch error. (UpdateStatus itself still surfaces
// ErrNotFound to single-row callers — see store.Store.UpdateStatus.)
func BatchUpdateStatusParallel(ctx context.Context, s SingleStore, statuses []*models.TransactionStatus) error {
	if len(statuses) == 0 {
		return nil
	}

	sem := make(chan struct{}, currentBatchConcurrency())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, st := range statuses {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.UpdateStatus(ctx, st); err != nil && !errors.Is(err, ErrNotFound) {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}
