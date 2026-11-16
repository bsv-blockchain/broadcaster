package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/bsv-blockchain/broadcaster/store"
)

// upsertAttempts bounds the duplicate-key retry of an upsert: the loser of an
// insert race for a brand-new _id retries once or twice and updates the
// document the winner inserted; anything persistent is a real conflict.
const upsertAttempts = 3

// kv and doc are terse, keyed constructors for bson.D literals. Keyed
// bson.E literals keep go vet's composites check quiet and read better than
// the {Key:, Value:} spelling repeated hundreds of times.
func kv(key string, value any) bson.E { return bson.E{Key: key, Value: value} }

// doc always returns a non-nil document: a nil bson.D marshals as BSON null,
// which the server rejects as a filter or update.
func doc(elems ...bson.E) bson.D {
	if elems == nil {
		return bson.D{}
	}
	return bson.D(elems)
}

// idFilter matches one document by primary key.
func idFilter(id string) bson.D { return doc(kv(fID, id)) }

// closeCursor releases a server cursor even when the caller's ctx is already
// cancelled — the usual reason iteration stopped early. Without a live
// context the killCursors command would be skipped and the server would hold
// the cursor until its idle timeout.
func closeCursor(ctx context.Context, cur *mongo.Cursor) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	_ = cur.Close(cctx)
}

// dedupe drops empty and repeated ids, preserving first-seen order.
func dedupe(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// chunks splits ids into slices of at most n.
func chunks(ids []string, n int) [][]string {
	if n <= 0 {
		n = defaultBatchSize
	}
	out := make([][]string, 0, len(ids)/n+1)
	for start := 0; start < len(ids); start += n {
		out = append(out, ids[start:min(start+n, len(ids))])
	}
	return out
}

// chunkInts splits idx into slices of at most n.
func chunkInts(idx []int, n int) [][]int {
	if n <= 0 {
		n = defaultBatchSize
	}
	out := make([][]int, 0, len(idx)/n+1)
	for start := 0; start < len(idx); start += n {
		out = append(out, idx[start:min(start+n, len(idx))])
	}
	return out
}

// msNow is the millisecond-aligned write timestamp. msTrunc normalizes to UTC
// so that a value this package hands back to a caller compares equal (==, not
// just Equal) to the same instant read back from the server, which always
// decodes as UTC.
func msNow() time.Time { return msTrunc(time.Now()) }

// forEach is store.ForEach. conc is the caller's choice because this package
// has two shapes of loop with different limits: the per-row block rewrites
// are bounded by the connection pool (s.rowConcurrency), while loops that
// merely want the operator's shared batch knob pass store.BatchConcurrency()
// like the other backends' batch helpers do.
func forEach(ctx context.Context, n, conc int, fn func(i int) error) error {
	return store.ForEach(ctx, n, conc, fn)
}

// withDupKeyRetry runs an upsert and retries it when it lost the insert race
// for a brand-new _id (E11000): the retry finds the winner's document and
// updates it, which is what the caller meant all along.
func (s *Store) withDupKeyRetry(ctx context.Context, op func(octx context.Context) error) error {
	var err error
	for attempt := 0; attempt < upsertAttempts; attempt++ {
		// A fresh op_timeout_ms per attempt. Sharing one budget across the
		// loop is self-defeating for a retry that exists to ride out
		// contention on a brand-new key: the attempts that lose the race are
		// exactly the slow ones, so by the last attempt there can be no budget
		// left, and a recoverable duplicate-key race surfaces as a deadline.
		octx, cancel := s.opCtx(ctx)
		err = op(octx)
		cancel()
		if err == nil || !mongo.IsDuplicateKeyError(err) {
			return err
		}
	}
	return err
}

// awaitWithin runs fn on its own goroutine and returns its result, or ctx's
// error if the deadline passes first. It exists for calls that cannot take a
// context themselves — the SRV/TXT lookups behind a mongodb+srv:// URI are
// the case in this package — so that boot can stop waiting on them.
//
// What it bounds is the caller's wait, not fn: an abandoned fn keeps running
// until it returns on its own, and its result is discarded. For a DNS lookup
// that is the OS resolver's own timeout (attempts × per-query timeout, tens of
// seconds), which is what makes abandoning it acceptable — the goroutine is
// leaked for a bounded time, not forever.
func awaitWithin[T any](ctx context.Context, fn func() T) (T, error) {
	done := make(chan T, 1)
	go func() { done <- fn() }()
	select {
	case v := <-done:
		return v, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
