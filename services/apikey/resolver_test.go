package apikey

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/broadcaster/store"
)

type fakeLookup struct {
	mu    sync.Mutex
	keys  map[string]*store.APIKey
	err   error
	calls int
	gate  chan struct{}
}

func (f *fakeLookup) GetAPIKeyByHash(_ context.Context, hash string) (*store.APIKey, error) {
	f.mu.Lock()
	f.calls++
	gate, err, k := f.gate, f.err, f.keys[hash]
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, store.ErrAPIKeyNotFound
	}
	return k, nil
}

func (f *fakeLookup) set(fn func(f *fakeLookup)) { f.mu.Lock(); fn(f); f.mu.Unlock() }
func (f *fakeLookup) n() int                     { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestResolver(t *testing.T) (*Resolver, *fakeLookup, *fakeClock, string) {
	t.Helper()
	raw, err := Generate()
	require.NoError(t, err)
	lk := &fakeLookup{keys: map[string]*store.APIKey{
		Hash(raw): {ID: "key_1", Tier: "pro", Status: store.APIKeyActive},
	}}
	clk := &fakeClock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	r := NewResolver(lk, 30*time.Second, 10*time.Minute)
	r.now = clk.now
	return r, lk, clk, raw
}

// waitForRefresh wires r's background-refresh test hook to a channel so a
// test can deterministically wait for a background refresh attempt to
// finish (success or failure) instead of polling or sleeping -- which
// matters because that attempt runs in its own goroutine, reading the same
// fake clock the test's goroutine writes.
func waitForRefresh(r *Resolver) <-chan struct{} {
	done := make(chan struct{}, 8)
	r.onRefreshDone = func() { done <- struct{}{} }
	return done
}

func TestResolveCachesWithinTTL(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	for range 3 {
		k, err := r.Resolve(context.Background(), raw)
		require.NoError(t, err)
		require.Equal(t, "key_1", k.ID)
	}
	require.Equal(t, 1, lk.n())

	// The store is healthy, so an expired entry is re-read synchronously --
	// no background refresh involved.
	clk.advance(31 * time.Second)
	k, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "key_1", k.ID)
	require.Equal(t, 2, lk.n(), "an expired entry is re-read synchronously while the store is healthy")
}

func TestResolveRejections(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)

	_, err := r.Resolve(context.Background(), "not-a-key")
	require.ErrorIs(t, err, ErrUnauthorized)
	require.Equal(t, 0, lk.n(), "a malformed key never reaches the store")

	unknown, err := Generate()
	require.NoError(t, err)
	_, err = r.Resolve(context.Background(), unknown)
	require.ErrorIs(t, err, ErrUnauthorized)
	_, err = r.Resolve(context.Background(), unknown)
	require.ErrorIs(t, err, ErrUnauthorized)
	require.Equal(t, 1, lk.n(), "unknown keys are negatively cached")

	k, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "key_1", k.ID)

	// The store "revokes" the key by swapping in a brand-new record, not by
	// mutating the one the resolver already has cached: a real store never
	// hands back the same pointer twice, and the resolver must not depend
	// on it doing so.
	lk.set(func(f *fakeLookup) {
		f.keys[Hash(raw)] = &store.APIKey{ID: "key_1", Tier: "pro", Status: store.APIKeyRevoked}
	})
	k, err = r.Resolve(context.Background(), raw)
	require.NoError(t, err, "still served from cache within the TTL")
	require.Equal(t, "key_1", k.ID)

	// The store is healthy, so the revocation is observed synchronously
	// once the TTL expires -- not delayed up to staleMax.
	clk.advance(31 * time.Second)
	_, err = r.Resolve(context.Background(), raw)
	require.ErrorIs(t, err, ErrUnauthorized, "revoked, seen synchronously because the store is healthy")

	past := clk.t.Add(-time.Second)
	expiring, err := Generate()
	require.NoError(t, err)
	lk.set(func(f *fakeLookup) {
		f.keys[Hash(expiring)] = &store.APIKey{ID: "key_2", Status: store.APIKeyActive, ExpiresAt: &past}
	})
	_, err = r.Resolve(context.Background(), expiring)
	require.ErrorIs(t, err, ErrUnauthorized, "expired")
}

// TestResolveReturnsIndependentCopies checks that the resolver's cache
// never shares a pointer with its callers: mutating what one Resolve call
// returned must not change what the next one returns.
func TestResolveReturnsIndependentCopies(t *testing.T) {
	r, _, _, raw := newTestResolver(t)

	k1, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	k1.Tier = "corrupted"

	k2, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "pro", k2.Tier, "mutating one caller's copy must not corrupt the cache for others")
	require.NotSame(t, k1, k2, "each call gets its own copy")
}

// TestResolveStaleOnStoreError checks the full lifecycle of a key the store
// fails to confirm: the first stale-eligible call pays for (and fails) a
// synchronous attempt, marking the key failing; stale keeps being served up
// to staleMax; past staleMax, the failure finally surfaces; and an
// uncached key gets no stale fallback at all while the store is down.
func TestResolveStaleOnStoreError(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })

	clk.advance(5 * time.Minute)
	k, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err, "falls back to stale once the synchronous attempt fails")
	require.Equal(t, "key_1", k.ID)
	require.Equal(t, 2, lk.n(), "exactly one synchronous attempt was made")

	clk.advance(6 * time.Minute) // total 11m from the original fetch: past the 10m staleMax
	_, err = r.Resolve(context.Background(), raw)
	require.ErrorIs(t, err, ErrUnavailable, "past stale max the store error surfaces")

	other, err := Generate()
	require.NoError(t, err)
	_, err = r.Resolve(context.Background(), other)
	require.ErrorIs(t, err, ErrUnavailable, "an uncached key cannot be verified while the store is down")
}

// TestResolveDeletedKeyDoesNotResurrect checks that once a key is confirmed
// deleted (negatively cached), a later store outage cannot fall back to the
// stale positive entry from before the deletion and serve the deleted key
// as if it were still valid.
func TestResolveDeletedKeyDoesNotResurrect(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { delete(f.keys, Hash(raw)) })

	// The store is healthy, so the deletion is observed synchronously.
	clk.advance(31 * time.Second) // past the 30s TTL
	_, err = r.Resolve(context.Background(), raw)
	require.ErrorIs(t, err, ErrUnauthorized, "the deletion is observed synchronously")

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second) // past the negative entry's TTL too
	_, err = r.Resolve(context.Background(), raw)
	require.ErrorIs(t, err, ErrUnavailable, "a deleted key must not resurrect from a stale positive entry during an outage")
}

// TestResolveStaleRetryThrottle checks that once a key is marked failing, it
// does not retry the store on every subsequent call -- only after
// staleRetryBackoff elapses, and then via a background refresh -- so a
// sustained outage does not turn every request into a fresh store attempt.
func TestResolveStaleRetryThrottle(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, 1, lk.n())

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second) // past the 30s TTL

	// The first call after going stale makes its own synchronous attempt
	// (the store is presumed healthy until proven otherwise); it fails, so
	// this call marks the key failing and serves stale.
	k, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err, "falls back to stale once the synchronous attempt fails")
	require.Equal(t, "key_1", k.ID)
	require.Equal(t, 2, lk.n(), "exactly one synchronous attempt was made")

	// Within the backoff, more calls are served stale without triggering a
	// background attempt.
	k, err = r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "key_1", k.ID)
	require.Equal(t, 2, lk.n(), "within the retry backoff, no new attempt is triggered")

	done := waitForRefresh(r)
	clk.advance(staleRetryBackoff + time.Millisecond)
	k, err = r.Resolve(context.Background(), raw)
	require.NoError(t, err, "still served immediately even though the backoff just passed")
	require.Equal(t, "key_1", k.ID)
	<-done
	require.Equal(t, 3, lk.n(), "past the backoff, a background attempt is triggered")
}

// TestResolveStaleOnlyWhileStoreIsFailing is the key regression test for
// the request-start-relative backoff bug, and for the healthy-store
// behavior that bug's original fix regressed: while the store is healthy,
// going stale costs a real (possibly slow or hung) synchronous attempt --
// so a revocation is bounded by ttl -- but once that attempt fails and the
// key is marked failing, every later call is immediate, and only one
// background refresh is ever in flight no matter how many concurrent
// callers arrive while it hangs.
func TestResolveStaleOnlyWhileStoreIsFailing(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, 1, lk.n())

	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate; f.err = errors.New("store down") })

	clk.advance(31 * time.Second) // past the 30s TTL, well within the 10m staleMax

	// Several concurrent callers all land on the same stale-but-not-yet-
	// failing entry. The store is presumed healthy, so each pays for a real
	// attempt -- but singleflight still merges them into exactly one.
	const n = 5
	results := make(chan error, n)
	for range n {
		go func() {
			_, resolveErr := r.Resolve(context.Background(), raw)
			results <- resolveErr
		}()
	}

	require.Eventually(t, func() bool { return lk.n() == 2 }, time.Second, time.Millisecond,
		"the stale-eligible callers share exactly one (blocking) store attempt")

	select {
	case early := <-results:
		t.Fatalf("a caller returned before the hung lookup was released (err=%v)", early)
	case <-time.After(50 * time.Millisecond):
	}

	close(gate) // let the hung lookup fail
	for range n {
		select {
		case resolveErr := <-results:
			require.NoError(t, resolveErr, "falls back to stale once the store is confirmed failing")
		case <-time.After(time.Second):
			t.Fatal("a caller did not return after the hung lookup was released")
		}
	}

	// The key is now marked failing. A later call is immediate, with no new
	// store attempt before the retry backoff passes.
	start := time.Now()
	k, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "key_1", k.ID)
	require.Less(t, time.Since(start), 500*time.Millisecond, "a known-failing key is served immediately")
	require.Equal(t, 2, lk.n(), "no new store attempt yet, within the retry backoff")
}

// TestResolveStaleRevocationSeenSynchronouslyWhileStoreHealthy is the
// coordinator's required regression test: a revocation (or any change) at
// a healthy store must be visible as soon as the TTL expires, not delayed
// up to staleMax the way it would be if every stale entry were served
// non-blocking regardless of store health.
func TestResolveStaleRevocationSeenSynchronouslyWhileStoreHealthy(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) {
		f.keys[Hash(raw)] = &store.APIKey{ID: "key_1", Tier: "pro", Status: store.APIKeyRevoked}
	})
	clk.advance(31 * time.Second) // past the 30s TTL; the store itself is healthy

	_, err = r.Resolve(context.Background(), raw)
	require.ErrorIs(t, err, ErrUnauthorized, "a healthy store's revocation is seen synchronously, not after staleMax")
}

func TestResolveSingleflight(t *testing.T) {
	r, lk, _, raw := newTestResolver(t)
	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate })
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.Resolve(context.Background(), raw)
			errs <- err
		}()
	}
	require.Eventually(t, func() bool { return lk.n() == 1 }, time.Second, time.Millisecond)
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, lk.n(), "concurrent misses for one key share a single store read")
}

// TestResolveContextCanceled checks that a caller whose context is
// canceled while a lookup for its key is in flight gets back ErrUnavailable
// (wrapping the cancellation) promptly, without waiting for the lookup --
// which keeps running in the background for any other caller sharing it.
func TestResolveContextCanceled(t *testing.T) {
	r, lk, _, raw := newTestResolver(t)
	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate })
	t.Cleanup(func() { close(gate) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Resolve(ctx, raw)
		done <- err
	}()

	require.Eventually(t, func() bool { return lk.n() == 1 }, time.Second, time.Millisecond, "the lookup must have started")
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrUnavailable)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Resolve did not return promptly after its context was canceled")
	}
}

// TestResolveServesStaleToAlreadyCanceledContext checks that once a key is
// already known failing, a caller whose ctx is already canceled still gets
// served the stale value: that path never even looks at ctx, since serving
// it (and deciding whether to kick off a background refresh) waits on
// nothing.
func TestResolveServesStaleToAlreadyCanceledContext(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second) // past the 30s TTL
	_, err = r.Resolve(context.Background(), raw)
	require.NoError(t, err, "the synchronous attempt fails, marking the key failing")
	require.True(t, r.isFailing(Hash(raw)), "the key is now known failing")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	k, err := r.Resolve(ctx, raw)
	require.NoError(t, err, "an already-canceled context still gets the stale value once the key is known failing")
	require.Equal(t, "key_1", k.ID)
}

// TestResolveCancelWhileHealthyReportsUnavailable checks the opposite case:
// while a stale entry's key is NOT yet known failing, a caller whose ctx is
// canceled during the synchronous attempt that triggers gets
// ErrUnavailable, not the (possibly already revoked) cached value. Serving
// stale here on a mere disconnect -- with no evidence the store itself is
// unavailable -- would let a just-revoked key go on authenticating for as
// long as its caller kept disconnecting mid-fetch.
func TestResolveCancelWhileHealthyReportsUnavailable(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate })
	t.Cleanup(func() { close(gate) })

	clk.advance(31 * time.Second) // past the 30s TTL, within staleMax; not yet failing

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = r.Resolve(ctx, raw)
	require.ErrorIs(t, err, ErrUnavailable, "a canceled caller gets unavailable while the store is still presumed healthy")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, r.isFailing(Hash(raw)), "a caller giving up does not itself mark the key failing")
}

// TestResolveClearsFailingOnSuccessfulFetch checks that a successful store
// round trip clears a failing marker even when that success is reached
// through the uncached/past-staleMax synchronous path rather than through
// the key's own background refresh -- otherwise the key would serve one
// extra stale response before a background refresh eventually caught up.
func TestResolveClearsFailingOnSuccessfulFetch(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second) // past the 30s TTL
	_, err = r.Resolve(context.Background(), raw)
	require.NoError(t, err, "falls back to stale, marking the key failing")
	require.True(t, r.isFailing(Hash(raw)))

	// The store recovers. Advancing past staleMax (from the ORIGINAL fetch)
	// routes the next call through Resolve's top-level fallback -- the same
	// path an uncached key uses -- rather than through resolveStale's own
	// background-refresh machinery.
	lk.set(func(f *fakeLookup) { f.err = nil })
	clk.advance(10 * time.Minute)
	k, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "key_1", k.ID)
	require.False(t, r.isFailing(Hash(raw)), "a successful fetch clears the failing marker even outside a background refresh")
}

// TestResolveFailedRefreshDoesNotRearmAfterClear: a background refresh
// that fails after a concurrent fetch succeeded (and cleared the failing
// marker) must not mark the key failing again -- the store was just seen
// healthy, so the key's next expiry should re-read synchronously, not
// serve stale.
func TestResolveFailedRefreshDoesNotRearmAfterClear(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second)
	_, err = r.Resolve(context.Background(), raw) // marks the key failing
	require.NoError(t, err)
	require.True(t, r.isFailing(Hash(raw)))

	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate })
	done := waitForRefresh(r)
	clk.advance(staleRetryBackoff + time.Millisecond)
	_, err = r.Resolve(context.Background(), raw) // kicks off the background refresh
	require.NoError(t, err)
	require.Eventually(t, func() bool { return lk.n() == 3 }, time.Second, time.Millisecond)

	r.clearFailing(Hash(raw)) // a concurrent fetch succeeds meanwhile
	close(gate)               // ...then the background attempt fails
	<-done
	require.False(t, r.isFailing(Hash(raw)), "a stale failure must not re-arm a marker a success cleared")
}

// TestResolveEvictionDropsFailingState: failing state is bounded by the
// positive cache -- a key marked failing and never requested again must
// not stay in the refresh map once its entry leaves the cache.
func TestResolveEvictionDropsFailingState(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second)
	_, err = r.Resolve(context.Background(), raw)
	require.NoError(t, err)
	require.True(t, r.isFailing(Hash(raw)))

	r.positive.Remove(Hash(raw))
	require.Zero(t, refreshLen(r), "evicting the positive entry drops its failing state")
}

// refreshLen is the number of keys currently tracked as failing.
func refreshLen(r *Resolver) int {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	return len(r.refresh)
}

// TestResolveFailedSyncAttemptAfterEvictionLeavesNoMarker: the synchronous
// attempt a stale entry triggers can outlive the entry itself (a flood of
// other keys evicts it while the store hangs), and at eviction time there
// is no marker yet for the eviction callback to clear. When the attempt
// then fails, the caller is still served the stale value it already holds,
// but no failing marker may be left behind: there is no entry to serve
// stale for, so it would be an orphan nothing clears -- and the bound on
// the refresh map by positiveCacheSize would not hold.
func TestResolveFailedSyncAttemptAfterEvictionLeavesNoMarker(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate; f.err = errors.New("store down") })
	clk.advance(31 * time.Second) // past the 30s TTL; not yet known failing

	type result struct {
		key *store.APIKey
		err error
	}
	done := make(chan result, 1)
	go func() {
		k, resolveErr := r.Resolve(context.Background(), raw)
		done <- result{k, resolveErr}
	}()
	require.Eventually(t, func() bool { return lk.n() == 2 }, time.Second, time.Millisecond,
		"the synchronous attempt must have started")

	r.positive.Remove(Hash(raw)) // evicted while the attempt hangs...
	close(gate)                  // ...which then fails

	select {
	case res := <-done:
		require.NoError(t, res.err, "the caller is still served the stale value it holds")
		require.Equal(t, "key_1", res.key.ID)
	case <-time.After(time.Second):
		t.Fatal("Resolve did not return after the hung lookup was released")
	}
	require.False(t, r.isFailing(Hash(raw)), "a failing marker must not outlive the evicted entry")
	require.Zero(t, refreshLen(r))
}

// TestResolveBackgroundKickoffSkipsClearedMarker: between a caller observing
// a key as failing and its kickoff taking refreshMu, the marker can be
// cleared -- the entry evicted, or a concurrent fetch succeeded. The kickoff
// must then do nothing rather than recreate the marker: a refresh started
// that way would, on failure, re-arm a marker for an entry that is no
// longer cached (an orphan), and on success only re-fetch a key the next
// synchronous lookup would fetch anyway.
func TestResolveBackgroundKickoffSkipsClearedMarker(t *testing.T) {
	r, lk, clk, raw := newTestResolver(t)
	_, err := r.Resolve(context.Background(), raw)
	require.NoError(t, err)

	lk.set(func(f *fakeLookup) { f.err = errors.New("store down") })
	clk.advance(31 * time.Second)
	_, err = r.Resolve(context.Background(), raw) // marks the key failing
	require.NoError(t, err)
	require.True(t, r.isFailing(Hash(raw)))
	require.Equal(t, 2, lk.n())

	// The caller has seen isFailing == true. Before its kickoff runs, the
	// entry is evicted, and the marker cleared with it.
	r.positive.Remove(Hash(raw))
	require.False(t, r.isFailing(Hash(raw)))

	// Any attempt started from here on would block in the store, so one
	// would be visible as a tracked in-flight marker and a third store call.
	gate := make(chan struct{})
	lk.set(func(f *fakeLookup) { f.gate = gate })
	t.Cleanup(func() { close(gate) })
	done := waitForRefresh(r)

	clk.advance(staleRetryBackoff + time.Millisecond)
	r.maybeRefreshInBackground(lk, Hash(raw), clk.now())

	require.Zero(t, refreshLen(r), "the kickoff must not recreate a marker for an untracked key")
	require.Equal(t, 2, lk.n(), "no background attempt is started for an untracked key")
	select {
	case <-done:
		t.Fatal("a background refresh ran for a key that is no longer tracked")
	case <-time.After(50 * time.Millisecond):
	}
}
