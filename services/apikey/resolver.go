package apikey

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

const (
	positiveCacheSize = 100_000
	negativeCacheSize = 10_000
	lookupTimeout     = 5 * time.Second
	staleRetryBackoff = 2 * time.Second
)

var (
	// ErrUnauthorized covers malformed, unknown, revoked and expired keys.
	ErrUnauthorized = errors.New("invalid api key")
	// ErrUnavailable means the key could not be verified: the store errored
	// and no usable cached entry exists.
	ErrUnavailable = errors.New("api key store unavailable")
)

// Lookup is the slice of store.APIKeyStore the resolver reads. An
// Introspector implements it for keys verified by an external provider.
type Lookup interface {
	GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error)
}

// Route sends every key issued under Prefix to Lookup. The resolver always
// routes Prefix ("bsvb_") to the API key store; WithRoutes adds the
// introspection providers from auth.introspection.
type Route struct {
	Prefix string
	Lookup Lookup
}

// ResolverOption configures a Resolver.
type ResolverOption func(*Resolver)

// WithRoutes adds routes for keys issued under other prefixes. Config
// validation keeps prefixes from overlapping; should two still match one
// key, the longer prefix wins.
func WithRoutes(routes ...Route) ResolverOption {
	return func(r *Resolver) { r.routes = append(r.routes, routes...) }
}

// cacheEntry is stored in both the positive and the negative cache. key is
// nil in the negative cache (and only there); fetched is when the entry was
// last confirmed against the store. The cache always holds its own copy of
// key, never the pointer a caller or the store handed in -- see cloneKey.
//
// Background-refresh bookkeeping (refreshState) deliberately does not live
// here: a background refresh goroutine writing this struct back to the LRU
// could otherwise clobber a fresher entry -- or resurrect a removed one --
// written by a different, concurrent path while it ran.
type cacheEntry struct {
	key     *store.APIKey
	fetched time.Time
}

// refreshState is the per-hash state for a key whose store lookup has
// failed at least once: retryAt is when the next background attempt may
// start (zero means the key is not known to be failing at all), and
// inFlight is whether one is currently running. Guarded by Resolver.refreshMu.
type refreshState struct {
	retryAt  time.Time
	inFlight bool
}

// Resolver maps raw keys to stored records through per-replica LRUs. An
// entry is trusted for ttl (the worst-case delay before a revocation or
// tier change takes effect); past that it is stale.
//
// A stale entry is served non-blocking -- stale-while-revalidate -- ONLY
// once the key is known to be failing (a synchronous attempt to refresh it
// has already failed). Until then, a stale entry is refreshed the same way
// an uncached key would be: synchronously, so a revocation or tier change
// is bounded by ttl, not staleMax, for as long as the store keeps working.
// Once a key is marked failing, further refresh attempts run in the
// background, deduplicated to at most one in flight per hash and throttled
// to one attempt per staleRetryBackoff -- armed before each attempt starts
// and re-armed from when it finishes, not from when it was requested, so a
// lookup that hangs for the full lookupTimeout cannot leave every caller in
// between re-triggering their own attempt once a request-start-relative
// backoff would (wrongly) look expired. Any successful store round trip --
// found or not, background or synchronous -- clears the failing state.
//
// A caller whose ctx is canceled while a stale entry is being synchronously
// refreshed gets ErrUnavailable, not the cached value: the key is not yet
// known to be failing, so there's no basis for trusting a possibly-revoked
// answer over a direct one. Once a key IS known failing, a canceled ctx
// makes no difference -- serving stale there waits on nothing.
//
// Unknown keys are cached too, but in a separate, smaller LRU, so a flood of
// random strings cannot evict legitimate keys from the positive cache.
// Throttling a single client's flood of invalid-key attempts is Middleware's
// job (the per-IP auth-failure limiter), not the resolver's.
type Resolver struct {
	routes   []Route // longest prefix first
	ttl      time.Duration
	staleMax time.Duration
	now      func() time.Time
	positive *lru.Cache[string, cacheEntry]
	negative *lru.Cache[string, cacheEntry]
	group    singleflight.Group

	refreshMu sync.Mutex
	refresh   map[string]refreshState

	// onRefreshDone, if set, is called after each background refresh
	// attempt finishes, successfully or not. It exists purely so tests can
	// wait deterministically for a refresh to complete instead of polling
	// or sleeping.
	onRefreshDone func()
}

// NewResolver builds a Resolver that verifies Prefix keys against lookup,
// plus any routes opts add.
func NewResolver(lookup Lookup, ttl, staleMax time.Duration, opts ...ResolverOption) *Resolver {
	r := &Resolver{
		routes: []Route{{Prefix: Prefix, Lookup: lookup}},
		ttl:    ttl, staleMax: staleMax, now: time.Now,
		refresh: make(map[string]refreshState),
	}
	for _, o := range opts {
		o(r)
	}
	slices.SortStableFunc(r.routes, func(a, b Route) int { return cmp.Compare(len(b.Prefix), len(a.Prefix)) })
	// Failing state only matters while hash has a positive entry to serve
	// stale (resolveStale), so it lives and dies with that entry: when the
	// entry leaves the cache (LRU eviction, or removal on a not-found), so
	// does its marker. Otherwise a key marked failing during an outage and
	// never requested again would stay in refresh forever; this bounds it
	// by positiveCacheSize. The bound holds only because the two paths
	// that write a marker never leave one behind for an entry that is
	// already gone: markFailing re-checks the cache after writing (a
	// synchronous attempt can outlive the stale entry it was refreshing),
	// and maybeRefreshInBackground declines to start for a hash whose
	// marker has been cleared since its caller looked. The callback runs
	// under the LRU's lock, which is safe: nothing holds refreshMu while
	// touching the caches.
	r.positive, _ = lru.NewWithEvict(positiveCacheSize, func(hash string, _ cacheEntry) { r.clearFailing(hash) }) // errors only for size <= 0
	r.negative, _ = lru.New[string, cacheEntry](negativeCacheSize)                                                // errors only for size <= 0
	return r
}

// Resolve returns the usable key for raw, ErrUnauthorized, or ErrUnavailable
// (also returned, wrapped, when ctx is canceled before a lookup in flight
// for raw completes -- the lookup itself keeps running for any other caller
// sharing it).
func (r *Resolver) Resolve(ctx context.Context, raw string) (*store.APIKey, error) {
	lk, ok := r.route(raw)
	if !ok {
		return nil, ErrUnauthorized
	}
	hash := Hash(raw)
	now := r.now()

	if e, ok := r.positive.Get(hash); ok {
		if now.Sub(e.fetched) < r.ttl {
			cacheResult("hit")
			return usable(e.key, now)
		}
		if now.Sub(e.fetched) < r.staleMax {
			return r.resolveStale(ctx, lk, hash, e, now)
		}
	}
	if e, ok := r.negative.Get(hash); ok && now.Sub(e.fetched) < r.ttl {
		cacheResult("negative")
		return nil, ErrUnauthorized
	}

	fr, ctxDone, err := r.syncFetch(ctx, lk, hash)
	if err != nil {
		if ctxDone {
			// The caller gave up, not the store; don't count this as a
			// store failure.
			cacheResult("canceled")
		} else {
			cacheResult("error")
		}
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return usable(fr.key, now)
}

// resolveStale handles a positive entry that is past its TTL but still
// within staleMax. See the Resolver doc comment for the two-tier policy
// this implements.
func (r *Resolver) resolveStale(ctx context.Context, lk Lookup, hash string, e cacheEntry, now time.Time) (*store.APIKey, error) {
	if !r.isFailing(hash) {
		fr, ctxDone, err := r.syncFetch(ctx, lk, hash)
		if err == nil {
			return usable(fr.key, now)
		}
		if ctxDone {
			// This caller gave up, not the store -- but while the key is
			// still presumed healthy, there's no evidence yet that a fresh
			// answer wasn't about to arrive. Serving the cached value here
			// would let a just-revoked key go on authenticating for as long
			// as its caller keeps disconnecting mid-fetch, so this reports
			// unavailable instead, same as an uncached lookup would. (Once
			// the key is actually known failing, the other branch below
			// serves stale unconditionally -- it has already earned that.)
			// The caller gave up, not the store, so this doesn't count as a
			// store failure either.
			cacheResult("canceled")
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		// The store itself failed. Mark the key failing from completion
		// time (not from when this attempt started) and fall back to the
		// stale value.
		r.markFailing(hash, r.now())
		cacheResult("failing")
		return usable(e.key, now)
	}

	//nolint:contextcheck // deliberately detached: the refresh must outlive
	// this request, so it always runs on context.Background
	// (see maybeRefreshInBackground), never on ctx.
	r.maybeRefreshInBackground(lk, hash, now)
	cacheResult("stale")
	return usable(e.key, now)
}

// syncFetch runs (or joins, via singleflight) a single store lookup for
// hash and waits for it, exactly as an uncached key would. ctxDone reports
// whether ctx.Done() ended the wait rather than the lookup itself
// finishing; callers that have a stale fallback available react to that
// differently than callers that don't.
func (r *Resolver) syncFetch(ctx context.Context, lk Lookup, hash string) (fr fetchResult, ctxDone bool, err error) {
	ch := r.group.DoChan(hash, func() (any, error) { return r.fetch(ctx, lk, hash) })
	select {
	case res := <-ch:
		if res.Err != nil {
			return fetchResult{}, false, res.Err
		}
		fr, _ = res.Val.(fetchResult)
		return fr, false, nil
	case <-ctx.Done():
		// This caller gave up; the shared fetch (if any) keeps running
		// detached from ctx (see fetch) so other callers waiting on the
		// same hash are unaffected.
		return fetchResult{}, true, ctx.Err()
	}
}

// cachedUsable reports whether raw is a well-formed key with a fresh,
// usable entry already sitting in the positive cache. It costs nothing
// beyond a cache lookup -- no store access, no singleflight -- so
// Middleware can use it to let a caller with a key already proven good skip
// the per-IP auth-failure throttle entirely.
func (r *Resolver) cachedUsable(raw string) bool {
	if _, ok := r.route(raw); !ok {
		return false
	}
	now := r.now()
	e, ok := r.positive.Get(Hash(raw))
	if !ok || now.Sub(e.fetched) >= r.ttl {
		return false
	}
	return e.key != nil && e.key.Usable(now)
}

// isFailing reports whether hash is currently known to be failing: a
// synchronous refresh attempt for it has failed and not yet been followed
// by a successful one.
func (r *Resolver) isFailing(hash string) bool {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	return !r.refresh[hash].retryAt.IsZero()
}

// markFailing records hash as failing, with its next eligible background
// attempt at finishedAt (when the attempt that discovered the failure
// completed) plus staleRetryBackoff.
//
// The attempt may have outlived the positive entry it was refreshing: a
// stale entry is as evictable as any other while its synchronous attempt
// hangs, and at eviction time there was no marker yet for the callback to
// clear. A marker only means something while there is an entry to serve
// stale, so if the entry is gone the marker is dropped again rather than
// left as an orphan that nothing would clear until the key happened to be
// looked up successfully. The cache is checked after the marker is written,
// not before, so an eviction between the two is covered as well (its
// callback removes what was just written); and outside refreshMu, because
// that callback takes refreshMu while the LRU's own lock is held.
func (r *Resolver) markFailing(hash string, finishedAt time.Time) {
	r.refreshMu.Lock()
	st := r.refresh[hash]
	st.retryAt = finishedAt.Add(staleRetryBackoff)
	r.refresh[hash] = st
	r.refreshMu.Unlock()

	if !r.positive.Contains(hash) {
		r.clearFailing(hash)
	}
}

// clearFailing removes any failing-state bookkeeping for hash. fetch calls
// it on every successful store round trip, found or not: a fresh answer
// means hash is no longer failing regardless of which caller's attempt
// produced it (a background refresh, or a synchronous one reached through
// the uncached/past-staleMax path). Without this, a key that recovers via
// that second path -- not through its own background refresh -- would
// serve one extra stale response before a background refresh eventually
// caught up, and a key that goes on to be negatively cached would never
// have its failing marker cleared at all. A no-op if hash wasn't failing.
func (r *Resolver) clearFailing(hash string) {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	delete(r.refresh, hash)
}

// maybeRefreshInBackground kicks off exactly one background refresh for an
// already-failing hash, if its backoff has passed and none is already
// running. The in-flight marker is claimed here, synchronously, under
// refreshMu, before the goroutine starts -- never inside it -- so two
// callers racing to see the same expired backoff can still only ever
// result in one goroutine per hash.
//
// A hash whose marker is gone by the time refreshMu is taken -- its entry
// was evicted, or a concurrent fetch succeeded, after the caller's
// isFailing check -- is left alone. Starting a refresh anyway would recreate
// the marker (as in flight) for an entry that no longer needs one, and a
// failed attempt would then re-arm it into exactly the orphan the eviction
// callback exists to prevent. The caller still serves the stale value it
// already holds; the next lookup for the key goes through the synchronous
// path, as an untracked key should.
func (r *Resolver) maybeRefreshInBackground(lk Lookup, hash string, now time.Time) {
	r.refreshMu.Lock()
	st, tracked := r.refresh[hash]
	if !tracked || st.inFlight || now.Before(st.retryAt) {
		r.refreshMu.Unlock()
		return
	}
	st.inFlight = true
	r.refresh[hash] = st
	r.refreshMu.Unlock()

	go func() {
		var fetchErr error
		defer func() {
			finishedAt := r.now()
			r.refreshMu.Lock()
			s, tracked := r.refresh[hash]
			if fetchErr != nil && tracked {
				// Re-arm from completion time, not kickoff time, so the
				// attempt's own duration (including a full hang up to
				// lookupTimeout) counts against the backoff too. Not if
				// the entry is gone: clearFailing ran meanwhile (a
				// concurrent fetch succeeded, or the positive entry was
				// evicted), and re-creating it would mark a key failing
				// against a store just seen healthy -- serving it stale
				// past ttl instead of re-reading it synchronously.
				s.inFlight = false
				s.retryAt = finishedAt.Add(staleRetryBackoff)
				r.refresh[hash] = s
			} else {
				delete(r.refresh, hash) // the key is healthy again
			}
			r.refreshMu.Unlock()
			if r.onRefreshDone != nil {
				r.onRefreshDone()
			}
		}()

		// Deliberately detached from any request: this refresh must outlive
		// whichever caller's stale read triggered it.
		ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
		defer cancel()
		_, fetchErr, _ = r.group.Do(hash, func() (any, error) { return r.fetch(ctx, lk, hash) })
	}()
}

// fetchResult wraps fetch's outcome so a not-found key (nil key, nil error)
// is never ambiguous with "fetch itself failed" -- singleflight's shared
// result is an any, and a bare *store.APIKey would make that nil/nil case
// indistinguishable from a zero value.
type fetchResult struct {
	key *store.APIKey // nil means confirmed not found, not an error
}

// fetch runs inside the singleflight group for hash, shared by every caller
// concurrently resolving the same key (a synchronous one waiting on it, or
// a background refresh, or both). It re-checks both caches first -- another
// goroutine may have just finished populating one while this call waited
// for the group slot -- before ever touching the store, and labels the
// eventual cache result itself so the caller doesn't have to infer it.
func (r *Resolver) fetch(ctx context.Context, lk Lookup, hash string) (fetchResult, error) {
	now := r.now()
	if e, ok := r.positive.Get(hash); ok && now.Sub(e.fetched) < r.ttl {
		cacheResult("hit")
		return fetchResult{key: e.key}, nil
	}
	if e, ok := r.negative.Get(hash); ok && now.Sub(e.fetched) < r.ttl {
		cacheResult("negative")
		return fetchResult{}, nil
	}

	// Detached from the request so one caller's cancellation does not fail
	// every request merged into this lookup; syncFetch's caller still
	// returns promptly for a canceled ctx via its own select on ctx.Done(),
	// and a background refresh always runs with its own detached context.
	lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lookupTimeout)
	defer cancel()
	k, err := lk.GetAPIKeyByHash(lctx, hash)
	if errors.Is(err, store.ErrAPIKeyNotFound) {
		k, err = nil, nil
	}
	if err != nil {
		return fetchResult{}, err
	}
	// A successful round trip -- whatever it found -- proves the store is
	// reachable for this key right now, so any failing marker left over
	// from an earlier attempt is stale.
	r.clearFailing(hash)
	fetched := r.now()
	if k == nil {
		// A key that was positively cached can later be deleted at the
		// store; drop any stale positive entry so a subsequent store outage
		// cannot fall back to serving the deleted key as if it were still
		// valid.
		r.positive.Remove(hash)
		r.negative.Add(hash, cacheEntry{fetched: fetched})
		cacheResult("negative")
		return fetchResult{}, nil
	}
	r.positive.Add(hash, cacheEntry{key: cloneKey(k), fetched: fetched})
	cacheResult("miss")
	return fetchResult{key: k}, nil
}

// route returns the lookup that verifies raw: the route whose prefix raw
// carries, provided raw is a well-formed key under it. The singleflight and
// both caches stay keyed by hash alone: Hash covers the prefix, so keys
// from different routes never share an entry.
func (r *Resolver) route(raw string) (Lookup, bool) {
	for _, rt := range r.routes {
		if strings.HasPrefix(raw, rt.Prefix) {
			return rt.Lookup, ValidateWithPrefix(raw, rt.Prefix) == nil
		}
	}
	return nil, false
}

func usable(k *store.APIKey, now time.Time) (*store.APIKey, error) {
	if k == nil || !k.Usable(now) {
		return nil, ErrUnauthorized
	}
	return cloneKey(k), nil
}

// cloneKey returns a deep copy of k, so the resolver's cache and every
// caller each hold an independent value: mutating one copy (including a
// copy shared with other goroutines via singleflight) can never corrupt the
// cache or another caller's copy.
func cloneKey(k *store.APIKey) *store.APIKey {
	c := *k
	if k.ExpiresAt != nil {
		t := *k.ExpiresAt
		c.ExpiresAt = &t
	}
	if k.RevokedAt != nil {
		t := *k.RevokedAt
		c.RevokedAt = &t
	}
	return &c
}

func cacheResult(result string) { metrics.APIKeyCacheTotal.WithLabelValues(result).Inc() }
