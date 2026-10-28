package sse

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

// tokenSet is the set of callback tokens subscribed to one txid. It is a
// slice, not a map: a txid carries a handful of tokens and a fan-out asks
// "does THIS client's token belong" a handful of times per event, so a linear
// scan beats a map on both speed and — at tens of thousands of cached
// entries — resident bytes. Treat it as immutable once published: fan-out
// reads it without holding the index lock.
type tokenSet []string

// has reports whether token is subscribed to the txid this set describes.
func (s tokenSet) has(token string) bool { return slices.Contains(s, token) }

// withHint returns s plus the token an intake event carried as its
// CallbackTokenHint (models.TransactionStatus): the submission row the index
// resolves tokens from is written asynchronously and may not exist yet when
// the RECEIVED event arrives. Always a copy, never an append in place — a
// cached set is read by other fan-outs without the index lock.
func (s tokenSet) withHint(hint string) tokenSet {
	if hint == "" || s.has(hint) {
		return s
	}
	out := make(tokenSet, 0, len(s)+1)
	out = append(out, s...)
	return append(out, hint)
}

// Cache bounds. The index is a pure accelerator — every miss is answered by
// the store — so the bounds are chosen for a small, predictable resident set
// rather than for completeness. The SSE pod runs with a 256Mi limit and has
// been OOM-killed before (#237/#238), so entries are bounded TWO ways, the
// same dual-bound discipline as store/bumpcache: a hard entry cap AND a
// resident-cost budget, because token strings are caller-supplied and a
// count-based limit alone says nothing about bytes.
const (
	// maxTokenCacheEntries hard-caps entry count.
	maxTokenCacheEntries = 65536
	// maxTokenCacheBytes budgets estimated resident bytes (see entryCost).
	// At the ~280 B/entry a 64-hex txid plus one token costs, 16 MiB is
	// reached around 60k entries — roughly a minute of unique txids at
	// 1,000 TPS, which spans the ACCEPTED → SEEN_ON_NETWORK →
	// SEEN_IN_ORPHAN_MEMPOOL window that supplies the hits.
	maxTokenCacheBytes = 16 << 20
	// tokenCacheTTL bounds how stale a cached set may be. A txid's token set
	// only changes if a further submission is registered for the SAME txid
	// under a DIFFERENT token, which is rare — but a cached set is used to
	// SUPPRESS delivery, so it must not be able to suppress forever.
	tokenCacheTTL = 2 * time.Minute
	// entryOverhead approximates the fixed per-entry heap cost the LRU pays
	// on top of the key and token bytes: list node, map bucket slot, slice
	// and string headers. Deliberately generous — under-counting is how a
	// "bounded" cache OOMs a pod.
	entryOverhead = 160
	// tokenOverhead is the per-token string header cost inside a set.
	tokenOverhead = 16
)

// cacheEntry is one txid's resolved token set plus its expiry.
type cacheEntry struct {
	tokens    tokenSet
	expiresAt time.Time
}

// entryCost estimates an entry's resident bytes for the budget.
func entryCost(txid string, tokens tokenSet) int {
	cost := entryOverhead + len(txid)
	for _, t := range tokens {
		cost += tokenOverhead + len(t)
	}
	return cost
}

// tokenIndex answers "which callback tokens are subscribed to this txid" for
// the SSE fan-out.
//
// It exists because fan-out used to ask the store that question once per
// (event, client) via an EXISTS probe. Measured against a 4.5M-row
// submissions table under load that probe cost 0.583 ms, and since the
// manager fans out on exactly ONE goroutine it capped delivery at ~1.6k
// events/s — against ~4k events/s of demand at 1,000 TPS (four transitions
// per transaction). Two changes remove the cap:
//
//   - resolve ONCE per event, then answer every client from the set in
//     memory, so query volume stops scaling with client count; and
//   - resolve a bulk event's whole txid list in one batch, so a block's MINED
//     unfan costs a handful of round-trips instead of one per transaction.
//
// A bounded LRU then absorbs the repeat lookups: a transaction emits several
// live transitions within a short window, and only the first has to reach the
// store.
//
// Concurrency: the index is mutex-guarded so it is safe to share, but the
// production caller is the manager's single fan-out goroutine. That is also
// why there is deliberately NO singleflight here (unlike store/bumpcache,
// which is hit concurrently by fan-out, webhook workers and GET /tx): with
// one caller there are no concurrent misses to collapse, and the group would
// be pure overhead on the hot path.
type tokenIndex struct {
	store  store.Store
	logger *zap.Logger
	now    func() time.Time // injectable for tests

	maxBytes int
	ttl      time.Duration

	// mu guards lru and bytes. The LRU's evict callback adjusts bytes and
	// runs synchronously inside mutations, so it MUST NOT take mu itself —
	// every call site below already holds it.
	mu    sync.Mutex
	lru   *simplelru.LRU[string, cacheEntry]
	bytes int
}

// newTokenIndex constructs the index with the package's production bounds.
func newTokenIndex(st store.Store, logger *zap.Logger) *tokenIndex {
	return newTokenIndexWithLimits(st, logger, maxTokenCacheEntries, maxTokenCacheBytes, tokenCacheTTL)
}

// newTokenIndexWithLimits is the constructor proper, split out so tests can
// exercise eviction and expiry without building 60k-entry working sets.
func newTokenIndexWithLimits(st store.Store, logger *zap.Logger, entries, bytes int, ttl time.Duration) *tokenIndex {
	ti := &tokenIndex{
		store:    st,
		logger:   logger,
		now:      time.Now,
		maxBytes: bytes,
		ttl:      ttl,
	}
	l, err := simplelru.NewLRU(entries, func(k string, v cacheEntry) {
		ti.bytes -= entryCost(k, v.tokens)
	})
	if err != nil {
		// Unreachable: NewLRU only fails for a non-positive size and every
		// caller passes a positive constant.
		panic(err)
	}
	ti.lru = l
	return ti
}

// lookup returns the tokens subscribed to txid, consulting the cache first
// and falling back to a single-txid store batch. A store error yields an
// empty set: fan-out then withholds the frame, exactly as the probe it
// replaced did, and the client recovers it via catchup.
func (t *tokenIndex) lookup(ctx context.Context, txid string) tokenSet {
	if t.store == nil || txid == "" {
		return nil
	}
	if tokens, ok := t.cached(txid); ok {
		metrics.SSETokenCacheTotal.WithLabelValues("hit").Inc()
		return tokens
	}
	metrics.SSETokenCacheTotal.WithLabelValues("miss").Inc()

	resolved, err := t.query(ctx, []string{txid}, "single")
	if err != nil {
		t.logger.Warn("submission token lookup failed", logfields.TxID(txid), zap.Error(err))
		return nil
	}
	tokens := tokenSet(resolved[txid])
	// Only NON-EMPTY sets are cached. An empty answer can be a race rather
	// than a fact: the api-server records submissions asynchronously, so a
	// status event can overtake its own submission row by a few
	// milliseconds. Caching that emptiness would suppress every later
	// transition of the transaction for the whole TTL; re-asking costs one
	// query and is what the old probe did on every event anyway.
	if len(tokens) > 0 {
		t.add(txid, tokens)
	}
	return tokens
}

// resolveBatch resolves a bulk event's whole txid list in one store batch.
// txids absent from the result have no subscriber.
//
// It deliberately neither reads nor writes the LRU. A bulk event is a block's
// MINED (or a reorg revert) burst: up to 5,000 terminal-state txids with no
// locality against the live working set, so admitting them would evict the
// entries that actually produce hits, and consulting the cache would only add
// variance to a query count that is already one per chunk.
func (t *tokenIndex) resolveBatch(ctx context.Context, txids []string) map[string]tokenSet {
	if t.store == nil || len(txids) == 0 {
		return nil
	}
	resolved, err := t.query(ctx, txids, "batch")
	if err != nil {
		t.logger.Warn("submission token batch lookup failed",
			logfields.TxIDCount(len(txids)), zap.Error(err))
		return nil
	}
	out := make(map[string]tokenSet, len(resolved))
	for txid, tokens := range resolved {
		out[txid] = tokens
	}
	return out
}

// query runs one store resolution and records its latency under kind.
func (t *tokenIndex) query(ctx context.Context, txids []string, kind string) (map[string][]string, error) {
	start := t.now()
	resolved, err := t.store.TokensForTxIDs(ctx, txids)
	metrics.SSETokenLookupDuration.WithLabelValues(kind).Observe(t.now().Sub(start).Seconds())
	return resolved, err
}

// cached returns a live entry's tokens. An expired entry is dropped and
// reported as a miss.
func (t *tokenIndex) cached(txid string) (tokenSet, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.lru.Get(txid)
	if !ok {
		return nil, false
	}
	if t.now().After(entry.expiresAt) {
		t.lru.Remove(txid)
		return nil, false
	}
	return entry.tokens, true
}

// add inserts an entry and enforces the byte budget. Replacing an existing key
// removes it first so the evict callback keeps the byte count exact (the LRU's
// in-place update path skips the callback).
func (t *tokenIndex) add(txid string, tokens tokenSet) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lru.Remove(txid)
	t.lru.Add(txid, cacheEntry{tokens: tokens, expiresAt: t.now().Add(t.ttl)})
	t.bytes += entryCost(txid, tokens)
	// Unlike bumpcache there is no "keep the newest even alone over budget"
	// exception: a token set is tiny, so the budget can always accommodate at
	// least one entry, and stopping at Len() > 1 keeps eviction terminating
	// if a pathological entry ever exceeded the budget on its own.
	for t.bytes > t.maxBytes && t.lru.Len() > 1 {
		t.lru.RemoveOldest()
	}
	metrics.SSETokenCacheEntries.Set(float64(t.lru.Len()))
}

// len reports the current entry count. Exposed for tests.
func (t *tokenIndex) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lru.Len()
}
