package pebble

import (
	"fmt"
	"time"
)

// Pebble is a flat KV store, so we encode primary records and secondary
// indexes into a single keyspace using short ASCII prefixes. The split
// between primary rows ("tx:…", "bump:…", "sub:…", …) and index entries
// ("idx:…") matters on writes because we batch them together in one
// IndexedBatch so reads of the in-flight index see the primary rows.
//
// Index entries store no value — their existence is the payload. We read
// the referenced txid/id off the end of the key. This keeps index writes
// to a single Set() per index and avoids a second lookup round-trip.
//
// The one exception is idx:apikey:hash:<hash>, which stores the key id as
// its value: a hash lookup must resolve to the id so GetAPIKeyByHash can
// fetch the primary row without encoding the id into the key (the hash is
// already the full key suffix).
const (
	prefixTx         = "tx:"
	prefixBump       = "bump:"
	prefixStump      = "stump:"
	prefixSub        = "sub:"
	prefixLease      = "lease:"
	prefixDatahub    = "dh:"
	prefixPeerPolicy = "pp:"

	prefixAPIKey        = "apikey:"
	prefixIdxAPIKeyHash = "idx:apikey:hash:" //nolint:gosec // index-prefix string, not a credential

	prefixIdxTxStatus        = "idx:tx:status:"
	prefixIdxTxBlock         = "idx:tx:block:"
	prefixIdxTxRetryReady    = "idx:tx:retry_ready:"
	prefixIdxTxUpdated       = "idx:tx:updated:"
	prefixIdxSubTxID         = "idx:sub:txid:"
	prefixIdxSubToken        = "idx:sub:token:" //nolint:gosec // index-prefix string, not a credential
	prefixIdxStumpBlock      = "idx:stump:block:"
	prefixBlockProc          = "blkproc:"
	prefixBlockExpected      = "blkexp:"
	prefixIdxBlockProcHeight = "idx:blkproc:height:"
)

func txKey(txid string) []byte             { return []byte(prefixTx + txid) }
func bumpKey(blockHash string) []byte      { return []byte(prefixBump + blockHash) }
func subKey(id string) []byte              { return []byte(prefixSub + id) }
func leaseKey(name string) []byte          { return []byte(prefixLease + name) }
func datahubEndpointKey(url string) []byte { return []byte(prefixDatahub + url) }
func datahubEndpointPrefix() []byte        { return []byte(prefixDatahub) }

func peerPolicyKey(peerID string) []byte { return []byte(prefixPeerPolicy + peerID) }
func peerPolicyPrefix() []byte           { return []byte(prefixPeerPolicy) }

func apiKeyKey(id string) []byte       { return []byte(prefixAPIKey + id) }
func apiKeyHashKey(hash string) []byte { return []byte(prefixIdxAPIKeyHash + hash) }
func apiKeyPrefix() []byte             { return []byte(prefixAPIKey) }

// stumpKey is content-addressed: several merkle-services may deliver the
// same subtree's STUMP, and a service that missed registrations delivers a
// different one. Identical bytes map to the same key; variants coexist under
// the block prefix for the bump-builder to merge.
func stumpKey(blockHash string, idx int, contentHash string) []byte {
	return []byte(fmt.Sprintf("%s%s:%010d:%s", prefixStump, blockHash, idx, contentHash))
}

// stumpBlockPrefix returns the prefix used to iterate all STUMP rows for a block.
func stumpBlockPrefix(blockHash string) []byte {
	return []byte(prefixStump + blockHash + ":")
}

// Secondary index key builders. All index keys embed the primary key at the end
// so the iterator can recover the target row without a second lookup.

func idxTxStatusKey(status, txid string) []byte {
	return []byte(prefixIdxTxStatus + status + ":" + txid)
}

// idxTxStatusPrefix returns the prefix used to iterate every row currently at
// one status — the census path's way of never visiting rows in other statuses.
func idxTxStatusPrefix(status string) []byte {
	return []byte(prefixIdxTxStatus + status + ":")
}

func idxTxBlockKey(blockHash, txid string) []byte {
	return []byte(prefixIdxTxBlock + blockHash + ":" + txid)
}

func idxTxBlockPrefix(blockHash string) []byte {
	return []byte(prefixIdxTxBlock + blockHash + ":")
}

// idxTxRetryReadyKey encodes next_retry_at as 16 zero-padded hex digits so
// prefix iteration returns rows in time order. Using unix-nanoseconds keeps
// the ordering stable across the epoch boundary.
func idxTxRetryReadyKey(nextRetryUnixNs int64, txid string) []byte {
	return []byte(fmt.Sprintf("%s%016x:%s", prefixIdxTxRetryReady, uint64(nextRetryUnixNs), txid)) //nolint:gosec // hex-encoded unix nanos; sign-flip is not meaningful for ordering
}

func idxTxRetryReadyPrefix() []byte {
	return []byte(prefixIdxTxRetryReady)
}

func idxTxUpdatedKey(updatedUnixNs int64, txid string) []byte {
	return []byte(fmt.Sprintf("%s%016x:%s", prefixIdxTxUpdated, uint64(updatedUnixNs), txid)) //nolint:gosec // hex-encoded unix nanos
}

func idxTxUpdatedPrefix() []byte {
	return []byte(prefixIdxTxUpdated)
}

// idxTxUpdatedLowerBound returns the first idx:tx:updated key a since-bounded
// scan needs. Keys encode uint64(updatedUnixNs), so lexical order matches time
// order only for non-negative timestamps; a zero or pre-epoch since falls back
// to the whole prefix and leaves the cutoff to the row-side check.
func idxTxUpdatedLowerBound(since time.Time) []byte {
	if since.IsZero() || since.UnixNano() < 0 {
		return idxTxUpdatedPrefix()
	}
	return idxTxUpdatedKey(since.UnixNano(), "")
}

func idxSubTxIDKey(txid, id string) []byte {
	return []byte(prefixIdxSubTxID + txid + ":" + id)
}

func idxSubTxIDPrefix(txid string) []byte {
	return []byte(prefixIdxSubTxID + txid + ":")
}

func idxSubTokenKey(token, id string) []byte {
	return []byte(prefixIdxSubToken + token + ":" + id)
}

func idxSubTokenPrefix(token string) []byte {
	return []byte(prefixIdxSubToken + token + ":")
}

func idxStumpBlockKey(blockHash string, subtreeIndex int, contentHash string) []byte {
	return []byte(fmt.Sprintf("%s%s:%010d:%s", prefixIdxStumpBlock, blockHash, subtreeIndex, contentHash))
}

// idxStumpBlockPrefix is the prefix of every idxStumpBlockKey for a block.
func idxStumpBlockPrefix(blockHash string) []byte {
	return []byte(prefixIdxStumpBlock + blockHash + ":")
}

// blockProcKey is the primary record key for a block-processing row.
func blockProcKey(blockHash string) []byte { return []byte(prefixBlockProc + blockHash) }

// blockExpectedKey holds a block's retained expected-subtree set
// (store.Store.AddBlockExpectedSubtrees) as a JSON int array. It lives beside
// the block-processing row rather than in it so the row's many rebuild-from-
// scratch writers cannot drop it.
func blockExpectedKey(blockHash string) []byte { return []byte(prefixBlockExpected + blockHash) }

// idxBlockProcHeightKey encodes block_height as the bitwise-inverted uint64
// rendered as 16 hex digits so a forward iterator over the index prefix
// returns rows in descending-height order. Same trick the retry index uses
// for time-ordered scans, but inverted because we want newest-first.
func idxBlockProcHeightKey(blockHeight uint64, blockHash string) []byte {
	return []byte(fmt.Sprintf("%s%016x:%s", prefixIdxBlockProcHeight, ^blockHeight, blockHash))
}

func idxBlockProcHeightPrefix() []byte { return []byte(prefixIdxBlockProcHeight) }

// idEndOfPrefix returns the upper bound of a prefix range for use with
// pebble.IterOptions.UpperBound. The returned slice is the shortest bytes
// that's strictly greater than any key starting with prefix.
func endOfPrefix(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // all 0xff — unbounded
}

// lastSegment returns the substring after the last ':' in key. Index keys
// always end with ":<txid>" or ":<id>" so this pulls out the referenced row.
func lastSegment(key []byte) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == ':' {
			return string(key[i+1:])
		}
	}
	return string(key)
}
