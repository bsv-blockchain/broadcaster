package api_server

import (
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/broadcaster/kafka"
)

// familyPartitionKeys returns the Kafka partition key for each tx of a
// submitted batch. Txs connected through in-batch spends — tx i lists an
// input whose source txid is also in the batch — form a dependency family
// and share one key: the lexicographically smallest txid of the family.
// Isolated txs key by their own txid (keys[i] == txids[i]), matching the
// pre-#295 behavior and the single-submit path.
//
// Same key ⇒ same partition ⇒ per-partition Kafka order delivers every
// parent's admit to its dispatcher before its children's, which is what
// lets the dep-aware dispatcher keep honoring Teranode's "no parent and
// child in one /txs batch" contract without a cross-partition dep index.
// The root is the smallest member (not the first submitted) so the key is
// order-independent: a client retrying the same family with the batch
// shuffled maps to the same partition as the original messages.
//
// Only same-submission edges join a family here. A parent submitted in an
// earlier HTTP request is handled by partitionKeyForSubmit /
// inheritStoredPartitionKeys (partition_inherit.go), which reuse the key an
// in-flight parent's row persisted; what that cannot cover (child before
// parent, parent already on the network) is absorbed by the propagator's
// missing-parent safety net (see PropagationConfig.Partitions).
//
// inputs[i] holds the source txids of tx i's inputs (collectInputTXIDs).
// Empty strings and self-references are ignored, mirroring the dispatcher's
// admission guards. O((N+E)·α(N)) for N txs and E total inputs.
func familyPartitionKeys(txids []string, inputs [][]string) []string {
	n := len(txids)
	if n == 0 {
		return nil
	}

	index := make(map[string]int, n)
	for i, id := range txids {
		// First occurrence wins; a duplicate txid (store==nil intake path)
		// unions with its first occurrence below, so both share a key.
		if _, ok := index[id]; !ok {
			index[id] = i
		}
	}

	parent := make([]int, n)
	size := make([]int, n)
	for i := range parent {
		parent[i] = i
		size[i] = 1
	}
	find := func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]] // path halving
			i = parent[i]
		}
		return i
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra == rb {
			return
		}
		if size[ra] < size[rb] {
			ra, rb = rb, ra
		}
		parent[rb] = ra
		size[ra] += size[rb]
	}

	for i, ins := range inputs {
		for _, src := range ins {
			if src == "" || src == txids[i] {
				continue
			}
			if j, ok := index[src]; ok {
				union(i, j)
			}
		}
	}
	// Duplicate txids: union every later occurrence with the first so the
	// whole family (including children referencing the txid) shares a key.
	for i, id := range txids {
		if first := index[id]; first != i {
			union(i, first)
		}
	}

	// Smallest member txid per component, then one key per tx.
	keyOf := make(map[int]string, n)
	for i, id := range txids {
		r := find(i)
		if cur, ok := keyOf[r]; !ok || id < cur {
			keyOf[r] = id
		}
	}
	keys := make([]string, n)
	for i := range txids {
		keys[i] = keyOf[find(i)]
	}
	return keys
}

// parsedItem is one transaction decoded from a POST /txs body: the parsed
// transaction, its original wire bytes, and its canonical txid.
type parsedItem struct {
	tx   *sdkTx.Transaction
	raw  []byte
	txid string
}

// familyKeysBySubmittedTxid computes the dependency-family partition key and
// the parent-txid list for every transaction in a submitted batch, indexed by
// txid.
//
// It deliberately runs over the FULL submitted batch rather than over the
// post-dedup publish set, because the family key is min(family) and therefore
// moves whenever a member leaves the set — and members leave constantly, since
// the intake dedup CAS drops every tx already known at a non-REJECTED status.
//
// The failure that causes: a client sends POST {P}, then POST {P, C} moments
// later (re-sending the parent for safety). P dedups out, so a key computed
// over the publish set sees no in-batch parent for C and falls back to C's own
// txid — routing C to a partition its still-in-flight parent is not on, where
// that dispatcher's inFlight map has never heard of P and cannot hold C behind
// it. C draws TX_MISSING_PARENT and parks at PENDING_RETRY, when on a
// single-partition topic it was admitted in milliseconds. The partial-produce
// retry path has the same shape: the members that already landed dedup out on
// the retry, and the remainder recomputes onto a different partition than its
// own in-flight siblings.
//
// A deduped-out member remaining as the family representative is fine: the key
// is only a hash input and need not be a txid this request publishes.
func familyKeysBySubmittedTxid(parsed []parsedItem) (keyByTxid map[string]string, inputsByTxid map[string][]string) {
	txids := make([]string, len(parsed))
	inputs := make([][]string, len(parsed))
	for i, p := range parsed {
		txids[i] = p.txid
		inputs[i] = collectInputTXIDs(p.tx)
	}
	keys := familyPartitionKeys(txids, inputs)

	keyByTxid = make(map[string]string, len(parsed))
	inputsByTxid = make(map[string][]string, len(parsed))
	for i, p := range parsed {
		if _, ok := keyByTxid[p.txid]; ok {
			continue // duplicate txid in one body: first occurrence wins
		}
		keyByTxid[p.txid] = keys[i]
		inputsByTxid[p.txid] = inputs[i]
	}
	return keyByTxid, inputsByTxid
}

// propagationMessages builds the Kafka envelopes for the txs a submission
// actually publishes, keyed by dependency family.
//
// input_txids drives the dispatcher's dep-aware admission — children of any
// in-flight parent are held until the parent terminalizes — and the key is
// what co-locates a family on one partition so that hold is possible at all.
//
// keyByTxid/inputsByTxid come from familyKeysBySubmittedTxid over the full
// submitted batch, so every published tx is present; the txid fallback is
// belt-and-braces for a future caller whose publish set is not a subset.
func propagationMessages(toPublish []parsedItem, keyByTxid map[string]string, inputsByTxid map[string][]string) []kafka.KeyValue {
	msgs := make([]kafka.KeyValue, 0, len(toPublish))
	for _, p := range toPublish {
		key, ok := keyByTxid[p.txid]
		if !ok {
			key = p.txid
		}
		msgs = append(msgs, kafka.KeyValue{
			Key: key,
			Value: map[string]interface{}{
				"txid":        p.txid,
				"raw_tx":      p.raw,
				"input_txids": inputsByTxid[p.txid],
			},
		})
	}
	return msgs
}
