package api_server

import (
	"context"
	"maps"
	"slices"
	"sort"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// maxBatchParentLookups caps the external parents one POST /txs consults. The
// batch's own txids are always looked up (their stored keys bind resubmits).
const maxBatchParentLookups = 256

// partitionKeyLookupChunk bounds the txids of one LookupPartitionKeys call. A
// POST /txs body may carry hundreds of thousands of transactions; a single
// read over all of them would build a Mongo $in past the 16 MB BSON document
// limit (failing the lookup, so inheritance silently falls back) or one
// unbounded Aerospike BatchGet.
const partitionKeyLookupChunk = 1000

// partitionKeyForSubmit decides the propagation partition key of a single
// POST /tx submission.
//
// familyPartitionKeys keeps the txs of ONE request together, but a wallet
// paying out of its own unconfirmed change submits a chain one request at a
// time, and each hop keyed by its own txid landed on a random partition. On
// the hosted 8-partition topology a child therefore reached a dispatcher
// that had never heard of its parent, drew TX_MISSING_PARENT from the
// datahub, burned the flat fast-path budget and parked for the reaper
// (hosted-service report, issue 2: hops 1–21 of a 50-deep chain in seconds,
// hops 22–50 parked for minutes).
//
// The fix: look the submission's parents up in the store and, when one is
// still held by a dispatcher (RECEIVED or SENT_TO_NETWORK), reuse the
// partition key that parent was published with.
// Same key ⇒ same partition ⇒ the dep-aware dispatcher holds the child behind
// its in-flight parent exactly as it does within one request. A parent at
// ACCEPTED_BY_NETWORK or beyond is already known to Teranode, so the child
// keys by its own txid and the funnel onto one partition lasts only as long
// as the parent is in flight. A resubmission reuses its own row's key so it
// rejoins the family its earlier message belongs to.
//
// Best-effort: a lookup failure, a parent written before keys were persisted,
// or more parents than ParentKeyLookupMaxInputs all fall back to the own-txid
// key and the missing-parent safety net. So does a parent being resubmitted
// after REJECTED: its row stays REJECTED until the rebroadcast's verdict, so
// it does not count as in flight.
//
// ownSeen reports whether the lookup found txid's own row: a CAS loser then
// already holds the stored key (or the row has none) and need not read it
// again (storedPartitionKey).
func (s *Server) partitionKeyForSubmit(ctx context.Context, txid string, inputs []string) (key string, ownSeen bool) {
	maxInputs := s.cfg.Propagation.ParentKeyLookupMaxInputs
	if s.store == nil || maxInputs <= 0 {
		return txid, false
	}
	parents := distinctParents(txid, inputs, maxInputs)
	rows, err := s.store.LookupPartitionKeys(ctx, append(parents, txid))
	if err != nil {
		s.logger.Warn("parent partition-key lookup failed; keying by txid", logfields.TxID(txid), zap.Error(err))
		return txid, false
	}
	own, ownSeen := rows[txid]
	if ownSeen && own.PartitionKey != "" {
		return own.PartitionKey, true
	}
	for _, p := range parents {
		if pk, ok := rows[p]; ok && pk.PartitionKey != "" && parentStillPending(pk.Status) {
			s.logger.Debug("partition key inherited from in-flight parent",
				logfields.TxID(txid), zap.String("parent", p), zap.String("parent_status", string(pk.Status)))
			return pk.PartitionKey, ownSeen
		}
	}
	return txid, ownSeen
}

// inheritStoredPartitionKeys re-keys the dependency families of a POST /txs
// batch from the store, before the CAS, so every row persists the key its
// message will carry. keyByTxid is updated in place, by family:
//
//  1. A member whose own row is still in flight or will be republished keeps
//     the key its earlier message went out under, and its family follows it.
//     An unchanged retry of a batch whose first send failed therefore
//     publishes under the stored keys even if an external parent has since
//     been accepted, rather than recomputing a natural key that a child
//     following the stored key would miss. When members disagree, one whose
//     message is still held by a dispatcher beats a REJECTED one, whose key
//     no message is pending under (bindingKeys).
//  2. Otherwise a family whose parent OUTSIDE the batch is still in flight
//     joins that parent's partition.
//  3. Otherwise the family keeps its natural key.
//
// A row inserted by a concurrent request between this lookup and the CAS is
// caught after the CAS by adoptStoredFamilyKeys.
func (s *Server) inheritStoredPartitionKeys(ctx context.Context, keyByTxid map[string]string, inputsByTxid map[string][]string) {
	maxInputs := s.cfg.Propagation.ParentKeyLookupMaxInputs
	if s.store == nil || maxInputs <= 0 || len(keyByTxid) == 0 {
		return
	}
	txids := make([]string, 0, len(inputsByTxid))
	for txid := range inputsByTxid {
		txids = append(txids, txid)
	}
	sort.Strings(txids) // deterministic lookup order and remap precedence

	external := make([]string, 0, 16)
	seen := make(map[string]struct{})
	parentsOf := make(map[string][]string, len(txids))
	for _, txid := range txids {
		var ext []string
		for _, p := range distinctParents(txid, inputsByTxid[txid], maxInputs) {
			if _, inBatch := keyByTxid[p]; inBatch {
				continue
			}
			ext = append(ext, p)
			if _, dup := seen[p]; !dup && len(external) < maxBatchParentLookups {
				seen[p] = struct{}{}
				external = append(external, p)
			}
		}
		parentsOf[txid] = ext
	}
	rows, err := s.lookupPartitionKeys(ctx, append(append(make([]string, 0, len(txids)+len(external)), txids...), external...))
	if err != nil {
		s.logger.Warn("batch partition-key lookup failed; keying by family", zap.Int("txs", len(txids)), zap.Int("parents", len(external)), zap.Error(err))
		return
	}
	remap := bindingKeys(txids, keyByTxid, rows, nil) // 1: a member's own stored key
	for _, txid := range txids {                      // 2: the first in-flight external parent
		fam, ok := keyByTxid[txid]
		if !ok {
			continue
		}
		if _, done := remap[fam]; done {
			continue
		}
		for _, p := range parentsOf[txid] {
			if pk, ok := rows[p]; ok && pk.PartitionKey != "" && parentStillPending(pk.Status) {
				remap[fam] = pk.PartitionKey
				break
			}
		}
	}
	for txid, fam := range keyByTxid {
		if nk, ok := remap[fam]; ok {
			keyByTxid[txid] = nk
		}
	}
}

// storedPartitionKey returns the key txid's row was persisted with, or
// fallback when the row has none (written before keys were persisted) or the
// lookup fails or is disabled. A CAS loser that republishes uses it: a
// concurrent identical submission may have chosen — and stored — a different
// key across a parent's acceptance, and republishing under the loser's own
// choice would put the same tx on two partitions. GetOrInsertStatus's
// existing row is not used because not every backend's status read carries the
// key; LookupPartitionKeys is the one read path for it.
//
// seen means partitionKeyForSubmit already read the row, so fallback is its
// stored key (or the row has none) and no second read is made. Only a row a
// concurrent request inserted after that lookup needs one.
func (s *Server) storedPartitionKey(ctx context.Context, txid, fallback string, seen bool) string {
	if seen || s.store == nil || s.cfg.Propagation.ParentKeyLookupMaxInputs <= 0 {
		return fallback
	}
	rows, err := s.store.LookupPartitionKeys(ctx, []string{txid})
	if err != nil {
		s.logger.Warn("own partition-key lookup failed; keeping computed key", logfields.TxID(txid), zap.Error(err))
		return fallback
	}
	if r, ok := rows[txid]; ok && r.PartitionKey != "" {
		return r.PartitionKey
	}
	return fallback
}

// adoptStoredFamilyKeys runs after the POST /txs CAS for the txids that lost
// it and are being republished. A row inserted by a concurrent request after
// inheritStoredPartitionKeys' lookup may carry a different key; the loser's
// whole family moves to the stored key so the republish rejoins the earlier
// message and the family stays together. keyByTxid is updated in place.
//
// Families are grouped by famByTxid, the natural family keys taken before
// inheritance, not by the current routing key: inheritance can give separate
// families the same key (two children of one in-flight parent), and moving
// one of them must not drag the other off the key its rows were stored under.
//
// The family settles on the key of its best-ranked republished member
// (bindingKeys): a RECEIVED member's pending message pins the family even
// when that key is the one already chosen, so a REJECTED member's stale key
// sorting later cannot move the family off it.
//
// republished maps each loser to the status its CAS returned, the one that
// selected it for republication. A member ranks by that status or the one the
// lookup reads, whichever binds harder: the row can advance in between (the
// concurrent winner's message reaches ACCEPTED_BY_NETWORK or parks), but this
// request still republishes it, and only under the stored key does that copy
// rejoin the winner's message rather than reach a second dispatcher.
//
// Residual: a member of that family that THIS request inserted has already
// persisted the pre-CAS key, and a child later following it can miss the
// dispatcher. That needs two concurrent batches straddling a parent's
// acceptance, and falls back to the missing-parent safety net.
func (s *Server) adoptStoredFamilyKeys(ctx context.Context, keyByTxid, famByTxid map[string]string, republished map[string]models.Status) {
	if s.store == nil || s.cfg.Propagation.ParentKeyLookupMaxInputs <= 0 || len(republished) == 0 {
		return
	}
	ids := slices.Sorted(maps.Keys(republished)) // deterministic precedence when members disagree
	rows, err := s.lookupPartitionKeys(ctx, ids)
	if err != nil {
		s.logger.Warn("republish partition-key lookup failed; keeping computed keys", zap.Int("txs", len(ids)), zap.Error(err))
		return
	}
	remap := bindingKeys(ids, famByTxid, rows, republished) // natural family key -> stored key
	for txid, fam := range famByTxid {
		if nk, ok := remap[fam]; ok {
			keyByTxid[txid] = nk
		}
	}
}

// bindingKeys picks, per family (famOf), the stored key the family must carry:
// that of the best-ranked member among txids (keyBindRank), the first in
// txids order on a tie. Members with no stored key or a non-binding status
// pick nothing; a family with no pick is absent from the result. selectedAt,
// when non-nil, gives a member an earlier-observed status that ranks it too.
func bindingKeys(txids []string, famOf map[string]string, rows map[string]store.ParentKey, selectedAt map[string]models.Status) map[string]string {
	picked := make(map[string]string)
	rank := make(map[string]int)
	for _, txid := range txids {
		fam, ok := famOf[txid]
		if !ok {
			continue
		}
		r, ok := rows[txid]
		if !ok || r.PartitionKey == "" {
			continue
		}
		rk := keyBindRank(r.Status)
		if sel, ok := selectedAt[txid]; ok {
			rk = max(rk, keyBindRank(sel))
		}
		if rk > rank[fam] {
			rank[fam] = rk
			picked[fam] = r.PartitionKey
		}
	}
	return picked
}

// lookupPartitionKeys is LookupPartitionKeys in partitionKeyLookupChunk-sized
// reads, merged. Any chunk's error fails the whole lookup, as a single read
// would.
func (s *Server) lookupPartitionKeys(ctx context.Context, txids []string) (map[string]store.ParentKey, error) {
	if len(txids) <= partitionKeyLookupChunk {
		return s.store.LookupPartitionKeys(ctx, txids)
	}
	out := make(map[string]store.ParentKey, len(txids))
	for chunk := range slices.Chunk(txids, partitionKeyLookupChunk) {
		rows, err := s.store.LookupPartitionKeys(ctx, chunk)
		if err != nil {
			return nil, err
		}
		maps.Copy(out, rows)
	}
	return out, nil
}

// distinctParents returns up to limit distinct source txids of a transaction,
// in input order, skipping empties and self-references.
func distinctParents(txid string, inputs []string, limit int) []string {
	out := make([]string, 0, min(len(inputs), limit))
	seen := map[string]struct{}{txid: {}}
	for _, p := range inputs {
		if len(out) >= limit {
			break
		}
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// keyBindRank ranks how strongly a batch member's stored key must carry over
// to this submission; 0 means it does not bind. A member whose earlier message
// is still held by a dispatcher (parentStillPending) ranks highest: a second
// copy under another key would reach a second dispatcher, and the family's
// children must follow it. A REJECTED member is republished and keeps the key
// it went out under, but no message is pending under that key, so it yields
// to a pending member. An ACCEPTED-or-later or parked member does not hold its
// family.
func keyBindRank(st models.Status) int {
	switch {
	case parentStillPending(st):
		return 2
	case st == models.StatusRejected:
		return 1
	default:
		return 0
	}
}

// parentStillPending reports whether a parent's message is still held by a
// dispatcher, so its child must reach the same dispatcher to be held behind
// it. ACCEPTED_BY_NETWORK and beyond mean Teranode has the parent and the
// child can go anywhere. PENDING_RETRY does not count: the dispatcher drops a
// parked tx from its in-flight set and the reaper rebroadcasts it from the
// store, not through Kafka, so following its key holds nothing and only
// funnels a parked chain onto one partition. A REJECTED parent is left to the
// missing-parent cascade, which rejects the child from the store.
func parentStillPending(st models.Status) bool {
	switch st {
	case models.StatusReceived, models.StatusSentToNetwork:
		return true
	default:
		return false
	}
}
