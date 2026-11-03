package api_server

import (
	"context"
	"sort"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/models"
)

// maxBatchParentLookups caps the external parents one POST /txs consults. The
// batch's own txids are always looked up (their stored keys bind resubmits),
// so one projected read covers at most the batch plus this many parents.
const maxBatchParentLookups = 256

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
// still at a status the network may not have acknowledged (RECEIVED or
// PENDING_RETRY), reuse the partition key that parent was published with.
// Same key ⇒ same partition ⇒ the dep-aware dispatcher holds the child behind
// its in-flight parent exactly as it does within one request. A parent at
// ACCEPTED_BY_NETWORK or beyond is already known to Teranode, so the child
// keys by its own txid and the funnel onto one partition lasts only as long
// as the parent is in flight. A resubmission reuses its own row's key so it
// rejoins the family its earlier message belongs to.
//
// Best-effort: a lookup failure, a parent written before keys were persisted,
// or more parents than ParentKeyLookupMaxInputs all fall back to the own-txid
// key and the missing-parent safety net.
func (s *Server) partitionKeyForSubmit(ctx context.Context, txid string, inputs []string) string {
	maxInputs := s.cfg.Propagation.ParentKeyLookupMaxInputs
	if s.store == nil || maxInputs <= 0 {
		return txid
	}
	parents := distinctParents(txid, inputs, maxInputs)
	rows, err := s.store.LookupPartitionKeys(ctx, append(parents, txid))
	if err != nil {
		s.logger.Warn("parent partition-key lookup failed; keying by txid", logfields.TxID(txid), zap.Error(err))
		return txid
	}
	if own, ok := rows[txid]; ok && own.PartitionKey != "" {
		return own.PartitionKey
	}
	for _, p := range parents {
		if pk, ok := rows[p]; ok && pk.PartitionKey != "" && parentStillPending(pk.Status) {
			s.logger.Debug("partition key inherited from in-flight parent",
				logfields.TxID(txid), zap.String("parent", p), zap.String("parent_status", string(pk.Status)))
			return pk.PartitionKey
		}
	}
	return txid
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
//     following the stored key would miss.
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
	rows, err := s.store.LookupPartitionKeys(ctx, append(append(make([]string, 0, len(txids)+len(external)), txids...), external...))
	if err != nil {
		s.logger.Warn("batch partition-key lookup failed; keying by family", zap.Int("txs", len(txids)), zap.Int("parents", len(external)), zap.Error(err))
		return
	}
	remap := make(map[string]string) // family key -> inherited key
	for _, txid := range txids {     // 1: a member's own stored key
		fam, ok := keyByTxid[txid]
		if !ok {
			continue
		}
		if _, done := remap[fam]; done {
			continue
		}
		if r, ok := rows[txid]; ok && r.PartitionKey != "" && ownKeyBinds(r.Status) {
			remap[fam] = r.PartitionKey
		}
	}
	for _, txid := range txids { // 2: the first in-flight external parent
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
func (s *Server) storedPartitionKey(ctx context.Context, txid, fallback string) string {
	if s.store == nil || s.cfg.Propagation.ParentKeyLookupMaxInputs <= 0 {
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
// Residual: a member of that family that THIS request inserted has already
// persisted the pre-CAS key, and a child later following it can miss the
// dispatcher. That needs two concurrent batches straddling a parent's
// acceptance, and falls back to the missing-parent safety net.
func (s *Server) adoptStoredFamilyKeys(ctx context.Context, keyByTxid, famByTxid map[string]string, republished []string) {
	if s.store == nil || s.cfg.Propagation.ParentKeyLookupMaxInputs <= 0 || len(republished) == 0 {
		return
	}
	ids := append([]string(nil), republished...)
	sort.Strings(ids) // deterministic precedence when members disagree
	rows, err := s.store.LookupPartitionKeys(ctx, ids)
	if err != nil {
		s.logger.Warn("republish partition-key lookup failed; keeping computed keys", zap.Int("txs", len(ids)), zap.Error(err))
		return
	}
	remap := make(map[string]string) // natural family key -> stored key
	for _, txid := range ids {
		fam, ok := famByTxid[txid]
		if !ok {
			continue
		}
		if _, done := remap[fam]; done {
			continue
		}
		if r, ok := rows[txid]; ok && r.PartitionKey != "" && r.PartitionKey != keyByTxid[txid] {
			remap[fam] = r.PartitionKey
		}
	}
	for txid, fam := range famByTxid {
		if nk, ok := remap[fam]; ok {
			keyByTxid[txid] = nk
		}
	}
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

// ownKeyBinds reports whether a batch member's stored key must carry over to
// this submission: its earlier message may still be in flight (a child in the
// family must follow it), or the row is republished (RECEIVED, REJECTED) and
// must keep the key it went out under. An ACCEPTED-or-later member is on the
// network and does not hold its family.
func ownKeyBinds(st models.Status) bool {
	return parentStillPending(st) || st == models.StatusRejected
}

// parentStillPending reports whether a parent's row is at a status the network
// may not have acknowledged yet, so its child must reach the same dispatcher
// to be held behind it. ACCEPTED_BY_NETWORK and beyond mean Teranode has the
// parent and the child can go anywhere; a REJECTED parent is left to the
// missing-parent cascade, which rejects the child from the store.
func parentStillPending(st models.Status) bool {
	switch st {
	case models.StatusReceived, models.StatusSentToNetwork, models.StatusPendingRetry:
		return true
	default:
		return false
	}
}
