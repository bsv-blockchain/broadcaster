package aerospike

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"

	"github.com/bsv-blockchain/broadcaster/store"
)

var (
	_ store.APIKeyStore = (*Store)(nil)
	_ store.QuotaStore  = (*Store)(nil)
)

const (
	// setAPIKeys holds one record per key, keyed by the key's SHA-256 so the
	// per-request lookup is a single primary-key read. setAPIKeyIDs maps the
	// public id to that hash for the admin paths. Like every other set in
	// this backend, the names keep the legacy "arcade_" prefix (see
	// aerospike.go).
	setAPIKeys   = "arcade_api_keys"
	setAPIKeyIDs = "arcade_api_key_ids" //nolint:gosec // Aerospike set name, not a credential
	setAPIQuota  = "arcade_api_quota"

	// apiKeyCASAttempts bounds mutateAPIKey's generation-checked retry loop
	// (UpdateAPIKeyTier / RevokeAPIKey). ReserveQuota needs no such bound:
	// it is a single atomic Operate call, not a CAS loop — see below.
	apiKeyCASAttempts = 16

	// apiKeyCompensationTimeout bounds compensating operations after an
	// in-doubt or definite CreateAPIKey failure: the rollback delete of the
	// id claim, and the read-back used to resolve an in-doubt hash Put.
	// These run detached from the caller's ctx (see compensationCtx) under
	// their own fixed budget, since that ctx may itself already be the
	// reason compensation is needed.
	apiKeyCompensationTimeout = 5 * time.Second
)

func apiKeyMs(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

func apiKeyTime(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

func apiKeyBins(k *store.APIKey) aero.BinMap {
	return aero.BinMap{
		"id": k.ID, "hash": k.Hash, "hint": k.Hint, "name": k.Name, "tier": k.Tier, "status": string(k.Status),
		"expires_at": apiKeyMs(k.ExpiresAt), "created_at": k.CreatedAt.UnixMilli(),
		"updated_at": k.UpdatedAt.UnixMilli(), "revoked_at": apiKeyMs(k.RevokedAt),
	}
}

func apiKeyFromRecord(rec *aero.Record) *store.APIKey {
	return &store.APIKey{
		ID: getString(rec, "id"), Hash: getString(rec, "hash"), Hint: getString(rec, "hint"),
		Name: getString(rec, "name"), Tier: getString(rec, "tier"),
		Status:    store.APIKeyStatus(getString(rec, "status")),
		ExpiresAt: apiKeyTime(getInt64(rec, "expires_at")),
		CreatedAt: time.UnixMilli(getInt64(rec, "created_at")).UTC(),
		UpdatedAt: time.UnixMilli(getInt64(rec, "updated_at")).UTC(),
		RevokedAt: apiKeyTime(getInt64(rec, "revoked_at")),
	}
}

// apiKeyWritePolicy builds a write policy for the two API-key sets
// (setAPIKeys, setAPIKeyIDs). Those records must survive regardless of the
// namespace's default-ttl configuration, so Expiration is pinned to
// TTLDontExpire on every write to them — both create and mutate paths.
// Quota windows (setAPIQuota) are not built from this: they keep their own
// QuotaRetention-based TTL.
func (s *Store) apiKeyWritePolicy(ctx context.Context) *aero.WritePolicy {
	p := s.writePolicy(ctx)
	p.Expiration = aero.TTLDontExpire
	return p
}

// compensationCtx returns a context detached from ctx's cancellation, with
// its own fixed timeout, for rollback work that must run to completion
// regardless of why the caller's ctx is in whatever state it's in — it may
// itself already be expired, which is precisely why compensation is needed.
func compensationCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), apiKeyCompensationTimeout)
}

// deleteIDClaim removes the id->hash claim left by a create that didn't end
// up owning hashKey, running under a detached compensation context. Its own
// failure is folded into createErr via errors.Join — the store has no
// logger, so this is the only way the detail survives to the caller.
func (s *Store) deleteIDClaim(ctx context.Context, idKey *aero.Key, createErr error) error {
	cctx, cancel := compensationCtx(ctx)
	defer cancel()
	if _, derr := s.client.Delete(s.writePolicy(cctx), idKey); derr != nil {
		return errors.Join(createErr, fmt.Errorf("rollback id claim: %w", derr))
	}
	return createErr
}

// CreateAPIKey claims the id first, then the hash, both CREATE_ONLY with
// MaxRetries=0 on both Puts. Every hash-Put error — including KEY_EXISTS —
// is treated as in-doubt and resolved the same way:
//
//   - KEY_EXISTS is NOT trusted as a definite failure. With retries enabled
//     a CREATE_ONLY Put can land on the server, time out before the
//     response arrives, and then have the client's own retry collide with
//     its own first write, surfacing KEY_EXISTS for a write that actually
//     succeeded. MaxRetries=0 closes that specific self-collision, but a
//     plain timeout (request reached the server, response lost) is still
//     possible either way, so every error — KEY_EXISTS included — gets the
//     same read-back treatment below rather than a fast path that assumes
//     the write never landed.
//   - We read hashKey back — detached from ctx, which may itself be why
//     we're here — to resolve the ambiguity: if it now holds our own id,
//     the write landed after all and this was a false failure, so we
//     report success; if it's missing, or holds a different key's id (a
//     hash collision, real or self-inflicted), the write never gave us
//     ownership, so the id claim is rolled back.
//   - If even that read-back fails, we can't tell either way. The id claim
//     is left in place: a key still reachable by id is recoverable, one
//     that silently vanished from both sets is not. The read failure is
//     folded into the returned error.
func (s *Store) CreateAPIKey(ctx context.Context, k *store.APIKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	idKey, err := s.key(setAPIKeyIDs, k.ID)
	if err != nil {
		return err
	}
	hashKey, err := s.key(setAPIKeys, k.Hash)
	if err != nil {
		return err
	}

	p := s.apiKeyWritePolicy(ctx)
	p.RecordExistsAction = aero.CREATE_ONLY
	p.MaxRetries = 0
	if aerr := s.client.Put(p, idKey, aero.BinMap{"hash": k.Hash}); aerr != nil {
		return fmt.Errorf("create api key %s: %w", k.ID, aerr)
	}

	p = s.apiKeyWritePolicy(ctx)
	p.RecordExistsAction = aero.CREATE_ONLY
	p.MaxRetries = 0
	aerr := s.client.Put(p, hashKey, apiKeyBins(k))
	if aerr == nil {
		return nil
	}
	createErr := fmt.Errorf("create api key %s: %w", k.ID, aerr)

	cctx, cancel := compensationCtx(ctx)
	defer cancel()
	rec, rerr := s.client.Get(s.readPolicy(cctx), hashKey)
	switch {
	case rerr == nil && rec != nil && getString(rec, "id") == k.ID:
		// The write landed after all; this was a false failure.
		return nil
	case rerr == nil || isKeyNotFound(rerr):
		// hashKey belongs to a different key, or is genuinely missing —
		// either way we don't own it.
		return s.deleteIDClaim(ctx, idKey, createErr)
	default:
		return errors.Join(createErr, fmt.Errorf("resolve in-doubt create for %s: %w", k.ID, rerr))
	}
}

func (s *Store) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := s.key(setAPIKeys, hash)
	if err != nil {
		return nil, err
	}
	rec, aerr := s.client.Get(s.readPolicy(ctx), key)
	if aerr != nil {
		if isKeyNotFound(aerr) {
			return nil, store.ErrAPIKeyNotFound
		}
		return nil, fmt.Errorf("get api key by hash: %w", aerr)
	}
	return apiKeyFromRecord(rec), nil
}

func (s *Store) hashForID(ctx context.Context, id string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	key, err := s.key(setAPIKeyIDs, id)
	if err != nil {
		return "", err
	}
	rec, aerr := s.client.Get(s.readPolicy(ctx), key)
	if aerr != nil {
		if isKeyNotFound(aerr) {
			return "", store.ErrAPIKeyNotFound
		}
		return "", fmt.Errorf("get api key %s: %w", id, aerr)
	}
	return getString(rec, "hash"), nil
}

// GetAPIKey resolves id -> hash -> record. It then checks that the resolved
// record's own id bin equals id: a CreateAPIKey rollback that failed to
// delete the id claim (e.g. a transient error on the compensating Delete)
// can leave an id record pointing at a hash that belongs to a different key,
// and handing that record back under the wrong id would be worse than a
// not-found.
func (s *Store) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	hash, err := s.hashForID(ctx, id)
	if err != nil {
		return nil, err
	}
	k, err := s.GetAPIKeyByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	if k.ID != id {
		return nil, store.ErrAPIKeyNotFound
	}
	return k, nil
}

// ListAPIKeys scans the whole arcade_api_keys set. A set scan walks the
// namespace's primary index unless the set has `enable-index true`
// configured, so this is deliberately an admin-only path (see the plan's
// "unpaginated" deviation): the key count is expected to stay small, and
// this is never called from a request hot path.
func (s *Store) ListAPIKeys(ctx context.Context) ([]*store.APIKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sp := aero.NewScanPolicy()
	sp.TotalTimeout = remaining(ctx, s.queryTimeout)
	sp.SocketTimeout = s.socketTimeout
	rs, aerr := s.client.ScanAll(sp, s.namespace, setAPIKeys)
	if aerr != nil {
		return nil, fmt.Errorf("list api keys: %w", aerr)
	}
	defer func() { _ = rs.Close() }()
	var out []*store.APIKey
	for res := range rs.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("list api keys: %w", res.Err)
		}
		out = append(out, apiKeyFromRecord(res.Record))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *Store) UpdateAPIKeyTier(ctx context.Context, id, tier string, at time.Time) error {
	return s.mutateAPIKey(ctx, id, func(k *store.APIKey) bool {
		k.Tier = tier
		k.UpdatedAt = at
		return true
	})
}

func (s *Store) RevokeAPIKey(ctx context.Context, id string, at time.Time) error {
	return s.mutateAPIKey(ctx, id, func(k *store.APIKey) bool {
		if k.Status == store.APIKeyRevoked {
			return false
		}
		k.Status = store.APIKeyRevoked
		k.RevokedAt = &at
		k.UpdatedAt = at
		return true
	})
}

// mutateAPIKey is a generation-checked read-modify-write on the hash record.
// Like GetAPIKey, it refuses to operate on a record whose id doesn't match
// the one we resolved through — the same stale id-claim guard.
func (s *Store) mutateAPIKey(ctx context.Context, id string, fn func(*store.APIKey) bool) error {
	hash, err := s.hashForID(ctx, id)
	if err != nil {
		return err
	}
	key, err := s.key(setAPIKeys, hash)
	if err != nil {
		return err
	}
	for range apiKeyCASAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, aerr := s.client.Get(s.readPolicy(ctx), key)
		if aerr != nil {
			if isKeyNotFound(aerr) {
				return store.ErrAPIKeyNotFound
			}
			return fmt.Errorf("update api key %s: %w", id, aerr)
		}
		k := apiKeyFromRecord(rec)
		if k.ID != id {
			return store.ErrAPIKeyNotFound
		}
		if !fn(k) {
			return nil
		}
		p := s.apiKeyWritePolicy(ctx)
		p.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		p.Generation = rec.Generation
		perr := s.client.Put(p, key, apiKeyBins(k))
		if perr == nil {
			return nil
		}
		if !isGenerationErr(perr) {
			return fmt.Errorf("update api key %s: %w", id, perr)
		}
	}
	return fmt.Errorf("update api key %s: gave up after %d contended attempts", id, apiKeyCASAttempts)
}

// quotaTTLSeconds returns the Expiration (in seconds) for a quota window's
// record so it expires exactly store.QuotaRetention after windowStart,
// regardless of how many reservations land in the window in the meantime —
// never the namespace's default-ttl (which in the dev namespace is 0,
// meaning "never expire"). Rounds up and clamps to at least 1s: 0 itself
// means TTLServerDefault, not "expire immediately".
func quotaTTLSeconds(windowStart, now time.Time) uint32 {
	left := windowStart.Add(store.QuotaRetention).Sub(now)
	secs := int64(left / time.Second)
	if left%time.Second != 0 {
		secs++
	}
	if secs < 1 {
		secs = 1
	}
	return uint32(secs) // clamped to [1, ~QuotaRetention], far below uint32 range
}

// ReserveQuota is a single atomic Operate call built from expressions: one
// round trip both reads the window's current usage and writes the clamped
// result, in the same per-record server-side transaction. Aerospike
// serializes concurrent operations on a single record, so this is exact
// under concurrency without a generation-checked retry loop — contrast
// mutateAPIKey above, whose read and write are two separate calls and so
// does need one.
//
// usedExpr reads the "used" bin, or 0 if the record (or the bin) doesn't
// exist yet — the same expression is referenced twice (once for the
// ExpReadOp, once inside nextExpr) and is evaluated fresh each time, but
// nothing else in this call writes "used" before nextExpr evaluates it, so
// both reads see the same pre-write value. ExpReadOp surfaces that value as
// the "prev" bin in the response; granted and usedAfter are then computed
// client side from prev with the exact same store.NextQuotaUsed formula
// nextExpr applies server side, so they're guaranteed to agree — there is
// no need to also read back what was written.
func (s *Store) ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	key, err := s.key(setAPIQuota, keyID+"|"+strconv.FormatInt(windowStart.UnixMilli(), 10))
	if err != nil {
		return 0, 0, err
	}

	usedExpr := aero.ExpCond(aero.ExpBinExists("used"), aero.ExpIntBin("used"), aero.ExpIntVal(0))
	nextExpr := aero.ExpMax(usedExpr, aero.ExpMin(aero.ExpNumAdd(usedExpr, aero.ExpIntVal(want)), aero.ExpIntVal(limit)))

	p := s.writePolicy(ctx)
	p.Expiration = quotaTTLSeconds(windowStart, time.Now())
	p.RespondPerEachOp = true
	// MaxRetries=0: a retried Operate would re-read "used" (now reflecting
	// the first attempt's own write, if it actually landed before timing
	// out) and grant again on top of it — a double-spend for one logical
	// reservation. The caller (the rate limiter) fails open on error, so
	// there's nothing to gain from retrying here.
	p.MaxRetries = 0

	rec, aerr := s.client.Operate(p, key,
		aero.ExpReadOp("prev", usedExpr, aero.ExpReadFlagDefault),
		aero.ExpWriteOp("used", nextExpr, aero.ExpWriteFlagDefault),
	)
	if aerr != nil {
		return 0, 0, fmt.Errorf("reserve quota %s: %w", keyID, aerr)
	}
	prev := getInt64(rec, "prev")
	next := store.NextQuotaUsed(prev, want, limit)
	return next - prev, next, nil
}
