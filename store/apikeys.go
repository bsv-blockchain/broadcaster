package store

import (
	"context"
	"errors"
	"time"
)

// APIKeyStatus is the lifecycle state of an API key.
type APIKeyStatus string

const (
	APIKeyActive  APIKeyStatus = "active"
	APIKeyRevoked APIKeyStatus = "revoked"
)

// ErrAPIKeyNotFound is returned by APIKeyStore lookups and mutations when no
// key matches.
var ErrAPIKeyNotFound = errors.New("api key not found")

// APIKey is a stored client credential. Only the SHA-256 of the full key is
// persisted (Hash): the plaintext is shown once, at creation, and never
// stored. Callers write times as UTC with millisecond precision so every
// backend round-trips them exactly.
type APIKey struct {
	ID        string
	Hash      string
	Hint      string
	Name      string
	Tier      string
	Status    APIKeyStatus
	ExpiresAt *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
	RevokedAt *time.Time
	// LimitID names the allowance the key draws quota and concurrency from;
	// keys on one plan share it. Only keys verified by a delegated-auth
	// provider carry one, and no backend persists it: empty means the key
	// is its own allowance.
	LimitID string
	// Provider and ProviderKeyID identify a key verified by a delegated-auth
	// provider: the provider's name and the key id it issued, before ID's
	// "<provider>:" namespace. Usage reported to that provider is keyed by
	// ProviderKeyID. Empty for the broadcaster's own keys; never persisted.
	Provider      string
	ProviderKeyID string
}

// QuotaID is the identity the rate limiter accounts this key under: its
// LimitID when it has one, else its ID.
func (k *APIKey) QuotaID() string {
	if k.LimitID != "" {
		return k.LimitID
	}
	return k.ID
}

// Usable reports whether the key may authenticate a request at now.
func (k *APIKey) Usable(now time.Time) bool {
	return k.Status == APIKeyActive && (k.ExpiresAt == nil || now.Before(*k.ExpiresAt))
}

// APIKeyStore persists API keys. Like Leaser it is separate from Store so
// the api-server can discover it by type assertion; every backend
// implements it.
type APIKeyStore interface {
	// CreateAPIKey inserts k. It fails when k.ID or k.Hash already exists and
	// leaves no partial row behind.
	CreateAPIKey(ctx context.Context, k *APIKey) error
	// GetAPIKeyByHash returns ErrAPIKeyNotFound when no key has this hash.
	GetAPIKeyByHash(ctx context.Context, hash string) (*APIKey, error)
	// GetAPIKey returns ErrAPIKeyNotFound when no key has this id.
	GetAPIKey(ctx context.Context, id string) (*APIKey, error)
	// ListAPIKeys returns every key ordered by CreatedAt, then ID.
	ListAPIKeys(ctx context.Context) ([]*APIKey, error)
	// UpdateAPIKeyTier sets the tier and UpdatedAt=at. This is allowed even
	// on a revoked key: the tier is recorded but the key stays revoked.
	UpdateAPIKeyTier(ctx context.Context, id, tier string, at time.Time) error
	// RevokeAPIKey marks the key revoked at at. The first call sets Status,
	// RevokedAt and UpdatedAt to at; a repeat revoke is a no-op that changes
	// nothing, including UpdatedAt.
	RevokeAPIKey(ctx context.Context, id string, at time.Time) error
}

// QuotaRetention is how long a quota window's counter is kept after the
// window starts. Windows are one minute; the slack covers clock skew between
// replicas and late reservations.
const QuotaRetention = 10 * time.Minute

// QuotaStore holds the shared per-key, per-window usage counters behind the
// rate limiter.
type QuotaStore interface {
	// ReserveQuota atomically grants up to want units from the window keyed
	// (keyID, windowStart): used' = NextQuotaUsed(used, want, limit) and
	// granted = used' - used. It returns the grant and used'. Counters expire
	// QuotaRetention after windowStart. Callers pass want > 0 and limit > 0;
	// an unbounded tier never calls ReserveQuota at all. windowStart is a UTC
	// minute boundary (t.UTC().Truncate(time.Minute)).
	ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (granted, usedAfter int64, err error)
}

// NextQuotaUsed is the reservation arithmetic every QuotaStore applies. The
// outer max keeps a limit lowered mid-window (a tier downgrade) from
// shrinking a window's recorded usage.
func NextQuotaUsed(used, want, limit int64) int64 {
	return max(used, min(used+want, limit))
}
