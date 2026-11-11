package store

import (
	"context"
	"time"
)

// RetentionExtender is implemented by backends that enforce data retention:
// they persist models.TransactionStatus.ExpiresAt and
// models.Submission.ExpiresAt at insert and delete rows once it passes.
// Like APIKeyStore it is separate from Store so the api-server discovers it
// by type assertion; a backend without it keeps every row forever.
type RetentionExtender interface {
	// EnsureRetentionIndexes provisions whatever deletes expired rows (for
	// MongoDB, the TTL indexes). Called once at api-server boot when a tier
	// configures retention; safe to repeat.
	EnsureRetentionIndexes(ctx context.Context) error
	// ExtendTransactionExpiry raises the transaction row's expiry to until
	// when a later submitter is entitled to longer retention than the one
	// that created the row. until nil means "keep forever" and removes the
	// expiry. It never shortens a row's retention, never adds an expiry to
	// a row kept forever, and is a no-op for a txid with no row.
	ExtendTransactionExpiry(ctx context.Context, txid string, until *time.Time) error
}
