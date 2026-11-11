package api_server

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/store"
)

// extendSlack is how much later a resubmitter's expiry must be than the
// row's before the row is rewritten. Without it every resubmit by a caller
// of the same tier (clients that poll by resubmitting) would be a write.
const extendSlack = 24 * time.Hour

// initRetention enables per-tier data retention when a tier configures
// retention_days and the store can enforce it, and provisions what deletes
// expired rows. On a store that cannot, rows are kept forever and the boot
// log says so, rather than failing: the same config is shared by
// deployments on different backends. A failed index build is logged, not
// fatal: rows are still stamped, and expire once the indexes exist (built
// on a later boot, or ahead of time by an operator).
func (s *Server) initRetention(ctx context.Context) {
	if s.store == nil || !s.cfg.RateLimit.RetentionConfigured() {
		return
	}
	ext, ok := s.store.(store.RetentionExtender)
	if !ok {
		s.logger.Warn("ratelimit tiers configure retention_days but this store backend does not enforce retention; rows are kept forever",
			zap.String("backend", s.cfg.Store.Backend))
		return
	}
	if err := ext.EnsureRetentionIndexes(ctx); err != nil {
		s.logger.Error("could not provision retention indexes; expired rows are not deleted until they exist", zap.Error(err))
	}
	s.retention = ext
}

// retentionExpiry is when the rows this request writes may be deleted,
// from the caller's tier (anonymous when auth is off). nil keeps them
// forever: retention is off, or the tier keeps forever.
func (s *Server) retentionExpiry(c *gin.Context) *time.Time {
	if s.retention == nil {
		return nil
	}
	d := s.cfg.RateLimit.RetentionFor(apikey.PrincipalFrom(c).Tier)
	if d <= 0 {
		return nil
	}
	t := time.Now().Add(d).UTC()
	return &t
}

// extendOnResubmit extends an existing row's retention after a
// GetOrInsertStatus that found the row already there (a resubmit). A fresh
// insert already carries this request's expiry, and a failed CAS leaves the
// row state unknown.
func (s *Server) extendOnResubmit(ctx context.Context, txid string, existing *models.TransactionStatus, inserted bool, err error, until *time.Time) {
	if err != nil || inserted || existing == nil || !extends(existing.ExpiresAt, until) {
		return
	}
	s.extendRetention(ctx, txid, until)
}

// extends reports whether until is meaningfully longer retention than the
// row's current expiry: forever beats any date, nothing beats forever, and
// a later date must be more than extendSlack later to be worth a write.
func extends(current, until *time.Time) bool {
	switch {
	case current == nil:
		return false
	case until == nil:
		return true
	default:
		return until.Sub(*current) > extendSlack
	}
}

// extendRetention raises an existing transaction row's expiry to this
// submitter's, so a row lives as long as its most entitled submitter needs.
// Best-effort: a failure leaves the row's earlier retention in place.
func (s *Server) extendRetention(ctx context.Context, txid string, until *time.Time) {
	if s.retention == nil {
		return
	}
	if err := s.retention.ExtendTransactionExpiry(ctx, txid, until); err != nil {
		s.logger.Warn("could not extend transaction retention", logfields.TxID(txid), zap.Error(err))
	}
}
