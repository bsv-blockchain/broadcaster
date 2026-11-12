package api_server

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
	"github.com/bsv-blockchain/broadcaster/store"
)

// maxExtendSlack bounds how much later a resubmitter's expiry must be than
// the row's before the row is rewritten. Without a slack every resubmit by
// a caller of the same tier (clients that poll by resubmitting) would be a
// write. The slack is a thirtieth of the retention, capped here, so a short
// retention is not cut noticeably short: a 7-day row is refreshed by a
// resubmit more than ~5.6h after its last extension.
const maxExtendSlack = 24 * time.Hour

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
	s.retention = ext
	// In the background: on a large collection the build can take up to
	// index_timeout_ms, and readiness must not wait on it. Rows are stamped
	// meanwhile and expire once the indexes exist.
	go func() {
		if err := ext.EnsureRetentionIndexes(ctx); err != nil {
			s.logger.Error("could not provision retention indexes; expired rows are not deleted until they exist", zap.Error(err))
		}
	}()
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

// extendOnResubmit extends an existing row's retention after a successful
// GetOrInsertStatus that found the row already there (a resubmit). A fresh
// insert already carries this request's expiry. Callers handle a failed
// CAS first: it leaves the row state unknown.
//
// A failed extension is returned: the caller answers 503 so the client's
// retry repeats it (extending is idempotent), rather than acknowledging a
// submission whose row would expire before the caller's retention.
func (s *Server) extendOnResubmit(ctx context.Context, txid string, existing *models.TransactionStatus, inserted bool, until *time.Time) error {
	if s.retention == nil || inserted || existing == nil || !extends(existing.ExpiresAt, until, time.Now()) {
		return nil
	}
	if err := s.retention.ExtendTransactionExpiry(ctx, txid, until); err != nil {
		metrics.RetentionExtendFailuresTotal.Inc()
		s.logger.Error("could not extend transaction retention", logfields.TxID(txid), zap.Error(err))
		return err
	}
	return nil
}

// extends reports whether until is meaningfully longer retention than the
// row's current expiry: forever beats any date, nothing beats forever, and
// a later date must beat the current one by the slack (a thirtieth of the
// retention until represents, at most maxExtendSlack) to be worth a write.
func extends(current, until *time.Time, now time.Time) bool {
	switch {
	case current == nil:
		return false
	case until == nil:
		return true
	default:
		slack := min(maxExtendSlack, until.Sub(now)/30)
		return until.Sub(*current) > slack
	}
}
