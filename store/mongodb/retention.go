package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// EnsureRetentionIndexes implements store.RetentionExtender: the TTL indexes
// that delete transaction and submission rows once their expires_at passes.
// They are not part of EnsureIndexes, so an upgrade does not build them on
// a large collection in every service at boot; only an api-server whose
// config enables retention does, bounded by index_timeout_ms. A changed
// expireAfterSeconds is repaired with collMod, as for the other TTLs.
func (s *Store) EnsureRetentionIndexes(ctx context.Context) error {
	return s.provisionIndexes(ctx, []indexSpec{
		{s.tx, []mongo.IndexModel{retentionTTL(idxTxExpires)}},
		{s.subs, []mongo.IndexModel{retentionTTL(idxSubExpires)}},
	}, map[string][]ttlIndex{
		collTransactions: {{idxTxExpires, 0}},
		collSubmissions:  {{idxSubExpires, 0}},
	})
}

// ExtendTransactionExpiry implements store.RetentionExtender. The filter
// does the comparison, so a concurrent extension from another replica can
// only ever raise the expiry: a row without expires_at (kept forever) never
// matches, and a row whose expiry is already at or past until is left alone.
func (s *Store) ExtendTransactionExpiry(ctx context.Context, txid string, until *time.Time) error {
	filter := bson.D{{Key: fID, Value: txid}}
	var update bson.D
	if until == nil {
		filter = append(filter, bson.E{Key: fExpiresAt, Value: bson.D{{Key: opExists, Value: true}}})
		update = bson.D{{Key: opUnset, Value: bson.D{{Key: fExpiresAt, Value: ""}}}}
	} else {
		t := msTrunc(*until)
		filter = append(filter, bson.E{Key: fExpiresAt, Value: bson.D{{Key: opLt, Value: t}}})
		update = bson.D{{Key: opSet, Value: bson.D{{Key: fExpiresAt, Value: t}}}}
	}
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.tx.UpdateOne(octx, filter, update); err != nil {
		return fmt.Errorf("extend expiry of tx %s: %w", txid, err)
	}
	return nil
}
