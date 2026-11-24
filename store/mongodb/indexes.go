package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/broadcaster/models"
)

// Index names. Hints reference these, so a rename here must be paired with
// the hint sites (grep the constant).
const (
	idxTxStatusHeight  = "tx_status_block_height_id"
	idxTxStatusTS      = "tx_status_timestamp"
	idxTxBlockHash     = "tx_block_hash"
	idxTxTimestamp     = "tx_timestamp"
	idxTxRetryReady    = "tx_retry_ready"
	idxTxExpires       = "tx_expires_ttl"
	idxSubTxIDToken    = "sub_txid_token" //nolint:gosec // an index name, not a credential
	idxSubTokenTxID    = "sub_token_txid" //nolint:gosec // an index name, not a credential
	idxSubRetryReady   = "sub_retry_ready"
	idxSubExpires      = "sub_expires_ttl"
	idxBPHeightDesc    = "bp_height_desc"
	idxBPStatusHeight  = "bp_status_height"
	idxBPStaleSeen     = "bp_stale_seen"
	idxBPOrphaned      = "bp_orphaned_unreconciled"
	idxLeaseExpires    = "lease_expires_ttl"
	idxDHNetwork       = "dh_network"
	idxDHLastSeen      = "dh_last_seen"
	idxPPNetwork       = "pp_network"
	idxPPLastSeen      = "pp_last_seen"
	idxBumpsBlockHash  = "bumps_block_hash"
	idxStumpsBlockHash = "stumps_block_hash_subtree"
	idxStumpManifests  = "stump_manifest_block_hash"
	idxAPIKeyHash      = "api_key_hash" //nolint:gosec // an index name, not a credential
	idxAPIKeyCreated   = "api_key_created"
	idxQuotaExpires    = "api_quota_expires_ttl"
	// The two GridFS-spec indexes keep the driver's default names so a
	// database the driver (or a restore) already provisioned is recognised as
	// identical instead of failing createIndexes with a name conflict.
	idxGridFSFiles  = "filename_1_uploadDate_1"
	idxGridFSChunks = "files_id_1_n_1"
)

// leaseReapGraceSeconds is how long after expires_at the TTL monitor may
// delete a lease row. See the leases index for why it is not zero.
const leaseReapGraceSeconds = 3600

// ttlIndex names a TTL index and the expireAfterSeconds this build wants for
// it — the one index option createIndexes can repair rather than refuse.
type ttlIndex struct {
	name    string
	seconds int32
}

// indexOptionsConflict is the server code for "an index with this name and
// keys exists with different options" — what createIndexes returns when a
// deployment provisioned before a TTL change asks for the new value.
const indexOptionsConflict = 85

// createIndexes is CreateMany with one repair: a TTL index whose
// expireAfterSeconds has changed since the database was provisioned. Every
// other option mismatch is a genuine conflict and still fails loudly
// (IndexKeySpecsConflict for different keys, IndexOptionsConflict for
// anything but a TTL), but expireAfterSeconds is the one option this package
// tunes, and the server's own remedy for it is collMod, not drop-and-rebuild.
// So on IndexOptionsConflict, each TTL index the spec declares is collMod'd to
// its requested value and the create is retried once; if the conflict was not
// about a TTL, the retry fails the same way and that error is returned.
func createIndexes(ctx context.Context, coll *mongo.Collection, models []mongo.IndexModel, ttls []ttlIndex) error {
	_, err := coll.Indexes().CreateMany(ctx, models)
	var se mongo.ServerError
	if err == nil || len(ttls) == 0 || !errors.As(err, &se) || !se.HasErrorCode(indexOptionsConflict) {
		return err
	}
	for _, t := range ttls {
		cmd := bson.D{
			{Key: "collMod", Value: coll.Name()},
			{Key: "index", Value: bson.D{
				{Key: "name", Value: t.name},
				{Key: "expireAfterSeconds", Value: t.seconds},
			}},
		}
		if cerr := coll.Database().RunCommand(ctx, cmd).Err(); cerr != nil {
			return fmt.Errorf("%w (and collMod of TTL index %s failed: %w)", err, t.name, cerr)
		}
	}
	_, err = coll.Indexes().CreateMany(ctx, models)
	return err
}

// EnsureIndexes provisions every secondary index the queries in this package
// rely on. createIndexes is idempotent for an identical spec and fails
// loudly (IndexKeySpecsConflict) when a named index exists with a different
// spec, which is the behaviour we want: a silently coexisting stale index
// would let a hint point at the wrong plan.
//
// The index set mirrors store/postgres/schema.sql one-for-one; partial
// indexes stand in for Postgres' partial indexes where MongoDB can express
// the predicate (equality, $gt, $exists:true). `IS NULL` predicates cannot be
// partial-index conditions, so the block_processing indexes are compound
// over the nullable field instead — a missing field indexes as null, so the
// query {status:"active", processed_at:null} still gets tight bounds (and
// null, unlike $exists:false, also matches a document that carries an
// explicit null — what a migration from Postgres writes for NULL). GridFS's own indexes are created explicitly too: the driver only
// auto-creates them when the files collection is empty at first write, which
// a restore or migration can defeat.
//
// One exception: a TTL index whose expireAfterSeconds differs from the one
// this build wants is repaired with collMod and retried (see createIndexes),
// because that is the one option this package tunes. collMod is a dbAdmin
// action, not part of readWrite, so the application user needs dbAdmin (or
// the collMod privilege) on the database as well as readWrite.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	// The TTL indexes each collection declares, keyed by collection name, so
	// createIndexes can repair a changed expireAfterSeconds (see there).
	ttlByColl := map[string][]ttlIndex{
		collLeases:   {{idxLeaseExpires, leaseReapGraceSeconds}},
		collAPIQuota: {{idxQuotaExpires, 0}},
	}
	specs := append(s.authIndexSpecs(), []indexSpec{
		{s.tx, []mongo.IndexModel{
			idx(idxTxStatusHeight, bson.D{{Key: fStatus, Value: 1}, {Key: fBlockHeight, Value: 1}, {Key: fID, Value: 1}}),
			idx(idxTxStatusTS, bson.D{{Key: fStatus, Value: 1}, {Key: fTimestamp, Value: 1}}),
			// {block_hash, _id, status}: equality on the hash plus an
			// _id-ordered scan for SetStatusByBlockHash's keyset pages, no
			// in-memory sort. status is the third key, after the sort key, so
			// the page stays index-ordered — it is here to be READ, not to
			// bound the scan. Without it blockPage's status != IMMUTABLE is a
			// residual predicate, so the server FETCHes every candidate
			// document (raw_tx and all) out of WiredTiger just to test one
			// string inequality and project _id back. With it the page is
			// PROJECTION_COVERED and examines zero documents — measured on
			// mongod 7 as 550 -> 0 docsExamined for one 500-row page, over a
			// scan SetStatusByBlockHash repeats for the whole block and then
			// re-walks once per drain pass.
			idxPartial(idxTxBlockHash, bson.D{{Key: fBlockHash, Value: 1}, {Key: fID, Value: 1}, {Key: fStatus, Value: 1}},
				bson.D{{Key: fBlockHash, Value: bson.D{{Key: opExists, Value: true}}}}),
			idx(idxTxTimestamp, bson.D{{Key: fTimestamp, Value: 1}}),
			idxPartial(idxTxRetryReady, bson.D{{Key: fNextRetryAt, Value: 1}},
				bson.D{{Key: fStatus, Value: string(models.StatusPendingRetry)}}),
		}},
		{s.subs, []mongo.IndexModel{
			idx(idxSubTxIDToken, bson.D{{Key: fTxID, Value: 1}, {Key: fCallbackToken, Value: 1}}),
			idx(idxSubTokenTxID, bson.D{{Key: fCallbackToken, Value: 1}, {Key: fTxID, Value: 1}}),
			idxPartial(idxSubRetryReady, bson.D{{Key: fNextRetryAt, Value: 1}},
				bson.D{{Key: fRetryCount, Value: bson.D{{Key: opGt, Value: 0}}}}),
		}},
		{s.blocks, []mongo.IndexModel{
			idx(idxBPHeightDesc, bson.D{{Key: fBlockHeight, Value: -1}, {Key: fID, Value: 1}}),
			idx(idxBPStatusHeight, bson.D{{Key: fStatus, Value: 1}, {Key: fBlockHeight, Value: -1}}),
			idx(idxBPStaleSeen, bson.D{{Key: fStatus, Value: 1}, {Key: fProcessedAt, Value: 1}, {Key: fHeaderSeenAt, Value: 1}}),
			idx(idxBPOrphaned, bson.D{{Key: fStatus, Value: 1}, {Key: fReconciledAt, Value: 1}, {Key: fOrphanedAt, Value: 1}}),
		}},
		// TTL with a grace period, not at expiry. A TTL of zero would make the
		// server's clock a second authority on lease expiry: a mongod running
		// ahead of the pods by more than (ttl - renew interval) would delete
		// a live lease and let the next contender's upsert win — two leaders.
		// The client-side expires_at comparison is the only expiry that
		// counts; this reaps rows that have been dead for an hour.
		{s.leases, []mongo.IndexModel{{
			Keys:    bson.D{{Key: fExpiresAt, Value: 1}},
			Options: options.Index().SetName(idxLeaseExpires).SetExpireAfterSeconds(leaseReapGraceSeconds),
		}}},
		{s.datahubs, []mongo.IndexModel{
			idx(idxDHNetwork, bson.D{{Key: fNetwork, Value: 1}}),
			idx(idxDHLastSeen, bson.D{{Key: fLastSeen, Value: 1}}),
		}},
		{s.peers, []mongo.IndexModel{
			idx(idxPPNetwork, bson.D{{Key: fNetwork, Value: 1}}),
			idx(idxPPLastSeen, bson.D{{Key: fLastSeen, Value: 1}}),
		}},
		{s.stumps.manifests, []mongo.IndexModel{
			idx(idxStumpManifests, bson.D{{Key: fBlockHash, Value: 1}, {Key: fSubtreeIndex, Value: 1}}),
		}},
		// Blob reads go through the manifests; the metadata indexes on the
		// files collections serve only the stale-upload sweep in blobs.go,
		// which ages files by their completion time (uploadDate).
		{s.bumps.bucket.GetFilesCollection(), append(gridfsFilesIndexes(),
			idx(idxBumpsBlockHash, bson.D{{Key: fMetaBlockHash, Value: 1}, {Key: fUploadDate, Value: 1}}))},
		{s.bumps.bucket.GetChunksCollection(), gridfsChunksIndexes()},
		{s.stumps.bucket.GetFilesCollection(), append(gridfsFilesIndexes(),
			idx(idxStumpsBlockHash, bson.D{{Key: fMetaBlockHash, Value: 1}, {Key: fMetaSubtreeIndex, Value: 1}, {Key: fUploadDate, Value: 1}}))},
		{s.stumps.bucket.GetChunksCollection(), gridfsChunksIndexes()},
	}...)
	return s.provisionIndexes(ctx, specs, ttlByColl)
}

// indexSpec is one collection's wanted index set.
type indexSpec struct {
	coll   *mongo.Collection
	models []mongo.IndexModel
}

// authIndexSpecs is the index set behind APIKeyStore and QuotaStore, shared
// by EnsureIndexes and AuthStore.EnsureIndexes so the two produce identical
// index names and specs and can coexist in one database.
func (s *Store) authIndexSpecs() []indexSpec {
	return []indexSpec{
		{s.apiKeys, []mongo.IndexModel{
			{Keys: bson.D{{Key: fAPIKeyHash, Value: 1}}, Options: options.Index().SetName(idxAPIKeyHash).SetUnique(true)},
			idx(idxAPIKeyCreated, bson.D{{Key: fCreatedAt, Value: 1}, {Key: fID, Value: 1}}),
		}},
		// expires_at is window_start + QuotaRetention, so a zero TTL reaps a
		// window once it is past any use; unlike leases, a clock-skewed early
		// reap here only forgets usage that no longer counts.
		{s.apiQuota, []mongo.IndexModel{{
			Keys:    bson.D{{Key: fExpiresAt, Value: 1}},
			Options: options.Index().SetName(idxQuotaExpires).SetExpireAfterSeconds(0),
		}}},
	}
}

// provisionIndexes creates every spec's indexes, repairing the TTL indexes
// ttlByColl names (see createIndexes).
func (s *Store) provisionIndexes(ctx context.Context, specs []indexSpec, ttlByColl map[string][]ttlIndex) error {
	// Collections are independent, so provision them concurrently: every
	// collection's sequential createIndexes round-trip is the dominant cost
	// of a boot against an already-indexed database.
	//
	// Bounded by index_timeout_ms (Postgres's schema_apply_timeout_ms is the
	// precedent): createIndexes on a restored, populated collection is the
	// one boot-time operation whose duration scales with data, and without a
	// deadline a stalled build sits silently past the liveness probe. Every
	// failure is returned, not just the first — every collection can fail
	// for its own reason and the operator should see all of them.
	if s.indexTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.indexTimeout)
		defer cancel()
	}
	errs := make(chan error, len(specs))
	var wg sync.WaitGroup
	for _, sp := range specs {
		wg.Add(1)
		go func(sp indexSpec) {
			defer wg.Done()
			if err := createIndexes(ctx, sp.coll, sp.models, ttlByColl[sp.coll.Name()]); err != nil {
				errs <- fmt.Errorf("mongodb: create indexes on %s: %w", sp.coll.Name(), err)
			}
		}(sp)
	}
	wg.Wait()
	close(errs)
	var all []error
	for err := range errs {
		all = append(all, err)
	}
	return errors.Join(all...)
}

func idx(name string, keys bson.D) mongo.IndexModel {
	return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name)}
}

// retentionTTL is the data-retention TTL index on expires_at: the TTL
// monitor deletes a document once its own expires_at has passed
// (expireAfterSeconds 0). Partial on the field existing, so rows kept
// forever (no expires_at) are not indexed at all.
func retentionTTL(name string) mongo.IndexModel {
	return mongo.IndexModel{
		Keys: bson.D{{Key: fExpiresAt, Value: 1}},
		Options: options.Index().SetName(name).SetExpireAfterSeconds(0).
			SetPartialFilterExpression(bson.D{{Key: fExpiresAt, Value: bson.D{{Key: opExists, Value: true}}}}),
	}
}

func idxPartial(name string, keys, filter bson.D) mongo.IndexModel {
	return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name).SetPartialFilterExpression(filter)}
}

// gridfsFilesIndexes is the {filename, uploadDate} index the GridFS spec
// requires on <bucket>.files.
func gridfsFilesIndexes() []mongo.IndexModel {
	return []mongo.IndexModel{idx(idxGridFSFiles, bson.D{{Key: fFilename, Value: 1}, {Key: fUploadDate, Value: 1}})}
}

// gridfsChunksIndexes is the unique {files_id, n} index the GridFS spec
// requires on <bucket>.chunks.
func gridfsChunksIndexes() []mongo.IndexModel {
	return []mongo.IndexModel{{
		Keys:    bson.D{{Key: fFilesID, Value: 1}, {Key: fChunkN, Value: 1}},
		Options: options.Index().SetName(idxGridFSChunks).SetUnique(true),
	}}
}
