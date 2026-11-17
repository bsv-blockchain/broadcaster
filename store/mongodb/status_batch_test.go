//go:build mongodb

package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/broadcaster/models"
)

// requireNativeStatusBatch skips unless the server has client bulkWrite, so
// these tests exercise the native path rather than the per-row fallback.
func requireNativeStatusBatch(t *testing.T, s *Store) {
	t.Helper()
	if !s.clientBulkWriteSupported(context.Background()) {
		t.Skip("server has no client-level bulkWrite (MongoDB < 8.0); the per-row fallback runs instead")
	}
}

func seedRowStatus(t *testing.T, s *Store, txid string, status models.Status) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid}); err != nil {
		t.Fatalf("insert %s: %v", txid, err)
	}
	if status != models.StatusReceived {
		if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: status}); err != nil {
			t.Fatalf("set %s to %s: %v", txid, status, err)
		}
	}
}

func currentStatus(t *testing.T, s *Store, txid string) (models.Status, int64) {
	t.Helper()
	var d txDoc
	if err := s.tx.FindOne(context.Background(), idFilter(txid)).Decode(&d); err != nil {
		t.Fatalf("read %s: %v", txid, err)
	}
	return models.Status(d.Status), d.Version
}

// Each row resolves exactly as UpdateStatusReturning would: an applied row
// reports the row it overwrote as Prev, a lattice-blocked row reports the
// row as Current and is left alone, an unknown txid is the zero value, and a
// row written before version existed is still updated.
func TestBatchUpdateStatusReturning_Native_ResolvesEachRow(t *testing.T) {
	s := newTestStore(t)
	requireNativeStatusBatch(t, s)
	ctx := context.Background()

	seedRowStatus(t, s, "nb-received", models.StatusReceived)
	seedRowStatus(t, s, "nb-seen", models.StatusSeenOnNetwork)
	seedRowStatus(t, s, "nb-mined", models.StatusMined)
	if _, err := s.tx.InsertOne(ctx, bson.D{{Key: fID, Value: "nb-noversion"}, {Key: fStatus, Value: string(models.StatusAcceptedByNetwork)}}); err != nil {
		t.Fatalf("insert versionless row: %v", err)
	}
	_, seenVersion := currentStatus(t, s, "nb-seen")

	in := []*models.TransactionStatus{
		{TxID: "nb-received", Status: models.StatusSeenMultipleNodes},
		{TxID: "nb-seen", Status: models.StatusSeenMultipleNodes},
		{TxID: "nb-mined", Status: models.StatusSeenMultipleNodes},
		{TxID: "nb-absent", Status: models.StatusSeenMultipleNodes},
		{TxID: "nb-noversion", Status: models.StatusSeenMultipleNodes},
	}
	res, err := s.BatchUpdateStatusReturning(ctx, in)
	if err != nil {
		t.Fatalf("BatchUpdateStatusReturning: %v", err)
	}

	for _, c := range []struct {
		i    int
		prev models.Status
	}{{0, models.StatusReceived}, {1, models.StatusSeenOnNetwork}, {4, models.StatusAcceptedByNetwork}} {
		if r := res[c.i]; r.Prev == nil || r.Prev.Status != c.prev || r.Current != nil {
			t.Errorf("results[%d] (%s) = %+v, want applied over %s", c.i, in[c.i].TxID, r, c.prev)
		}
		if got, _ := currentStatus(t, s, in[c.i].TxID); got != models.StatusSeenMultipleNodes {
			t.Errorf("%s stored as %s, want SEEN_MULTIPLE_NODES", in[c.i].TxID, got)
		}
	}
	if _, v := currentStatus(t, s, "nb-seen"); v != seenVersion+1 {
		t.Errorf("nb-seen version = %d, want %d (one write)", v, seenVersion+1)
	}
	if r := res[2]; r.Prev != nil || r.Current == nil || r.Current.Status != models.StatusMined {
		t.Errorf("results[2] (lattice-blocked) = %+v, want Current MINED", r)
	}
	if got, _ := currentStatus(t, s, "nb-mined"); got != models.StatusMined {
		t.Errorf("nb-mined stored as %s, want it left MINED", got)
	}
	if r := res[3]; r.Prev != nil || r.Current != nil {
		t.Errorf("results[3] (absent) = %+v, want the zero value", r)
	}
}

// A write that lands between the batch's read and its bulk update makes the
// version guard miss. That row must be resolved afresh, reporting the
// intervening write as its pre-image, never the stale row from the read and
// never "skipped".
func TestBatchUpdateStatusReturning_Native_ConcurrentWriteRetried(t *testing.T) {
	s := newTestStore(t)
	requireNativeStatusBatch(t, s)
	ctx := context.Background()

	seedRowStatus(t, s, "nb-race", models.StatusAcceptedByNetwork)
	seedRowStatus(t, s, "nb-calm", models.StatusAcceptedByNetwork)
	s.afterStatusRead = func() {
		s.afterStatusRead = nil
		if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: "nb-race", Status: models.StatusSeenOnNetwork}); err != nil {
			t.Errorf("intervening write: %v", err)
		}
	}

	res, err := s.BatchUpdateStatusReturning(ctx, []*models.TransactionStatus{
		{TxID: "nb-race", Status: models.StatusSeenMultipleNodes},
		{TxID: "nb-calm", Status: models.StatusSeenMultipleNodes},
	})
	if err != nil {
		t.Fatalf("BatchUpdateStatusReturning: %v", err)
	}
	if r := res[0]; r.Prev == nil || r.Prev.Status != models.StatusSeenOnNetwork {
		t.Errorf("raced row = %+v, want applied over the intervening SEEN_ON_NETWORK", r)
	}
	if r := res[1]; r.Prev == nil || r.Prev.Status != models.StatusAcceptedByNetwork {
		t.Errorf("calm row = %+v, want applied over ACCEPTED_BY_NETWORK", r)
	}
	if got, _ := currentStatus(t, s, "nb-race"); got != models.StatusSeenMultipleNodes {
		t.Errorf("raced row stored as %s, want SEEN_MULTIPLE_NODES", got)
	}
}

// A txid repeated in one batch is applied in input order: each later
// occurrence sees the earlier one's write as its pre-image.
func TestBatchUpdateStatusReturning_Native_RepeatedTxidInOrder(t *testing.T) {
	s := newTestStore(t)
	requireNativeStatusBatch(t, s)
	ctx := context.Background()

	seedRowStatus(t, s, "nb-rep", models.StatusAcceptedByNetwork)
	res, err := s.BatchUpdateStatusReturning(ctx, []*models.TransactionStatus{
		{TxID: "nb-rep", Status: models.StatusSeenOnNetwork},
		{TxID: "nb-rep", Status: models.StatusSeenMultipleNodes},
	})
	if err != nil {
		t.Fatalf("BatchUpdateStatusReturning: %v", err)
	}
	if r := res[0]; r.Prev == nil || r.Prev.Status != models.StatusAcceptedByNetwork {
		t.Errorf("first occurrence = %+v, want applied over ACCEPTED_BY_NETWORK", r)
	}
	if r := res[1]; r.Prev == nil || r.Prev.Status != models.StatusSeenOnNetwork {
		t.Errorf("second occurrence = %+v, want applied over the first's SEEN_ON_NETWORK", r)
	}
	if got, v := currentStatus(t, s, "nb-rep"); got != models.StatusSeenMultipleNodes || v != 4 {
		t.Errorf("stored %s at version %d, want SEEN_MULTIPLE_NODES at 4", got, v)
	}
}

// A batch larger than batch_size is written in several bulkWrites; every row
// still resolves.
func TestBatchUpdateStatusReturning_Native_ManyChunks(t *testing.T) {
	s := newTestStore(t)
	requireNativeStatusBatch(t, s)
	s.batchSize = 7
	ctx := context.Background()

	var in []*models.TransactionStatus
	for i := 0; i < 30; i++ {
		txid := pgBatchTxID(i)
		seedRowStatus(t, s, txid, models.StatusAcceptedByNetwork)
		in = append(in, &models.TransactionStatus{TxID: txid, Status: models.StatusSeenOnNetwork})
	}
	res, err := s.BatchUpdateStatusReturning(ctx, in)
	if err != nil {
		t.Fatalf("BatchUpdateStatusReturning: %v", err)
	}
	for i, r := range res {
		if r.Prev == nil || r.Prev.Status != models.StatusAcceptedByNetwork {
			t.Fatalf("results[%d] = %+v, want applied over ACCEPTED_BY_NETWORK", i, r)
		}
	}
}

// A retention extend bumps version like every other write, so it cannot
// slip between a batch's read and its guarded update unseen.
func TestExtendTransactionExpiry_BumpsVersion(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	soon := time.Now().Add(24 * time.Hour)
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: "nb-ret", ExpiresAt: &soon}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, before := currentStatus(t, s, "nb-ret")
	later := time.Now().Add(90 * 24 * time.Hour)
	if err := s.ExtendTransactionExpiry(ctx, "nb-ret", &later); err != nil {
		t.Fatalf("ExtendTransactionExpiry: %v", err)
	}
	if _, after := currentStatus(t, s, "nb-ret"); after != before+1 {
		t.Errorf("version %d -> %d, want one bump", before, after)
	}
}
