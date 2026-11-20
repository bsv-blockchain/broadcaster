//go:build integration

package aerospike

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// These tests require a running Aerospike instance on localhost:3200
// (docker compose --profile aerospike up -d aerospike). Run with:
//
//	go test -tags=integration ./store/...

func integrationStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(context.Background(), config.Aero{
		Hosts:           []string{"localhost:3200"},
		Namespace:       "bsv_broadcaster",
		BatchSize:       100,
		PoolSize:        64,
		QueryTimeoutMs:  8000,
		OpTimeoutMs:     3000,
		SocketTimeoutMs: 5000,
	})
	if err != nil {
		t.Skipf("aerospike unavailable on localhost:3200: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGetStumpsByBlockHash_CancelledCtx(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := s.GetStumpsByBlockHash(ctx, "any-block-hash")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("want error for cancelled ctx, got nil")
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled ctx should short-circuit; took %v", elapsed)
	}
}

func TestGetStumpsByBlockHash_ShortDeadline(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _ = s.GetStumpsByBlockHash(ctx, "any-block-hash")
	elapsed := time.Since(start)

	// Should return within ~100ms even if the query would naturally take longer.
	// We don't assert on err value because a zero-result query is not an error
	// and the ctx may or may not fire before the query naturally completes.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("short deadline should bound query time; took %v", elapsed)
	}
}

func TestGetStumpsByBlockHash_HappyPath(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Empty block hash should just return empty slice, no error.
	stumps, err := s.GetStumpsByBlockHash(ctx, "nonexistent-block-for-integration-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stumps) != 0 {
		t.Fatalf("expected 0 stumps, got %d", len(stumps))
	}
}

// TestUpdateStatus_UnknownTxidReturnsErrNotFound is the regression for F-033
// (#91): UpdateStatus on a txid that has no existing record must return
// store.ErrNotFound and must NOT create a phantom row. Aerospike previously
// used CREATE_ONLY on the no-existing-row path which actively wrote a record;
// we now require the row to exist (UPDATE_ONLY semantics).
func TestUpdateStatus_UnknownTxidReturnsErrNotFound(t *testing.T) {
	s := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txid := "ghost-tx-f033-" + time.Now().Format("150405.000000000")

	err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound for unknown txid, got %v", err)
	}

	// And critically: no phantom row was created.
	got, gerr := s.GetStatus(ctx, txid)
	if gerr != nil {
		t.Fatalf("GetStatus after rejected update: %v", gerr)
	}
	if got != nil {
		t.Fatalf("expected nil status for ghost txid, got %+v", got)
	}
}

// TestUpdateStatus_ExistingTxidStillWorks covers the F-033 happy-path: the
// guard must not break legitimate updates against rows that were inserted
// via GetOrInsertStatus.
func TestUpdateStatus_ExistingTxidStillWorks(t *testing.T) {
	s := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txid := "real-tx-f033-" + time.Now().Format("150405.000000000")

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusReceived,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("UpdateStatus on existing row: %v", err)
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("expected SEEN_ON_NETWORK, got %+v", got)
	}
}

// TestMarkMerkleRegisteredByTxIDs_UpdatesExistingRows pins the issue #145
// fix: every successful merkle-service register must stamp the row so the
// next startup replay can skip it.
func TestMarkMerkleRegisteredByTxIDs_UpdatesExistingRows(t *testing.T) {
	s := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stamp := time.Now().Format("150405.000000000")
	txids := []string{"mr-1-" + stamp, "mr-2-" + stamp, "mr-3-" + stamp}
	for _, txid := range txids {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusReceived,
		}); err != nil {
			t.Fatalf("seed %s: %v", txid, err)
		}
	}

	ts := time.Now().Add(-5 * time.Minute).Truncate(time.Millisecond)
	if err := s.MarkMerkleRegisteredByTxIDs(ctx, txids, ts); err != nil {
		t.Fatalf("MarkMerkleRegisteredByTxIDs: %v", err)
	}

	for _, txid := range txids {
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatalf("GetStatus %s: %v", txid, err)
		}
		if got == nil {
			t.Fatalf("%s: missing after mark", txid)
		}
		if !got.MerkleRegisteredAt.Equal(ts) {
			t.Errorf("%s: MerkleRegisteredAt=%v want %v", txid, got.MerkleRegisteredAt, ts)
		}
	}
}

// TestMarkMerkleRegisteredByTxIDs_SkipsUnknownTxIDs verifies the
// UPDATE_ONLY contract: passing a txid with no existing row is a silent
// no-op rather than creating a phantom row (matching SetMinedByTxIDs).
func TestMarkMerkleRegisteredByTxIDs_SkipsUnknownTxIDs(t *testing.T) {
	s := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stamp := time.Now().Format("150405.000000000")
	known := "mr-known-" + stamp
	ghosts := []string{"mr-ghost-a-" + stamp, "mr-ghost-b-" + stamp}

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: known, Status: models.StatusReceived,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	all := append([]string{known}, ghosts...)
	if err := s.MarkMerkleRegisteredByTxIDs(ctx, all, time.Now()); err != nil {
		t.Fatalf("mark: %v", err)
	}

	if got, _ := s.GetStatus(ctx, known); got == nil || got.MerkleRegisteredAt.IsZero() {
		t.Errorf("known: MerkleRegisteredAt should be set, got %+v", got)
	}
	for _, txid := range ghosts {
		if got, _ := s.GetStatus(ctx, txid); got != nil {
			t.Errorf("%s: unknown txid should not have created a row", txid)
		}
	}
}
