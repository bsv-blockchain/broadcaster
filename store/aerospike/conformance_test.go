//go:build integration

package aerospike

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/storetest"
)

// Requires a live Aerospike on localhost:3200 (integrationStore skips
// otherwise). No CI workflow passes -tags=integration today, so run this by
// hand before merging changes to IterateTrackerRows:
//
//	go test -tags=integration -run TestIterateTrackerRows ./store/aerospike/...
func TestIterateTrackerRows_Conformance(t *testing.T) {
	storetest.RunTrackerRowsSuite(t, func(t *testing.T) storetest.Backend {
		t.Helper()
		return integrationStore(t)
	})
}

// The generation-checked block_processing writes (issue #339): ReactivateBlock,
// MarkBlocksOrphaned's applied-transition count, MarkBlockReconciled's CAS.
// The suite uses its own hash prefix and heights and reads its rows back
// directly, so it is safe in the shared namespace.
func TestBlockStatus_Conformance(t *testing.T) {
	storetest.RunBlockStatusSuite(t, func(t *testing.T) storetest.BlockStatusBackend {
		t.Helper()
		return integrationStore(t)
	})
}

// STUMP rows are content-addressed so several merkle-services can deliver
// the same subtree: identical bytes collapse, divergent variants coexist.
func TestStumps_Conformance(t *testing.T) {
	storetest.RunStumpSuite(t, func(t *testing.T) storetest.StumpBackend {
		t.Helper()
		return integrationStore(t)
	})
}

// A double spend is discovered after the row exists, so the competing
// spender must persist through the status-update paths, not only on insert.
func TestCompetingTxs_Conformance(t *testing.T) {
	storetest.RunCompetingTxsSuite(t, func(t *testing.T) storetest.CompetingTxsBackend {
		t.Helper()
		return integrationStore(t)
	})
}

// The last network response must survive across durable retry attempts so
// the reaper's give-up reason can quote it.
func TestRetryReason_Conformance(t *testing.T) {
	storetest.RunRetryReasonSuite(t, func(t *testing.T) storetest.RetryReasonBackend {
		t.Helper()
		return integrationStore(t)
	})
}

func TestAPIKeys_Conformance(t *testing.T) {
	storetest.RunAPIKeySuite(t, func(t *testing.T) store.APIKeyStore {
		t.Helper()
		return integrationStore(t)
	})
}

func TestQuota_Conformance(t *testing.T) {
	storetest.RunQuotaSuite(t, func(t *testing.T) store.QuotaStore {
		t.Helper()
		return integrationStore(t)
	})
}

// TestReserveQuota_CancelledCtx_NoSpend pins the ctx-handling fix: remaining()
// returns 0 for an already-done ctx, which the v7 client treats as "no
// deadline" rather than "expired", so without an explicit ctx.Err() check
// ReserveQuota would still issue the Operate call and spend quota against a
// cancelled request. A cancelled attempt must both report ctx.Err() and
// leave the window untouched for the next, real reservation.
func TestReserveQuota_CancelledCtx_NoSpend(t *testing.T) {
	s := integrationStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	keyID := "key_ctxcancel_" + time.Now().Format("150405.000000000")
	w := time.Now().UTC().Truncate(time.Minute)

	if _, _, err := s.ReserveQuota(ctx, keyID, w, 10, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	granted, used, err := s.ReserveQuota(context.Background(), keyID, w, 10, 100)
	if err != nil {
		t.Fatalf("follow-up reserve: %v", err)
	}
	if granted != 10 || used != 10 {
		t.Fatalf("follow-up reserve after cancelled attempt: granted=%d used=%d, want 10/10", granted, used)
	}
}

// A positive network verdict retires the retry bookkeeping, so a recovered
// transaction stops reporting its park reason and retry schedule.
func TestRetryRecovery_Conformance(t *testing.T) {
	storetest.RunRetryRecoverySuite(t, func(t *testing.T) storetest.RetryRecoveryBackend {
		t.Helper()
		return retryRecoveryStore{integrationStore(t)}
	})
}

// retryRecoveryStore adds the suite's direct read of the persisted
// retry_reason, which no store read path exposes for a recovered row.
type retryRecoveryStore struct{ *Store }

func (s retryRecoveryStore) StoredRetryReason(ctx context.Context, txid string) (string, error) {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return "", err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key, "retry_reason")
	if err != nil {
		return "", err
	}
	return getString(rec, "retry_reason"), nil
}

// The propagation partition key persists with the row so a child submitted
// later can key like its in-flight parent.
func TestPartitionKey_Conformance(t *testing.T) {
	storetest.RunPartitionKeySuite(t, func(t *testing.T) storetest.PartitionKeyBackend {
		t.Helper()
		return integrationStore(t)
	})
}
