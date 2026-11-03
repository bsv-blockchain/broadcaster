//go:build postgres

package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/storetest"
)

func TestIterateTrackerRows_Conformance(t *testing.T) {
	storetest.RunTrackerRowsSuite(t, func(t *testing.T) storetest.Backend {
		t.Helper()
		return newTestStore(t)
	})
}

// The generation-checked block_processing writes (issue #339): ReactivateBlock,
// MarkBlocksOrphaned's applied-transition count, MarkBlockReconciled's CAS.
func TestBlockStatus_Conformance(t *testing.T) {
	storetest.RunBlockStatusSuite(t, func(t *testing.T) storetest.BlockStatusBackend {
		t.Helper()
		return newTestStore(t)
	})
}

// STUMP rows are content-addressed so several merkle-services can deliver
// the same subtree: identical bytes collapse, divergent variants coexist.
func TestStumps_Conformance(t *testing.T) {
	storetest.RunStumpSuite(t, func(t *testing.T) storetest.StumpBackend {
		t.Helper()
		return newTestStore(t)
	})
}

// A double spend is discovered after the row exists, so the competing
// spender must persist through the status-update paths, not only on insert.
func TestCompetingTxs_Conformance(t *testing.T) {
	storetest.RunCompetingTxsSuite(t, func(t *testing.T) storetest.CompetingTxsBackend {
		t.Helper()
		return newTestStore(t)
	})
}

// The last network response must survive across durable retry attempts so
// the reaper's give-up reason can quote it.
func TestRetryReason_Conformance(t *testing.T) {
	storetest.RunRetryReasonSuite(t, func(t *testing.T) storetest.RetryReasonBackend {
		t.Helper()
		return newTestStore(t)
	})
}

func TestIterateTrackerRows_NullBlockHeightKept(t *testing.T) {
	// block_height is nullable (schema.sql). A MINED row with a NULL height
	// must be kept at any cutoff — the COALESCE disjunct in the query is what
	// makes that true, and a naive `block_height >= $2` would silently drop it
	// because NULL comparisons are never true.
	s := newTestStore(t)
	ctx := context.Background()

	const txid = "00000000000000000000000000000000000000000000000000000000000000bb"
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID:   txid,
		Status: models.StatusMined,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE transactions SET block_height = NULL WHERE txid = $1`, txid); err != nil {
		t.Fatalf("null out block_height: %v", err)
	}

	found := false
	if err := s.IterateTrackerRows(ctx, store.NewTrackerScan(900_000, true), func(row store.TrackerRow) error {
		if row.TxID == txid {
			found = true
			if row.BlockHeight != 0 {
				t.Errorf("NULL block_height should surface as 0, got %d", row.BlockHeight)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("IterateTrackerRows: %v", err)
	}
	if !found {
		t.Fatal("MINED row with NULL block_height was pruned")
	}
}

func TestAPIKeys_Conformance(t *testing.T) {
	storetest.RunAPIKeySuite(t, func(t *testing.T) store.APIKeyStore {
		t.Helper()
		return newTestStore(t)
	})
}

func TestQuota_Conformance(t *testing.T) {
	storetest.RunQuotaSuite(t, func(t *testing.T) store.QuotaStore {
		t.Helper()
		return newTestStore(t)
	})
}

func TestIterateTrackerRows_PlanHasNoSort(t *testing.T) {
	// The regression guard for issue #276. The query hydration used to run
	// carried ORDER BY timestamp_at DESC, so on a multi-million-row store the
	// planner added an external merge sort over 14 detoasted columns — that
	// sort is what drove BSV Broadcaster and its Postgres out of memory. Hydration
	// builds a map, so any ordering in this plan is pure waste.
	s := newTestStore(t)
	ctx := context.Background()

	statuses := store.TrackerStatuses()
	names := make([]string, len(statuses))
	for i, st := range statuses {
		names[i] = string(st)
	}

	const q = `
EXPLAIN (VERBOSE)
SELECT txid, status, COALESCE(block_height, 0)
FROM transactions
WHERE status = ANY($1)
  AND (status <> $2 OR COALESCE(block_height, 0) = 0 OR block_height >= $3)`

	rows, err := s.pool.Query(ctx, q, names, string(models.StatusMined), int64(899_901))
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}

	got := plan.String()
	if strings.Contains(got, "Sort") {
		t.Errorf("hydration query plan contains a Sort node (issue #276 regression):\n%s", got)
	}
	// The projection must not pull the heavy columns.
	for _, col := range []string{"raw_tx", "merkle_path", "competing_txs", "orphaned_anchors"} {
		if strings.Contains(got, col) {
			t.Errorf("hydration query plan references heavy column %q:\n%s", col, got)
		}
	}
}

// A positive network verdict retires the retry bookkeeping, so a recovered
// transaction stops reporting its park reason and retry schedule.
func TestRetryRecovery_Conformance(t *testing.T) {
	storetest.RunRetryRecoverySuite(t, func(t *testing.T) storetest.RetryRecoveryBackend {
		t.Helper()
		return retryRecoveryStore{newTestStore(t)}
	})
}

// retryRecoveryStore adds the suite's direct read of the persisted
// retry_reason, which no store read path exposes for a recovered row.
type retryRecoveryStore struct{ *Store }

func (s retryRecoveryStore) StoredRetryReason(ctx context.Context, txid string) (string, error) {
	var reason string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(retry_reason, '') FROM transactions WHERE txid = $1`, txid).Scan(&reason)
	return reason, err
}

// The propagation partition key persists with the row so a child submitted
// later can key like its in-flight parent.
func TestPartitionKey_Conformance(t *testing.T) {
	storetest.RunPartitionKeySuite(t, func(t *testing.T) storetest.PartitionKeyBackend {
		t.Helper()
		return newTestStore(t)
	})
}
