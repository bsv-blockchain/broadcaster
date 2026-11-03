//go:build mongodb

package mongodb

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/storetest"
)

// Requires a live mongod (newTestStore skips otherwise). Run by hand before
// merging changes to IterateTrackerRows or trackerFilter:
//
//	go test -tags=mongodb -run TestIterateTrackerRows ./store/mongodb/...
func TestIterateTrackerRows_Conformance(t *testing.T) {
	storetest.RunTrackerRowsSuite(t, func(t *testing.T) storetest.Backend { return newTestStore(t) })
}

// The generation-checked block_processing writes (issue #339): ReactivateBlock,
// MarkBlocksOrphaned's applied-transition count, MarkBlockReconciled's CAS.
func TestBlockStatus_Conformance(t *testing.T) {
	storetest.RunBlockStatusSuite(t, func(t *testing.T) storetest.BlockStatusBackend { return newTestStore(t) })
}

// STUMP rows are content-addressed so several merkle-services can deliver
// the same subtree: identical bytes collapse, divergent variants coexist.
func TestStumps_Conformance(t *testing.T) {
	storetest.RunStumpSuite(t, func(t *testing.T) storetest.StumpBackend { return newTestStore(t) })
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

// The status filter is what keeps a REJECTED-heavy store out of memory, so a
// row that leaves the tracked set must stop being emitted immediately.
func TestIterateTrackerRows_FollowsTransitions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "aa"
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived}); err != nil {
		t.Fatal(err)
	}
	if n := countTracker(t, s); n != 1 {
		t.Fatalf("expected 1 tracked row, got %d", n)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusRejected}); err != nil {
		t.Fatal(err)
	}
	if n := countTracker(t, s); n != 0 {
		t.Fatalf("REJECTED row still emitted (%d rows)", n)
	}
}

func countTracker(t *testing.T, s *Store) int {
	t.Helper()
	n := 0
	if err := s.IterateTrackerRows(context.Background(), store.TrackerScan{}, func(store.TrackerRow) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

// The plan must be an index scan with no in-memory SORT stage: the sort over
// the full table is what pinned gigabytes on Postgres (issue #276), and the
// hint exists so MongoDB can never pick a collection scan either.
func TestIterateTrackerRows_PlanHasNoSort(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: collTransactions},
			{Key: "filter", Value: trackerFilter(store.TrackerScan{PruneMinedBelow: 100})},
			{Key: "projection", Value: projTracker},
			{Key: "hint", Value: idxTxStatusHeight},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}
	var out bson.M
	if err := s.db.RunCommand(ctx, cmd).Decode(&out); err != nil {
		t.Fatalf("explain: %v", err)
	}
	plan := fmtPlan(out)
	for _, bad := range []string{"SORT", "COLLSCAN"} {
		if containsStage(plan, bad) {
			t.Fatalf("plan contains %s stage:\n%v", bad, out)
		}
	}
	if !containsStage(plan, "IXSCAN") {
		t.Fatalf("plan has no IXSCAN stage:\n%v", out)
	}
}

// fmtPlan flattens the queryPlanner.winningPlan tree into its stage names.
// Rejected plans are deliberately excluded — they may legitimately contain a
// SORT or COLLSCAN the planner turned down.
func fmtPlan(explain bson.M) []string {
	var stages []string
	var walk func(v any)
	walk = func(v any) {
		switch node := v.(type) {
		case bson.M:
			for k, child := range node {
				if st, ok := child.(string); ok && k == "stage" {
					stages = append(stages, st)
				}
				walk(child)
			}
		case bson.D:
			for _, e := range node {
				if st, ok := e.Value.(string); ok && e.Key == "stage" {
					stages = append(stages, st)
				}
				walk(e.Value)
			}
		case bson.A:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(lookup(lookup(explain, "queryPlanner"), "winningPlan"))
	return stages
}

// lookup reads one key from a decoded document of either map or ordered form.
func lookup(v any, key string) any {
	switch node := v.(type) {
	case bson.M:
		return node[key]
	case bson.D:
		for _, e := range node {
			if e.Key == key {
				return e.Value
			}
		}
	}
	return nil
}

func containsStage(stages []string, want string) bool {
	for _, s := range stages {
		if s == want {
			return true
		}
	}
	return false
}

func TestAPIKeys_Conformance(t *testing.T) {
	storetest.RunAPIKeySuite(t, func(t *testing.T) store.APIKeyStore { return newTestStore(t) })
}

func TestQuota_Conformance(t *testing.T) {
	storetest.RunQuotaSuite(t, func(t *testing.T) store.QuotaStore { return newTestStore(t) })
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
	var d struct {
		RetryReason string `bson:"retry_reason"`
	}
	err := s.tx.FindOne(ctx, idFilter(txid)).Decode(&d)
	return d.RetryReason, err
}

// The propagation partition key persists with the row so a child submitted
// later can key like its in-flight parent.
func TestPartitionKey_Conformance(t *testing.T) {
	storetest.RunPartitionKeySuite(t, func(t *testing.T) storetest.PartitionKeyBackend {
		t.Helper()
		return newTestStore(t)
	})
}
