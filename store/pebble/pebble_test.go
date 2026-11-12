package pebble

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := New(config.Pebble{
		Path:                  dir,
		MemTableSizeMB:        4,
		L0CompactionThreshold: 2,
		SyncWrites:            false,
	})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGetOrInsertStatus_InsertsNew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := &models.TransactionStatus{TxID: "abc", Status: models.StatusReceived}
	got, inserted, err := s.GetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("GetOrInsertStatus: %v", err)
	}
	if !inserted {
		t.Fatal("expected inserted=true for new txid")
	}
	if got.TxID != "abc" || got.Status != models.StatusReceived {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestGetOrInsertStatus_ReturnsExisting(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	first := &models.TransactionStatus{TxID: "abc", Status: models.StatusReceived}
	_, inserted, err := s.GetOrInsertStatus(ctx, first)
	if err != nil || !inserted {
		t.Fatalf("first insert: inserted=%v err=%v", inserted, err)
	}

	second := &models.TransactionStatus{TxID: "abc", Status: models.StatusSentToNetwork}
	got, inserted, err := s.GetOrInsertStatus(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("expected inserted=false for existing txid")
	}
	if got.Status != models.StatusReceived {
		t.Fatalf("expected existing status RECEIVED, got %s", got.Status)
	}
}

// TestGetOrInsertStatus_ConcurrentRace verifies that N goroutines inserting
// the same txid collapse to a single insert — proving the per-txid mutex
// actually serializes read-modify-write.
func TestGetOrInsertStatus_ConcurrentRace(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const N = 100
	var wg sync.WaitGroup
	var insertedCount int64
	var mu sync.Mutex

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, ok, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: "racey", Status: models.StatusReceived,
			})
			if err != nil {
				t.Errorf("concurrent insert: %v", err)
				return
			}
			if ok {
				mu.Lock()
				insertedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if insertedCount != 1 {
		t.Fatalf("expected exactly 1 successful insert, got %d", insertedCount)
	}
}

func TestUpdateStatus_ClearsOldStatusIndex(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed with RECEIVED then transition to SENT_TO_NETWORK.
	txid := "tx1"
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusSentToNetwork, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	// The RECEIVED index entry should no longer resolve to this txid.
	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusSentToNetwork {
		t.Fatalf("expected SENT_TO_NETWORK, got %s", got.Status)
	}

	// Manually verify no ghost index entry by scanning RECEIVED index.
	v, closer, err := s.db.Get(idxTxStatusKey(string(models.StatusReceived), txid))
	if err == nil {
		_ = closer.Close()
		t.Fatal("old status index entry was not cleaned up — ghost row risk")
	}
	_ = v
}

// TestUpdateStatus_TerminalNotOverwritten is the regression for F-003 (#61):
// once a tx is in a strictly-terminal status (MINED, IMMUTABLE,
// DOUBLE_SPEND_ATTEMPTED), a later lower-priority UpdateStatus call (e.g. a
// stray SEEN_ON_NETWORK callback) must be a silent no-op rather than a clobber.
// REJECTED is covered separately in TestUpdateStatus_RejectedRecovery — it
// blocks regressions to pre-broadcast states but allows forward recovery to
// ACCEPTED_BY_NETWORK / SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES so a late peer
// acceptance can correct an earlier rejection.
func TestUpdateStatus_TerminalNotOverwritten(t *testing.T) {
	terminals := []models.Status{
		models.StatusMined,
		models.StatusImmutable,
		models.StatusDoubleSpendAttempted,
	}
	regressions := []models.Status{
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
		models.StatusSentToNetwork,
		models.StatusPendingRetry,
	}
	for _, terminal := range terminals {
		for _, regression := range regressions {
			name := string(terminal) + "_then_" + string(regression)
			t.Run(name, func(t *testing.T) {
				s := newTestStore(t)
				ctx := context.Background()
				txid := "tx-" + name

				// Seed a row in the terminal state.
				if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
					TxID: txid, Status: models.StatusReceived,
				}); err != nil {
					t.Fatal(err)
				}
				if err := s.UpdateStatus(ctx, &models.TransactionStatus{
					TxID: txid, Status: terminal, Timestamp: time.Now(),
				}); err != nil {
					t.Fatalf("seed terminal: %v", err)
				}

				// A late lower-priority callback must not regress the row.
				if err := s.UpdateStatus(ctx, &models.TransactionStatus{
					TxID: txid, Status: regression, Timestamp: time.Now(),
				}); err != nil {
					t.Fatalf("regression update: %v", err)
				}

				got, err := s.GetStatus(ctx, txid)
				if err != nil {
					t.Fatal(err)
				}
				if got.Status != terminal {
					t.Fatalf("terminal status %s was overwritten by %s (got %s)",
						terminal, regression, got.Status)
				}
			})
		}
	}
}

// TestUpdateStatusReturning_SkipIsNotApplied pins the applied-result
// contract. A lattice skip must not hand back the blocking row: a non-nil
// previous row means this call wrote the requested transition.
func TestUpdateStatusReturning_SkipIsNotApplied(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const txid = "tx-returning-skip"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusMined, BlockHash: "blk", BlockHeight: 9, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	prev, err := s.UpdateStatusReturning(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("lattice skip must not be an error, got %v", err)
	}
	if prev.Prev != nil {
		t.Fatalf("lattice skip returned applied prev %+v", prev.Prev)
	}
	if prev.Current == nil || prev.Current.Status != models.StatusMined {
		t.Fatalf("known skip current = %+v, want MINED", prev.Current)
	}
	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusMined {
		t.Fatalf("status = %s, want MINED", got.Status)
	}
}

func TestUpdateStatusReturning_AppliedReturnsPrevious(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const txid = "tx-returning-applied"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	prev, err := s.UpdateStatusReturning(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if prev.Prev == nil || prev.Prev.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("prev = %+v, want ACCEPTED_BY_NETWORK", prev.Prev)
	}
	if prev.Current != nil {
		t.Fatalf("applied write must not also report a skip: %+v", prev.Current)
	}
	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("status = %s, want SEEN_ON_NETWORK", got.Status)
	}
}

// TestUpdateStatus_RejectedRecovery pins REJECTED's partial-terminal behavior
// at the store layer: forward recovery (ACCEPTED_BY_NETWORK / SEEN_ON_NETWORK
// / SEEN_MULTIPLE_NODES) is allowed so a late callback from a peer that did
// accept the tx can correct the status, but regressions to pre-broadcast or
// retry states must still be silently dropped.
func TestUpdateStatus_RejectedRecovery(t *testing.T) {
	allowedForward := []models.Status{
		models.StatusAcceptedByNetwork,
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
	}
	blockedRegressions := []models.Status{
		models.StatusSentToNetwork,
		models.StatusPendingRetry,
	}
	for _, next := range allowedForward {
		t.Run("REJECTED_then_"+string(next), func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			txid := "tx-rejrec-" + string(next)

			if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusReceived,
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusRejected, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("seed REJECTED: %v", err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: next, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("forward update: %v", err)
			}
			got, err := s.GetStatus(ctx, txid)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != next {
				t.Fatalf("REJECTED → %s wrongly dropped (got %s)", next, got.Status)
			}
		})
	}
	for _, next := range blockedRegressions {
		t.Run("REJECTED_then_"+string(next)+"_blocked", func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			txid := "tx-rejblk-" + string(next)

			if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusReceived,
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusRejected, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("seed REJECTED: %v", err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: next, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("regression update: %v", err)
			}
			got, err := s.GetStatus(ctx, txid)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != models.StatusRejected {
				t.Fatalf("REJECTED was overwritten by %s (got %s)", next, got.Status)
			}
		})
	}
}

// TestUpdateStatus_UnknownTxidReturnsErrNotFound is the regression for F-033
// (#91): UpdateStatus on a txid that has no existing row must return
// store.ErrNotFound and must NOT create a phantom record. Previously,
// callbacks pointing at unknown txids could synthesize bogus rows in the
// store, turning the callback endpoint into a write-anywhere primitive.
func TestUpdateStatus_UnknownTxidReturnsErrNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "ghost-tx"

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

	// The status index must also be empty — i.e. no idx:tx:status:SEEN_ON_NETWORK:ghost-tx.
	v, closer, gErr := s.db.Get(idxTxStatusKey(string(models.StatusSeenOnNetwork), txid))
	if gErr == nil {
		_ = closer.Close()
		t.Fatalf("phantom secondary index entry was written for ghost txid: %x", v)
	}
}

// TestUpdateStatus_ExistingTxidStillWorks is the regression for the happy
// path: the F-033 guard must not break legitimate updates against rows that
// were created via GetOrInsertStatus.
func TestUpdateStatus_ExistingTxidStillWorks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "real-tx"

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

// TestUpdateStatus_ForwardTransitionsStillWork sanity-checks that the lattice
// guard does not break legitimate forward transitions.
func TestUpdateStatus_ForwardTransitionsStillWork(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "tx-forward"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusReceived,
	}); err != nil {
		t.Fatal(err)
	}
	for _, st := range []models.Status{
		models.StatusSentToNetwork,
		models.StatusAcceptedByNetwork,
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
		models.StatusMined,
		models.StatusImmutable,
	} {
		if err := s.UpdateStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: st, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("forward transition to %s: %v", st, err)
		}
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != st {
			t.Fatalf("forward transition lost: expected %s, got %s", st, got.Status)
		}
	}
}

func TestPendingRetryLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	txid := "retry-tx"
	rawTx := []byte{0x01, 0x02}
	nextRetry := time.Now().Add(-time.Second) // already due

	// Seed the row first.
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived}); err != nil {
		t.Fatal(err)
	}

	n, err := s.BumpRetryCount(ctx, txid)
	if err != nil || n != 1 {
		t.Fatalf("BumpRetryCount: n=%d err=%v", n, err)
	}

	if sErr := s.SetPendingRetryFields(ctx, txid, rawTx, nextRetry, ""); sErr != nil {
		t.Fatal(sErr)
	}

	ready, err := s.GetReadyRetries(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].TxID != txid {
		t.Fatalf("GetReadyRetries: %+v", ready)
	}
	if ready[0].RetryCount != 1 {
		t.Fatalf("expected RetryCount=1, got %d", ready[0].RetryCount)
	}

	if cErr := s.ClearRetryState(ctx, txid, models.StatusRejected, "final"); cErr != nil {
		t.Fatal(cErr)
	}
	ready, err = s.GetReadyRetries(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready retries after clear, got %d", len(ready))
	}
}

func TestGetReadyRetries_SkipsFutureEntries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now()
	cases := []struct {
		txid    string
		delay   time.Duration
		isReady bool
	}{
		{"past-1", -2 * time.Second, true},
		{"past-2", -time.Second, true},
		{"future-1", time.Hour, false},
	}
	for _, c := range cases {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: c.txid, Status: models.StatusReceived}); err != nil {
			t.Fatal(err)
		}
		if err := s.SetPendingRetryFields(ctx, c.txid, []byte{0xff}, now.Add(c.delay), ""); err != nil {
			t.Fatal(err)
		}
	}

	ready, err := s.GetReadyRetries(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	gotReady := map[string]bool{}
	for _, r := range ready {
		gotReady[r.TxID] = true
	}
	for _, c := range cases {
		if gotReady[c.txid] != c.isReady {
			t.Errorf("%s: isReady=%v, got=%v", c.txid, c.isReady, gotReady[c.txid])
		}
	}
}

func TestSetStatusByBlockHash_UpdatesAllInBlock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	blockHash := "bh-1"
	txids := []string{"t1", "t2", "t3"}
	for _, txid := range txids {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined, BlockHash: blockHash, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Transition to SEEN_ON_NETWORK (reorg path) — block fields should clear.
	updated, err := s.SetStatusByBlockHash(ctx, blockHash, models.StatusSeenOnNetwork)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated) != 3 {
		t.Fatalf("expected 3 updated txids, got %d", len(updated))
	}
	for _, txid := range txids {
		got, _ := s.GetStatus(ctx, txid)
		if got == nil || got.Status != models.StatusSeenOnNetwork {
			t.Errorf("%s: expected SEEN_ON_NETWORK, got %+v", txid, got)
		}
		if got.BlockHash != "" {
			t.Errorf("%s: expected empty BlockHash after reorg, got %s", txid, got.BlockHash)
		}
	}
}

func TestMarkMerkleRegisteredByTxIDs_UpdatesExistingRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	txids := []string{"mr-1", "mr-2", "mr-3"}
	for _, txid := range txids {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusReceived, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", txid, err)
		}
	}

	ts := time.Now().Add(-5 * time.Minute)
	if err := s.MarkMerkleRegisteredByTxIDs(ctx, txids, ts); err != nil {
		t.Fatalf("mark: %v", err)
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

func TestMarkMerkleRegisteredByTxIDs_SkipsUnknownTxIDs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "known", Status: models.StatusReceived, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.MarkMerkleRegisteredByTxIDs(ctx, []string{"known", "ghost-a", "ghost-b"}, time.Now()); err != nil {
		t.Fatalf("mark: %v", err)
	}

	got, _ := s.GetStatus(ctx, "known")
	if got == nil || got.MerkleRegisteredAt.IsZero() {
		t.Errorf("known: MerkleRegisteredAt should be set, got %+v", got)
	}
	for _, txid := range []string{"ghost-a", "ghost-b"} {
		got, _ := s.GetStatus(ctx, txid)
		if got != nil {
			t.Errorf("%s: unknown txid should not have created a row", txid)
		}
	}
}

func TestMarkMerkleRegisteredByTxIDs_RoundTripsThroughIterate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "iter-1", Status: models.StatusReceived, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	ts := time.Now()
	if err := s.MarkMerkleRegisteredByTxIDs(ctx, []string{"iter-1"}, ts); err != nil {
		t.Fatal(err)
	}

	var seen *models.TransactionStatus
	if err := s.IterateStatusesSince(ctx, time.Now().Add(-time.Hour), func(st *models.TransactionStatus) error {
		if st.TxID == "iter-1" {
			seen = st
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen == nil {
		t.Fatalf("iter-1 not seen")
	}
	if !seen.MerkleRegisteredAt.Equal(ts) {
		t.Errorf("MerkleRegisteredAt=%v want %v", seen.MerkleRegisteredAt, ts)
	}
}

func TestSubmissions_InsertAndQueryByTxID(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	sub := &models.Submission{
		SubmissionID: "sub-1",
		TxID:         "tx-a",
		CallbackURL:  "https://example.test/cb",
		CreatedAt:    time.Now(),
	}
	if err := s.InsertSubmission(ctx, sub); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSubmissionsByTxID(ctx, "tx-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SubmissionID != "sub-1" {
		t.Fatalf("GetSubmissionsByTxID: %+v", got)
	}
}

func TestLease_AcquireAndRenew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	held, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if held.IsZero() {
		t.Fatal("expected non-zero heldUntil for fresh lease")
	}

	// Same holder can renew.
	renewed, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-a", time.Second)
	if err != nil || renewed.IsZero() {
		t.Fatalf("renew: heldUntil=%v err=%v", renewed, err)
	}

	// Different holder is blocked.
	blocked, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !blocked.IsZero() {
		t.Fatal("expected zero heldUntil for contention")
	}
}

// Verify BumpRetryCount returns ErrNotFound for unknown txids so callers can
// distinguish real errors from a row that was cleared concurrently.
func TestBumpRetryCount_UnknownTxID(t *testing.T) {
	s := newTestStore(t)
	_, err := s.BumpRetryCount(context.Background(), "ghost")
	if err == nil {
		t.Fatal("expected error for unknown txid")
	}
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDatahubEndpoints_UpsertAndList(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	in := []store.DatahubEndpoint{
		{URL: "https://a.example", Network: ppNetMainnet, Source: store.DatahubEndpointSourceConfigured, LastSeen: now},
		{URL: "https://b.example", Network: ppNetMainnet, Source: store.DatahubEndpointSourceDiscovered, LastSeen: now.Add(time.Minute)},
	}
	for _, ep := range in {
		if err := s.UpsertDatahubEndpoint(ctx, ep); err != nil {
			t.Fatalf("upsert %s: %v", ep.URL, err)
		}
	}

	out, err := s.ListDatahubEndpoints(ctx, ppNetMainnet)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 endpoints, got %d: %+v", len(out), out)
	}
	got := map[string]store.DatahubEndpoint{}
	for _, ep := range out {
		got[ep.URL] = ep
	}
	for _, want := range in {
		gotEp, ok := got[want.URL]
		if !ok {
			t.Fatalf("missing endpoint %s", want.URL)
		}
		if gotEp.Network != want.Network {
			t.Errorf("%s network: got %q want %q", want.URL, gotEp.Network, want.Network)
		}
		if gotEp.Source != want.Source {
			t.Errorf("%s source: got %q want %q", want.URL, gotEp.Source, want.Source)
		}
		if !gotEp.LastSeen.Equal(want.LastSeen) {
			t.Errorf("%s last_seen: got %v want %v", want.URL, gotEp.LastSeen, want.LastSeen)
		}
	}
}

// TestDatahubEndpoints_NetworkScoped asserts that List returns only endpoints
// for the requested network. This is the regression for the bug where a
// regtest pod served mainnet URLs left in the store from a prior run.
func TestDatahubEndpoints_NetworkScoped(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	rows := []store.DatahubEndpoint{
		{URL: "https://main-a.example", Network: ppNetMainnet, Source: store.DatahubEndpointSourceDiscovered, LastSeen: now},
		{URL: "https://main-b.example", Network: ppNetMainnet, Source: store.DatahubEndpointSourceDiscovered, LastSeen: now},
		{URL: "https://regtest-a.example", Network: "regtest", Source: store.DatahubEndpointSourceConfigured, LastSeen: now},
	}
	for _, ep := range rows {
		if err := s.UpsertDatahubEndpoint(ctx, ep); err != nil {
			t.Fatalf("upsert %s: %v", ep.URL, err)
		}
	}

	regtest, err := s.ListDatahubEndpoints(ctx, "regtest")
	if err != nil {
		t.Fatalf("list regtest: %v", err)
	}
	if len(regtest) != 1 || regtest[0].URL != "https://regtest-a.example" {
		t.Fatalf("regtest list: got %+v", regtest)
	}

	mainnet, err := s.ListDatahubEndpoints(ctx, ppNetMainnet)
	if err != nil {
		t.Fatalf("list mainnet: %v", err)
	}
	if len(mainnet) != 2 {
		t.Fatalf("mainnet list: got %d entries, want 2: %+v", len(mainnet), mainnet)
	}

	empty, err := s.ListDatahubEndpoints(ctx, "")
	if err != nil {
		t.Fatalf("list empty: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty network filter must not match scoped rows: %+v", empty)
	}
}

func TestDatahubEndpoints_UpsertOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t1 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	if err := s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{
		URL: "https://a.example", Network: ppNetMainnet, Source: store.DatahubEndpointSourceConfigured, LastSeen: t1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{
		URL: "https://a.example", Network: ppNetMainnet, Source: store.DatahubEndpointSourceDiscovered, LastSeen: t2,
	}); err != nil {
		t.Fatal(err)
	}

	out, err := s.ListDatahubEndpoints(ctx, ppNetMainnet)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 endpoint after upsert overwrite, got %d", len(out))
	}
	if out[0].Source != store.DatahubEndpointSourceDiscovered {
		t.Errorf("source not overwritten: %q", out[0].Source)
	}
	if !out[0].LastSeen.Equal(t2) {
		t.Errorf("last_seen not overwritten: got %v want %v", out[0].LastSeen, t2)
	}
}

func TestDatahubEndpoints_RejectsEmptyURL(t *testing.T) {
	s := newTestStore(t)
	err := s.UpsertDatahubEndpoint(context.Background(), store.DatahubEndpoint{
		URL: "", Source: store.DatahubEndpointSourceDiscovered, LastSeen: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

// --- Block processing status ---

func TestBlockProcessing_Upsert_Header_Then_Processed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "aa11"
	t0 := time.Unix(1700000000, 0).UTC()
	t1 := t0.Add(2 * time.Second)
	t2 := t0.Add(4 * time.Second)

	if err := s.UpsertBlockHeaderSeen(ctx, hash, 100, t0); err != nil {
		t.Fatalf("header seen: %v", err)
	}
	if err := s.MarkBlockProcessed(ctx, hash, 100, t1); err != nil {
		t.Fatalf("processed: %v", err)
	}
	if err := s.MarkBlockBUMPBuilt(ctx, hash, 100, t2); err != nil {
		t.Fatalf("bump built: %v", err)
	}

	got, err := s.GetBlockProcessingStatus(ctx, hash)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.BlockHeight != 100 {
		t.Errorf("BlockHeight=%d, want 100", got.BlockHeight)
	}
	if !got.HeaderSeenAt.Equal(t0) {
		t.Errorf("HeaderSeenAt=%v, want %v", got.HeaderSeenAt, t0)
	}
	if got.ProcessedAt == nil || !got.ProcessedAt.Equal(t1) {
		t.Errorf("ProcessedAt=%v, want %v", got.ProcessedAt, t1)
	}
	if got.BUMPBuiltAt == nil || !got.BUMPBuiltAt.Equal(t2) {
		t.Errorf("BUMPBuiltAt=%v, want %v", got.BUMPBuiltAt, t2)
	}
	if got.Status != models.BlockStatusActive {
		t.Errorf("Status=%q, want active", got.Status)
	}
}

func TestBlockProcessing_OutOfOrder_Processed_Then_Header(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "bb22"
	tProc := time.Unix(1700000010, 0).UTC()
	tSeen := tProc.Add(time.Second)

	// BLOCK_PROCESSED arrives first with height=0 (callback has no height).
	if err := s.MarkBlockProcessed(ctx, hash, 0, tProc); err != nil {
		t.Fatalf("processed: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, hash)
	if !got.HeaderSeenAt.Equal(tProc) {
		t.Errorf("synthesized HeaderSeenAt=%v, want %v", got.HeaderSeenAt, tProc)
	}
	if got.BlockHeight != 0 {
		t.Errorf("BlockHeight before header=%d, want 0", got.BlockHeight)
	}

	// Header lands later with the real height.
	if err := s.UpsertBlockHeaderSeen(ctx, hash, 200, tSeen); err != nil {
		t.Fatalf("header seen: %v", err)
	}
	got, _ = s.GetBlockProcessingStatus(ctx, hash)
	if got.BlockHeight != 200 {
		t.Errorf("BlockHeight after header=%d, want 200 (chaintracks authoritative)", got.BlockHeight)
	}
	// Original processed_at must be preserved.
	if got.ProcessedAt == nil || !got.ProcessedAt.Equal(tProc) {
		t.Errorf("ProcessedAt clobbered: got %v, want %v", got.ProcessedAt, tProc)
	}
	// Original synthesized header_seen_at must be preserved (not bumped to tSeen).
	if !got.HeaderSeenAt.Equal(tProc) {
		t.Errorf("HeaderSeenAt should be preserved at %v, got %v", tProc, got.HeaderSeenAt)
	}
}

func TestBlockProcessing_HeaderReArrival_Idempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "cc33"
	t0 := time.Unix(1700000020, 0).UTC()

	if err := s.UpsertBlockHeaderSeen(ctx, hash, 300, t0); err != nil {
		t.Fatalf("first seen: %v", err)
	}
	if err := s.MarkBlockProcessed(ctx, hash, 300, t0.Add(time.Second)); err != nil {
		t.Fatalf("processed: %v", err)
	}
	// Re-arrive with a later timestamp.
	if err := s.UpsertBlockHeaderSeen(ctx, hash, 300, t0.Add(time.Hour)); err != nil {
		t.Fatalf("second seen: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, hash)
	if !got.HeaderSeenAt.Equal(t0) {
		t.Errorf("HeaderSeenAt should be preserved at %v, got %v", t0, got.HeaderSeenAt)
	}
	if got.ProcessedAt == nil {
		t.Error("ProcessedAt cleared on header re-arrival")
	}
}

func TestBlockProcessing_MarkOrphaned_AndResurrection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "dd44"
	t0 := time.Unix(1700000030, 0).UTC()

	if err := s.UpsertBlockHeaderSeen(ctx, hash, 400, t0); err != nil {
		t.Fatalf("seen: %v", err)
	}
	if _, err := s.MarkBlocksOrphaned(ctx, []string{hash}, t0.Add(time.Minute)); err != nil {
		t.Fatalf("orphan: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, hash)
	if got.Status != models.BlockStatusOrphaned {
		t.Errorf("Status=%q, want orphaned", got.Status)
	}
	if got.OrphanedAt == nil {
		t.Error("OrphanedAt should be set")
	}

	// Resurrection: header re-arrives.
	if err := s.UpsertBlockHeaderSeen(ctx, hash, 400, t0.Add(time.Hour)); err != nil {
		t.Fatalf("resurrect: %v", err)
	}
	got, _ = s.GetBlockProcessingStatus(ctx, hash)
	if got.Status != models.BlockStatusActive {
		t.Errorf("Status after resurrection=%q, want active", got.Status)
	}
	if got.OrphanedAt != nil {
		t.Errorf("OrphanedAt should be cleared, got %v", got.OrphanedAt)
	}
}

func TestBlockProcessing_MarkOrphaned_MissingRow_NoOp(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.MarkBlocksOrphaned(ctx, []string{"never-seen"}, time.Now()); err != nil {
		t.Fatalf("orphan missing: %v", err)
	}
}

func TestBlockProcessing_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetBlockProcessingStatus(context.Background(), "missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err=%v, want ErrNotFound", err)
	}
}

func TestBlockProcessing_List_DescendingHeight_Pagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000100, 0).UTC()
	for i := uint64(1); i <= 75; i++ {
		hash := fmt.Sprintf("h%04d", i)
		if err := s.UpsertBlockHeaderSeen(ctx, hash, i, t0); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Walk via cursor pages of 20.
	var seen []uint64
	cursor := uint64(0)
	for {
		page, err := s.ListBlockProcessingStatus(ctx, cursor, 20)
		if err != nil {
			t.Fatalf("list cursor=%d: %v", cursor, err)
		}
		if len(page) == 0 {
			break
		}
		for _, bp := range page {
			seen = append(seen, bp.BlockHeight)
		}
		// Next page starts strictly before the lowest height we just saw.
		cursor = page[len(page)-1].BlockHeight
		if len(page) < 20 {
			break
		}
	}
	if len(seen) != 75 {
		t.Fatalf("walked %d rows, want 75", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] <= seen[i] {
			t.Fatalf("not strictly descending at i=%d: %d <= %d", i, seen[i-1], seen[i])
		}
	}
	if seen[0] != 75 || seen[len(seen)-1] != 1 {
		t.Errorf("unexpected endpoints first=%d last=%d", seen[0], seen[len(seen)-1])
	}
}

func TestBlockProcessing_List_BeforeHeight_ExcludesEqualAndAbove(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000200, 0).UTC()
	for _, h := range []uint64{10, 20, 30, 40, 50} {
		if err := s.UpsertBlockHeaderSeen(ctx, fmt.Sprintf("h%d", h), h, t0); err != nil {
			t.Fatalf("seed %d: %v", h, err)
		}
	}
	page, err := s.ListBlockProcessingStatus(ctx, 30, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("got %d rows, want 2 (heights 10, 20)", len(page))
	}
	if page[0].BlockHeight != 20 || page[1].BlockHeight != 10 {
		t.Errorf("got heights %d,%d want 20,10", page[0].BlockHeight, page[1].BlockHeight)
	}
}

func TestBlockProcessing_GetActiveTipBlockHeight(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if h, err := s.GetActiveTipBlockHeight(ctx); err != nil || h != 0 {
		t.Fatalf("empty: got h=%d err=%v want 0/nil", h, err)
	}

	t0 := time.Unix(1700003000, 0).UTC()
	for _, h := range []uint64{100, 200, 150} {
		if err := s.UpsertBlockHeaderSeen(ctx, fmt.Sprintf("h%d", h), h, t0); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetActiveTipBlockHeight(ctx)
	if err != nil || got != 200 {
		t.Errorf("tip=%d err=%v want 200/nil", got, err)
	}

	// Orphaning the highest row drops the tip to the next active row.
	if _, oErr := s.MarkBlocksOrphaned(ctx, []string{"h200"}, t0); oErr != nil {
		t.Fatal(oErr)
	}
	got, err = s.GetActiveTipBlockHeight(ctx)
	if err != nil || got != 150 {
		t.Errorf("after orphan tip=%d err=%v want 150/nil", got, err)
	}
}

func TestBlockProcessing_ListStale_FiltersAndOrders(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	baseSeen := time.Unix(1700004000, 0).UTC()
	threshold := baseSeen.Add(5 * time.Minute)

	if err := s.UpsertBlockHeaderSeen(ctx, "recent", 500, threshold.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "processed", 400, baseSeen); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkBlockProcessed(ctx, "processed", 400, baseSeen.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "orphaned", 410, baseSeen); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkBlocksOrphaned(ctx, []string{"orphaned"}, baseSeen.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "old", 100, baseSeen.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "stale-a", 450, baseSeen); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "stale-b", 460, baseSeen.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	rows, err := s.ListStaleBlockProcessingStatus(ctx, threshold, 200, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].BlockHash != "stale-a" || rows[1].BlockHash != "stale-b" {
		t.Errorf("got %v want [stale-a stale-b]", hashesOf(rows))
	}

	rows, err = s.ListStaleBlockProcessingStatus(ctx, threshold, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3 (%v)", len(rows), hashesOf(rows))
	}

	rows, err = s.ListStaleBlockProcessingStatus(ctx, threshold, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].BlockHash != "stale-a" {
		t.Errorf("limit truncation: %v", hashesOf(rows))
	}
}

func hashesOf(rows []*models.BlockProcessingStatus) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.BlockHash
	}
	return out
}

func TestBlockProcessing_MarkParked(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)

	if err := s.UpsertBlockHeaderSeen(ctx, "parkme", 500, t0); err != nil {
		t.Fatalf("seen: %v", err)
	}
	if err := s.MarkBlocksParked(ctx, []string{"parkme"}); err != nil {
		t.Fatalf("park: %v", err)
	}
	got, err := s.GetBlockProcessingStatus(ctx, "parkme")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != models.BlockStatusParked {
		t.Errorf("Status=%q, want parked", got.Status)
	}

	// Parked rows leave the stale scan (status='active' predicate).
	stale, err := s.ListStaleBlockProcessingStatus(ctx, time.Now(), 0, 10)
	if err != nil {
		t.Fatalf("list stale: %v", err)
	}
	for _, r := range stale {
		if r.BlockHash == "parkme" {
			t.Error("parked row must not surface in the stale scan")
		}
	}

	// A fresh header arrival revives the row to active (recovery resumes).
	if err := s.UpsertBlockHeaderSeen(ctx, "parkme", 500, t0.Add(time.Minute)); err != nil {
		t.Fatalf("re-seen: %v", err)
	}
	got, _ = s.GetBlockProcessingStatus(ctx, "parkme")
	if got.Status != models.BlockStatusActive {
		t.Errorf("Status after re-seen=%q, want active", got.Status)
	}
}

func TestBlockProcessing_MarkParked_SkipsOrphanedAndMissing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Now().Add(-time.Hour)

	if err := s.UpsertBlockHeaderSeen(ctx, "reorged", 501, t0); err != nil {
		t.Fatalf("seen: %v", err)
	}
	if _, err := s.MarkBlocksOrphaned(ctx, []string{"reorged"}, t0.Add(time.Minute)); err != nil {
		t.Fatalf("orphan: %v", err)
	}
	// Parking must not relabel an orphaned (off-chain) row, and a missing
	// row is a silent no-op.
	if err := s.MarkBlocksParked(ctx, []string{"reorged", "never-seen"}); err != nil {
		t.Fatalf("park: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, "reorged")
	if got.Status != models.BlockStatusOrphaned {
		t.Errorf("Status=%q, want orphaned (park must not override)", got.Status)
	}
}

// TestIterateStatusesByToken covers the SSE-catchup hot path: distinct
// txids, since/only filters, ascending order, and the no-heavy-fields
// projection (raw tx / merkle path stripped).
func TestIterateStatusesByToken(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	base := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)
	rows := []*models.TransactionStatus{
		{TxID: "tx-mined", Status: models.StatusMined, Timestamp: base, RawTx: []byte{0xde, 0xad}, MerklePath: []byte{0xbe, 0xef}},
		{TxID: "tx-pending", Status: models.StatusSeenOnNetwork, Timestamp: base.Add(time.Minute)},
		{TxID: "tx-received", Status: models.StatusReceived, Timestamp: base.Add(2 * time.Minute)},
		{TxID: "tx-other", Status: models.StatusReceived, Timestamp: base.Add(3 * time.Minute)},
	}
	for _, r := range rows {
		if _, _, err := s.GetOrInsertStatus(ctx, r); err != nil {
			t.Fatalf("insert %s: %v", r.TxID, err)
		}
	}
	subs := []*models.Submission{
		{SubmissionID: "s1", TxID: "tx-mined", CallbackToken: "tok-1", CreatedAt: base},
		{SubmissionID: "s2", TxID: "tx-pending", CallbackToken: "tok-1", CreatedAt: base},
		// Duplicate submission for the same txid — must not double-emit.
		{SubmissionID: "s3", TxID: "tx-pending", CallbackToken: "tok-1", CreatedAt: base},
		{SubmissionID: "s4", TxID: "tx-received", CallbackToken: "tok-1", CreatedAt: base},
		{SubmissionID: "s5", TxID: "tx-other", CallbackToken: "tok-2", CreatedAt: base},
	}
	for _, sub := range subs {
		if err := s.InsertSubmission(ctx, sub); err != nil {
			t.Fatalf("insert submission %s: %v", sub.SubmissionID, err)
		}
	}

	collect := func(since time.Time, only []models.Status) []*models.TransactionStatus {
		t.Helper()
		var got []*models.TransactionStatus
		if err := s.IterateStatusesByToken(ctx, "tok-1", since, only, func(st *models.TransactionStatus) error {
			got = append(got, st)
			return nil
		}); err != nil {
			t.Fatalf("IterateStatusesByToken: %v", err)
		}
		return got
	}

	// Unfiltered: 3 distinct txids in ascending timestamp order, projected.
	got := collect(time.Time{}, nil)
	if len(got) != 3 {
		t.Fatalf("unfiltered rows = %d, want 3 (%+v)", len(got), got)
	}
	wantOrder := []string{"tx-mined", "tx-pending", "tx-received"}
	for i, want := range wantOrder {
		if got[i].TxID != want {
			t.Errorf("row %d = %s, want %s", i, got[i].TxID, want)
		}
	}
	if len(got[0].RawTx) != 0 || len(got[0].MerklePath) != 0 {
		t.Errorf("projection leaked heavy fields: rawTx=%d merklePath=%d bytes", len(got[0].RawTx), len(got[0].MerklePath))
	}

	// onlyStatuses filter: non-terminal only drops the MINED row.
	got = collect(time.Time{}, models.NonTerminalStatuses())
	if len(got) != 2 || got[0].TxID != "tx-pending" || got[1].TxID != "tx-received" {
		t.Fatalf("non-terminal rows = %+v, want [tx-pending tx-received]", got)
	}

	// since filter is strictly-after: cutting at tx-pending's timestamp
	// leaves only tx-received.
	got = collect(base.Add(time.Minute), nil)
	if len(got) != 1 || got[0].TxID != "tx-received" {
		t.Fatalf("since rows = %+v, want [tx-received]", got)
	}

	// fn error stops iteration and propagates.
	sentinel := errors.New("stop")
	calls := 0
	err := s.IterateStatusesByToken(ctx, "tok-1", time.Time{}, nil, func(*models.TransactionStatus) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("fn error not propagated: %v", err)
	}
	if calls != 1 {
		t.Errorf("iteration continued after fn error: %d calls", calls)
	}
}

// TestTokensForTxIDs covers the SSE fan-out membership resolution: multiple
// tokens on one txid, a txid with a single token, unknown txids, duplicate
// inputs, submissions without a token, and the empty batch.
func TestTokensForTxIDs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	subs := []*models.Submission{
		{SubmissionID: "p1", TxID: "tx-probe", CallbackToken: "tok-1", CreatedAt: time.Now()},
		{SubmissionID: "p2", TxID: "tx-probe", CallbackToken: "tok-2", CreatedAt: time.Now()},
		// Same (txid, token) twice: the result must be de-duplicated.
		{SubmissionID: "p2b", TxID: "tx-probe", CallbackToken: "tok-2", CreatedAt: time.Now()},
		{SubmissionID: "p3", TxID: "tx-solo", CallbackToken: "tok-1", CreatedAt: time.Now()},
		// SSE-less submission: no callback token, so it contributes nothing.
		{SubmissionID: "p4", TxID: "tx-notoken", CreatedAt: time.Now()},
	}
	for _, sub := range subs {
		if err := s.InsertSubmission(ctx, sub); err != nil {
			t.Fatalf("insert %s: %v", sub.SubmissionID, err)
		}
	}

	// One batch resolves every txid, duplicates included.
	got, err := s.TokensForTxIDs(ctx, []string{"tx-probe", "tx-probe", "tx-solo", "tx-notoken", "tx-ghost"})
	if err != nil {
		t.Fatalf("TokensForTxIDs: %v", err)
	}
	want := map[string][]string{
		"tx-probe": {"tok-1", "tok-2"},
		"tx-solo":  {"tok-1"},
	}
	if len(got) != len(want) {
		t.Fatalf("TokensForTxIDs returned %d txids (%v), want %d", len(got), got, len(want))
	}
	for txid, wantTokens := range want {
		gotTokens := append([]string(nil), got[txid]...)
		sort.Strings(gotTokens)
		if !slices.Equal(gotTokens, wantTokens) {
			t.Errorf("tokens for %s = %v, want %v", txid, gotTokens, wantTokens)
		}
	}
	// Absent, never present-but-empty: fan-out treats a missing key as
	// "nobody subscribed".
	for _, txid := range []string{"tx-ghost", "tx-notoken"} {
		if _, ok := got[txid]; ok {
			t.Errorf("%s must be absent from the result, got %v", txid, got[txid])
		}
	}

	empty, err := s.TokensForTxIDs(ctx, nil)
	if err != nil {
		t.Fatalf("TokensForTxIDs(nil): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("TokensForTxIDs(nil) = %v, want empty", empty)
	}
}

// TestSetPendingRetryFields_RespectsStatusLattice is the pebble half of the
// backend-parity guard; see the postgres test of the same name for why the
// second write of the park path must not bypass the lattice.
func TestSetPendingRetryFields_RespectsStatusLattice(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, prev := range models.StatusPendingRetry.DisallowedPreviousStatuses() {
		t.Run(string(prev), func(t *testing.T) {
			txid := "lattice-" + string(prev)
			if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusReceived,
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: txid, Status: prev}); err != nil {
				t.Fatalf("seed %s: %v", prev, err)
			}

			if err := s.SetPendingRetryFields(ctx, txid, []byte{0xaa}, time.Now().Add(-time.Second), ""); err != nil {
				t.Fatalf("SetPendingRetryFields: %v", err)
			}

			got, err := s.GetStatus(ctx, txid)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != prev {
				t.Errorf("status = %s, want %s left untouched", got.Status, prev)
			}
			ready, err := s.GetReadyRetries(ctx, time.Now(), 10)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range ready {
				if r.TxID == txid {
					t.Errorf("%s row entered the durable retry queue", prev)
				}
			}
		})
	}
}

// TestSubmissionsByIndexBounded_StopsDuringIteration pins WHERE the replay scan
// budget is enforced.
//
// Checking the length of an already-materialized list is no protection: by the
// time such a check could run, the allocation that causes the OOM has already
// happened. The bound has to abandon the scan mid-iteration, so peak memory is
// capped at the limit regardless of how many submissions the token really has.
func TestSubmissionsByIndexBounded_StopsDuringIteration(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const (
		token = "tok-huge"
		total = 50
		limit = 10
	)
	for i := 0; i < total; i++ {
		sub := &models.Submission{
			SubmissionID:  fmt.Sprintf("sub-%03d", i),
			TxID:          fmt.Sprintf("tx-%03d", i),
			CallbackToken: token,
		}
		if err := s.InsertSubmission(ctx, sub); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Unbounded still returns everything — the bound must not change the
	// behavior of the normal read path.
	all, err := s.GetSubmissionsByToken(ctx, token)
	if err != nil {
		t.Fatalf("unbounded read: %v", err)
	}
	if len(all) != total {
		t.Fatalf("unbounded read returned %d submissions, want %d", len(all), total)
	}

	got, err := s.submissionsByIndexBounded(ctx, idxSubTokenPrefix(token), limit)
	if !errors.Is(err, store.ErrReplayUnavailable) {
		t.Fatalf("bounded read err = %v, want store.ErrReplayUnavailable", err)
	}
	if got != nil {
		t.Errorf("bounded read returned %d submissions alongside its refusal; "+
			"the point of refusing is to not hold them", len(got))
	}

	// A token that fits stays served.
	fits, err := s.submissionsByIndexBounded(ctx, idxSubTokenPrefix(token), total+1)
	if err != nil {
		t.Fatalf("within-budget read: %v", err)
	}
	if len(fits) != total {
		t.Errorf("within-budget read returned %d, want %d", len(fits), total)
	}
}

// TestDatahubEndpoints_AdvertisedPolicy covers the policy a node advertises
// alongside its URL: it round-trips intact, "advertised none" stays
// distinguishable from "advertised zeros", and — the subtle one — a write
// carrying no policy leaves a recorded one alone.
//
// That last rule is what keeps the configured-URL seed, which re-upserts every
// URL on each process start with no policy attached, from blanking an announced
// endpoint's policy on every restart.
func TestDatahubEndpoints_AdvertisedPolicy(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	const url = "https://announced.example"
	policy := store.EndpointPolicy{
		MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
		MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000,
		MaxTxSigopsCountsPolicy: 4_294_967_295,
	}

	upsert := func(t *testing.T, ep store.DatahubEndpoint) {
		t.Helper()
		if err := s.UpsertDatahubEndpoint(ctx, ep); err != nil {
			t.Fatalf("upsert %s: %v", ep.URL, err)
		}
	}
	get := func(t *testing.T, want string) store.DatahubEndpoint {
		t.Helper()
		out, err := s.ListDatahubEndpoints(ctx, ppNetMainnet)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, ep := range out {
			if ep.URL == want {
				return ep
			}
		}
		t.Fatalf("endpoint %s missing from %+v", want, out)
		return store.DatahubEndpoint{}
	}

	// An announced endpoint carries its policy; a seeded one carries none.
	upsert(t, store.DatahubEndpoint{
		URL: url, Network: ppNetMainnet,
		Source: store.DatahubEndpointSourceDiscovered, Policy: &policy, LastSeen: now,
	})
	upsert(t, store.DatahubEndpoint{
		URL: "https://seeded.example", Network: ppNetMainnet,
		Source: store.DatahubEndpointSourceConfigured, LastSeen: now,
	})

	if got := get(t, url).Policy; got == nil {
		t.Fatal("announced endpoint lost its policy")
	} else if *got != policy {
		t.Errorf("policy round-trip: got %+v want %+v", *got, policy)
	}
	if got := get(t, "https://seeded.example").Policy; got != nil {
		t.Errorf("a seeded endpoint advertises no policy, got %+v", *got)
	}

	// The restart case: the seed re-upserts the announced URL with no policy.
	upsert(t, store.DatahubEndpoint{
		URL: url, Network: ppNetMainnet,
		Source: store.DatahubEndpointSourceConfigured, LastSeen: now.Add(time.Hour),
	})
	got := get(t, url)
	if got.Policy == nil {
		t.Fatal("a policy-less write erased the recorded policy; the seed would blank it on every restart")
	}
	if *got.Policy != policy {
		t.Errorf("preserved policy changed: got %+v want %+v", *got.Policy, policy)
	}
	// The rest of the row still updates normally.
	if got.Source != store.DatahubEndpointSourceConfigured {
		t.Errorf("source: got %q want %q", got.Source, store.DatahubEndpointSourceConfigured)
	}
	if !got.LastSeen.Equal(now.Add(time.Hour)) {
		t.Errorf("last_seen: got %v want %v", got.LastSeen, now.Add(time.Hour))
	}

	// A write that does carry a policy replaces the old one.
	updated := policy
	updated.MaxTxSizePolicy = 5_000_000
	upsert(t, store.DatahubEndpoint{
		URL: url, Network: ppNetMainnet,
		Source: store.DatahubEndpointSourceDiscovered, Policy: &updated, LastSeen: now.Add(2 * time.Hour),
	})
	if got := get(t, url).Policy; got == nil || got.MaxTxSizePolicy != 5_000_000 {
		t.Errorf("a policy-carrying write must replace the old policy, got %+v", got)
	}

	// A zero-valued policy is a real advertisement, not absence.
	upsert(t, store.DatahubEndpoint{
		URL: "https://zero.example", Network: ppNetMainnet,
		Source: store.DatahubEndpointSourceDiscovered, Policy: &store.EndpointPolicy{}, LastSeen: now,
	})
	if got := get(t, "https://zero.example").Policy; got == nil {
		t.Error("an all-zero policy must survive as advertised, not read back as nil")
	}
}

func TestLookupPartitionKeys_ReturnsStatusAndKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, row := range []*models.TransactionStatus{
		{TxID: "keyed", Status: models.StatusReceived, RawTx: []byte{0x01, 0x02, 0x03}, PartitionKey: "kP", Timestamp: time.Now()},
		{TxID: "unkeyed", Status: models.StatusRejected, RawTx: []byte{0x04}, Timestamp: time.Now()},
	} {
		if _, _, err := s.GetOrInsertStatus(ctx, row); err != nil {
			t.Fatalf("insert %s: %v", row.TxID, err)
		}
	}
	got, err := s.LookupPartitionKeys(ctx, []string{"keyed", "unkeyed", "missing"})
	if err != nil {
		t.Fatalf("LookupPartitionKeys: %v", err)
	}
	want := map[string]store.ParentKey{
		"keyed":   {Status: models.StatusReceived, PartitionKey: "kP"},
		"unkeyed": {Status: models.StatusRejected},
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s = %+v, want %+v", id, got[id], w)
		}
	}
}

// TestLookupRawTxs_CorruptRowReadsAbsent: one row that does not unmarshal
// must not fail the batch, or the propagation consumer would retry the same
// claim batch forever.
func TestLookupRawTxs_CorruptRowReadsAbsent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	good := "aa" + fmt.Sprintf("%062d", 1)
	bad := "bb" + fmt.Sprintf("%062d", 2)
	raw := []byte{0x01, 0x02}
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: good, Status: models.StatusReceived, RawTx: raw, Timestamp: time.Now()}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.db.Set(txKey(bad), []byte("{not json"), nil); err != nil {
		t.Fatalf("write corrupt row: %v", err)
	}

	got, err := s.LookupRawTxs(ctx, []string{good, bad})
	if err != nil {
		t.Fatalf("lookup failed on a corrupt row: %v", err)
	}
	if _, ok := got[bad]; ok {
		t.Errorf("corrupt row returned: %+v", got[bad])
	}
	if row := got[good]; row.Status != models.StatusReceived || string(row.RawTx) != string(raw) {
		t.Errorf("good row = %+v", row)
	}
}
