package store_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// fakeStore implements the SingleStore subset the parallel-loop helpers need.
// Behavior is configurable per-txid so tests can exercise all-new / all-dup /
// mixed / error scenarios.
type fakeStore struct {
	insertCalls atomic.Int64
	updateCalls atomic.Int64
	// existing maps txid → status of an "already inserted" row.
	existing map[string]models.Status
	// insertErr maps txid → error returned from GetOrInsertStatus.
	insertErr map[string]error
	// updateErr maps txid → error returned from UpdateStatus.
	updateErr map[string]error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		existing:  make(map[string]models.Status),
		insertErr: make(map[string]error),
		updateErr: make(map[string]error),
	}
}

func (f *fakeStore) GetOrInsertStatus(_ context.Context, s *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	f.insertCalls.Add(1)
	if err, ok := f.insertErr[s.TxID]; ok {
		return nil, false, err
	}
	if existingStatus, ok := f.existing[s.TxID]; ok {
		return &models.TransactionStatus{TxID: s.TxID, Status: existingStatus}, false, nil
	}
	return s, true, nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, s *models.TransactionStatus) error {
	f.updateCalls.Add(1)
	if err, ok := f.updateErr[s.TxID]; ok {
		return err
	}
	return nil
}

// All-new: every input is a fresh txid, every result must be Inserted=true.
func TestBatchGetOrInsertStatusParallel_AllNew(t *testing.T) {
	f := newFakeStore()

	in := []*models.TransactionStatus{
		{TxID: "tx-1"},
		{TxID: "tx-2"},
		{TxID: "tx-3"},
	}
	results, err := store.BatchGetOrInsertStatusParallel(context.Background(), f, in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	for i, r := range results {
		if !r.Inserted {
			t.Errorf("results[%d].Inserted = false, want true", i)
		}
		if r.Existing != nil {
			t.Errorf("results[%d].Existing = %+v, want nil", i, r.Existing)
		}
	}
	if got := f.insertCalls.Load(); got != 3 {
		t.Errorf("insertCalls = %d, want 3", got)
	}
}

// All-duplicates: every txid is already in the store. Each result must be
// Inserted=false with Existing populated, and the existing row's Status must
// flow through unchanged.
func TestBatchGetOrInsertStatusParallel_AllDuplicates(t *testing.T) {
	f := newFakeStore()
	f.existing["tx-1"] = models.StatusSeenOnNetwork
	f.existing["tx-2"] = models.StatusSeenMultipleNodes
	f.existing["tx-3"] = models.StatusRejected

	in := []*models.TransactionStatus{{TxID: "tx-1"}, {TxID: "tx-2"}, {TxID: "tx-3"}}
	results, err := store.BatchGetOrInsertStatusParallel(context.Background(), f, in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	wantStatus := []models.Status{models.StatusSeenOnNetwork, models.StatusSeenMultipleNodes, models.StatusRejected}
	for i, r := range results {
		if r.Inserted {
			t.Errorf("results[%d].Inserted = true, want false", i)
		}
		if r.Existing == nil {
			t.Fatalf("results[%d].Existing is nil", i)
		}
		if r.Existing.Status != wantStatus[i] {
			t.Errorf("results[%d].Existing.Status = %q, want %q", i, r.Existing.Status, wantStatus[i])
		}
	}
}

// Mixed: input is a blend of new and duplicate txids. Output positions must
// match input positions (no implicit reordering). Required for per-position
// result stitching by callers.
func TestBatchGetOrInsertStatusParallel_MixedPreservesOrder(t *testing.T) {
	f := newFakeStore()
	f.existing["tx-2"] = models.StatusSeenOnNetwork
	f.existing["tx-4"] = models.StatusRejected

	in := []*models.TransactionStatus{
		{TxID: "tx-1"}, {TxID: "tx-2"}, {TxID: "tx-3"}, {TxID: "tx-4"}, {TxID: "tx-5"},
	}
	results, err := store.BatchGetOrInsertStatusParallel(context.Background(), f, in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Expected: 1, 3, 5 newly inserted; 2 and 4 are duplicates.
	wantInserted := []bool{true, false, true, false, true}
	for i, r := range results {
		if r.Inserted != wantInserted[i] {
			t.Errorf("results[%d].Inserted = %v, want %v (txid=%s)",
				i, r.Inserted, wantInserted[i], in[i].TxID)
		}
	}
}

// A single-row failure must not pollute the rest of the batch — the parallel
// loop returns the first error encountered, but the other rows still complete
// and their results are populated. Partial-success contract: drop one bad tx,
// keep the rest.
func TestBatchGetOrInsertStatusParallel_PartialFailure(t *testing.T) {
	f := newFakeStore()
	wantErr := errors.New("transient db error")
	f.insertErr["tx-bad"] = wantErr

	// The failing row is last so both good rows start before it fails: the
	// helper stops starting rows after a failure.
	in := []*models.TransactionStatus{
		{TxID: "tx-good-1"}, {TxID: "tx-good-2"}, {TxID: "tx-bad"},
	}
	results, err := store.BatchGetOrInsertStatusParallel(context.Background(), f, in)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapping wantErr", err)
	}
	// The two good rows still got results.
	if !results[0].Inserted {
		t.Errorf("results[0].Inserted = false, want true")
	}
	if !results[1].Inserted {
		t.Errorf("results[1].Inserted = false, want true")
	}
	// The bad row's slot is the zero value — Inserted=false, Existing=nil.
	// This is the signal callers use to tell it from an inserted row.
	if results[2].Inserted || results[2].Existing != nil {
		t.Errorf("results[2] = %+v, want zero value", results[2])
	}
}

// After a failure no further rows start: with the store gone, a large batch
// would otherwise pay a timeout for every remaining row before returning.
func TestBatchGetOrInsertStatusParallel_StopsAtFirstFailure(t *testing.T) {
	store.SetBatchConcurrency(1)
	t.Cleanup(func() { store.SetBatchConcurrency(0) })
	f := newFakeStore()
	wantErr := errors.New("store unavailable")
	f.insertErr["tx-bad"] = wantErr

	in := []*models.TransactionStatus{{TxID: "tx-bad"}, {TxID: "tx-2"}, {TxID: "tx-3"}}
	results, err := store.BatchGetOrInsertStatusParallel(context.Background(), f, in)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapping wantErr", err)
	}
	if got := f.insertCalls.Load(); got != 1 {
		t.Errorf("insertCalls = %d, want 1: rows after the failure must not start", got)
	}
	for i, r := range results {
		if r.Inserted || r.Existing != nil {
			t.Errorf("results[%d] = %+v, want zero value", i, r)
		}
	}
}

// Canceling the context mid-call returns ctx.Err() and bails out without
// waiting for outstanding goroutines to finish.
func TestBatchGetOrInsertStatusParallel_ContextCancel(t *testing.T) {
	f := newFakeStore()
	in := make([]*models.TransactionStatus, 100)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: makeTxID(i)}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.BatchGetOrInsertStatusParallel(ctx, f, in)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// BatchUpdateStatusParallel: happy path, all rows update; one failed call
// still produces the first error return but doesn't block the others.
func TestBatchUpdateStatusParallel_HappyPath(t *testing.T) {
	f := newFakeStore()
	in := []*models.TransactionStatus{
		{TxID: "tx-1", Status: models.StatusRejected},
		{TxID: "tx-2", Status: models.StatusRejected},
	}
	if err := store.BatchUpdateStatusParallel(context.Background(), f, in); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got := f.updateCalls.Load(); got != 2 {
		t.Errorf("updateCalls = %d, want 2", got)
	}
}

func TestBatchUpdateStatusParallel_PartialFailure(t *testing.T) {
	f := newFakeStore()
	wantErr := errors.New("transient db error")
	f.updateErr["tx-bad"] = wantErr

	in := []*models.TransactionStatus{
		{TxID: "tx-good-1", Status: models.StatusRejected},
		{TxID: "tx-bad", Status: models.StatusRejected},
		{TxID: "tx-good-2", Status: models.StatusRejected},
	}
	err := store.BatchUpdateStatusParallel(context.Background(), f, in)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapping wantErr", err)
	}
	// All three calls still happened — the helper doesn't short-circuit on
	// the first error.
	if got := f.updateCalls.Load(); got != 3 {
		t.Errorf("updateCalls = %d, want 3", got)
	}
}

func TestBatchOps_EmptyInput(t *testing.T) {
	f := newFakeStore()
	if r, err := store.BatchGetOrInsertStatusParallel(context.Background(), f, nil); err != nil || r != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", r, err)
	}
	if err := store.BatchUpdateStatusParallel(context.Background(), f, nil); err != nil {
		t.Errorf("expected nil err on empty update, got %v", err)
	}
	if got := f.insertCalls.Load() + f.updateCalls.Load(); got != 0 {
		t.Errorf("expected 0 store calls on empty input, got %d", got)
	}
}

// makeTxID returns a deterministic txid string for table-driven tests.
func makeTxID(i int) string {
	const hex = "0123456789abcdef"
	var b [4]byte
	for k := 0; k < 4; k++ {
		b[k] = hex[(i>>(k*4))&0xf]
	}
	return "tx-" + string(b[:])
}
