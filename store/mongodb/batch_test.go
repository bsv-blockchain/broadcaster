//go:build mongodb

package mongodb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/broadcaster/models"
)

// All-new batch insert: every txid is fresh, every result has Inserted=true.
// The xmax trick depends on RETURNING firing for every row, including ones
// that hit ON CONFLICT — this test pins down the inserted=true side.
func TestBatchGetOrInsertStatus_AllNew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := make([]*models.TransactionStatus, 50)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: pgBatchTxID(i)}
	}

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	if len(results) != len(in) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(in))
	}
	for i, r := range results {
		if !r.Inserted {
			t.Errorf("results[%d].Inserted = false, want true (%s)", i, in[i].TxID)
		}
	}
}

// All-duplicates: every txid is already present. Results must report
// Inserted=false and Existing must reflect the persisted row, not the input
// — verified by mutating the row out-of-band before re-running the batch.
func TestBatchGetOrInsertStatus_AllDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := []*models.TransactionStatus{
		{TxID: "pg-dup-1"}, {TxID: "pg-dup-2"},
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, in); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID: "pg-dup-1", Status: models.StatusSeenOnNetwork,
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	for i, r := range results {
		if r.Inserted {
			t.Errorf("results[%d].Inserted = true, want false", i)
		}
		if r.Existing == nil {
			t.Fatalf("results[%d].Existing is nil", i)
		}
	}
	if results[0].Existing.Status != models.StatusSeenOnNetwork {
		t.Errorf("pg-dup-1 status = %q, want %q",
			results[0].Existing.Status, models.StatusSeenOnNetwork)
	}
}

// Mixed batch: input ordering must be preserved. Postgres doesn't guarantee
// RETURNING order matches input order, so the implementation explicitly
// reassembles by txid. Regressing the reassembly silently breaks any caller
// that relies on positional results.
func TestBatchGetOrInsertStatus_MixedPreservesOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.BatchGetOrInsertStatus(ctx, []*models.TransactionStatus{
		{TxID: "pg-mix-2"}, {TxID: "pg-mix-4"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	in := []*models.TransactionStatus{
		{TxID: "pg-mix-1"},
		{TxID: "pg-mix-2"},
		{TxID: "pg-mix-3"},
		{TxID: "pg-mix-4"},
		{TxID: "pg-mix-5"},
	}
	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	wantInserted := []bool{true, false, true, false, true}
	for i, r := range results {
		if r.Inserted != wantInserted[i] {
			t.Errorf("results[%d].Inserted = %v, want %v (%s)",
				i, r.Inserted, wantInserted[i], in[i].TxID)
		}
	}
}

// Concurrent batches with overlapping txids: each unique txid is inserted
// exactly once across all goroutines. Surfaces any gap in the
// ON CONFLICT DO UPDATE semantics under contention.
func TestBatchGetOrInsertStatus_ConcurrentRace(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const goroutines = 8
	const batchSize = 30
	in := make([]*models.TransactionStatus, batchSize)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: pgBatchTxID(i)}
	}

	var insertedTotal int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Every backend defaults Status/Timestamp/CreatedAt onto the
			// caller's struct, so sharing one set of pointers across
			// goroutines would be a data race in the test, not the store.
			batch := make([]*models.TransactionStatus, len(in))
			for i, st := range in {
				c := *st
				batch[i] = &c
			}
			results, err := s.BatchGetOrInsertStatus(ctx, batch)
			if err != nil {
				t.Errorf("BatchGetOrInsertStatus: %v", err)
				return
			}
			n := 0
			for _, r := range results {
				if r.Inserted {
					n++
				}
			}
			mu.Lock()
			insertedTotal += n
			mu.Unlock()
		}()
	}
	wg.Wait()

	if insertedTotal != batchSize {
		t.Errorf("insertedTotal = %d, want %d (each txid must insert exactly once across %d goroutines)",
			insertedTotal, batchSize, goroutines)
	}
}

// Duplicate txids within a single batch must not blow up the multi-row
// INSERT … ON CONFLICT DO UPDATE (Postgres rejects with SQLSTATE 21000 if
// the same key appears twice in VALUES). The first occurrence keeps the
// real Inserted flag; later occurrences must report Inserted=false with
// Existing populated, so callers don't double-process.
func TestBatchGetOrInsertStatus_DuplicateInBatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := []*models.TransactionStatus{
		{TxID: "pg-dupbatch-1"},
		{TxID: "pg-dupbatch-2"},
		{TxID: "pg-dupbatch-1"},
		{TxID: "pg-dupbatch-1"},
		{TxID: "pg-dupbatch-2"},
	}
	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus with duplicate txids: %v", err)
	}
	if len(results) != len(in) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(in))
	}
	wantInserted := []bool{true, true, false, false, false}
	for i, r := range results {
		if r.Inserted != wantInserted[i] {
			t.Errorf("results[%d] (%s).Inserted = %v, want %v",
				i, in[i].TxID, r.Inserted, wantInserted[i])
		}
		if !r.Inserted && r.Existing == nil {
			t.Errorf("results[%d] (%s): duplicate slot must have Existing populated",
				i, in[i].TxID)
		}
	}

	// Re-running the same input must mark every position as already-existing.
	results, err = s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	for i, r := range results {
		if r.Inserted {
			t.Errorf("results[%d] (%s).Inserted = true on re-run, want false", i, in[i].TxID)
		}
		if r.Existing == nil {
			t.Errorf("results[%d] (%s).Existing is nil on re-run", i, in[i].TxID)
		}
	}
}

// BatchUpdateStatus: bulk reject. Every row's Status, ExtraInfo, and
// timestamp must be persisted in a single round-trip. Verified by reading
// each row back through the single-row API.
func TestBatchUpdateStatus_BulkReject(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := []*models.TransactionStatus{
		{TxID: "pg-upd-1"}, {TxID: "pg-upd-2"}, {TxID: "pg-upd-3"},
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, in); err != nil {
		t.Fatalf("seed: %v", err)
	}

	updates := []*models.TransactionStatus{
		{TxID: "pg-upd-1", Status: models.StatusRejected, ExtraInfo: "bad-1", Timestamp: time.Now()},
		{TxID: "pg-upd-2", Status: models.StatusRejected, ExtraInfo: "bad-2", Timestamp: time.Now()},
		{TxID: "pg-upd-3", Status: models.StatusRejected, ExtraInfo: "bad-3", Timestamp: time.Now()},
	}
	if err := s.BatchUpdateStatus(ctx, updates); err != nil {
		t.Fatalf("BatchUpdateStatus: %v", err)
	}

	for _, u := range updates {
		got, err := s.GetStatus(ctx, u.TxID)
		if err != nil || got == nil {
			t.Fatalf("GetStatus(%s): %v / %v", u.TxID, got, err)
		}
		if got.Status != models.StatusRejected {
			t.Errorf("%s.Status = %q, want REJECTED", u.TxID, got.Status)
		}
		if got.ExtraInfo != u.ExtraInfo {
			t.Errorf("%s.ExtraInfo = %q, want %q", u.TxID, got.ExtraInfo, u.ExtraInfo)
		}
	}
}

// BatchUpdateStatus partial-update semantics: empty fields don't overwrite
// existing values. Mirrors the single-row UpdateStatus contract via
// COALESCE/NULLIF in the multi-row UPDATE FROM VALUES query.
func TestBatchUpdateStatus_PartialDoesNotClobber(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.BatchGetOrInsertStatus(ctx, []*models.TransactionStatus{
		{TxID: "pg-pres-1", Status: models.StatusReceived, BlockHash: "block-AAA", BlockHeight: 42},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Update with an empty BlockHash / zero BlockHeight — the existing values
	// must survive.
	if err := s.BatchUpdateStatus(ctx, []*models.TransactionStatus{
		{TxID: "pg-pres-1", Status: models.StatusSeenOnNetwork, Timestamp: time.Now()},
	}); err != nil {
		t.Fatalf("BatchUpdateStatus: %v", err)
	}

	got, err := s.GetStatus(ctx, "pg-pres-1")
	if err != nil || got == nil {
		t.Fatalf("GetStatus: %v / %v", got, err)
	}
	if got.Status != models.StatusSeenOnNetwork {
		t.Errorf("Status = %q, want SEEN_ON_NETWORK", got.Status)
	}
	if got.BlockHash != "block-AAA" {
		t.Errorf("BlockHash = %q, want block-AAA (must not be clobbered)", got.BlockHash)
	}
	if got.BlockHeight != 42 {
		t.Errorf("BlockHeight = %d, want 42 (must not be clobbered)", got.BlockHeight)
	}
}

// Empty input must be a no-op.
func TestBatchOps_Empty(t *testing.T) {
	s := newTestStore(t)
	if r, err := s.BatchGetOrInsertStatus(context.Background(), nil); err != nil || r != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", r, err)
	}
	if err := s.BatchUpdateStatus(context.Background(), nil); err != nil {
		t.Errorf("expected nil err, got %v", err)
	}
}

// BenchmarkBatchVsSerialInsert compares the batched xmax SQL against the
// per-row INSERT loop the legacy serial-insert path used. Run with:
//
//	go test -tags=postgres -bench=BatchVsSerialInsert ./store/postgres/...
//
// Results inform the issue's "benchmark" acceptance criterion. Postgres
// embedded is single-threaded so absolute numbers are conservative; what
// matters is the ratio.

// pgBatchTxID builds a deterministic 8-hex-char suffix so seeded rows are
// unique and human-readable in failure output.
func pgBatchTxID(i int) string {
	const hex = "0123456789abcdef"
	var buf [8]byte
	for k := 0; k < 8; k++ {
		buf[k] = hex[(i>>(k*4))&0xf]
	}
	return "pg-batch-" + string(buf[:])
}

// A batch larger than batch_size is inserted chunk by chunk. Rows that
// already exist and txids repeated across chunks must still resolve per
// position: pre-existing rows report Existing, every later repeat reports the
// first occurrence's outcome.
func TestBatchGetOrInsertStatus_MultiChunkMixed(t *testing.T) {
	s := newTestStore(t)
	s.batchSize = 7
	ctx := context.Background()

	id := func(i int) string { return fmt.Sprintf("chunk-%02d", i) }
	var seed []*models.TransactionStatus
	for i := 0; i < 30; i += 4 {
		seed = append(seed, &models.TransactionStatus{TxID: id(i), Status: models.StatusSeenOnNetwork})
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var in []*models.TransactionStatus
	for i := 0; i < 30; i++ {
		in = append(in, &models.TransactionStatus{TxID: id(i)})
	}
	in = append(in, &models.TransactionStatus{TxID: id(1)}, &models.TransactionStatus{TxID: id(4)})

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	for i := 0; i < 30; i++ {
		existed := i%4 == 0
		r := results[i]
		if r.Inserted == existed {
			t.Errorf("results[%d].Inserted = %v, want %v", i, r.Inserted, !existed)
		}
		if existed && (r.Existing == nil || r.Existing.Status != models.StatusSeenOnNetwork) {
			t.Errorf("results[%d].Existing = %+v, want the seeded SEEN_ON_NETWORK row", i, r.Existing)
		}
	}
	if r := results[30]; r.Inserted || r.Existing == nil || r.Existing.TxID != id(1) {
		t.Errorf("repeat of an inserted txid = %+v, want Existing for %s", r, id(1))
	}
	if r := results[31]; r.Inserted || r.Existing == nil || r.Existing.Status != models.StatusSeenOnNetwork {
		t.Errorf("repeat of an existing txid = %+v, want the seeded row", r)
	}
}

// A row the server rejects for a reason other than a duplicate key fails the
// batch, but the rows around it are still inserted (the insert is unordered)
// and must be reported as Inserted: the caller publishes RECEIVED for exactly
// those.
func TestBatchGetOrInsertStatus_WriteErrorKeepsOtherRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.db.RunCommand(ctx, bson.D{
		{Key: "collMod", Value: s.tx.Name()},
		{Key: "validator", Value: bson.D{{Key: "partition_key", Value: bson.D{{Key: "$ne", Value: "poison"}}}}},
	}).Err(); err != nil {
		t.Fatalf("collMod validator: %v", err)
	}

	in := []*models.TransactionStatus{
		{TxID: "werr-1"},
		{TxID: "werr-2", PartitionKey: "poison"},
		{TxID: "werr-3"},
	}
	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err == nil {
		t.Fatal("err = nil, want the rejected row's error")
	}
	if !results[0].Inserted || !results[2].Inserted {
		t.Errorf("results = %+v, want rows 0 and 2 Inserted", results)
	}
	if results[1].Inserted || results[1].Existing != nil {
		t.Errorf("results[1] = %+v, want the zero value (outcome unknown)", results[1])
	}
	for _, txid := range []string{"werr-1", "werr-3"} {
		if st, err := s.GetStatus(ctx, txid); err != nil || st == nil {
			t.Errorf("GetStatus(%s) = %v, %v; want the inserted row", txid, st, err)
		}
	}
}
