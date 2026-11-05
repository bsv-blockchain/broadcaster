package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// RawTxBackend is the slice of store.Store the raw-tx suite drives.
type RawTxBackend interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error
	LookupRawTxs(ctx context.Context, txids []string) (map[string]store.RawTxRow, error)
	SetRawTx(ctx context.Context, txid string, rawTx []byte) error
}

// RunRawTxSuite asserts the contract txid-only propagation messages rely on
// (#329): the raw tx intake inserts comes back from LookupRawTxs with the
// row's current status; ClearRetryState drops it; SetRawTx puts it back
// without touching the status and never creates a row; unknown txids are
// absent.
func RunRawTxSuite(t *testing.T, newBackend func(t *testing.T) RawTxBackend) {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()
	base := time.Now().UnixNano()
	txid := fmt.Sprintf("%048x%016x", base, 0xc1)
	unknown := fmt.Sprintf("%048x%016x", base, 0xc2)
	raw := []byte{0x01, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef}

	if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived, RawTx: raw, Timestamp: time.Now()}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := b.LookupRawTxs(ctx, []string{txid, unknown})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if row := got[txid]; row.Status != models.StatusReceived || !bytes.Equal(row.RawTx, raw) {
		t.Errorf("inserted row = {%s %x}, want {RECEIVED %x}", row.Status, row.RawTx, raw)
	}
	if _, ok := got[unknown]; ok {
		t.Errorf("unknown txid returned: %+v", got[unknown])
	}

	if cerr := b.ClearRetryState(ctx, txid, models.StatusRejected, "test"); cerr != nil {
		t.Fatalf("clear retry state: %v", cerr)
	}
	got, err = b.LookupRawTxs(ctx, []string{txid})
	if err != nil {
		t.Fatalf("lookup after clear: %v", err)
	}
	if row, ok := got[txid]; !ok || row.Status != models.StatusRejected || len(row.RawTx) != 0 {
		t.Errorf("after clear = %+v (present=%v), want REJECTED with no raw_tx", row, ok)
	}

	if serr := b.SetRawTx(ctx, txid, raw); serr != nil {
		t.Fatalf("set raw tx: %v", serr)
	}
	got, err = b.LookupRawTxs(ctx, []string{txid})
	if err != nil {
		t.Fatalf("lookup after set: %v", err)
	}
	if row := got[txid]; row.Status != models.StatusRejected || !bytes.Equal(row.RawTx, raw) {
		t.Errorf("after set = {%s %x}, want {REJECTED %x}", row.Status, row.RawTx, raw)
	}

	if serr := b.SetRawTx(ctx, unknown, raw); !errors.Is(serr, store.ErrNotFound) {
		t.Errorf("set raw tx on unknown txid: err = %v, want ErrNotFound", serr)
	}
	if got, err := b.LookupRawTxs(ctx, []string{unknown}); err != nil || len(got) != 0 {
		t.Errorf("SetRawTx created a row for an unknown txid: %v, %v", got, err)
	}

	if got, err := b.LookupRawTxs(ctx, nil); err != nil || len(got) != 0 {
		t.Errorf("empty lookup = %v, %v; want empty, nil", got, err)
	}
}
