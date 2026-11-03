package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// PartitionKeyBackend is the slice of store.Store the partition-key suite drives.
type PartitionKeyBackend interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error
	LookupPartitionKeys(ctx context.Context, txids []string) (map[string]store.ParentKey, error)
}

// RunPartitionKeySuite asserts the partition-key contract the intake keying
// relies on (api_server.partitionKeyForSubmit): the key a row is inserted with
// comes back from LookupPartitionKeys together with the row's CURRENT status,
// survives later status writes that carry no key, is "" for rows inserted
// without one, and unknown txids are simply absent.
func RunPartitionKeySuite(t *testing.T, newBackend func(t *testing.T) PartitionKeyBackend) {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()
	base := time.Now().UnixNano()
	keyed := fmt.Sprintf("%048x%016x", base, 0xb1)
	bare := fmt.Sprintf("%048x%016x", base, 0xb2)
	unknown := fmt.Sprintf("%048x%016x", base, 0xb3)
	const key = "0000000000000000000000000000000000000000000000000000000000000001"

	if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: keyed, Status: models.StatusReceived, PartitionKey: key, Timestamp: time.Now()}); err != nil {
		t.Fatalf("insert keyed: %v", err)
	}
	if _, _, err := b.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: bare, Status: models.StatusReceived, Timestamp: time.Now()}); err != nil {
		t.Fatalf("insert bare: %v", err)
	}

	got, err := b.LookupPartitionKeys(ctx, []string{keyed, bare, unknown})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if pk := got[keyed]; pk.PartitionKey != key || pk.Status != models.StatusReceived {
		t.Errorf("keyed row = %+v, want key %q at RECEIVED", pk, key)
	}
	if pk, ok := got[bare]; !ok || pk.PartitionKey != "" || pk.Status != models.StatusReceived {
		t.Errorf("bare row = %+v (present=%v), want present with empty key at RECEIVED", pk, ok)
	}
	if _, ok := got[unknown]; ok {
		t.Errorf("unknown txid returned: %+v", got[unknown])
	}

	// A status write that carries no key must not erase it, and the lookup
	// must reflect the new status so intake can tell in-flight from accepted.
	if uerr := b.UpdateStatus(ctx, &models.TransactionStatus{TxID: keyed, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()}); uerr != nil {
		t.Fatalf("accept keyed: %v", uerr)
	}
	got, err = b.LookupPartitionKeys(ctx, []string{keyed})
	if err != nil {
		t.Fatalf("lookup after accept: %v", err)
	}
	if pk := got[keyed]; pk.PartitionKey != key || pk.Status != models.StatusAcceptedByNetwork {
		t.Errorf("after accept = %+v, want key %q at ACCEPTED_BY_NETWORK", pk, key)
	}

	if got, err := b.LookupPartitionKeys(ctx, nil); err != nil || len(got) != 0 {
		t.Errorf("empty lookup = %v, %v; want empty, nil", got, err)
	}
}
