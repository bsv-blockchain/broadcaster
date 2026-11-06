package aerospike

import (
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"

	"github.com/bsv-blockchain/broadcaster/models"
)

func TestStatusUpdateReadBinsAreStatusFieldsOnly(t *testing.T) {
	want := map[string]struct{}{
		"status":       {},
		"timestamp":    {},
		"block_hash":   {},
		"block_height": {},
		"extra_info":   {},
	}
	if len(statusUpdateReadBins) != len(want) {
		t.Fatalf("statusUpdateReadBins = %v, want the five status fields", statusUpdateReadBins)
	}
	for _, bin := range statusUpdateReadBins {
		if _, ok := want[bin]; !ok {
			t.Fatalf("status update read fetches unrelated bin %q", bin)
		}
		delete(want, bin)
	}
	for _, forbidden := range []string{"raw_tx", "merkle_path", "competing_txs"} {
		for _, bin := range statusUpdateReadBins {
			if bin == forbidden {
				t.Fatalf("status update read must not fetch %s", forbidden)
			}
		}
	}
}

func TestStatusFromUpdateBinsIgnoresUnrelatedBins(t *testing.T) {
	rec := &aero.Record{Bins: aero.BinMap{
		"status":        "MINED",
		"timestamp":     1_700_000_000_000,
		"block_hash":    "abc",
		"block_height":  10,
		"extra_info":    "note",
		"raw_tx":        []byte("do-not-decode"),
		"merkle_path":   []byte("do-not-decode"),
		"competing_txs": []byte(`["other"]`),
	}}
	got := statusFromUpdateBins(rec, "txid")
	if got.TxID != "txid" || got.Status != models.StatusMined || got.BlockHash != "abc" || got.BlockHeight != 10 || got.ExtraInfo != "note" {
		t.Fatalf("decoded status = %+v", got)
	}
	if !got.Timestamp.Equal(time.UnixMilli(1_700_000_000_000)) {
		t.Fatalf("timestamp = %s", got.Timestamp)
	}
	if len(got.RawTx) != 0 || len(got.MerklePath) != 0 || len(got.CompetingTxs) != 0 {
		t.Fatalf("unrelated bins were decoded: %+v", got)
	}
}

// The stale-row reaper reads rows through IterateStatusesSince, which decodes
// with scanRecordToStatus, and skips any row without a raw tx — so dropping
// the bin there leaves a stuck RECEIVED row unrecoverable on Aerospike. The
// other reads keep the body out of memory.
func TestScanRecordToStatusDecodesRawTx(t *testing.T) {
	raw := []byte{0x01, 0x00, 0x00, 0x00, 0xde, 0xad}
	rec := &aero.Record{Bins: aero.BinMap{
		"status":    "RECEIVED",
		"timestamp": 1_700_000_000_000,
		"raw_tx":    raw,
	}}
	got := scanRecordToStatus(rec, "txid")
	if got.Status != models.StatusReceived || string(got.RawTx) != string(raw) {
		t.Fatalf("decoded status = {%s %x}, want {RECEIVED %x}", got.Status, got.RawTx, raw)
	}
	if plain := recordToStatus(rec, "txid"); len(plain.RawTx) != 0 {
		t.Fatalf("recordToStatus decoded raw_tx %x; only the scan path should", plain.RawTx)
	}

	cleared := scanRecordToStatus(&aero.Record{Bins: aero.BinMap{"status": "REJECTED"}}, "txid")
	if len(cleared.RawTx) != 0 {
		t.Fatalf("absent raw_tx bin decoded as %x", cleared.RawTx)
	}
}
