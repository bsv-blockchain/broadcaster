package propagation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
)

// txidOnlyMsg is the envelope intake publishes since #329.
func txidOnlyMsg(txid string) []byte {
	return []byte(`{"txid":"` + txid + `"}`)
}

const parentTxID = "aa00000000000000000000000000000000000000000000000000000000000001"

// TestHydrate_TxidOnly_LoadsRawTxAndParents: a txid-only message takes its
// bytes from the store row and its parents from those bytes, which is what
// the dispatcher's dep-aware admission keys on.
func TestHydrate_TxidOnly_LoadsRawTxAndParents(t *testing.T) {
	ms := newMockStore()
	txid, raw := spendingTx(t, parentTxID, 0, 1)
	ms.setStatusRow(txid, models.StatusReceived, raw)
	p := newPropagator("", "http://unused", ms)

	msgs := []propagationMsg{{TXID: txid}}
	drops, err := p.hydrate(t.Context(), msgs, nil)
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if drops[0] != "" {
		t.Fatalf("dropped: %s", drops[0])
	}
	if !bytes.Equal(msgs[0].RawTx, raw) {
		t.Error("RawTx not loaded from the store row")
	}
	if !slices.Equal(msgs[0].InputTXIDs, []string{parentTxID}) {
		t.Errorf("InputTXIDs = %v, want [%s]", msgs[0].InputTXIDs, parentTxID)
	}
}

// TestHydrate_TrailingBytes_StillDerivesParents: /tx stores the request
// body and parses it as a stream, so bytes after the tx are accepted at
// intake; the parents must still come out of them.
func TestHydrate_TrailingBytes_StillDerivesParents(t *testing.T) {
	ms := newMockStore()
	txid, raw := spendingTx(t, parentTxID, 0, 1)
	ms.setStatusRow(txid, models.StatusReceived, append(raw, '\n'))
	p := newPropagator("", "http://unused", ms)

	msgs := []propagationMsg{{TXID: txid}}
	if _, err := p.hydrate(t.Context(), msgs, nil); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if !slices.Equal(msgs[0].InputTXIDs, []string{parentTxID}) {
		t.Errorf("InputTXIDs = %v, want [%s]", msgs[0].InputTXIDs, parentTxID)
	}
}

// TestHydrate_DropReasons pins each way a message is unpropagatable, and
// that each is counted under its own label.
func TestHydrate_DropReasons(t *testing.T) {
	ms := newMockStore()
	ms.setStatusRow("rejected-cleared", models.StatusRejected, nil)
	ms.setStatusRow("received-no-raw", models.StatusReceived, nil)
	p := newPropagator("", "http://unused", ms)

	cases := []struct {
		msg  propagationMsg
		want string
	}{
		{propagationMsg{TXID: "absent"}, dropRowMissing},
		{propagationMsg{TXID: "rejected-cleared"}, dropTerminal},
		{propagationMsg{TXID: "received-no-raw"}, dropRawTxMissing},
		{propagationMsg{}, dropMalformed},
	}
	before := map[string]float64{}
	for _, c := range cases {
		before[c.want] = testutil.ToFloat64(metrics.PropagationHydrateDroppedTotal.WithLabelValues(c.want))
	}
	msgs := make([]propagationMsg, len(cases))
	for i, c := range cases {
		msgs[i] = c.msg
	}
	drops, err := p.hydrate(t.Context(), msgs, nil)
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	for i, c := range cases {
		if drops[i] != c.want {
			t.Errorf("%q: drop = %q, want %q", c.msg.TXID, drops[i], c.want)
		}
		if d := testutil.ToFloat64(metrics.PropagationHydrateDroppedTotal.WithLabelValues(c.want)) - before[c.want]; d != 1 {
			t.Errorf("%s counter delta = %v, want 1", c.want, d)
		}
	}
}

// TestHydrate_InlineRawTx_SkipsStore: a message from a pre-#329 producer
// carries raw_tx and input_txids inline and must work with no store read,
// so propagation can roll out before api-server.
func TestHydrate_InlineRawTx_SkipsStore(t *testing.T) {
	ms := newMockStore()
	p := newPropagator("", "http://unused", ms)

	var msg propagationMsg
	legacy := `{"txid":"legacy","raw_tx":"3q2+7w==","input_txids":["` + parentTxID + `"]}`
	if err := json.Unmarshal([]byte(legacy), &msg); err != nil {
		t.Fatalf("decode legacy envelope: %v", err)
	}
	msgs := []propagationMsg{msg}
	drops, err := p.hydrate(t.Context(), msgs, nil)
	if err != nil || drops[0] != "" {
		t.Fatalf("hydrate = (%v, %v), want no drop", drops, err)
	}
	if n := ms.lookupRawCallCount(); n != 0 {
		t.Errorf("LookupRawTxs called %d times for an inline-raw_tx message", n)
	}
	if !bytes.Equal(msgs[0].RawTx, []byte{0xde, 0xad, 0xbe, 0xef}) || !slices.Equal(msgs[0].InputTXIDs, []string{parentTxID}) {
		t.Errorf("inline fields changed: %+v", msgs[0])
	}
}

// TestHydrate_StoreError_DropsNothing: a failed read is an error for the
// caller to retry, never a drop.
func TestHydrate_StoreError_DropsNothing(t *testing.T) {
	ms := newMockStore()
	ms.setLookupRawErr(errors.New("store down"))
	p := newPropagator("", "http://unused", ms)

	drops, err := p.hydrate(t.Context(), []propagationMsg{{TXID: "x"}}, nil)
	if err == nil {
		t.Fatalf("hydrate err = nil, drops = %v", drops)
	}
}

// TestHydrateClaim_PreservesOrderAcrossFormats: one hydration batch mixing
// txid-only, legacy and undecodable messages comes out in claim order, each
// with its own outcome. Per-partition order is what delivers parents before
// children.
func TestHydrateClaim_PreservesOrderAcrossFormats(t *testing.T) {
	ms := newMockStore()
	a, rawA := spendingTx(t, parentTxID, 0, 1)
	b, rawB := spendingTx(t, parentTxID, 1, 2)
	ms.setStatusRow(a, models.StatusReceived, rawA)
	ms.setStatusRow(b, models.StatusReceived, rawB)
	p := newPropagator("", "http://unused", ms)

	in := make(chan *kafka.Message, 8)
	in <- &kafka.Message{Offset: 1, Value: txidOnlyMsg(a)}
	in <- &kafka.Message{Offset: 2, Value: makePropMsg("legacy")}
	in <- &kafka.Message{Offset: 3, Value: []byte("not json")}
	in <- &kafka.Message{Offset: 4, Value: txidOnlyMsg(b)}
	in <- &kafka.Message{Offset: 5, Value: txidOnlyMsg("absent")}
	close(in)

	var got []hydratedMsg
	for h := range p.hydrateClaim(t.Context(), in) {
		got = append(got, h)
	}
	want := []struct {
		offset int64
		txid   string
		drop   string
	}{
		{1, a, ""},
		{2, "legacy", ""},
		{3, "", dropMalformed},
		{4, b, ""},
		{5, "absent", dropRowMissing},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d messages, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].kmsg.Offset != w.offset || got[i].msg.TXID != w.txid || got[i].drop != w.drop {
			t.Errorf("[%d] = offset %d txid %q drop %q, want %+v", i, got[i].kmsg.Offset, got[i].msg.TXID, got[i].drop, w)
		}
	}
	if !bytes.Equal(got[0].msg.RawTx, rawA) || !bytes.Equal(got[3].msg.RawTx, rawB) {
		t.Error("txid-only messages not hydrated")
	}
}

// TestRunDispatcher_TxidOnlyMessage_Broadcasts is the end-to-end path: a
// txid-only message off the claim is hydrated, broadcast and committed.
func TestRunDispatcher_TxidOnlyMessage_Broadcasts(t *testing.T) {
	events := &eventLog{}
	teranodeSrv := newTeranodeServer(events, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	txid, raw := spendingTx(t, parentTxID, 0, 1)
	ms.setStatusRow(txid, models.StatusReceived, raw)
	p := newPropagator("", teranodeSrv.URL, ms)
	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 3, Value: txidOnlyMsg(txid)}
	waitForMark(t, claim, 3, "a hydrated txid-only message must broadcast and commit")
	if events.count("broadcast") == 0 {
		t.Error("no broadcast reached Teranode")
	}
}

// TestRunDispatcher_HydrateStoreError_HoldsOffsetUntilRecovery: while the
// store read fails the offset must not be committed (a crash would lose the
// tx to the reaper's one-hour window); once the store recovers the same
// message goes through.
func TestRunDispatcher_HydrateStoreError_HoldsOffsetUntilRecovery(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	txid, raw := spendingTx(t, parentTxID, 0, 1)
	ms.setStatusRow(txid, models.StatusReceived, raw)
	ms.setLookupRawErr(errors.New("store down"))
	p := newPropagator("", teranodeSrv.URL, ms)
	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	errsBefore := testutil.ToFloat64(metrics.PropagationHydrateErrorsTotal)
	claim.ch <- &kafka.Message{Offset: 9, Value: txidOnlyMsg(txid)}
	deadline := time.Now().Add(3 * time.Second)
	for ms.lookupRawCallCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := ms.lookupRawCallCount(); n < 2 {
		t.Fatalf("store read not retried: %d calls", n)
	}
	// Give the 50ms flush tick a few passes to (wrongly) mark it.
	time.Sleep(200 * time.Millisecond)
	if claim.isMarked(9) {
		t.Fatal("offset committed while its raw tx could not be loaded")
	}
	if testutil.ToFloat64(metrics.PropagationHydrateErrorsTotal) <= errsBefore {
		t.Error("hydrate errors not counted")
	}

	ms.setLookupRawErr(nil)
	waitForMark(t, claim, 9, "the message must go through once the store recovers")
}

// TestRunDispatcher_DroppedMessage_IsCommitted: an unpropagatable message
// must not pin the watermark.
func TestRunDispatcher_DroppedMessage_IsCommitted(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, newMockStore())
	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 4, Value: txidOnlyMsg("absent")}
	waitForMark(t, claim, 4, "a message with no store row must still be committed")
}

// TestHandleMessage_TxidOnly_Hydrates: the admit path used outside the
// claim loop hydrates the same way, and reports an unpropagatable message
// as an error naming the reason.
func TestHandleMessage_TxidOnly_Hydrates(t *testing.T) {
	ms := newMockStore()
	txid, raw := spendingTx(t, parentTxID, 0, 1)
	ms.setStatusRow(txid, models.StatusReceived, raw)
	p, cleanup := newPropagatorForDepTest(t, ms)
	defer cleanup()

	if err := p.handleMessage(context.Background(), &kafka.Message{Value: txidOnlyMsg(txid)}); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	pending := p.drainPending()
	if len(pending) != 1 || !bytes.Equal(pending[0].RawTx, raw) {
		t.Fatalf("pending = %+v, want the hydrated tx", pending)
	}

	err := p.handleMessage(context.Background(), &kafka.Message{Value: txidOnlyMsg("absent")})
	if err == nil || !strings.Contains(err.Error(), dropRowMissing) {
		t.Errorf("handleMessage(absent) err = %v, want %s", err, dropRowMissing)
	}
}
