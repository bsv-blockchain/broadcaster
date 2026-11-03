package sse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
)

// Intake publishes RECEIVED from the api-server while the propagator publishes
// the verdict, so on the events topic RECEIVED can trail the verdict: the
// inserter pauses between its CAS and its publish while a concurrent duplicate's
// propagation attempt is rejected, or the two events sit on different
// partitions under consumer lag. These tests feed fan-out the event order such
// a race produces, with the store in the state the propagator leaves it in
// (verdict row written before the verdict is published), and assert on what
// the stream actually carries.

const (
	staleTok = "stale-tok"
	staleTx  = "stale-tx"
)

// drainFrames returns every frame queued on c without blocking.
func drainFrames(c *Client) []*models.TransactionStatus {
	var out []*models.TransactionStatus
	for {
		select {
		case f := <-c.Ch:
			out = append(out, f)
		default:
			return out
		}
	}
}

// assertFrames checks the stream carries exactly want, as txid:status pairs.
func assertFrames(t *testing.T, name string, c *Client, want ...string) {
	t.Helper()
	got := drainFrames(c)
	gotPairs := make([]string, len(got))
	for i, f := range got {
		gotPairs[i] = f.TxID + ":" + string(f.Status)
	}
	if len(gotPairs) != len(want) {
		t.Fatalf("%s stream = %v, want %v", name, gotPairs, want)
	}
	for i := range want {
		if gotPairs[i] != want[i] {
			t.Fatalf("%s stream = %v, want %v", name, gotPairs, want)
		}
	}
}

func received(txid, hint string) *models.TransactionStatus {
	return &models.TransactionStatus{TxID: txid, Status: models.StatusReceived, Timestamp: time.Now(), CallbackTokenHint: hint}
}

func rejected(txid string) *models.TransactionStatus {
	return &models.TransactionStatus{TxID: txid, Status: models.StatusRejected, Timestamp: time.Now(), ExtraInfo: "bad-txns"}
}

// TestFanOut_DelayedInserterReceivedAfterVerdictIsDropped is the reviewer's
// reproduction: request A inserts the row and pauses; duplicate B queues
// propagation, the propagator stores and publishes REJECTED; A resumes and
// publishes RECEIVED. A rebroadcast rejected again is a non-transition and
// publishes nothing, so a RECEIVED delivered here would be the stream's last
// word. Neither the token-scoped nor the firehose client may see it.
func TestFanOut_DelayedInserterReceivedAfterVerdictIsDropped(t *testing.T) {
	mgr, st, cancel := newFanOutFixture(t, []string{staleTok}, []string{staleTx})
	defer cancel()
	scoped := mgr.NewClient(staleTok)
	firehose := mgr.NewClient("")
	for _, c := range []*Client{scoped, firehose} {
		mgr.Register(c)
		defer mgr.Unregister(c.ID)
	}

	verdict := rejected(staleTx)
	st.setStatus(staleTx, verdict) // propagator: row first, then publish
	mgr.fanOut(context.Background(), verdict)
	mgr.fanOut(context.Background(), received(staleTx, staleTok)) // A resumes

	assertFrames(t, "scoped", scoped, staleTx+":REJECTED")
	assertFrames(t, "firehose", firehose, staleTx+":REJECTED")
}

// TestFanOut_ReceivedBeforeVerdictIsDelivered: the ordinary order, and the
// losing-duplicate case — RECEIVED fans out while the row is still RECEIVED,
// so no verdict has been published yet and the verdict that follows lands
// after it on the stream.
func TestFanOut_ReceivedBeforeVerdictIsDelivered(t *testing.T) {
	mgr, st, cancel := newFanOutFixture(t, []string{staleTok}, []string{staleTx})
	defer cancel()
	c := mgr.NewClient(staleTok)
	mgr.Register(c)
	defer mgr.Unregister(c.ID)

	st.setStatus(staleTx, &models.TransactionStatus{TxID: staleTx, Status: models.StatusReceived})
	mgr.fanOut(context.Background(), received(staleTx, staleTok))
	verdict := rejected(staleTx)
	st.setStatus(staleTx, verdict)
	mgr.fanOut(context.Background(), verdict)

	assertFrames(t, "scoped", c, staleTx+":RECEIVED", staleTx+":REJECTED")
}

// TestFanOutBulk_DropsOnlyStaleReceived: a /txs batch whose RECEIVED trails
// one member's verdict. Only that member's frame is withheld; members whose
// row is still RECEIVED, or has no row, keep theirs.
func TestFanOutBulk_DropsOnlyStaleReceived(t *testing.T) {
	txids := []string{"b-rejected", "b-received", "b-norow"}
	mgr, st, cancel := newFanOutFixture(t, []string{staleTok}, txids)
	defer cancel()
	c := mgr.NewClient(staleTok)
	mgr.Register(c)
	defer mgr.Unregister(c.ID)

	st.setStatus("b-received", &models.TransactionStatus{TxID: "b-received", Status: models.StatusReceived})
	verdict := rejected("b-rejected")
	st.setStatus("b-rejected", verdict)
	mgr.fanOut(context.Background(), verdict)
	mgr.fanOutBulk(context.Background(), &models.TransactionStatus{
		TxIDs: txids, Status: models.StatusReceived, Timestamp: time.Now(), CallbackTokenHint: staleTok,
	})

	assertFrames(t, "scoped", c, "b-rejected:REJECTED", "b-received:RECEIVED", "b-norow:RECEIVED")
}

// TestFanOut_StaleReceivedAfterAnyLaterStatusIsDropped: the guard is "row has
// moved past RECEIVED", not "row is REJECTED" — a RECEIVED trailing an
// ACCEPTED_BY_NETWORK would regress the stream just the same.
func TestFanOut_StaleReceivedAfterAnyLaterStatusIsDropped(t *testing.T) {
	for _, later := range []models.Status{models.StatusAcceptedByNetwork, models.StatusPendingRetry, models.StatusSeenOnNetwork, models.StatusMined} {
		t.Run(string(later), func(t *testing.T) {
			mgr, st, cancel := newFanOutFixture(t, []string{staleTok}, []string{staleTx})
			defer cancel()
			c := mgr.NewClient(staleTok)
			mgr.Register(c)
			defer mgr.Unregister(c.ID)

			st.setStatus(staleTx, &models.TransactionStatus{TxID: staleTx, Status: later})
			mgr.fanOut(context.Background(), received(staleTx, staleTok))
			assertFrames(t, "scoped", c)
		})
	}
}

// TestFanOut_ReceivedStoreErrorIsDropped: with the row's state unknown the
// frame is withheld — the verdict or a reconnect's catchup supersedes a lost
// RECEIVED, while a stale one after a terminal verdict is never corrected.
func TestFanOut_ReceivedStoreErrorIsDropped(t *testing.T) {
	mgr, st, cancel := newFanOutFixture(t, []string{staleTok}, []string{staleTx})
	defer cancel()
	c := mgr.NewClient(staleTok)
	mgr.Register(c)
	defer mgr.Unregister(c.ID)

	st.mu.Lock()
	st.statusErr = errors.New("store down")
	st.mu.Unlock()
	mgr.fanOut(context.Background(), received(staleTx, staleTok))
	assertFrames(t, "scoped", c)
}

// TestFanOut_StaleReceivedCheckCost: the row is read once per RECEIVED event
// however many clients want it, never for a txid no client wants, and never
// for any other status.
func TestFanOut_StaleReceivedCheckCost(t *testing.T) {
	mgr, st, cancel := newFanOutFixture(t, []string{staleTok, "tok-2"}, []string{staleTx})
	defer cancel()
	for range 3 {
		c := mgr.NewClient(staleTok)
		mgr.Register(c)
		defer mgr.Unregister(c.ID)
	}
	calls := func() int {
		st.mu.RLock()
		defer st.mu.RUnlock()
		return st.statusCalls
	}

	mgr.fanOut(context.Background(), received("tx-unwatched", ""))
	if got := calls(); got != 0 {
		t.Fatalf("unwatched RECEIVED read the row %d times, want 0", got)
	}
	mgr.fanOut(context.Background(), received(staleTx, staleTok))
	if got := calls(); got != 1 {
		t.Fatalf("RECEIVED with 3 recipients read the row %d times, want 1", got)
	}
	mgr.fanOut(context.Background(), rejected(staleTx))
	if got := calls(); got != 1 {
		t.Fatalf("REJECTED read the row (%d calls), want no read", got)
	}
}
