package sse

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
)

// Intake publishes RECEIVED from the api-server while the propagator publishes
// the verdict, so on the events topic RECEIVED can trail the verdict: the
// inserting request pauses between its CAS and its publish while a concurrent
// duplicate's attempt is rejected, or the two events sit on different
// partitions under consumer lag. These tests publish the orders such races
// produce through the real KafkaPublisher and read GET /events off the wire.

const (
	staleTok      = "stale-tok"
	staleSentinel = "stale-sentinel"
)

// staleStream is an open GET /events body. reader persists across reads:
// readNextSSEFrame reuses an existing *bufio.Reader, so bytes it buffered past
// one frame are not lost to the next call.
type staleStream struct {
	reader *bufio.Reader
}

// openStaleStream subscribes staleTok to txids (plus the sentinel) and opens
// its stream, closed when the test ends.
func openStaleStream(t *testing.T, h *e2eHarness, txids ...string) staleStream {
	t.Helper()
	subs := make([]*models.Submission, 0, len(txids)+1)
	for _, txid := range append(txids, staleSentinel) {
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: staleTok})
	}
	h.store.mu.Lock()
	h.store.subsByToken[staleTok] = subs
	h.store.mu.Unlock()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, h.httpSrv.URL+"/events?callbackToken="+staleTok, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return staleStream{reader: bufio.NewReader(resp.Body)}
}

// publish emits status onto the events stream; persist first writes it as
// the txid's row, as the propagator does for a verdict.
func publish(t *testing.T, h *e2eHarness, status *models.TransactionStatus, persist bool) {
	t.Helper()
	if persist {
		h.store.setStatus(status.TxID, status)
	}
	var err error
	if len(status.TxIDs) > 0 {
		err = h.publisher.PublishBulk(t.Context(), status)
	} else {
		err = h.publisher.Publish(t.Context(), status)
	}
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func ev(txid string, status models.Status) *models.TransactionStatus {
	return &models.TransactionStatus{TxID: txid, Status: status, Timestamp: time.Now().UTC(), CallbackTokenHint: staleTok}
}

// readUntilSentinel publishes a sentinel frame (single-partition memory broker,
// so it is consumed after everything before it) and returns the stream's
// frames before it as txid:status pairs.
func readUntilSentinel(t *testing.T, h *e2eHarness, body staleStream) []string {
	t.Helper()
	publish(t, h, ev(staleSentinel, models.StatusAcceptedByNetwork), true)
	var got []string
	for {
		frame, err := readNextSSEFrame(body.reader, 3*time.Second)
		if err != nil {
			t.Fatalf("read frame after %v: %v", got, err)
		}
		pair := framePair(frame)
		if strings.HasPrefix(pair, staleSentinel+":") {
			return got
		}
		got = append(got, pair)
	}
}

// framePair renders a status frame as txid:status.
func framePair(frame string) string {
	field := func(key string) string {
		i := strings.Index(frame, `"`+key+`":"`)
		if i < 0 {
			return ""
		}
		rest := frame[i+len(key)+4:]
		return rest[:strings.IndexByte(rest, '"')]
	}
	return field("txid") + ":" + field("txStatus")
}

func assertStream(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stream = %v, want %v", got, want)
	}
}

// TestSSE_StaleReceived_DelayedInserterIsSkipped is the reviewer's
// reproduction: request A inserts the row and pauses; duplicate B queues
// propagation, the propagator stores and publishes REJECTED; A resumes and
// publishes RECEIVED. A rebroadcast rejected again publishes nothing, so a
// RECEIVED written here would be the stream's last word.
func TestSSE_StaleReceived_DelayedInserterIsSkipped(t *testing.T) {
	h := newE2EHarness(t)
	t.Cleanup(h.Close)
	waitForSubscriberReady()
	body := openStaleStream(t, h, "tx-a")
	time.Sleep(100 * time.Millisecond)

	publish(t, h, ev("tx-a", models.StatusRejected), true)
	publish(t, h, ev("tx-a", models.StatusReceived), false)

	assertStream(t, readUntilSentinel(t, h, body), "tx-a:REJECTED")
}

// TestSSE_StaleReceived_LaggingPodStillDeliversReceived: under consumer lag the
// propagator has already STORED the verdict by the time this pod reaches the
// RECEIVED that precedes it on the topic. That RECEIVED is not stale — this
// stream has not been written the verdict yet — so it must still be the first
// frame. A guard that reads the store would drop it here and bring back the
// "first frame is ACCEPTED_BY_NETWORK" report this PR fixes.
func TestSSE_StaleReceived_LaggingPodStillDeliversReceived(t *testing.T) {
	h := newE2EHarness(t)
	t.Cleanup(h.Close)
	waitForSubscriberReady()
	body := openStaleStream(t, h, "tx-a")
	time.Sleep(100 * time.Millisecond)

	h.store.setStatus("tx-a", ev("tx-a", models.StatusAcceptedByNetwork))
	publish(t, h, ev("tx-a", models.StatusReceived), false)
	publish(t, h, ev("tx-a", models.StatusAcceptedByNetwork), false)

	assertStream(t, readUntilSentinel(t, h, body), "tx-a:RECEIVED", "tx-a:ACCEPTED_BY_NETWORK")
}

// TestSSE_StaleReceived_BulkSkipsOnlyProgressedTxids: a /txs batch's bulk
// RECEIVED trailing one member's verdict skips only that member.
func TestSSE_StaleReceived_BulkSkipsOnlyProgressedTxids(t *testing.T) {
	h := newE2EHarness(t)
	t.Cleanup(h.Close)
	waitForSubscriberReady()
	body := openStaleStream(t, h, "b1", "b2")
	time.Sleep(100 * time.Millisecond)

	publish(t, h, ev("b1", models.StatusRejected), true)
	publish(t, h, &models.TransactionStatus{TxIDs: []string{"b1", "b2"}, Status: models.StatusReceived, Timestamp: time.Now().UTC(), CallbackTokenHint: staleTok}, false)

	assertStream(t, readUntilSentinel(t, h, body), "b1:REJECTED", "b2:RECEIVED")
}

// TestSSE_StaleReceived_AfterCatchupIsSkipped: a stream opened after the
// verdict gets it from connect-time catchup; a RECEIVED arriving live after
// that must not regress it.
func TestSSE_StaleReceived_AfterCatchupIsSkipped(t *testing.T) {
	h := newE2EHarness(t)
	t.Cleanup(h.Close)
	waitForSubscriberReady()
	h.store.setStatus("tx-a", ev("tx-a", models.StatusAcceptedByNetwork))
	body := openStaleStream(t, h, "tx-a")

	frame, err := readNextSSEFrame(body.reader, 3*time.Second)
	if err != nil {
		t.Fatalf("read catchup frame: %v", err)
	}
	if got := framePair(frame); got != "tx-a:ACCEPTED_BY_NETWORK" {
		t.Fatalf("catchup frame = %s, want tx-a:ACCEPTED_BY_NETWORK", got)
	}
	publish(t, h, ev("tx-a", models.StatusReceived), false)

	assertStream(t, readUntilSentinel(t, h, body))
}

// TestClientAdmit covers the rules the stream tests rely on, including the
// ones they cannot reach cheaply: block statuses are not recorded, and the
// set is bounded.
func TestClientAdmit(t *testing.T) {
	st := func(txid string, s models.Status) *models.TransactionStatus {
		return &models.TransactionStatus{TxID: txid, Status: s}
	}
	c := &Client{}
	if !c.admit(st("a", models.StatusReceived)) {
		t.Fatal("first RECEIVED refused")
	}
	if !c.admit(st("a", models.StatusSeenOnNetwork)) || c.admit(st("a", models.StatusReceived)) {
		t.Fatal("RECEIVED after SEEN_ON_NETWORK admitted")
	}
	if !c.admit(st("m", models.StatusMined)) || !c.admit(st("m", models.StatusReceived)) {
		t.Fatal("MINED must not be recorded")
	}
	if !(*Client)(nil).admit(st("a", models.StatusReceived)) {
		t.Fatal("nil client must admit")
	}

	r := newTxidRing(2)
	r.add("x")
	r.add("y")
	r.add("x") // already present: no eviction
	r.add("z") // evicts x, the oldest
	if r.has("x") || !r.has("y") || !r.has("z") || len(r.set) != 2 {
		t.Fatalf("ring = %v, want {y,z}", r.set)
	}
}
