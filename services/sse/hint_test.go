package sse

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/broadcaster/models"
)

// TestFanOutHonoursCallbackTokenHint: an intake event arrives before the
// submission row exists, so the token index knows nothing about the txid.
// The hint on the event must still route the frame to the right token, must
// not leak it to other tokens, and must be consumed (blanked) on the way.
func TestFanOutHonoursCallbackTokenHint(t *testing.T) {
	mgr, _, cancel := newFanOutFixture(t, []string{"tok-1", "tok-2"}, nil)
	defer cancel()
	mine := mgr.NewClient("tok-1")
	other := mgr.NewClient("tok-2")
	all := mgr.NewClient("")
	for _, c := range []*Client{mine, other, all} {
		mgr.Register(c)
		defer mgr.Unregister(c.ID)
	}

	st := &models.TransactionStatus{TxID: "tx-unregistered", Status: models.StatusReceived, Timestamp: time.Now(), CallbackTokenHint: "tok-1"}
	mgr.fanOut(context.Background(), st)

	if got := len(mine.Ch); got != 1 {
		t.Errorf("hinted token received %d frames, want 1", got)
	}
	if got := len(other.Ch); got != 0 {
		t.Errorf("other token received %d frames, want 0", got)
	}
	if got := len(all.Ch); got != 1 {
		t.Errorf("unscoped client received %d frames, want 1", got)
	}
	if st.CallbackTokenHint != "" {
		t.Errorf("hint not consumed: %q", st.CallbackTokenHint)
	}
	// Without a hint the same unregistered txid reaches no token-scoped client.
	mgr.fanOut(context.Background(), &models.TransactionStatus{TxID: "tx-unregistered", Status: models.StatusReceived, Timestamp: time.Now()})
	if got := len(mine.Ch); got != 1 {
		t.Errorf("unhinted event delivered to a token-scoped client: %d frames", got)
	}
}

func TestFanOutBulkHonoursCallbackTokenHint(t *testing.T) {
	mgr, _, cancel := newFanOutFixture(t, []string{"tok-1", "tok-2"}, nil)
	defer cancel()
	mine := mgr.NewClient("tok-1")
	other := mgr.NewClient("tok-2")
	mgr.Register(mine)
	mgr.Register(other)
	defer mgr.Unregister(mine.ID)
	defer mgr.Unregister(other.ID)

	st := &models.TransactionStatus{TxIDs: []string{"b1", "b2", "b3"}, Status: models.StatusReceived, Timestamp: time.Now(), CallbackTokenHint: "tok-1"}
	mgr.fanOutBulk(context.Background(), st)

	if got := len(mine.Ch); got != 3 {
		t.Errorf("hinted token received %d frames, want 3", got)
	}
	if got := len(other.Ch); got != 0 {
		t.Errorf("other token received %d frames, want 0", got)
	}
	for i := 0; i < 3; i++ {
		f := <-mine.Ch
		if f.CallbackTokenHint != "" {
			t.Errorf("frame %d still carries the hint", i)
		}
	}
}

func TestTokenSetWithHint(t *testing.T) {
	base := tokenSet{"a", "b"}
	if got := base.withHint(""); len(got) != 2 {
		t.Errorf("empty hint changed the set: %v", got)
	}
	if got := base.withHint("a"); len(got) != 2 {
		t.Errorf("known hint duplicated: %v", got)
	}
	got := base.withHint("c")
	if len(got) != 3 || !got.has("c") || len(base) != 2 {
		t.Errorf("withHint must copy, not append in place: got %v base %v", got, base)
	}
	if got := tokenSet(nil).withHint("x"); len(got) != 1 || !got.has("x") {
		t.Errorf("nil set with hint = %v", got)
	}
}
