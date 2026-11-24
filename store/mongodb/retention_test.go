//go:build mongodb

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

var _ store.RetentionExtender = (*Store)(nil)

// TestRetention_InsertPersistsExpiry pins that the expiry stamped at insert
// round-trips on both collections, and that a row without one stays without.
func TestRetention_InsertPersistsExpiry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	until := time.Now().Add(730 * 24 * time.Hour).UTC().Truncate(time.Millisecond)

	_, inserted, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: "tx-expiring", ExpiresAt: &until})
	if err != nil || !inserted {
		t.Fatalf("insert: inserted=%v err=%v", inserted, err)
	}
	if _, _, err = s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: "tx-forever"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetStatus(ctx, "tx-expiring")
	if err != nil || got.ExpiresAt == nil || !got.ExpiresAt.Equal(until) {
		t.Fatalf("expiring row: %+v err=%v, want expires_at %v", got, err, until)
	}
	got, err = s.GetStatus(ctx, "tx-forever")
	if err != nil || got.ExpiresAt != nil {
		t.Fatalf("forever row: expires_at=%v err=%v, want none", got.ExpiresAt, err)
	}

	if err = s.InsertSubmission(ctx, &models.Submission{
		SubmissionID: "sub-1", TxID: "tx-expiring", CallbackToken: "tok", CreatedAt: time.Now(), ExpiresAt: &until,
	}); err != nil {
		t.Fatal(err)
	}
	subs, err := s.GetSubmissionsByTxID(ctx, "tx-expiring")
	if err != nil || len(subs) != 1 || subs[0].ExpiresAt == nil || !subs[0].ExpiresAt.Equal(until) {
		t.Fatalf("submission: %+v err=%v, want expires_at %v", subs, err, until)
	}
}

// TestRetention_ExtendTransactionExpiry pins the "longest submitter wins"
// rule: an expiry only ever moves later, nil (keep forever) beats any date,
// and a row kept forever never gains one.
func TestRetention_ExtendTransactionExpiry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Millisecond)
	day := 24 * time.Hour
	at := func(d time.Duration) *time.Time { t := base.Add(d); return &t }

	for _, tc := range []struct {
		name    string
		initial *time.Time
		extend  *time.Time
		want    *time.Time
	}{
		{"later expiry extends", at(30 * day), at(730 * day), at(730 * day)},
		{"earlier expiry is ignored", at(730 * day), at(30 * day), at(730 * day)},
		{"equal expiry is a no-op", at(30 * day), at(30 * day), at(30 * day)},
		{"keep-forever submitter removes the expiry", at(30 * day), nil, nil},
		{"a row kept forever never gains an expiry", nil, at(730 * day), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txid := fmt.Sprintf("tx-%s", tc.name)
			if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, ExpiresAt: tc.initial}); err != nil {
				t.Fatal(err)
			}
			if err := s.ExtendTransactionExpiry(ctx, txid, tc.extend); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetStatus(ctx, txid)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.want == nil && got.ExpiresAt != nil:
				t.Fatalf("expires_at = %v, want none", *got.ExpiresAt)
			case tc.want != nil && (got.ExpiresAt == nil || !got.ExpiresAt.Equal(*tc.want)):
				t.Fatalf("expires_at = %v, want %v", got.ExpiresAt, *tc.want)
			}
		})
	}

	if err := s.ExtendTransactionExpiry(ctx, "no-such-tx", at(day)); err != nil {
		t.Fatalf("missing txid: %v", err)
	}
	if got, _ := s.GetStatus(ctx, "no-such-tx"); got != nil {
		t.Fatalf("extending a missing txid created a row: %+v", got)
	}
}

// TestRetention_TTLIndexes pins the TTL index shape: expireAfterSeconds 0
// (the document's own expires_at is the deadline) and partial on the field
// existing, so rows kept forever are never indexed.
func TestRetention_TTLIndexes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if got := indexNames(t, s.tx); got[idxTxExpires] {
		t.Fatal("EnsureIndexes built the retention TTL; it must wait for EnsureRetentionIndexes")
	}
	for range 2 { // idempotent
		if err := s.EnsureRetentionIndexes(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for coll, name := range map[string]string{collTransactions: idxTxExpires, collSubmissions: idxSubExpires} {
		cur, err := s.db.Collection(coll).Indexes().List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var specs []bson.M
		if err := cur.All(ctx, &specs); err != nil {
			t.Fatal(err)
		}
		var found bson.M
		for _, sp := range specs {
			if sp["name"] == name {
				found = sp
			}
		}
		if found == nil {
			t.Fatalf("%s: index %s missing", coll, name)
		}
		if fmt.Sprint(found["expireAfterSeconds"]) != "0" {
			t.Errorf("%s.%s expireAfterSeconds = %v, want 0", coll, name, found["expireAfterSeconds"])
		}
		if found["partialFilterExpression"] == nil {
			t.Errorf("%s.%s has no partialFilterExpression", coll, name)
		}
	}
}
