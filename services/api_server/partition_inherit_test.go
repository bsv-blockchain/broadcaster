package api_server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// keyLookupStore answers LookupPartitionKeys from a canned map and records
// every lookup so the input cap can be asserted.
type keyLookupStore struct {
	store.Store

	rows  map[string]store.ParentKey
	asked [][]string
	err   error
}

func (f *keyLookupStore) LookupPartitionKeys(_ context.Context, txids []string) (map[string]store.ParentKey, error) {
	f.asked = append(f.asked, append([]string(nil), txids...))
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]store.ParentKey{}
	for _, id := range txids {
		if r, ok := f.rows[id]; ok {
			out[id] = r
		}
	}
	return out, nil
}

func keyingServer(st store.Store, maxInputs int) *Server {
	return &Server{
		cfg:    &config.Config{Propagation: config.PropagationConfig{ParentKeyLookupMaxInputs: maxInputs}},
		store:  st,
		logger: zap.NewNop(),
	}
}

func TestPartitionKeyForSubmit(t *testing.T) {
	const child = "cc"
	cases := []struct {
		name   string
		rows   map[string]store.ParentKey
		inputs []string
		max    int
		err    error
		want   string
	}{
		{name: "no parents keys by own txid", inputs: nil, max: 8, want: child},
		{name: "unknown parent keys by own txid", inputs: []string{"p1"}, max: 8, want: child},
		{
			name:   "in-flight parent lends its key",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusReceived, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   "root",
		},
		{
			name:   "sent parent lends its key",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusSentToNetwork, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   "root",
		},
		{
			// The dispatcher drops a parked tx from its in-flight set and the
			// reaper rebroadcasts it outside Kafka: its key holds nothing.
			name:   "parked parent does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusPendingRetry, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name:   "accepted parent does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusAcceptedByNetwork, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name:   "rejected parent does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusRejected, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name:   "parent written before keys were persisted does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusReceived}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name: "first pending parent in input order wins",
			rows: map[string]store.ParentKey{
				"p1": {Status: models.StatusMined, PartitionKey: "old"},
				"p2": {Status: models.StatusReceived, PartitionKey: "k2"},
				"p3": {Status: models.StatusReceived, PartitionKey: "k3"},
			},
			inputs: []string{"p1", "p2", "p3"},
			max:    8,
			want:   "k2",
		},
		{
			name: "own row keeps its earlier key",
			rows: map[string]store.ParentKey{
				child: {Status: models.StatusRejected, PartitionKey: "mine"},
				"p1":  {Status: models.StatusReceived, PartitionKey: "root"},
			},
			inputs: []string{"p1"},
			max:    8,
			want:   "mine",
		},
		{name: "lookup error keys by own txid", inputs: []string{"p1"}, max: 8, err: errors.New("down"), want: child},
		{
			name:   "disabled keys by own txid",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusReceived, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    0,
			want:   child,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &keyLookupStore{rows: tc.rows, err: tc.err}
			got, ownSeen := keyingServer(st, tc.max).partitionKeyForSubmit(context.Background(), child, tc.inputs)
			if got != tc.want {
				t.Fatalf("key = %q, want %q", got, tc.want)
			}
			if _, has := tc.rows[child]; ownSeen != (has && tc.err == nil && tc.max > 0) {
				t.Fatalf("ownSeen = %v with own row present = %v", ownSeen, has)
			}
			if tc.max == 0 && len(st.asked) != 0 {
				t.Fatalf("disabled lookup still asked the store: %v", st.asked)
			}
		})
	}
}

func TestPartitionKeyForSubmit_CapsLookups(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{"p9": {Status: models.StatusReceived, PartitionKey: "late"}}}
	inputs := []string{"p1", "p1", "", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"}
	got, _ := keyingServer(st, 3).partitionKeyForSubmit(context.Background(), "cc", inputs)
	if got != "cc" {
		t.Fatalf("key = %q, want own txid (p9 is beyond the cap)", got)
	}
	if len(st.asked) != 1 || len(st.asked[0]) != 4 { // p1, p2, p3 + own txid
		t.Fatalf("asked = %v, want one lookup of 3 distinct parents plus the txid", st.asked)
	}
}

func TestInheritStoredPartitionKeys(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"P": {Status: models.StatusReceived, PartitionKey: "kP"},
		"Q": {Status: models.StatusSeenOnNetwork, PartitionKey: "kQ"},
	}}
	keyByTxid := map[string]string{"c1": "c1", "c2": "c1", "d": "d", "e": "e"}
	inputsByTxid := map[string][]string{
		"c1": {"P"},  // external, in flight: the whole family follows P
		"c2": {"c1"}, // in-batch parent: same family as c1
		"d":  {"Q"},  // external but already on the network: keep own key
		"e":  {},     // nothing to inherit
	}
	keyingServer(st, 8).inheritStoredPartitionKeys(context.Background(), keyByTxid, inputsByTxid)
	want := map[string]string{"c1": "kP", "c2": "kP", "d": "d", "e": "e"}
	for txid, k := range want {
		if keyByTxid[txid] != k {
			t.Errorf("%s: key = %q, want %q", txid, keyByTxid[txid], k)
		}
	}
	if len(st.asked) != 1 {
		t.Fatalf("expected exactly one store lookup for the batch, got %d", len(st.asked))
	}
}

// With no external parents only the batch's own rows are consulted, in one
// lookup, and a family with no stored key keeps its natural key.
func TestInheritStoredPartitionKeys_NoExternalParentsLooksUpOwnRowsOnly(t *testing.T) {
	st := &keyLookupStore{}
	keyByTxid := map[string]string{"a": "a", "b": "a"}
	keyingServer(st, 8).inheritStoredPartitionKeys(context.Background(), keyByTxid, map[string][]string{"a": {}, "b": {"a"}})
	if len(st.asked) != 1 || len(st.asked[0]) != 2 || st.asked[0][0] != "a" || st.asked[0][1] != "b" {
		t.Fatalf("asked = %v, want one lookup of [a b]", st.asked)
	}
	if keyByTxid["a"] != "a" || keyByTxid["b"] != "a" {
		t.Fatalf("keys changed with nothing stored: %v", keyByTxid)
	}
}

// A member's own stored key binds its family ahead of an external parent,
// but only while that row is in flight or will be republished.
func TestInheritStoredPartitionKeys_OwnStoredKeyBindsFamily(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"P":  {Status: models.StatusReceived, PartitionKey: "kP"},
		"c1": {Status: models.StatusReceived, PartitionKey: "kOld"}, // retry: keeps its key
		"d1": {Status: models.StatusRejected, PartitionKey: "kD"},   // republished: keeps its key
		"e1": {Status: models.StatusMined, PartitionKey: "kE"},      // on the network: does not bind
	}}
	keyByTxid := map[string]string{"c1": "c1", "c2": "c1", "d1": "d1", "e1": "e1", "e2": "e1"}
	inputsByTxid := map[string][]string{
		"c1": {"P"}, "c2": {"c1"},
		"d1": {},
		"e1": {"P"}, "e2": {"e1"},
	}
	keyingServer(st, 8).inheritStoredPartitionKeys(context.Background(), keyByTxid, inputsByTxid)
	want := map[string]string{"c1": "kOld", "c2": "kOld", "d1": "kD", "e1": "kP", "e2": "kP"}
	for txid, k := range want {
		if keyByTxid[txid] != k {
			t.Errorf("%s: key = %q, want %q", txid, keyByTxid[txid], k)
		}
	}
}

func TestAdoptStoredFamilyKeys(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"c1": {Status: models.StatusReceived, PartitionKey: "kP"},
		"d1": {Status: models.StatusReceived}, // stored before keys were persisted
	}}
	keyByTxid := map[string]string{"c1": "c1", "c2": "c1", "d1": "d1", "e1": "e1"}
	famByTxid := map[string]string{"c1": "c1", "c2": "c1", "d1": "d1", "e1": "e1"}
	keyingServer(st, 8).adoptStoredFamilyKeys(context.Background(), keyByTxid, famByTxid, []string{"d1", "c1"})
	want := map[string]string{"c1": "kP", "c2": "kP", "d1": "d1", "e1": "e1"}
	for txid, k := range want {
		if keyByTxid[txid] != k {
			t.Errorf("%s: key = %q, want %q", txid, keyByTxid[txid], k)
		}
	}

	st.asked = nil
	keyingServer(st, 0).adoptStoredFamilyKeys(context.Background(), keyByTxid, famByTxid, []string{"c1"})
	if len(st.asked) != 0 {
		t.Errorf("disabled lookup still asked the store: %v", st.asked)
	}
}

// TestAdoptStoredFamilyKeys_SharedRoutingKey: families a and b both inherited
// kR. a's stored key moved to kA; b's is still kR. Grouping by routing key
// would drag b (and b2) to kA.
func TestAdoptStoredFamilyKeys_SharedRoutingKey(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"a": {Status: models.StatusReceived, PartitionKey: "kA"},
		"b": {Status: models.StatusReceived, PartitionKey: "kR"},
	}}
	keyByTxid := map[string]string{"a": "kR", "a2": "kR", "b": "kR", "b2": "kR"}
	famByTxid := map[string]string{"a": "a", "a2": "a", "b": "b", "b2": "b"}
	keyingServer(st, 8).adoptStoredFamilyKeys(context.Background(), keyByTxid, famByTxid, []string{"a", "b"})
	want := map[string]string{"a": "kA", "a2": "kA", "b": "kR", "b2": "kR"}
	for txid, k := range want {
		if keyByTxid[txid] != k {
			t.Errorf("%s: key = %q, want %q", txid, keyByTxid[txid], k)
		}
	}
}

func TestStoredPartitionKey(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"a": {Status: models.StatusReceived, PartitionKey: "kA"},
		"b": {Status: models.StatusReceived},
	}}
	srv := keyingServer(st, 8)
	for _, tc := range []struct{ txid, want string }{{"a", "kA"}, {"b", "fallback"}, {"z", "fallback"}} {
		if got := srv.storedPartitionKey(context.Background(), tc.txid, "fallback", false); got != tc.want {
			t.Errorf("%s: key = %q, want %q", tc.txid, got, tc.want)
		}
	}
	st.err = errors.New("down")
	if got := srv.storedPartitionKey(context.Background(), "a", "fallback", false); got != "fallback" {
		t.Errorf("lookup error: key = %q, want fallback", got)
	}
}

// TestInheritStoredPartitionKeys_PendingMemberBeatsRejected: family {a, b},
// a sorts first and is REJECTED with a stale key; b is RECEIVED and its
// earlier message is pending under kP. The family must stay on kP — moving it
// to a's key would republish b onto a second partition.
func TestInheritStoredPartitionKeys_PendingMemberBeatsRejected(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"a": {Status: models.StatusRejected, PartitionKey: "kStale"},
		"b": {Status: models.StatusReceived, PartitionKey: "kP"},
	}}
	keyByTxid := map[string]string{"a": "a", "b": "a"}
	keyingServer(st, 8).inheritStoredPartitionKeys(context.Background(), keyByTxid, map[string][]string{"a": {}, "b": {"a"}})
	if keyByTxid["a"] != "kP" || keyByTxid["b"] != "kP" {
		t.Fatalf("keys = %v, want the family on the pending member's kP", keyByTxid)
	}
}

// TestAdoptStoredFamilyKeys_AgreeingPendingMemberLocksFamily: both members lost
// the CAS. a is RECEIVED and its stored key kP already matches the routing key;
// b sorts later and is REJECTED with kB. The agreeing pending member must lock
// the family on kP rather than let b's key move it.
func TestAdoptStoredFamilyKeys_AgreeingPendingMemberLocksFamily(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"a": {Status: models.StatusReceived, PartitionKey: "kP"},
		"b": {Status: models.StatusRejected, PartitionKey: "kB"},
	}}
	keyByTxid := map[string]string{"a": "kP", "b": "kP"}
	famByTxid := map[string]string{"a": "a", "b": "a"}
	keyingServer(st, 8).adoptStoredFamilyKeys(context.Background(), keyByTxid, famByTxid, []string{"b", "a"})
	if keyByTxid["a"] != "kP" || keyByTxid["b"] != "kP" {
		t.Fatalf("keys = %v, want the family locked on a's kP", keyByTxid)
	}
}

// TestBatchPartitionKeyLookupsAreChunked: a large POST /txs never sends one
// unbounded LookupPartitionKeys, and the chunked results are merged.
func TestBatchPartitionKeyLookupsAreChunked(t *testing.T) {
	n := 2*partitionKeyLookupChunk + 7
	keyByTxid := make(map[string]string, n)
	inputsByTxid := make(map[string][]string, n)
	for i := range n {
		id := fmt.Sprintf("t%06d", i)
		keyByTxid[id] = id
		inputsByTxid[id] = nil
	}
	last := fmt.Sprintf("t%06d", n-1)
	st := &keyLookupStore{rows: map[string]store.ParentKey{last: {Status: models.StatusReceived, PartitionKey: "kLast"}}}
	keyingServer(st, 8).inheritStoredPartitionKeys(context.Background(), keyByTxid, inputsByTxid)
	if len(st.asked) != 3 {
		t.Fatalf("lookups = %d, want 3 chunks", len(st.asked))
	}
	for i, ids := range st.asked {
		if len(ids) > partitionKeyLookupChunk {
			t.Fatalf("lookup %d carried %d txids, cap %d", i, len(ids), partitionKeyLookupChunk)
		}
	}
	if keyByTxid[last] != "kLast" {
		t.Fatalf("row from the last chunk was not merged: key = %q", keyByTxid[last])
	}

	st.asked = nil
	ids := make([]string, 0, n)
	for id := range keyByTxid {
		ids = append(ids, id)
	}
	keyingServer(st, 8).adoptStoredFamilyKeys(context.Background(), keyByTxid, maps.Clone(keyByTxid), ids)
	for i, got := range st.asked {
		if len(got) > partitionKeyLookupChunk {
			t.Fatalf("adopt lookup %d carried %d txids, cap %d", i, len(got), partitionKeyLookupChunk)
		}
	}
}
