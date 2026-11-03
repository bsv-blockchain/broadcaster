package api_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// keyedMemStore is an in-memory dedup CAS plus partition-key lookup, enough to
// drive the submit endpoints end to end and read back what they persisted. Like
// Postgres, the existing row GetOrInsertStatus returns does NOT carry the
// partition key; only LookupPartitionKeys reads it.
type keyedMemStore struct {
	*mockStore

	mu   sync.Mutex
	rows map[string]*models.TransactionStatus
	// beforeCAS runs once per txid ahead of its first GetOrInsertStatus, to
	// model a concurrent request inserting between lookup and CAS.
	beforeCAS map[string]func()
}

func newKeyedMemStore() *keyedMemStore {
	return &keyedMemStore{
		mockStore: &mockStore{},
		rows:      map[string]*models.TransactionStatus{},
		beforeCAS: map[string]func(){},
	}
}

func (s *keyedMemStore) put(txid string, st models.Status, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[txid] = &models.TransactionStatus{TxID: txid, Status: st, PartitionKey: key}
}

func (s *keyedMemStore) storedKey(t *testing.T, txid string) string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[txid]
	if !ok {
		t.Fatalf("no row persisted for %s", txid)
	}
	return r.PartitionKey
}

func (s *keyedMemStore) GetOrInsertStatus(_ context.Context, in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	s.mu.Lock()
	hook := s.beforeCAS[in.TxID]
	delete(s.beforeCAS, in.TxID)
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rows[in.TxID]; ok {
		return &models.TransactionStatus{TxID: r.TxID, Status: r.Status}, false, nil
	}
	cp := *in
	s.rows[in.TxID] = &cp
	return in, true, nil
}

func (s *keyedMemStore) LookupPartitionKeys(_ context.Context, txids []string) (map[string]store.ParentKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]store.ParentKey{}
	for _, id := range txids {
		if r, ok := s.rows[id]; ok {
			out[id] = store.ParentKey{Status: r.Status, PartitionKey: r.PartitionKey}
		}
	}
	return out, nil
}

func keyedServer(t *testing.T, st store.Store) (*kafka.RecordingBroker, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	broker := &kafka.RecordingBroker{}
	srv := &Server{
		cfg: &config.Config{
			CallbackToken: testCallbackToken,
			Propagation:   config.PropagationConfig{ParentKeyLookupMaxInputs: 8},
		},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          st,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	t.Cleanup(func() { close(srv.submissionStop) })
	router := gin.New()
	srv.registerRoutes(router)
	return broker, router
}

func postKeyed(t *testing.T, router *gin.Engine, path string, body []byte) int {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w.Code
}

// spendOutput0 builds a tx spending output 0 of parent, with one output of its own.
func spendOutput0(parent *chainhash.Hash) *sdkTx.Transaction {
	tx := sdkTx.NewTransaction()
	tx.Inputs = append(tx.Inputs, &sdkTx.TransactionInput{
		SourceTXID:       parent,
		SourceTxOutIndex: 0,
		SequenceNumber:   sdkTx.DefaultSequenceNumber,
	})
	tx.AddOutput(&sdkTx.TransactionOutput{Satoshis: 0, LockingScript: &script.Script{}})
	return tx
}

// externalParentChain returns the txid of an external parent P (never
// submitted here) and a chain c1 → c2 → c3 hanging off it.
func externalParentChain() (p string, c1, c2, c3 *sdkTx.Transaction) {
	pt := sdkTx.NewTransaction()
	pt.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1, LockingScript: &script.Script{}})
	c1 = spendOutput0(pt.TxID())
	c2 = spendOutput0(c1.TxID())
	c3 = spendOutput0(c2.TxID())
	return pt.TxID().String(), c1, c2, c3
}

func joinRaw(txs ...*sdkTx.Transaction) []byte {
	var b []byte
	for _, tx := range txs {
		b = append(b, tx.Bytes()...)
	}
	return b
}

// batchKeys maps txid → key over every SendBatch the broker recorded, the
// last batch winning.
func batchKeys(t *testing.T, broker *kafka.RecordingBroker) map[string]string {
	t.Helper()
	broker.Lock()
	defer broker.Unlock()
	keys := map[string]string{}
	for _, batch := range broker.Batches {
		for _, kv := range batch {
			raw, err := json.Marshal(kv.Value)
			if err != nil {
				t.Fatalf("marshal recorded value: %v", err)
			}
			var env struct {
				TXID string `json:"txid"`
			}
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("unmarshal recorded envelope: %v", err)
			}
			keys[env.TXID] = kv.Key
		}
	}
	return keys
}

func lastSendKey(t *testing.T, broker *kafka.RecordingBroker) string {
	t.Helper()
	broker.Lock()
	defer broker.Unlock()
	if len(broker.Sends) == 0 {
		t.Fatal("nothing was sent")
	}
	return broker.Sends[len(broker.Sends)-1].Key
}

// TestSubmitTransaction_InheritsInFlightParentKey: POST /tx for a child of a
// parent still at RECEIVED publishes AND persists the parent's key.
func TestSubmitTransaction_InheritsInFlightParentKey(t *testing.T) {
	p, c1, _, _ := externalParentChain()
	st := newKeyedMemStore()
	st.put(p, models.StatusReceived, "kP")
	broker, router := keyedServer(t, st)

	if code := postKeyed(t, router, "/tx", c1.Bytes()); code != http.StatusAccepted {
		t.Fatalf("POST /tx = %d", code)
	}
	if got := lastSendKey(t, broker); got != "kP" {
		t.Errorf("published key = %q, want the in-flight parent's kP", got)
	}
	if got := st.storedKey(t, c1.TxID().String()); got != "kP" {
		t.Errorf("persisted key = %q, want kP", got)
	}
}

// TestSubmitTransaction_CASLoserRepublishesUnderStoredKey: two identical
// submissions straddle the parent's acceptance. The winner stored kP while
// the parent was in flight; the loser looked up after the acceptance and
// computed its own txid. Its republish must use the winner's stored key, or
// the same tx is in flight on two partitions.
func TestSubmitTransaction_CASLoserRepublishesUnderStoredKey(t *testing.T) {
	p, c1, _, _ := externalParentChain()
	c1id := c1.TxID().String()
	st := newKeyedMemStore()
	st.put(p, models.StatusAcceptedByNetwork, "kP")
	st.beforeCAS[c1id] = func() { st.put(c1id, models.StatusReceived, "kP") }
	broker, router := keyedServer(t, st)

	if code := postKeyed(t, router, "/tx", c1.Bytes()); code != http.StatusAccepted {
		t.Fatalf("POST /tx = %d", code)
	}
	if got := lastSendKey(t, broker); got != "kP" {
		t.Errorf("loser republished under %q, want the winner's stored kP", got)
	}
}

// TestSubmitTransactions_RetryAfterFailedSendKeepsStoredKeys: a batch whose
// family inherited an in-flight external parent's key fails to send; the
// parent is then accepted and the client retries the same body plus a new
// grandchild. The retry must publish every member — and persist the new one —
// under the stored key, not the family's natural key, or a later child that
// follows the stored key misses its parent's dispatcher.
func TestSubmitTransactions_RetryAfterFailedSendKeepsStoredKeys(t *testing.T) {
	p, c1, c2, c3 := externalParentChain()
	st := newKeyedMemStore()
	st.put(p, models.StatusReceived, "kP")
	broker, router := keyedServer(t, st)

	broker.BatchErr = errors.New("broker down")
	if code := postKeyed(t, router, "/txs", joinRaw(c1, c2)); code != http.StatusInternalServerError {
		t.Fatalf("first POST /txs = %d, want 500 from the failed send", code)
	}
	for _, tx := range []*sdkTx.Transaction{c1, c2} {
		if got := st.storedKey(t, tx.TxID().String()); got != "kP" {
			t.Fatalf("%s persisted key %q, want inherited kP", tx.TxID(), got)
		}
	}

	st.put(p, models.StatusAcceptedByNetwork, "kP")
	broker.BatchErr = nil
	if code := postKeyed(t, router, "/txs", joinRaw(c1, c2, c3)); code != http.StatusAccepted {
		t.Fatalf("retry POST /txs = %d", code)
	}
	keys := batchKeys(t, broker)
	for _, tx := range []*sdkTx.Transaction{c1, c2, c3} {
		id := tx.TxID().String()
		if keys[id] != "kP" {
			t.Errorf("%s published under %q, want stored kP", id, keys[id])
		}
	}
	if got := st.storedKey(t, c3.TxID().String()); got != "kP" {
		t.Errorf("new grandchild persisted key %q, want its family's kP", got)
	}
}

// TestSubmitTransactions_CASLoserAdoptsStoredFamilyKey: a concurrent request
// inserts a batch member with kP between this request's lookup (parent
// already accepted, so it chose the natural key) and its CAS. The loser and
// its whole family publish under the stored key.
func TestSubmitTransactions_CASLoserAdoptsStoredFamilyKey(t *testing.T) {
	p, c1, c2, _ := externalParentChain()
	c1id := c1.TxID().String()
	st := newKeyedMemStore()
	st.put(p, models.StatusAcceptedByNetwork, "kP")
	st.beforeCAS[c1id] = func() { st.put(c1id, models.StatusReceived, "kP") }
	broker, router := keyedServer(t, st)

	if code := postKeyed(t, router, "/txs", joinRaw(c1, c2)); code != http.StatusAccepted {
		t.Fatalf("POST /txs = %d", code)
	}
	keys := batchKeys(t, broker)
	for _, tx := range []*sdkTx.Transaction{c1, c2} {
		id := tx.TxID().String()
		if keys[id] != "kP" {
			t.Errorf("%s published under %q, want the stored kP", id, keys[id])
		}
	}
}

// TestSubmitTransactions_AdoptionKeepsSeparateFamiliesApart: independent A
// and B both spend in-flight R, so the batch keys both families kR — A by
// inheritance, B by its own row, persisted under kR after an earlier failed
// send. Between lookup and CAS, R is accepted and a concurrent POST /tx wins
// A's CAS under A's own txid. A's family adopts that stored key; B shares
// only the routing key kR, not the family, and must stay on its stored kR.
func TestSubmitTransactions_AdoptionKeepsSeparateFamiliesApart(t *testing.T) {
	rt := sdkTx.NewTransaction()
	rt.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1, LockingScript: &script.Script{}})
	rt.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1, LockingScript: &script.Script{}})
	r := rt.TxID().String()
	a := spendOutput0(rt.TxID())
	b := spendOutput0(rt.TxID())
	b.Inputs[0].SourceTxOutIndex = 1
	aid, bid := a.TxID().String(), b.TxID().String()

	st := newKeyedMemStore()
	st.put(r, models.StatusReceived, "kR")
	st.put(bid, models.StatusReceived, "kR")
	st.beforeCAS[aid] = func() {
		st.put(r, models.StatusAcceptedByNetwork, "kR")
		st.put(aid, models.StatusReceived, aid)
	}
	broker, router := keyedServer(t, st)

	if code := postKeyed(t, router, "/txs", joinRaw(a, b)); code != http.StatusAccepted {
		t.Fatalf("POST /txs = %d", code)
	}
	keys := batchKeys(t, broker)
	if keys[aid] != aid {
		t.Errorf("A published under %q, want the concurrent winner's stored %s", keys[aid], aid)
	}
	if keys[bid] != "kR" {
		t.Errorf("B published under %q, want its stored kR", keys[bid])
	}
	if got := st.storedKey(t, bid); got != "kR" {
		t.Errorf("B persisted key %q, want kR", got)
	}
}
