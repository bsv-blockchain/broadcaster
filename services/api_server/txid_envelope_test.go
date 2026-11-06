package api_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/models"
)

// publishedEnvelopes decodes every TopicPropagation message the broker saw,
// single sends and batches alike.
func publishedEnvelopes(t *testing.T, broker *kafka.RecordingBroker) []map[string]any {
	t.Helper()
	var out []map[string]any
	decode := func(b []byte) {
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("decode envelope %q: %v", b, err)
		}
		out = append(out, m)
	}
	for _, s := range broker.Sends {
		if s.Topic == kafka.TopicPropagation {
			decode(s.Value)
		}
	}
	for _, batch := range broker.Batches {
		for _, kv := range batch {
			b, err := json.Marshal(kv.Value)
			if err != nil {
				t.Fatalf("marshal batch value: %v", err)
			}
			decode(b)
		}
	}
	return out
}

func submit(t *testing.T, router http.Handler, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// TestSubmit_PropagationEnvelopeIsTxidOnly pins #329: the propagation
// message carries the txid and nothing else, so its size never tracks the
// tx size or input count. The raw tx rides on the store row instead.
func TestSubmit_PropagationEnvelopeIsTxidOnly(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	for _, tc := range []struct {
		path  string
		body  []byte
		txids []string
	}{
		{"/tx", rawA, []string{txidA}},
		{"/txs", append(append([]byte(nil), rawA...), rawB...), []string{txidA, txidB}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ms := &mockStore{}
			var stored [][]byte
			ms.getOrInsertFn = func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
				stored = append(stored, in.RawTx)
				return in, true, nil
			}
			_, broker, router := newReceivedServer(ms)
			if w := submit(t, router, tc.path, tc.body); w.Code != http.StatusAccepted && w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			envs := publishedEnvelopes(t, broker)
			if len(envs) != len(tc.txids) {
				t.Fatalf("published %d envelopes, want %d", len(envs), len(tc.txids))
			}
			for i, env := range envs {
				if len(env) != 1 || env["txid"] != tc.txids[i] {
					t.Errorf("envelope %d = %v, want only txid %s", i, env, tc.txids[i])
				}
			}
			for i, raw := range stored {
				if len(raw) == 0 {
					t.Errorf("row %d persisted without raw_tx; the consumer could not hydrate it", i)
				}
			}
		})
	}
}

// rejectedRowStore answers every CAS with an existing REJECTED row, the
// shape a resubmit after a terminal rejection sees.
func rejectedRowStore() *mockStore {
	return &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return &models.TransactionStatus{TxID: in.TxID, Status: models.StatusRejected}, false, nil
		},
	}
}

// TestSubmit_RepublishRestoresRawTx: a REJECTED row may have lost its raw_tx
// (ClearRetryState), so intake writes the submitted bytes back before it
// republishes the txid-only message.
func TestSubmit_RepublishRestoresRawTx(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	for _, tc := range []struct {
		path string
		body []byte
		want map[string][]byte
	}{
		{"/tx", rawA, map[string][]byte{txidA: rawA}},
		{"/txs", append(append([]byte(nil), rawA...), rawB...), map[string][]byte{txidA: rawA, txidB: rawB}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ms := rejectedRowStore()
			_, broker, router := newReceivedServer(ms)
			if w := submit(t, router, tc.path, tc.body); w.Code != http.StatusAccepted && w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			for txid, raw := range tc.want {
				if !bytes.Equal(ms.setRawTxCalls[txid], raw) {
					t.Errorf("SetRawTx(%s) = %x, want the submitted bytes", txid, ms.setRawTxCalls[txid])
				}
			}
			if n := len(publishedEnvelopes(t, broker)); n != len(tc.want) {
				t.Errorf("republished %d envelopes, want %d", n, len(tc.want))
			}
		})
	}
}

// TestSubmit_RepublishWithRawTx_SkipsRestore: a stranded RECEIVED row still
// carries its raw_tx, so intake republishes without rewriting it.
func TestSubmit_RepublishWithRawTx_SkipsRestore(t *testing.T) {
	rawA, rawB, _, _ := twoDistinctTxs(t)
	for _, tc := range []struct {
		path string
		body []byte
		want int
	}{
		{"/tx", rawA, 1},
		{"/txs", append(append([]byte(nil), rawA...), rawB...), 2},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ms := &mockStore{
				getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
					return &models.TransactionStatus{TxID: in.TxID, Status: models.StatusReceived, RawTx: in.RawTx}, false, nil
				},
			}
			_, broker, router := newReceivedServer(ms)
			if w := submit(t, router, tc.path, tc.body); w.Code != http.StatusAccepted && w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			if len(ms.setRawTxCalls) != 0 {
				t.Errorf("SetRawTx called for rows that still have raw_tx: %v", ms.setRawTxCalls)
			}
			if n := len(publishedEnvelopes(t, broker)); n != tc.want {
				t.Errorf("republished %d envelopes, want %d", n, tc.want)
			}
		})
	}
}

// TestSubmit_RejectedResubmit_RestoresEvenWithStoredRawTx: only
// ClearRetryState empties raw_tx, so a network-REJECTED row still holds the
// rejected bytes. A resubmit (say, with corrected EF metadata under the same
// txid) must republish the new ones.
func TestSubmit_RejectedResubmit_RestoresEvenWithStoredRawTx(t *testing.T) {
	rawA, rawB, txidA, txidB := twoDistinctTxs(t)
	for _, tc := range []struct {
		path string
		body []byte
		want map[string][]byte
	}{
		{"/tx", rawA, map[string][]byte{txidA: rawA}},
		{"/txs", append(append([]byte(nil), rawA...), rawB...), map[string][]byte{txidA: rawA, txidB: rawB}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ms := &mockStore{
				getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
					return &models.TransactionStatus{TxID: in.TxID, Status: models.StatusRejected, RawTx: []byte{0xba, 0xd0}}, false, nil
				},
			}
			_, _, router := newReceivedServer(ms)
			if w := submit(t, router, tc.path, tc.body); w.Code != http.StatusAccepted && w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			for txid, raw := range tc.want {
				if !bytes.Equal(ms.setRawTxCalls[txid], raw) {
					t.Errorf("SetRawTx(%s) = %x, want the resubmitted bytes", txid, ms.setRawTxCalls[txid])
				}
			}
		})
	}
}

// TestSubmitTx_TrailingBytes_StoresOnlyTheTx: the stored bytes are later
// concatenated into a Teranode /txs stream, so anything after the parsed tx
// must not reach the row.
func TestSubmitTx_TrailingBytes_StoresOnlyTheTx(t *testing.T) {
	rawA, _, _, _ := twoDistinctTxs(t)
	var stored []byte
	ms := &mockStore{
		getOrInsertFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			stored = append([]byte(nil), in.RawTx...)
			return in, true, nil
		},
	}
	_, _, router := newReceivedServer(ms)
	if w := submit(t, router, "/tx", append(append([]byte(nil), rawA...), '\n')); w.Code != http.StatusAccepted && w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	if !bytes.Equal(stored, rawA) {
		t.Errorf("stored %d bytes, want the %d-byte tx without the trailing newline", len(stored), len(rawA))
	}
}

// TestSubmit_RestoreRawTxFailure_Sheds503: without the bytes back on the row
// the republished message could not be hydrated, so the request fails
// retryably and nothing is published.
func TestSubmit_RestoreRawTxFailure_Sheds503(t *testing.T) {
	rawA, rawB, _, _ := twoDistinctTxs(t)
	for _, tc := range []struct {
		path string
		body []byte
	}{
		{"/tx", rawA},
		{"/txs", append(append([]byte(nil), rawA...), rawB...)},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ms := rejectedRowStore()
			ms.setRawTxErr = errors.New("store down")
			_, broker, router := newReceivedServer(ms)
			w := submit(t, router, tc.path, tc.body)
			if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
				t.Fatalf("status = %d (Retry-After %q), want 503 with Retry-After", w.Code, w.Header().Get("Retry-After"))
			}
			if n := len(publishedEnvelopes(t, broker)); n != 0 {
				t.Errorf("published %d envelopes after a failed raw_tx restore", n)
			}
		})
	}
}
