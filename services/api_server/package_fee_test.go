package api_server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/validator"
)

func apiPackageFeeFixture(t *testing.T) (parentRaw, childRaw []byte, parentID string) {
	parent := sdkTx.NewTransaction()
	parent.Version = 2
	require.NoError(t, parent.AddInputFrom(
		"0000000000000000000000000000000000000000000000000000000000000001",
		0, "51", 100_000, nil,
	))
	parent.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      99_999,
		LockingScript: script.NewFromBytes([]byte{script.OpTRUE}),
	})
	parent.Inputs[0].UnlockingScript = script.NewFromBytes([]byte{script.OpTRUE})
	child := sdkTx.NewTransaction()
	child.Version = 2
	child.AddInputFromTx(parent, 0, nil)
	child.Inputs[0].UnlockingScript = script.NewFromBytes([]byte{script.OpTRUE})
	child.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      98_999,
		LockingScript: script.NewFromBytes([]byte{script.OpTRUE}),
	})

	var err error
	parentRaw, err = parent.EF()
	require.NoError(t, err)
	childRaw, err = child.EF()
	require.NoError(t, err)
	return parentRaw, childRaw, parent.TxID().String()
}

func TestPackageFeeBatchRejectsUnderpaidAncestorBeforePropagation(t *testing.T) {
	parentRaw, childRaw, parentID := apiPackageFeeFixture(t)

	// Child first proves it has complete EF source data and is independently
	// valid; the later parent must still fail the batch's per-tx fee loop.
	body := append(append([]byte{}, childRaw...), parentRaw...)
	child, used, err := sdkTx.NewTransactionFromStream(body)
	require.NoError(t, err)
	require.Equal(t, len(childRaw), used)
	require.Len(t, child.Inputs, 1)
	require.NotNil(t, child.Inputs[0].SourceTxOutput())
	parent, used, err := sdkTx.NewTransactionFromStream(body[used:])
	require.NoError(t, err)
	require.Equal(t, len(parentRaw), used)
	require.Len(t, parent.Inputs, 1)
	require.NotNil(t, parent.Inputs[0].SourceTxOutput())

	broker := &kafka.RecordingBroker{}
	srv, router := setupServerWithStore(broker, &mockStore{})
	minFeePerKB := uint64(100)
	srv.validator = validator.NewValidator(&validator.Policy{MinFeePerKB: &minFeePerKB})
	gin.SetMode(gin.TestMode)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	var response struct {
		TxID   string `json:"txid"`
		Status int    `json:"status"`
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, parentID, response.TxID)
	assert.Equal(t, int(arcerrors.StatusFees), response.Status)
	assert.Contains(t, response.Reason, "fee is too low")
	assert.Equal(t, 0, totalMessages(broker), "fee rejection must publish no propagation messages")
}
