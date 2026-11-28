package propagation

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/teranode"
)

// TestReapOnce_RebroadcastChunksByBytes — the reaper's rebroadcast path
// shares broadcastInChunks with the dispatcher, so it inherits the byte cap:
// four stale 1000-byte RECEIVED rows under a 2500-byte cap must leave as two
// POST /txs, not one.
func TestReapOnce_RebroadcastChunksByBytes(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	stale := time.Now().Add(-2 * time.Hour) // older than staleReceivedAge
	for _, id := range []string{"tx-a", "tx-b", "tx-c", "tx-d"} {
		ms.replayRows = append(ms.replayRows, &models.TransactionStatus{
			TxID:      id,
			Status:    models.StatusReceived,
			RawTx:     make([]byte, 1000),
			Timestamp: stale,
		})
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	cfg := &config.Config{CallbackURL: testLocalCallbackURL}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.TeranodeMaxBatchBytes = 2500
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, mc)

	p.reapOnce(context.Background())

	if got := log.count("register:"); got != 4 {
		t.Fatalf("register count = %d, want 4; events=%v", got, log.all())
	}
	if got := log.count("broadcast"); got != 2 {
		t.Errorf("broadcast count = %d, want 2 (4 × 1000 B under a 2500 B cap); events=%v", got, log.all())
	}
}
