package propagation

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/teranode"
)

// TestReapOnce_RebroadcastsStaleReceived is the issue #83 / F-025 regression:
// a validated tx stranded at RECEIVED past staleReceivedAge (its no-verdict
// broadcast requeue was lost, or an intake Kafka-publish failure was never
// retried) must be rebroadcast by the reaper. Before RECEIVED was brought into
// reapOnce's scope this asserted register count = 0; it now expects 1.
func TestReapOnce_RebroadcastsStaleReceived(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{
			TxID:      "tx-stranded",
			Status:    models.StatusReceived,
			RawTx:     []byte{0xde, 0xad, 0xbe, 0xef},
			Timestamp: time.Now().Add(-2 * time.Hour), // older than staleReceivedAge
		},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	p.reapOnce(context.Background())

	if got := log.count("register:"); got != 1 {
		t.Fatalf("register count = %d, want 1 (stale RECEIVED row should be rebroadcast)", got)
	}
	if got := log.count("register:tx-stranded"); got != 1 {
		t.Errorf("stale tx not the one registered; events=%v", log.all())
	}
	if got := log.count("broadcast"); got != 1 {
		t.Errorf("broadcast count = %d, want 1", got)
	}
}

// TestReapOnce_ReceivedSelection pins which rows the RECEIVED arm picks up:
// only rows past staleReceivedAge that still carry RawTx. Recent rows (still in
// the normal intake/requeue path), body-less rows, and terminal rows are left
// alone, and so is a row older than staleScanLookback that a backend ignoring
// since (Aerospike, and this mock) still returns. The stale SEEN_ON_NETWORK
// row is a regression guard that the original rebroadcast behavior is
// unchanged.
func TestReapOnce_ReceivedSelection(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)    // > staleReceivedAge, < staleScanLookback
	recent := now.Add(-1 * time.Minute) // < staleReceivedAge
	ancient := now.Add(-staleScanLookback - time.Hour)

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "recv-stale", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: stale},
		{TxID: "recv-recent", Status: models.StatusReceived, RawTx: []byte{0x02}, Timestamp: recent},
		{TxID: "recv-nobody", Status: models.StatusReceived, Timestamp: stale}, // no RawTx → skip
		{TxID: "seen-stale", Status: models.StatusSeenOnNetwork, RawTx: []byte{0x03}, Timestamp: stale},
		{TxID: "mined-old", Status: models.StatusMined, RawTx: []byte{0x04}, Timestamp: stale},         // terminal → skip
		{TxID: "recv-ancient", Status: models.StatusReceived, RawTx: []byte{0x05}, Timestamp: ancient}, // outside lookback → skip
	}
	ms.ignoreSince = true

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	p.reapOnce(context.Background())

	want := map[string]int{
		"register:recv-stale":   1, // stale RECEIVED with body → rebroadcast
		"register:seen-stale":   1, // stale SEEN with body → rebroadcast (unchanged)
		"register:recv-recent":  0, // too recent
		"register:recv-nobody":  0, // no RawTx
		"register:mined-old":    0, // terminal
		"register:recv-ancient": 0, // outside staleScanLookback
	}
	for prefix, exp := range want {
		if got := log.count(prefix); got != exp {
			t.Errorf("%q count = %d, want %d; events=%v", prefix, got, exp, log.all())
		}
	}
	if got := log.count("register:"); got != 2 {
		t.Errorf("total register count = %d, want 2; events=%v", got, log.all())
	}
}

// gaugeVal reads a labeled gauge value for assertions.
func gaugeVal(t *testing.T, vec *prometheus.GaugeVec, label string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(label))
}

// TestReapOnce_StuckByCallbackAttribution pins the per-client breakdown of the
// stuck census: each stuck tx is attributed to its submission's callback host
// (or "none" when it has no callback), so an operator can see which client
// owns the ACCEPTED_BY_NETWORK backlog.
func TestReapOnce_StuckByCallbackAttribution(t *testing.T) {
	metrics.StuckTransientTxsByCallback.Reset()
	stale := time.Now().Add(-2 * time.Hour)

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "acc-a", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x01}, Timestamp: stale},
		{TxID: "acc-b", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x02}, Timestamp: stale},
		{TxID: "acc-c", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x03}, Timestamp: stale},
		{TxID: "acc-nocb", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x04}, Timestamp: stale},
	}
	// Two txs from one overlay host, one from another, one with no callback.
	ms.submissionsByTxID = map[string][]*models.Submission{
		"acc-a": {{TxID: "acc-a", CallbackURL: "https://low-overlay.example/arc-ingest"}},
		"acc-b": {{TxID: "acc-b", CallbackURL: "https://low-overlay.example/arc-ingest"}},
		"acc-c": {{TxID: "acc-c", CallbackURL: "https://other-client.example/cb"}},
		// acc-nocb: no submissions → "none"
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	acc := string(models.StatusAcceptedByNetwork)
	byHost := func(host string) float64 {
		return testutil.ToFloat64(metrics.StuckTransientTxsByCallback.WithLabelValues(acc, host))
	}
	if got := byHost("low-overlay.example"); got != 2 {
		t.Errorf("low-overlay.example = %v, want 2", got)
	}
	if got := byHost("other-client.example"); got != 1 {
		t.Errorf("other-client.example = %v, want 1", got)
	}
	if got := byHost("none"); got != 1 {
		t.Errorf("none (no-callback) = %v, want 1", got)
	}
	// Aggregate by-callback must equal the uncapped census for ACCEPTED (4).
	if got := gaugeVal(t, metrics.StuckTransientTxs, acc); got != 4 {
		t.Errorf("uncapped ACCEPTED census = %v, want 4", got)
	}
}

// TestReapOnce_StuckTransientCensus pins the stuck-transient gauges (issue:
// alerting on txs whose SEEN state-transfer never arrived):
//   - RECEIVED and ACCEPTED_BY_NETWORK rows older than stuckTransientAge are
//     counted — INCLUDING body-less rows the rebroadcast arm skips;
//   - fresh transient rows and terminal rows are not counted;
//   - the oldest-age gauge reflects the oldest stuck row per status.
func TestReapOnce_StuckTransientCensus(t *testing.T) {
	now := time.Now()
	stale2h := now.Add(-2 * time.Hour)
	stale3h := now.Add(-3 * time.Hour)
	recent := now.Add(-10 * time.Minute)

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		// counted: RECEIVED stuck (with and without body)
		{TxID: "recv-stale-body", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: stale2h},
		{TxID: "recv-stale-nobody", Status: models.StatusReceived, Timestamp: stale3h},
		// counted: ACCEPTED_BY_NETWORK stuck (the previously-invisible case)
		{TxID: "acc-stale", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x02}, Timestamp: stale2h},
		// not counted: fresh transient rows
		{TxID: "recv-recent", Status: models.StatusReceived, RawTx: []byte{0x03}, Timestamp: recent},
		{TxID: "acc-recent", Status: models.StatusAcceptedByNetwork, Timestamp: recent},
		// not counted: terminal + other states
		{TxID: "mined-old", Status: models.StatusMined, Timestamp: stale3h},
		{TxID: "seen-old", Status: models.StatusSeenOnNetwork, RawTx: []byte{0x04}, Timestamp: stale2h},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusReceived)); got != 2 {
		t.Errorf("stuck RECEIVED = %v, want 2 (body-less rows must be counted)", got)
	}
	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusAcceptedByNetwork)); got != 1 {
		t.Errorf("stuck ACCEPTED_BY_NETWORK = %v, want 1", got)
	}
	// Oldest RECEIVED is the 3h row: age must exceed 2.5h.
	if got := gaugeVal(t, metrics.OldestTransientTxAge, string(models.StatusReceived)); got < 2.5*3600 {
		t.Errorf("oldest RECEIVED age = %vs, want > 9000s", got)
	}
	// Stale ACCEPTED_BY_NETWORK with a body is now nudged by the rebroadcast
	// arm (see TestReapOnce_RebroadcastsStaleAccepted); the census still
	// counts it regardless.
	if got := log.count("register:acc-stale"); got != 1 {
		t.Errorf("stale ACCEPTED_BY_NETWORK with body should be rebroadcast; events=%v", log.all())
	}
	// Rebroadcast arm unchanged: stale RECEIVED with body + stale SEEN.
	if got := log.count("register:recv-stale-body"); got != 1 {
		t.Errorf("stale RECEIVED with body not rebroadcast; events=%v", log.all())
	}
}

// TestReapOnce_RebroadcastsStaleAccepted pins the ACCEPTED_BY_NETWORK
// self-heal arm (the stuck-at-ACCEPTED drain, CASE-26): a validated tx that
// reached ACCEPTED_BY_NETWORK but never advanced to SEEN past
// staleAcceptedByNetworkAge is re-registered with merkle-service and
// re-broadcast. Recent ACCEPTED rows (still in the normal ACCEPTED→SEEN
// window) and body-less rows are left alone.
func TestReapOnce_RebroadcastsStaleAccepted(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)    // > staleAcceptedByNetworkAge, < staleScanLookback
	recent := now.Add(-1 * time.Minute) // < staleAcceptedByNetworkAge

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "acc-stale", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x0a}, Timestamp: stale},
		{TxID: "acc-recent", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x0b}, Timestamp: recent},
		{TxID: "acc-nobody", Status: models.StatusAcceptedByNetwork, Timestamp: stale}, // no RawTx → skip
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	want := map[string]int{
		"register:acc-stale":  1, // stale ACCEPTED with body → rebroadcast
		"register:acc-recent": 0, // still in the normal ACCEPTED→SEEN window
		"register:acc-nobody": 0, // no RawTx to rebroadcast
	}
	for prefix, exp := range want {
		if got := log.count(prefix); got != exp {
			t.Errorf("%q count = %d, want %d; events=%v", prefix, got, exp, log.all())
		}
	}
	if got := log.count("register:"); got != 1 {
		t.Errorf("total register count = %d, want 1; events=%v", got, log.all())
	}
	if got := log.count("broadcast"); got != 1 {
		t.Errorf("broadcast count = %d, want 1", got)
	}
}

// TestReapOnce_StuckTransientCensus_ZeroesWhenClear: a scan finding nothing
// stuck must reset the gauges to 0 (no stale non-zero readings after a
// backlog drains).
func TestReapOnce_StuckTransientCensus_ZeroesWhenClear(t *testing.T) {
	// Seed non-zero values as if a previous tick found stuck rows.
	metrics.StuckTransientTxs.WithLabelValues(string(models.StatusReceived)).Set(7)
	metrics.OldestTransientTxAge.WithLabelValues(string(models.StatusAcceptedByNetwork)).Set(1234)

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "recv-recent", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: time.Now().Add(-time.Minute)},
	}
	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	for _, status := range stuckTransientStatuses {
		if got := gaugeVal(t, metrics.StuckTransientTxs, string(status)); got != 0 {
			t.Errorf("stuck %s = %v, want 0 after clean scan", status, got)
		}
		if got := gaugeVal(t, metrics.OldestTransientTxAge, string(status)); got != 0 {
			t.Errorf("oldest %s age = %v, want 0 after clean scan", status, got)
		}
	}
}

// TestReapOnce_CensusUncappedByRebroadcastBatch: the census must keep
// counting past the rebroadcast batch cap (the old walk aborted at
// the batch cap, silently truncating any census).
func TestReapOnce_CensusUncappedByRebroadcastBatch(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)

	log := &eventLog{}
	ms := newMockStore()
	total := defaultReaperRebroadcastBatch + 50
	for i := 0; i < total; i++ {
		ms.replayRows = append(ms.replayRows, &models.TransactionStatus{
			TxID:      fmt.Sprintf("recv-%04d", i),
			Status:    models.StatusReceived,
			RawTx:     []byte{0x01},
			Timestamp: stale,
		})
	}
	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusReceived)); got != float64(total) {
		t.Errorf("stuck RECEIVED = %v, want %d (census must not be capped at the rebroadcast batch)", got, total)
	}
	if got := log.count("register:"); got != defaultReaperRebroadcastBatch {
		t.Errorf("rebroadcast count = %d, want %d (batch cap unchanged)", got, defaultReaperRebroadcastBatch)
	}
}

// TestReapOnce_WalkAbortsAtBatch_NoFullScan pins the issue #290 fix: with the
// census resolved store-side (CensusStatusesSince), the IterateStatusesSince
// walk stops once the rebroadcast batch is full instead of scanning the whole
// table. It seeds far more eligible rows than the batch and asserts the walk
// visited only ~one batch of rows — so the reaper's cadence is
// reaper_interval_ms, not the multi-minute full-scan time it regressed to.
func TestReapOnce_WalkAbortsAtBatch_NoFullScan(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)

	log := &eventLog{}
	ms := newMockStore()
	total := defaultReaperRebroadcastBatch * 4 // far more eligible rows than one batch
	for i := 0; i < total; i++ {
		ms.replayRows = append(ms.replayRows, &models.TransactionStatus{
			TxID:      fmt.Sprintf("recv-%05d", i),
			Status:    models.StatusReceived,
			RawTx:     []byte{0x01},
			Timestamp: stale,
		})
	}
	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	// The walk visits one batch of rows, then aborts on the next (that row is
	// counted before fn returns the stop sentinel) → batch+1 at most.
	if ms.iterated > defaultReaperRebroadcastBatch+1 {
		t.Errorf("walk visited %d rows, want <= %d (must abort at the batch, not full-scan %d)",
			ms.iterated, defaultReaperRebroadcastBatch+1, total)
	}
	// Census is still uncapped (store-side aggregate sees every row).
	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusReceived)); got != float64(total) {
		t.Errorf("stuck RECEIVED census = %v, want %d (census must be uncapped via CensusStatusesSince)", got, total)
	}
	if got := log.count("register:"); got != defaultReaperRebroadcastBatch {
		t.Errorf("rebroadcast count = %d, want %d", got, defaultReaperRebroadcastBatch)
	}
}

// TestReapOnce_RebroadcastBatchConfigurable pins the configurable per-tick
// rebroadcast cap (the 2026-08-10 incident fix: ~178k txs stuck at RECEIVED
// could only drain at the hardcoded 200 per tick). With
// propagation.reaper_rebroadcast_batch set to 5 and 8 eligible stuck rows
// seeded, one reapOnce rebroadcasts exactly 5.
func TestReapOnce_RebroadcastBatchConfigurable(t *testing.T) {
	stale := time.Now().Add(-2 * time.Hour)

	log := &eventLog{}
	ms := newMockStore()
	for i := 0; i < 8; i++ {
		ms.replayRows = append(ms.replayRows, &models.TransactionStatus{
			TxID:      fmt.Sprintf("recv-%02d", i),
			Status:    models.StatusReceived,
			RawTx:     []byte{0x01},
			Timestamp: stale,
		})
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	cfg := &config.Config{CallbackURL: "http://localhost:8080/callback"}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.ReaperRebroadcastBatch = 5
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, mc)

	p.reapOnce(context.Background())

	if got := log.count("register:"); got != 5 {
		t.Errorf("rebroadcast count = %d, want 5 (configured reaper_rebroadcast_batch)", got)
	}
}

// TestNew_ReaperRebroadcastBatchDefault: leaving
// propagation.reaper_rebroadcast_batch unset (zero) resolves to the historical
// hardcoded cap, so behavior at default config is byte-identical to before the
// knob existed.
func TestNew_ReaperRebroadcastBatchDefault(t *testing.T) {
	p := newPropagator("", "http://localhost:0", newMockStore())
	if p.rebroadcastBatch != defaultReaperRebroadcastBatch {
		t.Errorf("rebroadcastBatch = %d, want %d (code default when config is unset)",
			p.rebroadcastBatch, defaultReaperRebroadcastBatch)
	}
}

// TestReapOnce_LogsBacklogDepth pins the per-tick backlog snapshot line (the
// 2026-08-10 load-test gap: a ~466k-tx uncommitted-offset backlog was
// invisible in logs because the pending gauge is capped at max_pending and
// the in-flight set was ungauged). reapOnce must emit exactly one Info line
// per tick carrying the dispatcher's pending depth, its uncapped in-flight
// depth, and this scan's reaper-ready depth.
func TestReapOnce_LogsBacklogDepth(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)

	httpLog := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "reap-ready", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: stale},
	}

	merkleSrv := newMerkleServer(httpLog, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(httpLog, http.StatusOK)
	defer teranodeSrv.Close()

	core, recorded := observer.New(zapcore.InfoLevel)
	cfg := &config.Config{CallbackURL: "http://localhost:8080/callback"}
	cfg.Propagation.MerkleConcurrency = 10
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.New(core), nil, nil, ms, nil, tc, mc)

	// The depth gauges are delta-accounted pod totals (#295): every live
	// dispatcher adds its own contribution, and test-mode dispatchers
	// from other tests in this process may still hold theirs. Assert the
	// gauge's CHANGE from this propagator's two admissions, not an
	// absolute value.
	gaugeBefore := testutil.ToFloat64(metrics.PropagationInflightDepth)

	// Seed dispatcher state: two admitted txs, the first drained into a
	// "mid-broadcast" batch — in-flight counts both, pending only the second.
	if res := p.admitToDispatcher(propagationMsg{TXID: "inflight-a", RawTx: []byte{0x0a}}, 1); !res.admitted {
		t.Fatalf("admit inflight-a: %+v", res)
	}
	_ = p.drainPending()
	if res := p.admitToDispatcher(propagationMsg{TXID: "inflight-b", RawTx: []byte{0x0b}}, 2); !res.admitted {
		t.Fatalf("admit inflight-b: %+v", res)
	}

	// The dispatcher publishes the depth mirrors at the top of its next
	// loop iteration — poll briefly rather than assuming scheduling order.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.inflightDepth.Load() == 2 && p.pendingDepth.Load() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := p.inflightDepth.Load(); got != 2 {
		t.Fatalf("inflightDepth mirror = %d, want 2 (admitted a + b, neither terminal)", got)
	}
	if got := testutil.ToFloat64(metrics.PropagationInflightDepth); got != gaugeBefore+2 {
		t.Fatalf("PropagationInflightDepth gauge = %v, want %v (before + this dispatcher's 2)", got, gaugeBefore+2)
	}

	p.reapOnce(context.Background())

	entries := recorded.FilterMessage("reaper: propagation backlog depth").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 backlog-depth line per tick, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["pending_depth"].(int64); got != 1 {
		t.Errorf("pending_depth = %d, want 1", got)
	}
	if got, _ := fields["inflight_depth"].(int64); got != 2 {
		t.Errorf("inflight_depth = %d, want 2", got)
	}
	if got, _ := fields["reaper_ready_depth"].(int64); got != 1 {
		t.Errorf("reaper_ready_depth = %d, want 1 (the stale RECEIVED row)", got)
	}
}
