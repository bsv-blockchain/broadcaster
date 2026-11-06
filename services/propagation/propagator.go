package propagation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	arcerrors "github.com/bsv-blockchain/broadcaster/errors"
	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/teranode"
)

// propagationTracerName identifies this package's spans/tracer to the OTEL
// SDK, per the same package-import-path convention as kafka.tracerName.
const propagationTracerName = "github.com/bsv-blockchain/broadcaster/services/propagation"

// batchSpanMaxLinks caps the number of producer-span links attached to a
// propagation.broadcast span. Per OTel's recommended batch-messaging
// pattern, individual producer spans are linked (not parented) onto the one
// batch span rather than opening a span per message — this bounds per-span
// overhead even when a flush batch is very large.
const batchSpanMaxLinks = 128

type propagationMsg struct {
	TXID string `json:"txid"`
	// RawTx is the serialized transaction. Intake no longer publishes it
	// (#329): the Kafka message carries the txid only and hydrate loads the
	// bytes from the store row. The json tag stays so a message from a
	// pre-#329 producer, which carries raw_tx inline (base64), still decodes
	// and skips the store read.
	RawTx []byte `json:"raw_tx,omitempty"`
	// InputTXIDs lists the txids this tx spends from, for the dispatcher's
	// dep-aware admission. hydrate derives it from RawTx; pre-#329 messages
	// carry it inline. Empty means coinbase or no parents.
	InputTXIDs []string `json:"input_txids,omitempty"`
	// spanCtx is the trace context extracted from the Kafka producer's
	// message headers at decode/admit time (see runDispatcher and
	// handleMessage). It is NOT serialized — trace.SpanContext has no
	// exported json-visible fields and this field is unexported regardless,
	// so encoding/json silently skips it on both Marshal and Unmarshal.
	// Zero value (invalid) when the producer never injected a trace context
	// (telemetry disabled, or no active span at produce time). Consumed by
	// processBatch to link the batch's "propagation.broadcast" span back to
	// each tx's originating producer span — see startBroadcastSpan.
	spanCtx trace.SpanContext
	// retryCount is how many times this tx has already been pushed back
	// through the NETWORK propagation requeue on THIS claim. Merkle /watch
	// failures do not increment it — registration and broadcast are
	// different failure domains, and a tx must not exhaust
	// propagation.retry_max_attempts before it has been broadcast.
	// Like spanCtx it is process-local and deliberately not serialized: a
	// tx re-read from Kafka after a restart or rebalance is a fresh attempt
	// against (probably) a different downstream state, so its budget resets.
	// The network requeue increments it and parks the tx at PENDING_RETRY
	// once it exceeds Propagator.retryMaxAttempts — without that ceiling a
	// batch that can never succeed loops forever and its in-flight entries
	// freeze the Kafka commit watermark (the 2026-08-11 dev-ovh-1 wedge:
	// 337,247 messages of lag stationary behind 4,679 in-flight txs).
	retryCount int
	// merkleRetryCount is how many times this tx has been requeued because
	// Merkle /watch failed, on THIS claim. Independent of retryCount.
	// F-024 still holds: the tx is not broadcast until registration
	// succeeds. Exhausting this counter parks at PENDING_RETRY without
	// debiting the network budget. Process-local; not serialized.
	merkleRetryCount int
	// merkleFailureReason is the last /watch error that sent this tx
	// through the merkle requeue, clipped for the park reason. Cleared
	// when registration succeeds so a later network park does not quote
	// it. Process-local; not serialized.
	merkleFailureReason string
	// retryReason, when non-empty, names the condition that sent this tx
	// through the network requeue loop (currently only the missing-parent
	// line from Teranode) so parkExhaustedRequeues can write a specific
	// PENDING_RETRY reason instead of the generic no-verdict text.
	// Process-local like retryCount; never serialized.
	retryReason string
}

type Propagator struct {
	services.ReadyHook

	cfg            *config.Config
	logger         *zap.Logger
	producer       *kafka.Producer
	publisher      events.Publisher // nil-safe; broadcasts post-broadcast status updates to SSE/webhooks
	store          store.Store
	leaser         store.Leaser
	teranodeClient *teranode.Client
	merkleClient   merkleservice.Service // nil when no merkle_service endpoint is configured
	// consumer is written by Start (after kafka.NewConsumerGroup returns) and
	// read by Stop. atomic.Pointer makes that handoff race-free without
	// needing a mutex: in tests the harness can call Stop concurrently with
	// an in-flight Start that's still blocked initializing the consumer.
	consumer atomic.Pointer[kafka.ConsumerGroup]

	maxPending int
	// defaultIO is the dispatcherIO of the New()-spawned test-mode
	// dispatcher; its channels feed that loop's dep-aware state machine
	// exactly as each per-claim io feeds a production loop. Production
	// claims each build their own io in handleClaim (#295: one dispatcher
	// per partition claim; a shared channel set would mis-route events
	// across partitions). See dispatcherIO in dispatcher.go.
	defaultIO *dispatcherIO
	// dispatchers is the live-dispatcher registry behind the reaper's
	// nil-io terminal fan-out. Guarded by dispatchersMu; every other
	// piece of dispatcher state stays goroutine-local.
	dispatchersMu     sync.RWMutex
	dispatchers       map[*dispatcherIO]struct{}
	dispatcherCancel  context.CancelFunc
	dispatcherDone    chan struct{}
	merkleConcurrency int
	reaperInterval    time.Duration
	reaperBatchSize   int
	// rebroadcastBatch caps stuck-tx rebroadcasts per reaper tick
	// (propagation.reaper_rebroadcast_batch, default
	// defaultReaperRebroadcastBatch). See reapOnce.
	rebroadcastBatch int
	// teranodeBatchCap and teranodeBatchBytesCap bound each POST /txs chunk by
	// transaction count and by payload bytes respectively (issue #271); see
	// planChunks. Both default from config.DefaultTeranodeMaxBatch*.
	teranodeBatchCap      int
	teranodeBatchBytesCap int
	broadcastWorkers      int
	maxParallelChunks     int
	holderID              string
	leaseTTL              time.Duration
	// retryMaxAttempts is the per-claim in-memory NETWORK requeue budget
	// (propagation.retry_max_attempts, default defaultRetryMaxAttempts).
	// Merkle /watch failures do not debit it. See requeueAfterDelay /
	// parkExhaustedRequeues for what happens when it runs out, and
	// config.PropagationConfig.RetryMaxAttempts for the incident that
	// motivated bounding it at all.
	retryMaxAttempts int
	// merkleRetryMaxAttempts bounds fast-path Merkle /watch requeues.
	// It is not propagation.retry_max_attempts. See
	// defaultMerkleRetryMaxAttempts.
	merkleRetryMaxAttempts int
	// requeueDelay is the flat wait before a requeue lands back on the
	// dispatcher. Initialized to defaultRequeueDelay; carried as a field
	// rather than referenced as a constant so tests can compress the
	// retry loop to milliseconds instead of spending
	// retryMaxAttempts × 2s of wall time per case.
	requeueDelay time.Duration
	// pendingRetryBackoff / pendingRetryMaxBackoff / pendingRetryMaxAttempts
	// govern the DURABLE retry schedule a parked tx follows once the
	// in-memory budget above is spent and the reaper owns it. See
	// schedulePendingRetry.
	pendingRetryBackoff     time.Duration
	pendingRetryMaxBackoff  time.Duration
	pendingRetryMaxAttempts int

	// broadcastJobs feeds the persistent worker pool that runs every
	// per-endpoint POST /txs call. Replaces the previous per-broadcast
	// `go func(ep)` spawn loop so sustained 50+ TPS doesn't produce
	// constant goroutine churn — total broadcast goroutine count stays
	// bounded at broadcastWorkers regardless of flush rate. Workers
	// exit when broadcastJobs is closed in Stop().
	broadcastJobs    chan broadcastJob
	broadcastWG      sync.WaitGroup
	broadcastRunning atomic.Bool // true while Start() workers are running

	// processBatchSem caps how many flushed batches run their register+
	// broadcast pipeline concurrently. With cap=1 (the historical default
	// before pipelining), batch N+1 cannot start its merkle /watch until
	// batch N's broadcast completes — at sustained 100 TPS that costs
	// ~half-a-pipeline-cycle of queue wait per tx. Cap>1 lets register and
	// broadcast overlap across adjacent batches. flushBatch acquires a
	// slot before spawning the processBatch goroutine, providing natural
	// backpressure to the kafka consumer.
	processBatchSem chan struct{}
	// inflightBatches counts processBatch goroutines that are still
	// running. Stop() blocks on this before tearing down the broadcast
	// worker pool so an in-flight batch doesn't lose its broadcast
	// results to a closed jobs channel.
	inflightBatches sync.WaitGroup
	// pendingDepth / inflightDepth mirror the PropagationPendingDepth /
	// PropagationInflightDepth gauges as plain atomics so non-dispatcher
	// goroutines (the reaper's per-tick backlog log line) can read the
	// current depths without a Prometheus read API or a round-trip onto the
	// dispatcher's channels. Written only by the dispatcher loop.
	pendingDepth  atomic.Int64
	inflightDepth atomic.Int64

	// backgroundWG tracks the long-running Start-spawned goroutines —
	// runReaper, runMerkleReplay, and any resync replay spawned by the
	// merkle pool's overflow hook — so Stop can wait for them to exit
	// before the surrounding app cleanup closes the store backing them.
	// Without this, a reaper mid-lease-release or mid-status-scan
	// races with store.Close and the test framework attributes the
	// resulting goroutine panic to the test's Cleanup callback.
	backgroundWG sync.WaitGroup

	// resync coalesces per-endpoint resync replays (see scheduleResync).
	// resyncStopped is set by Stop before it waits on backgroundWG so a late
	// hook cannot Add to it.
	resyncMu      sync.Mutex
	resync        map[string]*resyncState
	resyncStopped bool

	// initDone is closed once Start has finished its init phase (all
	// wg.Add()s done, all goroutines spawned) OR Start has returned with
	// an error before reaching that point. Stop blocks on it before
	// touching the WaitGroups so it never races Start's Add()s — that
	// race used to panic with "sync: WaitGroup is reused before previous
	// Wait has returned" when Stop's broadcastWG.Wait() interleaved with
	// the per-worker broadcastWG.Add(1) inside Start.
	//
	// initOnce gates the close so Start's deferred close becomes a no-op
	// when init completes successfully.
	//
	// startCalled lets Stop skip the wait entirely when Start was never
	// invoked (services.Service contract permits Stop-without-Start, and
	// existing tests rely on it).
	initDone    chan struct{}
	initOnce    sync.Once
	startCalled atomic.Bool
}

// broadcastJob is the unit of work the persistent broadcast pool consumes.
// One job represents one POST /txs call to one endpoint; the caller bundles
// a per-call result channel so it can collect outcomes from multiple
// endpoints in parallel without each worker carrying that bookkeeping.
//
// The ctx field is intentionally part of the value — the job travels through
// a channel so the cancellation token has to ride with it. The standard
// "context as first arg" pattern doesn't apply to message-passing handoffs;
// containedctx is suppressed deliberately at the type declaration.
type broadcastJob struct {
	ctx      context.Context //nolint:containedctx // travels with the work item through broadcastJobs channel
	endpoint string
	rawTxs   [][]byte
	resultCh chan<- broadcastJobResult
}

type broadcastJobResult struct {
	endpoint   string
	statusCode int
	// failures is the per-tx failure information extracted from a non-200 /txs
	// "Failed to process transactions:" body (Teranode upstream main, post
	// #879; the HTTP status varies by failure class — 409/422/400/500 all
	// carry the same shape). ByKey entries are keyed by the txid embedded in
	// "[ProcessTransaction][<txid>]" when present, else the first bare
	// 64-hex string in the line (which for conflict-family verdicts is an
	// alien hash — see alienFailureLines); the value is the full error line
	// verbatim (e.g. "TX_INVALID (31): [ProcessTransaction][<txid>] tx is
	// invalid because..."). Unkeyed holds lines naming no hash at all.
	// Absent here means accepted — but only when every line keyed to a
	// submitted tx. nil for 200, transport errors, and any non-200 that
	// doesn't match the Teranode failure-list shape — those cases drive the
	// whole-batch requeue path.
	failures *teranode.TxsFailures
	err      error
}

// txResultClass categorizes a per-tx broadcast outcome into the action
// the caller should take. The dep-aware pipeline collapses Teranode's
// rich error vocabulary into three buckets.
type txResultClass int

const (
	// txResultClassUnknown is the zero value — a result that hasn't been
	// classified yet. Should never reach the caller; if it does,
	// processBatch's default branch treats it the same as Requeue.
	txResultClassUnknown txResultClass = iota
	// txResultClassAccepted: terminalize as ACCEPTED_BY_NETWORK, dispatcher
	// releases waiters.
	txResultClassAccepted
	// txResultClassRejected: terminalize as REJECTED, dispatcher cascade-
	// rejects descendants. errMsg carries the Teranode code (e.g.
	// "TX_INVALID (31)") so it shows up in the wallet-visible row.
	txResultClassRejected
	// txResultClassRequeue: broadcast didn't produce a verdict for this
	// tx (no peer reachable, /txs returned 4xx/5xx with no per-slot body,
	// per-slot PROCESSING / infra-bucket code). processBatch routes the
	// tx through requeueAfterDelay → dispatcher.requeueCh after a flat
	// wait, which re-runs admission so the tx gets another flush. The
	// row stays at RECEIVED in the DB; the dispatcher inFlight entry and
	// pinned Kafka offset both persist across the requeue.
	txResultClassRequeue
	// txResultClassMissingParent: Teranode explicitly answered
	// TX_MISSING_PARENT / TX_NOT_FOUND — the tx's parent isn't in the
	// node's store YET. With multi-partition propagation (#295) this is
	// the expected signature of a child broadcast before its
	// cross-submission parent, so per the #254 principle it is a
	// CONDITION, not a verdict: never terminalize REJECTED on it alone.
	// processBatch consults BSV Broadcaster's own store first — a parent
	// terminally REJECTED cascades the child REJECTED (that's a real
	// verdict); anything else requeues, and budget exhaustion parks at
	// PENDING_RETRY for the reaper's durable retry. errMsg carries the
	// Teranode line for the park reason.
	txResultClassMissingParent
)

// broadcastJobBuffer sizes the job channel between broadcast helpers and the
// worker pool. Generous enough that flush-time fan-out doesn't block in
// steady state; bounded so a stalled pool can't grow unboundedly.
const broadcastJobBuffer = 1024

// defaultBroadcastWorkers is the fallback when cfg.Propagation.BroadcastWorkers
// is non-positive. Sized to cover the peak concurrent-job estimate at the
// other shipped defaults (8 concurrent batches × 4 parallel chunks ×
// ~8 healthy datahub endpoints = 256).
const defaultBroadcastWorkers = 256

// defaultMaxParallelChunks caps the per-batch chunk fan-out when
// cfg.Propagation.MaxParallelChunks is non-positive. Each chunk already fans
// out to every healthy endpoint, so the effective concurrency is
// defaultMaxParallelChunks × len(endpoints).
const defaultMaxParallelChunks = 4

// defaultRetryMaxAttempts is the in-memory requeue budget applied when
// propagation.retry_max_attempts is unset or non-positive. Kept equal to the
// shipped viper default (config.setDefaults) so the code and the config file
// can never disagree about what "unset" means.
//
// Five attempts × defaultRequeueDelay is ~10s of fast-path retry, which
// rides out a peer restart while guaranteeing a permanently unresolvable
// batch reaches its terminal escape in seconds rather than never.
// Merkle /watch failures are not part of this budget; see
// defaultMerkleRetryMaxAttempts.
const defaultRetryMaxAttempts = 5

// defaultMerkleRetryMaxAttempts bounds fast-path Merkle /watch requeues.
// It is intentionally not propagation.retry_max_attempts.
//
// Sharing the network counter parked transactions after five registration
// failures, before any Teranode broadcast. This ceiling stays strictly
// above defaultRetryMaxAttempts so a /watch burst that used to exhaust the
// network budget still gets a fast-path broadcast. It is still bounded:
// past this many failures the tx parks at PENDING_RETRY and the reaper
// owns it, so a prolonged Merkle outage cannot pin Kafka offsets or spin
// a hot loop. Sixteen attempts at defaultRequeueDelay is about 32s.
//
// There is no config knob. Wiring this to retry_max_attempts would give
// that setting two meanings. The wait between attempts is the existing
// propagation.retry_backoff_ms (requeueDelay).
const defaultMerkleRetryMaxAttempts = 16

// teranodeHardMaxBatchTxs and teranodeHardMaxBatchBytes mirror Teranode's
// propagation server (services/propagation/Server.go:
// maxTransactionsPerRequest and maxDataPerRequest). Since Teranode v0.15.0
// both are checked there with ">=" BEFORE each read, so a /txs body that
// merely reaches either value is refused with a bare 400 after the peer has
// already dispatched everything it read. The shipped defaults
// (config.DefaultTeranodeMaxBatchSize / DefaultTeranodeMaxBatchBytes) sit
// under them; New warns when a configured cap reaches them.
const (
	teranodeHardMaxBatchTxs   = 1024
	teranodeHardMaxBatchBytes = 32 << 20
)

// New constructs a Propagator. leaser may be nil, in which case the reaper
// runs unguarded — appropriate for tests and single-process deployments that
// don't need coordination. In production every replica should receive a
// non-nil Leaser so only one reaper is active at a time across the cluster.
func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, publisher events.Publisher, st store.Store, leaser store.Leaser, tc *teranode.Client, mc merkleservice.Service) *Propagator {
	// Export every series this service can emit at 0 from the first scrape —
	// a series born mid-burst is invisible to increase() until its second
	// sample, which zeroed the REJECTED count after every rollout.
	metrics.PreRegisterStatusTransitions(models.StatusAcceptedByNetwork, models.StatusRejected)
	merkleConcurrency := cfg.Propagation.MerkleConcurrency
	if merkleConcurrency <= 0 {
		merkleConcurrency = 10
	}
	// Reaper config drives the periodic SEEN_ON_NETWORK rebroadcast scan
	// and the lease that coordinates it across replicas.
	reaperInterval := time.Duration(cfg.Propagation.ReaperIntervalMs) * time.Millisecond
	if reaperInterval <= 0 {
		reaperInterval = 30 * time.Second
	}
	reaperBatch := cfg.Propagation.ReaperBatchSize
	if reaperBatch <= 0 {
		reaperBatch = 500
	}
	rebroadcastBatch := cfg.Propagation.ReaperRebroadcastBatch
	if rebroadcastBatch <= 0 {
		rebroadcastBatch = defaultReaperRebroadcastBatch
	}
	leaseTTL := time.Duration(cfg.Propagation.LeaseTTLMs) * time.Millisecond
	if leaseTTL <= 0 {
		leaseTTL = 3 * reaperInterval
	}
	teranodeBatchCap := cfg.Propagation.TeranodeMaxBatchSize
	if teranodeBatchCap <= 0 {
		teranodeBatchCap = config.DefaultTeranodeMaxBatchSize
	}
	teranodeBatchBytesCap := cfg.Propagation.TeranodeMaxBatchBytes
	if teranodeBatchBytesCap <= 0 {
		teranodeBatchBytesCap = config.DefaultTeranodeMaxBatchBytes
	}
	// Teranode refuses a chunk that merely REACHES its per-request limits
	// (see teranodeHardMaxBatch*). Warn rather than clamp: the operator asked
	// for the value, and the size-rejection narrowing path still copes — at
	// the cost of extra round trips on every full chunk.
	if teranodeBatchCap >= teranodeHardMaxBatchTxs {
		logger.Warn(
			"teranode_max_batch_size reaches teranode's per-request limit; a full chunk will be refused",
			zap.Int("teranode_max_batch_size", teranodeBatchCap),
			zap.Int("teranode_limit", teranodeHardMaxBatchTxs),
		)
	}
	if teranodeBatchBytesCap >= teranodeHardMaxBatchBytes {
		logger.Warn(
			"teranode_max_batch_bytes reaches teranode's per-request limit; a full chunk will be refused",
			zap.Int("teranode_max_batch_bytes", teranodeBatchBytesCap),
			zap.Int("teranode_limit", teranodeHardMaxBatchBytes),
		)
	}
	maxPending := cfg.Propagation.MaxPending
	if maxPending <= 0 {
		maxPending = 50000
	}
	maxConcurrentBatches := cfg.Propagation.MaxConcurrentBatches
	if maxConcurrentBatches <= 0 {
		maxConcurrentBatches = 4
	}
	broadcastWorkers := cfg.Propagation.BroadcastWorkers
	if broadcastWorkers <= 0 {
		broadcastWorkers = defaultBroadcastWorkers
	}
	maxParallelChunks := cfg.Propagation.MaxParallelChunks
	if maxParallelChunks <= 0 {
		maxParallelChunks = defaultMaxParallelChunks
	}
	// A non-positive retry_max_attempts falls back to the default rather than
	// meaning "unlimited": an unbounded requeue loop is precisely the failure
	// this budget exists to prevent, so there is no way to configure it away.
	retryMaxAttempts := cfg.Propagation.RetryMaxAttempts
	if retryMaxAttempts <= 0 {
		retryMaxAttempts = defaultRetryMaxAttempts
	}
	// retry_backoff_ms drives the flat requeue delay. It existed as a
	// declared-but-unread field long before #295 wired it up, so the viper
	// default was bumped 500 → 2000 in the same change to keep the shipped
	// cadence identical to the old hardcoded defaultRequeueDelay.
	requeueDelay := defaultRequeueDelay
	if cfg.Propagation.RetryBackoffMs > 0 {
		requeueDelay = time.Duration(cfg.Propagation.RetryBackoffMs) * time.Millisecond
	}
	pendingRetryBackoff := defaultPendingRetryBackoff
	if cfg.Propagation.PendingRetryBackoffMs > 0 {
		pendingRetryBackoff = time.Duration(cfg.Propagation.PendingRetryBackoffMs) * time.Millisecond
	}
	pendingRetryMaxBackoff := defaultPendingRetryMaxBackoff
	if cfg.Propagation.PendingRetryMaxBackoffMs > 0 {
		pendingRetryMaxBackoff = time.Duration(cfg.Propagation.PendingRetryMaxBackoffMs) * time.Millisecond
	}
	if pendingRetryMaxBackoff < pendingRetryBackoff {
		pendingRetryMaxBackoff = pendingRetryBackoff
	}
	pendingRetryMaxAttempts := defaultPendingRetryMaxAttempts
	if cfg.Propagation.PendingRetryMaxAttempts > 0 {
		pendingRetryMaxAttempts = cfg.Propagation.PendingRetryMaxAttempts
	}
	p := &Propagator{
		cfg:                    cfg,
		logger:                 logger.Named("propagation"),
		producer:               producer,
		publisher:              publisher,
		store:                  st,
		leaser:                 leaser,
		teranodeClient:         tc,
		merkleClient:           mc,
		maxPending:             maxPending,
		merkleConcurrency:      merkleConcurrency,
		reaperInterval:         reaperInterval,
		reaperBatchSize:        reaperBatch,
		rebroadcastBatch:       rebroadcastBatch,
		teranodeBatchCap:       teranodeBatchCap,
		teranodeBatchBytesCap:  teranodeBatchBytesCap,
		broadcastWorkers:       broadcastWorkers,
		maxParallelChunks:      maxParallelChunks,
		holderID:               newHolderID(),
		leaseTTL:               leaseTTL,
		retryMaxAttempts:       retryMaxAttempts,
		merkleRetryMaxAttempts: defaultMerkleRetryMaxAttempts,
		requeueDelay:           requeueDelay,

		pendingRetryBackoff:     pendingRetryBackoff,
		pendingRetryMaxBackoff:  pendingRetryMaxBackoff,
		pendingRetryMaxAttempts: pendingRetryMaxAttempts,

		broadcastJobs:   make(chan broadcastJob, broadcastJobBuffer),
		processBatchSem: make(chan struct{}, maxConcurrentBatches),
		defaultIO:       newDispatcherIO(),
		dispatchers:     make(map[*dispatcherIO]struct{}),
		initDone:        make(chan struct{}),
	}
	metrics.PreRegisterPropagationRetryOutcomes()
	// Start a dispatcher goroutine with a nil claim so tests that
	// construct via New and drive via admitCh / drainCh have a running
	// state machine without needing to invoke Start. In production
	// Start replaces this with the same loop running inside the kafka
	// ClaimHandler — see Start. The two paths can't both be live at
	// once: Start cancels this context AND waits on dispatcherDone
	// before subscribing, guaranteeing the test-mode goroutine has
	// fully exited before any production dispatcher loop runs.
	dispatcherCtx, dispatcherCancel := context.WithCancel(context.Background())
	p.dispatcherCancel = dispatcherCancel
	p.dispatcherDone = make(chan struct{})
	go func() {
		defer close(p.dispatcherDone)
		if err := p.runDispatcher(dispatcherCtx, nil, dispatcherConfig{maxPending: maxPending}, p.defaultIO); err != nil {
			p.logger.Error("test-mode dispatcher exited with error", zap.Error(err))
		}
	}()
	return p
}

// runBroadcastWorker pulls jobs off broadcastJobs and runs POST /txs against
// the named endpoint. Exits when broadcastJobs is closed (Stop()). The job's
// context governs cancellation — a winning sibling cancels the per-call
// broadcastCtx and a 15s deadline bounds worst-case wall time.
func (p *Propagator) runBroadcastWorker() {
	defer p.broadcastWG.Done()
	for job := range p.broadcastJobs {
		statusCode, failures, err := p.teranodeClient.SubmitTransactions(job.ctx, job.endpoint, job.rawTxs)
		// Non-blocking send — the caller always allocates resultCh with
		// capacity ≥ number of jobs it submits, so this never blocks. Using
		// non-blocking lets a Stop() racing with in-flight broadcasts not
		// deadlock the worker on an abandoned channel.
		select {
		case job.resultCh <- broadcastJobResult{endpoint: job.endpoint, statusCode: statusCode, failures: failures, err: err}:
		default:
		}
	}
}

// newHolderID returns a lease-holder identifier stable for this process's
// lifetime: "<hostname>-<8-hex-chars>". The random suffix disambiguates
// restarts — if an old expired-but-not-yet-purged record still names the
// previous incarnation by hostname alone, the new process will see it as a
// foreign holder and wait for TTL rather than believe it already owns the
// lease.
func newHolderID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return host + "-" + hex.EncodeToString(buf[:])
}

func (p *Propagator) Name() string { return "propagation" }

// applyTerminalStatuses persists the per-tx terminal statuses produced by
// processBatch in one BatchUpdateStatusReturning call, observes the
// RECEIVED→{ACCEPTED_BY_NETWORK,REJECTED,PENDING_RETRY} transition age, emits
// one PublishBulk per status, and notifies the dispatcher so it can
// release/cascade waiters and mark the Kafka offset Done.
//
// "Terminal" here means terminal FOR THE DISPATCHER — the point at which a tx
// stops pinning its Kafka offset — not terminal for the client. Three statuses
// qualify: ACCEPTED_BY_NETWORK, REJECTED, and PENDING_RETRY (the requeue-budget
// escape, see parkExhaustedRequeues). Routing the park through this one
// function is deliberate: it is the only place that both persists a status and
// tells the dispatcher to release the in-flight entry, and a park that failed
// to release would leave the consumer exactly as wedged as the unbounded
// requeue loop it replaced.
//
// It walks the batch with two deliberately separate accounting passes:
//
//   - Dispatcher notification covers EVERY terminalized tx. Each tx in
//     terminalStatuses still holds a live Kafka offset on the dispatcher's
//     offsetTracker; the dispatcher only marks that offset Done when it
//     receives the terminal event. This pass MUST NOT be filtered by
//     whether the store write moved the row — a tx re-read from Kafka
//     after a rebalance is typically already at its terminal status, so
//     BatchUpdateStatusReturning reports either no applied write (Prev ==
//     nil: a lattice skip or a reaped row) or an idempotent re-assert
//     (Prev.Status == st.Status). Skipping the notify for those strands
//     the offset on the tracker and pins LowestUnfinished() — the Kafka
//     commit watermark — forever, so the consumer group never commits and
//     every restart re-reads the whole topic from offset 0.
//
//   - Bulk publish covers only rows that actually transitioned. A lattice
//     no-op or a reaped row would be a phantom SSE/webhook event.
//
// Split out of processBatch so the surrounding flush loop stays under
// nesting-complexity limits.
// io names the dispatcher whose pipeline produced these terminals; nil
// (reaper path) fans the notifications out to every live dispatcher.
func (p *Propagator) applyTerminalStatuses(ctx context.Context, terminalStatuses []*models.TransactionStatus, accepted, rejected int, io *dispatcherIO) {
	if len(terminalStatuses) == 0 {
		return
	}
	prevs, err := p.store.BatchUpdateStatusReturning(ctx, terminalStatuses)
	if err != nil {
		p.logger.Error(
			"batch update propagation status failed",
			zap.Int("batch_size", len(terminalStatuses)),
			zap.Error(err),
		)
		// Continue: per-row entries may still be valid; bulk-publish
		// those whose prev row is populated below. The dispatcher
		// notification below runs regardless — the offset still has to
		// be released even when the store write failed.
	}

	// terminalAccepted/terminalRejected/terminalParked drive the dispatcher
	// notification: every terminalized tx, unconditionally.
	terminalAccepted := make([]string, 0, accepted)
	terminalRejected := make([]string, 0, rejected)
	var terminalParked []string
	// published* drive the bulk publish: only rows whose store status
	// actually transitioned. publishedRejected carries whole rows, not just
	// txids, because a rejection's payload (the reason BSV Broadcaster just persisted)
	// has to ride the event out — see publishRejections.
	publishedAccepted := make([]string, 0, accepted)
	publishedRejected := make([]*models.TransactionStatus, 0, rejected)
	var publishedParked []string
	now := time.Now()
	for i, st := range terminalStatuses {
		var prev *models.TransactionStatus
		if i < len(prevs) {
			prev = prevs[i].Prev
		}

		// Dispatcher accounting — unconditional. Callers only route
		// ACCEPTED_BY_NETWORK, REJECTED and PENDING_RETRY into
		// terminalStatuses; the defensive default keeps the switch exhaustive.
		switch st.Status {
		case models.StatusAcceptedByNetwork:
			terminalAccepted = append(terminalAccepted, st.TxID)
		case models.StatusRejected:
			terminalRejected = append(terminalRejected, st.TxID)
		case models.StatusPendingRetry:
			terminalParked = append(terminalParked, st.TxID)
		default:
		}

		// Publish accounting — only real transitions. An unknown txid
		// (row reaped between RECEIVED and broadcast, or a per-row store
		// error) or a lattice no-op is skipped here to avoid a phantom
		// event; the dispatcher notify above still fires for it.
		if prev == nil {
			continue
		}
		if !prev.Timestamp.IsZero() {
			metrics.StatusTransitionAge.
				WithLabelValues(string(prev.Status), string(st.Status)).
				Observe(time.Since(prev.Timestamp).Seconds())
		}
		if prev.Status == st.Status {
			continue
		}
		switch st.Status {
		case models.StatusAcceptedByNetwork:
			publishedAccepted = append(publishedAccepted, st.TxID)
		case models.StatusRejected:
			publishedRejected = append(publishedRejected, st)
			p.logger.Info(
				"transaction rejected",
				logfields.TxID(st.TxID),
				zap.String("reason", st.ExtraInfo),
				logfields.Stage(logfields.StageNetwork),
			)
		case models.StatusPendingRetry:
			publishedParked = append(publishedParked, st.TxID)
		default:
		}
	}

	// Full-coverage ACCEPTED_BY_NETWORK line(s): this runs on the async
	// processBatch goroutine (off the client's request path), so it's the
	// right place to emit every accepted txid — chunked, never capped — so a
	// txid search always finds the RECEIVED→ACCEPTED transition. The plain
	// component logger is used deliberately: a flushed batch aggregates txs
	// from many unrelated producer traces, so there is no single trace_id to
	// attach via telemetry.LoggerWith.
	logfields.ForEachTxIDChunk(publishedAccepted, func(chunk []string, chunkIdx, totalChunks int) {
		p.logger.Info(
			"transactions accepted by network",
			logfields.Stage(logfields.StageNetwork),
			logfields.TxIDCount(len(publishedAccepted)),
			logfields.TxIDs(chunk),
			zap.Int("chunk_index", chunkIdx),
			zap.Int("chunk_total", totalChunks),
		)
	})

	p.publishBulkStatus(ctx, models.StatusAcceptedByNetwork, publishedAccepted, now)
	p.publishRejections(ctx, publishedRejected, now)
	p.publishBulkStatus(ctx, models.StatusPendingRetry, publishedParked, now)

	// Notify the dispatcher ONLY when the batch status write fully
	// succeeded. BatchUpdateStatusReturning surfaces per-row failures as
	// a non-nil err with no per-row detail — so on any error we cannot
	// tell which rows actually persisted. Notifying anyway would mark
	// those txs' Kafka offsets Done and let the commit watermark advance
	// past a tx whose terminal status never reached the store, breaking
	// the at-least-once coupling between Kafka and the status ledger.
	// Skipping the notify leaves the whole batch's offsets uncommitted;
	// the next claim replays it and re-terminalizes once the store
	// recovers (the reaper is the secondary backstop). A partial-batch
	// over-replay on a rare store error is the correct trade.
	if err != nil {
		return
	}

	// Notify the dispatcher of every terminal status flip. ACCEPTED
	// releases waiters via the dispatcher itself (no caller action
	// needed — released msgs are appended directly to the
	// dispatcher's pendingMsgs). REJECTED returns cascaded
	// descendants we write REJECTED rows for; the cascade reason names
	// the rejected ancestor but never its cause — see
	// persistCascade.
	//
	// PENDING_RETRY behaves like REJECTED structurally (offset released,
	// held descendants collected) but the descendants are parked, not
	// rejected: their ancestor produced NO verdict, so condemning them would
	// invent one. They ride the same durable reaper retry as the parked
	// ancestor.
	var allRejectCascaded, allParkCascaded []cascadeRejection
	for _, txid := range terminalAccepted {
		p.notifyTerminalToDispatcher(ctx, io, txid, models.StatusAcceptedByNetwork)
	}
	for _, txid := range terminalRejected {
		r := p.notifyTerminalToDispatcher(ctx, io, txid, models.StatusRejected)
		for _, child := range r.cascaded {
			allRejectCascaded = append(allRejectCascaded, cascadeRejection{txid: child.txid, rawTx: child.rawTx, ancestor: txid})
		}
	}
	for _, txid := range terminalParked {
		r := p.notifyTerminalToDispatcher(ctx, io, txid, models.StatusPendingRetry)
		for _, child := range r.cascaded {
			allParkCascaded = append(allParkCascaded, cascadeRejection{txid: child.txid, rawTx: child.rawTx, ancestor: txid})
		}
	}
	if len(allRejectCascaded) > 0 {
		p.persistCascade(ctx, allRejectCascaded, models.StatusRejected, now)
	}
	if len(allParkCascaded) > 0 {
		p.persistCascade(ctx, allParkCascaded, models.StatusPendingRetry, now)
	}
}

// cascadeRejection pairs a cascade-terminalized tx with the ancestor whose
// outcome condemned (REJECTED) or parked (PENDING_RETRY) it, so the persisted
// reason can name the row a client should look at (and resubmit first).
type cascadeRejection struct {
	txid     string
	rawTx    []byte
	ancestor string
}

// cascadeReason renders the ExtraInfo for a tx that inherited its ancestor's
// outcome without ever being broadcast itself.
//
// "parent rejected" is kept as the stable prefix existing consumers match on.
// Both variants state the recovery path explicitly, because neither is a
// verdict about this tx's bytes — it is a consequence of queue state, so once
// the ancestor clears, a resubmission re-runs the pipeline (intake re-validates
// and re-broadcasts REJECTED rows).
func cascadeReason(status models.Status, ancestor string) string {
	if status == models.StatusPendingRetry {
		return fmt.Sprintf(
			"parent parked for durable retry (ancestor %s): retryable — the ancestor got no network verdict; both are rebroadcast by the reaper",
			ancestor,
		)
	}
	return fmt.Sprintf(
		"parent rejected (ancestor %s): retryable — resubmit after the ancestor is accepted",
		ancestor,
	)
}

// parentTxIDsOf returns msg's parent txids: InputTXIDs when the intake
// envelope populated them, else parsed from the raw bytes (reaper
// rebroadcast rows carry no InputTXIDs). Unparseable bytes yield nil —
// the caller then just requeues/parks, which is always safe. Parsed as a
// stream, as intake parses it: the stored bytes are the request body, and an
// exact-length parse would read trailing bytes after the tx as no parents.
func parentTxIDsOf(msg propagationMsg) []string {
	if len(msg.InputTXIDs) > 0 {
		return msg.InputTXIDs
	}
	tx, _, err := sdkTx.NewTransactionFromStream(msg.RawTx)
	if err != nil || tx == nil {
		return nil
	}
	parents := make([]string, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in == nil || in.SourceTXID == nil {
			continue
		}
		parents = append(parents, in.SourceTXID.String())
	}
	return parents
}

// rejectedAncestor consults BSV Broadcaster's own status store for msg's parents
// and returns the first that is terminally REJECTED. This is the ONLY
// path from a Teranode missing-parent condition to a REJECTED child: the
// ancestor's rejection is a real verdict, so inheriting it mirrors the
// dispatcher's same-partition cascade for parents that live on another
// partition (or terminalized before this pod ever saw them).
//
// Deliberately conservative: store.ErrNotFound, read errors, and every
// non-REJECTED status (RECEIVED, PENDING_RETRY, DOUBLE_SPEND_ATTEMPTED,
// …) return false — anything else would invent a verdict. Runs on
// processBatch / reaper worker goroutines only, never the dispatcher.
//
// The walk is transitive, bounded by maxAncestorWalkDepth and a visited set.
// A single hop was not enough to match the behavior this is documented as
// mirroring: cascadeReject is a recursive BFS over the waiter graph, so a
// REJECTED grandparent condemns a grandchild immediately on the same
// partition. Consulting direct parents only meant the cross-partition case
// needed the parent to cascade first, costing one durable retry cycle per
// generation before a doomed chain could be condemned.
func (p *Propagator) rejectedAncestor(ctx context.Context, msg propagationMsg) (string, bool) {
	if p.store == nil {
		return "", false
	}
	visited := map[string]struct{}{msg.TXID: {}}
	frontier := parentTxIDsOf(msg)

	for depth := 0; depth < maxAncestorWalkDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, ancestor := range frontier {
			if ancestor == "" {
				continue
			}
			if _, seen := visited[ancestor]; seen {
				continue
			}
			visited[ancestor] = struct{}{}

			row, err := p.store.GetStatus(ctx, ancestor)
			if err != nil || row == nil {
				// Not BSV Broadcaster's tx, or a transient read failure. Either way we
				// know nothing about it, and inventing a verdict from silence
				// is exactly what this function exists to avoid.
				continue
			}
			if row.Status == models.StatusRejected {
				return ancestor, true
			}
			// Keep climbing only through ancestors that are themselves still
			// unresolved. A tx that is ACCEPTED/SEEN/MINED is settled — its
			// own parents cannot retroactively condemn this child.
			if row.Status != models.StatusPendingRetry && row.Status != models.StatusReceived {
				continue
			}
			if len(row.RawTx) == 0 {
				continue
			}
			next = append(next, parentTxIDsOf(propagationMsg{TXID: ancestor, RawTx: row.RawTx})...)
		}
		frontier = next
	}
	return "", false
}

// maxAncestorWalkDepth bounds the transitive rejectedAncestor climb. Each
// level costs one GetStatus per unresolved ancestor, on a broadcast worker
// goroutine, so this trades a deeper cascade against per-event store load.
// Chains this long are already parked and draining a layer at a time; the
// bound only decides how many generations one pass can condemn at once.
const maxAncestorWalkDepth = 8

// Labels for the requeue side of PropagationMissingParentTotal. The fast path
// really does requeue; the reaper books a durable retry instead. Reporting
// both as "requeued" meant a constant population of unresolvable orphans
// pinned that counter at a steady rate forever, which reads exactly like the
// sustained family-keying regression #295's dashboard note tells operators to
// look for.
const (
	missingParentOutcomeRequeued    = "requeued"
	missingParentOutcomeReaperRetry = "reaper_retry"
)

// resolveMissingParents settles every txResultClassMissingParent result:
// a REJECTED ancestor in the store cascades the child REJECTED (returned
// as extra terminal statuses); everything else requeues with the
// Teranode line as its retry reason. Shared by processBatch and the
// reaper (which passes collect=false semantics via its own loop).
//
// outcome reports which caller this is, because the two do different things
// with the returned requeue slice: processBatch actually requeues, while the
// reaper discards it and books a durable retry instead. Labelling both
// "requeued" made a steady population of unresolvable orphans pin that
// counter at a constant rate forever — poisoning the exact signal #295's
// dashboard guidance says to watch for family-keying regressions.
func (p *Propagator) resolveMissingParents(ctx context.Context, msgs []propagationMsg, lines []string, outcome string) (cascaded []*models.TransactionStatus, requeue []propagationMsg) {
	now := time.Now()
	for i, msg := range msgs {
		if ancestor, ok := p.rejectedAncestor(ctx, msg); ok {
			metrics.PropagationMissingParentTotal.WithLabelValues("rejected_ancestor").Inc()
			p.logger.Info(
				"missing-parent child cascaded to REJECTED via store consult",
				logfields.TxID(msg.TXID),
				zap.String("ancestor", ancestor),
			)
			cascaded = append(cascaded, &models.TransactionStatus{
				TxID:      msg.TXID,
				Status:    models.StatusRejected,
				Timestamp: now,
				ExtraInfo: cascadeReason(models.StatusRejected, ancestor),
			})
			continue
		}
		metrics.PropagationMissingParentTotal.WithLabelValues(outcome).Inc()
		msg.retryReason = lines[i]
		requeue = append(requeue, msg)
	}
	return cascaded, requeue
}

// persistCascade writes rows for txs the dep cascade terminalized without ever
// broadcasting them — REJECTED when their ancestor was rejected, PENDING_RETRY
// when their ancestor was parked by the requeue-budget escape — then emits one
// bulk publish so SSE/webhook subscribers learn about the outcome.
// Best-effort: a store write failure is logged but doesn't undo the
// in-memory cascade state (the dispatcher has already terminalized
// them; we'd be reconciling at restart via Kafka replay anyway).
func (p *Propagator) persistCascade(ctx context.Context, cascaded []cascadeRejection, status models.Status, now time.Time) {
	statuses := make([]*models.TransactionStatus, len(cascaded))
	txids := make([]string, len(cascaded))
	for i, cr := range cascaded {
		txids[i] = cr.txid
		// The cascaded child didn't fail for any reason of its own; the
		// reason names the ancestor whose row carries the actual cause.
		reason := cascadeReason(status, cr.ancestor)
		statuses[i] = &models.TransactionStatus{
			TxID:      cr.txid,
			Status:    status,
			Timestamp: now,
			ExtraInfo: reason,
		}
		// One line per cascade-REJECTED tx preserves the existing searchable
		// "transaction rejected" record. The parked cascade gets a single
		// bounded line below instead — a park is not a verdict, and an
		// upstream outage can cascade a lot of them at once.
		if status == models.StatusRejected {
			p.logger.Info(
				"transaction rejected",
				logfields.TxID(cr.txid),
				zap.String("reason", reason),
				logfields.Stage(logfields.StageCascade),
			)
		}
	}
	if status == models.StatusPendingRetry {
		parkFields := append(
			[]zap.Field{logfields.Stage(logfields.StageCascade)},
			logfields.TxIDBatch(txids)...,
		)
		p.logger.Warn("transactions parked behind an ancestor with no network verdict", parkFields...)
	}
	if _, err := p.store.BatchUpdateStatusReturning(ctx, statuses); err != nil {
		p.logger.Warn(
			"cascade status write failed",
			logfields.Status(string(status)),
			zap.Int("count", len(cascaded)),
			zap.Error(err),
		)
	}
	// A cascade-parked descendant has to enter the durable retry queue too.
	// It reaches PENDING_RETRY without ever being broadcast — its ancestor
	// got no verdict, so it was never charged a requeue budget and never went
	// through parkExhaustedRequeues. Writing only the status row would leave
	// it with a NULL next_retry_at, which GetReadyRetries cannot see: the row
	// would sit at PENDING_RETRY forever with nothing scheduled to retry it.
	if status == models.StatusPendingRetry {
		for _, cr := range cascaded {
			// The descendant was never broadcast, so nothing on the network
			// ever spoke about it; its ancestor's park is the whole story.
			p.schedulePendingRetry(ctx, cr.txid, cr.rawTx, cascadeReason(status, cr.ancestor))
		}
	}
	if status == models.StatusRejected {
		// statuses (not txids): every cascaded child's reason names ITS OWN
		// ancestor, so the publish has to group by reason rather than fan one
		// payload-free event across the whole cascade — see publishRejections.
		p.publishRejections(ctx, statuses, now)
		return
	}
	p.publishBulkStatus(ctx, status, txids, now)
}

// publishBulkStatus fans a post-broadcast batch status update onto the
// events Publisher as a single bulk event. txids is the list of
// transactions that just transitioned to the same terminal status.
// Non-fatal: the durable store rows are already written, and SSE catchup
// recovers any dropped events.
func (p *Propagator) publishBulkStatus(ctx context.Context, status models.Status, txids []string, ts time.Time) {
	if p.publisher == nil || len(txids) == 0 {
		return
	}
	p.publishBulk(ctx, &models.TransactionStatus{
		Status:    status,
		Timestamp: ts,
		TxIDs:     txids,
	})
}

// publishRejections fans REJECTED transitions onto the events Publisher with
// the reason attached, grouped so that every txid on an event shares one
// reason.
//
// A bulk event carries no per-transaction payload by construction: one event,
// N txids, so any field it set would be right for at most one of them. That
// is correct for the uniform statuses it was built for (ACCEPTED_BY_NETWORK,
// MINED) and wrong for REJECTED, the one status whose payload decides what
// the client does next — UTXO_SPENT means "your view of the chain is stale,
// resync", TX_INVALID means "these bytes will never be accepted, don't
// retry". Until this, BSV Broadcaster parsed the Teranode reason, persisted it on the
// row, logged it, and then published REJECTED with an empty extraInfo.
//
// Nothing was lost — the row keeps the reason and GET /tx/<txid> serves it —
// but the push event did not describe itself, so learning why cost a round
// trip. REJECTED is terminal, which makes that trap worse than it sounds: a
// client that correctly stops polling on a terminal status stops exactly when
// polling is the only way to find out (issue #301, found on a 128-chain
// covenant workload where one rejection condemns a whole chain).
//
// Grouping — rather than one event per transaction — is deliberate, and it is
// the cascade that makes the difference. A dep-cascade condemns every
// descendant of one rejected ancestor with the SAME cascadeReason string, and
// that is the burst that gets large (one rejection at the head of a chain
// condemns the whole chain). Those still ride a single event, so the SSE
// fan-out keeps its one batched token resolution instead of paying a store
// round-trip per descendant on its single fan-out goroutine. A Teranode
// verdict, by contrast, names the offending txid inside its own failure line,
// so those reasons are distinct per transaction and the grouping degenerates
// to one event each — which is exactly what carrying a per-transaction reason
// requires.
//
// Only TxID, ExtraInfo, StatusCode and CompetingTxs are read from each status;
// the event's Status and Timestamp come from the caller, matching
// publishBulkStatus.
func (p *Propagator) publishRejections(ctx context.Context, rejected []*models.TransactionStatus, ts time.Time) {
	if p.publisher == nil || len(rejected) == 0 {
		return
	}
	// Grouped in first-seen order, not map order, so a batch's events come
	// out in the order the batch produced them and stay comparable with the
	// "transaction rejected" log lines emitted alongside them.
	type reason struct {
		extraInfo  string
		statusCode int
		competing  string
	}
	index := make(map[reason]int, len(rejected))
	groups := make([]*models.TransactionStatus, 0, len(rejected))
	for _, st := range rejected {
		// CompetingTxs is part of the key: an event's fields apply to every
		// txid it carries, and two double spends of different outpoints lose
		// to different spenders.
		key := reason{extraInfo: st.ExtraInfo, statusCode: st.StatusCode, competing: strings.Join(st.CompetingTxs, ",")}
		i, seen := index[key]
		if !seen {
			i = len(groups)
			index[key] = i
			groups = append(groups, &models.TransactionStatus{
				Status:       models.StatusRejected,
				StatusCode:   st.StatusCode,
				Timestamp:    ts,
				ExtraInfo:    st.ExtraInfo,
				CompetingTxs: st.CompetingTxs,
			})
		}
		groups[i].TxIDs = append(groups[i].TxIDs, st.TxID)
	}
	for _, g := range groups {
		p.publishBulk(ctx, g)
	}
}

// publishBulk sends one prepared bulk event. Non-fatal: the durable store
// rows are already written, and SSE catchup recovers any dropped events.
func (p *Propagator) publishBulk(ctx context.Context, template *models.TransactionStatus) {
	if err := p.publisher.PublishBulk(ctx, template); err != nil {
		p.logger.Warn(
			"failed to publish bulk propagation status",
			logfields.Status(string(template.Status)),
			zap.Int("count", len(template.TxIDs)),
			zap.Error(err),
		)
	}
}

func (p *Propagator) Start(ctx context.Context) error {
	// Mark Start as having been entered before anything else so Stop knows
	// to wait for our init handoff. Closing initDone on every exit path
	// (success or error) lets Stop's <-initDone unblock either way.
	p.startCalled.Store(true)
	defer p.initOnce.Do(func() { close(p.initDone) })

	// Stop the test-mode dispatcher goroutine started in New(); the
	// production lifecycle runs the same loop inside the kafka
	// ClaimHandler so dep state + offset marking stay on a single
	// goroutine. Wait for dispatcherDone before subscribing — cancel()
	// returns immediately, and the loop may still be parked on a
	// channel read when it does. Any future caller that pushes to
	// admitCh / requeueCh / terminalCh / drainCh during Start would
	// otherwise race the old goroutine.
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
		p.dispatcherCancel = nil
		if p.dispatcherDone != nil {
			<-p.dispatcherDone
			p.dispatcherDone = nil
		}
	}

	consumer, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:       p.producer.Broker(),
		GroupID:      p.cfg.Kafka.ConsumerGroup + "-propagation",
		Topics:       []string{kafka.TopicPropagation},
		Producer:     p.producer,
		MaxRetries:   p.cfg.Kafka.MaxRetries,
		Logger:       p.logger,
		ClaimHandler: p.handleClaim(ctx),
	})
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	p.consumer.Store(consumer)

	// Spin up the persistent broadcast worker pool. Workers exit when
	// Stop() closes the job channel; the WaitGroup lets Stop() block until
	// all in-flight submits drain. broadcastRunning gates submitBroadcastJobs
	// so callers don't push into an undrained channel before workers start
	// or after they exit (Stop, or never started in tests).
	//
	// Add(N) in one call BEFORE spawning workers so the counter never
	// transiently zeroes mid-loop (a worker can Done() faster than the
	// next iteration's Add(1) bumps the counter — that pattern lets a
	// concurrent Wait() return early and the next Add() then panics with
	// "WaitGroup is reused before previous Wait has returned").
	p.broadcastRunning.Store(true)
	p.broadcastWG.Add(p.broadcastWorkers)
	for i := 0; i < p.broadcastWorkers; i++ {
		go p.runBroadcastWorker()
	}

	// Replay in-flight registrations to merkle-service. One-shot; exits on
	// its own. Compensates for /watch state loss on the merkle-service side
	// (recreated namespace, data wipe, schema migration) which otherwise
	// silently disables STUMP callbacks for every previously-submitted tx.
	//
	// Reaper: scans the status store for non-terminal rows that have
	// been stuck longer than the per-status thresholds and rebroadcasts
	// them. The reaper is the durable retry surface — processBatch
	// itself runs each tx through the broadcast pipeline exactly once
	// and relies on the reaper to retry anything that didn't reach a
	// terminal verdict.
	//
	// Add(2) coalesced for the same reason as the broadcast loop above.
	p.backgroundWG.Add(2)
	go func() {
		defer p.backgroundWG.Done()
		p.runMerkleReplay(ctx)
	}()
	// With several merkle-services the pool re-sends every registration an
	// endpoint missed from a bounded per-endpoint queue. If that queue
	// overflowed, the dropped registrations are unknown and the endpoint
	// needs a full lookback replay (see runResyncReplay).
	if rp, ok := p.merkleClient.(merkleservice.Recoverable); ok {
		rp.OnResyncNeeded(func(endpoint string) {
			p.scheduleResync(ctx, rp, endpoint)
		})
	}
	go func() {
		defer p.backgroundWG.Done()
		p.runReaper(ctx)
	}()

	p.logger.Info(
		"propagation service started",
		zap.Duration("reaper_interval", p.reaperInterval),
		zap.Int("broadcast_workers", p.broadcastWorkers),
		zap.Int("max_parallel_chunks", p.maxParallelChunks),
		zap.Int("teranode_max_batch_size", p.teranodeBatchCap),
		zap.Int("teranode_max_batch_bytes", p.teranodeBatchBytesCap),
	)
	// Signal init complete before blocking on consumer.Run so a concurrent
	// Stop can proceed past <-p.initDone now that every wg.Add above has
	// happened-before any wg.Wait Stop will perform.
	p.initOnce.Do(func() { close(p.initDone) })
	// Readiness: the consumer group is constructed (brokers reachable) and
	// the workers and replay/reaper are up. Run blocks for the process
	// lifetime, so readiness cannot wait for it. The signal therefore
	// precedes the actual group join; a persistent join failure (ACL,
	// missing topic) keeps Run retrying and is not reflected in /ready.
	p.SignalReady()
	return consumer.Run(ctx)
}

// handleClaim returns the kafka.ClaimHandler that owns each per-partition
// session. The dispatcher loop runs in the goroutine Sarama hands us via
// claim, so dep state, Kafka offset tracking, and claim.MarkMessage all
// happen on the same goroutine.
func (p *Propagator) handleClaim(ctx context.Context) kafka.ClaimHandler {
	cfg := dispatcherConfig{maxPending: p.maxPending}
	return func(claim kafka.Claim) error {
		// Use the claim's context as a child of the service context so
		// shutdown OR a rebalance both unblock the loop.
		claimCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-claim.Context().Done():
				cancel()
			case <-claimCtx.Done():
			}
		}()
		// One dispatcherIO per claim: Sarama runs one ConsumeClaim per
		// assigned partition, and each loop's events must be unreachable
		// by its siblings (#295 — see dispatcherIO).
		return p.runDispatcher(claimCtx, claim, cfg, newDispatcherIO())
	}
}

// WaitForBatches blocks until every processBatch goroutine spawned by
// flushBatch has finished. Used by tests to assert post-flush invariants
// against the in-memory mockStore, and reused by Stop() to drain in-flight
// pipelines before tearing down the broadcast worker pool.
func (p *Propagator) WaitForBatches() {
	p.inflightBatches.Wait()
}

func (p *Propagator) Stop() error {
	// If Start was ever called, wait for its init phase to finish (or
	// fail) before reading any WaitGroups it populates. Otherwise the
	// Wait() calls below can race Start's Add()s and panic with
	// "sync: WaitGroup is reused before previous Wait has returned".
	// Stop-without-Start is permitted by the services.Service contract
	// (and exercised by existing tests), so skip the wait in that case.
	if p.startCalled.Load() {
		<-p.initDone
	}
	p.logger.Info("stopping propagation service")
	var consumerErr error
	if c := p.consumer.Load(); c != nil {
		consumerErr = c.Close()
	}
	// Wait for in-flight processBatch goroutines to finish before tearing
	// down the broadcast worker pool. Otherwise an in-flight batch would
	// push jobs into a channel we're about to close, deadlocking the
	// broadcast collect loop on a resultCh that never receives.
	p.inflightBatches.Wait()
	// Closing broadcastJobs lets every worker drain its current iteration
	// and exit. Flip broadcastRunning first so any in-flight submit fan-out
	// falls back to the goroutine path rather than pushing into a channel
	// we're about to close.
	if p.broadcastRunning.Swap(false) {
		close(p.broadcastJobs)
		p.broadcastWG.Wait()
	}
	// Cancel the test-mode dispatcher goroutine started in New (if
	// Start was never called and so didn't already drain it). Wait on
	// dispatcherDone so the goroutine has fully exited before Stop
	// returns — otherwise it may still be holding references to the
	// store the surrounding test cleanup is about to close.
	if p.dispatcherCancel != nil {
		p.dispatcherCancel()
		if p.dispatcherDone != nil {
			<-p.dispatcherDone
		}
	}
	// Wait for the long-running Start-spawned goroutines (reaper,
	// merkle replay) to exit before returning. The surrounding app
	// cleanup will close the store as soon as Stop returns; if the
	// reaper is mid-IterateStatusesSince or mid-lease-release on a
	// closed store, the goroutine panics and the test framework
	// attributes the failure to t.Cleanup. Their parent ctx has
	// already been canceled by the caller, so this only blocks on
	// in-flight scan/release work.
	p.resyncMu.Lock()
	p.resyncStopped = true
	p.resyncMu.Unlock()
	p.backgroundWG.Wait()
	return consumerErr
}

// handleMessage decodes the propagation envelope and queues it for the next
// flushBatch. Cheap on purpose: no HTTP, no DB.
//
// F-024 durability is preserved at the batch level: processBatch runs
// RegisterBatchWithResults before broadcasting, and any tx whose registration
// failed is excluded from the broadcast. That failure is retried on the
// Merkle registration budget, which does not consume a network propagation
// attempt. Exhausting the Merkle budget parks the tx at PENDING_RETRY for
// the reaper; it is still not broadcast unregistered.
//
// A high-water-mark on pendingMsgs guards against unbounded growth if a
// downstream stall lasts longer than the consumer's offset commit window.
func (p *Propagator) handleMessage(ctx context.Context, msg *kafka.Message) error {
	var propMsg propagationMsg
	if err := json.Unmarshal(msg.Value, &propMsg); err != nil {
		return fmt.Errorf("unmarshaling propagation message: %w", err)
	}

	// Same hydration as the claim path (hydrateClaim), inline: this runs on
	// the caller's goroutine, not the dispatcher's.
	one := []propagationMsg{propMsg}
	drops, err := p.hydrate(ctx, one, nil)
	if err != nil {
		return fmt.Errorf("loading raw tx for %s: %w", propMsg.TXID, err)
	}
	if drops[0] != "" {
		return fmt.Errorf("propagation message for %q not propagatable: %s", propMsg.TXID, drops[0])
	}
	propMsg = one[0]

	// See runDispatcher's claim branch for the production decode path; this
	// mirrors it so the two decode sites behave identically — including the
	// caveat that the base ctx must not carry an ambient span (a header-less
	// message would otherwise inherit it as a bogus producer span). In tests
	// this is called with context.Background(); in production handleMessage
	// runs only via the ClaimHandler path whose ctx carries no span.
	propMsg.spanCtx = trace.SpanContextFromContext(kafka.ExtractTraceContext(ctx, msg.Headers))

	// All admission logic — parent dep check, pendingMsgs append,
	// offset tracker bookkeeping — happens on the dispatcher
	// goroutine. When pending is at its cap, the dispatcher's select
	// excludes admitCh, so this send blocks. The Kafka consumer
	// goroutine waits here until the dispatcher has room, which
	// naturally pauses Kafka pulls and lets backpressure flow back to
	// the broker. No DLQ, no error to the client; the only observable
	// effect is briefly increased consumer lag.
	_ = p.admitToDispatcher(propMsg, msg.Offset)
	return nil
}

// flushBatch hands the drained pending slice off to a processBatch goroutine
// and returns. Concurrency is bounded by processBatchSem: while batch N runs
// its register+broadcast pipeline (~4s at 100 TPS), the kafka consumer can
// drain batch N+1 and begin its own pipeline in parallel up to the configured
// cap (MaxConcurrentBatches, default 4). Sustained-100-TPS RECEIVED→
// ACCEPTED_BY_NETWORK latency benefits roughly by half-a-pipeline-cycle per
// tx because pendingMsgs no longer sits idle waiting for the prior batch's
// broadcast to finish.
//
// Acquiring the semaphore inside flushBatch (rather than firing the goroutine
// unconditionally) provides natural backpressure: when MaxConcurrentBatches
// pipelines are already in flight, the kafka consumer's flush call blocks
// here until a slot frees. This bounds peak in-memory pendingMsgs depth and
// gives the kafka claim a clean cancellation point.
//
// The context comes from the current Kafka claim — it is canceled when the
// claim ends (shutdown or rebalance). Downstream HTTP broadcasts and store
// writes observe that cancellation and unwind cleanly, so a revoked partition
// doesn't keep doing work on behalf of a partition it no longer owns.
//
// F-024 ("register before broadcast") is preserved per-batch: each goroutine
// drives one batch through registerBatch and broadcastInChunks sequentially.
// Across batches, status writes pass through the lattice so a slower batch's
// ACCEPTED_BY_NETWORK can't regress a tx that a faster sibling already moved
// to SEEN_ON_NETWORK.
func (p *Propagator) flushBatch(ctx context.Context) error {
	// Drain pendingMsgs from the dispatcher goroutine via the
	// drainCh request/reply. The dispatcher owns the slice; we
	// receive a snapshot and own it from here on.
	batch := p.drainPending()

	if len(batch) == 0 {
		return nil
	}

	select {
	case p.processBatchSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.inflightBatches.Add(1)
	metrics.PropagationInflightBatches.Set(float64(len(p.processBatchSem)))
	go func() {
		defer func() {
			<-p.processBatchSem
			p.inflightBatches.Done()
			metrics.PropagationInflightBatches.Set(float64(len(p.processBatchSem)))
		}()
		p.processBatch(ctx, batch, p.defaultIO)
	}()
	return nil
}

// registerBatch invokes merkle-service /watch for every tx in the batch and
// returns the subset that registered successfully. Txs whose /watch failed
// returns the successfully-registered subset for broadcast and the failed
// subset for caller-driven requeue. Per-tx errors: failures (network
// blip on one /watch call, etc.) go to the failed slice so processBatch
// can requeue them through the dispatcher after a short flat wait.
//
// When the merkle integration is disabled (client nil or no callback URL),
// every tx is treated as registered.
func (p *Propagator) registerBatch(ctx context.Context, batch []propagationMsg) (registered, failed []propagationMsg) {
	if p.merkleClient == nil || p.cfg.CallbackURL == "" {
		return batch, nil
	}

	regs := make([]merkleservice.Registration, len(batch))
	for i, m := range batch {
		regs[i] = merkleservice.Registration{
			TxID:          m.TXID,
			CallbackURL:   p.cfg.CallbackURL,
			CallbackToken: p.cfg.CallbackToken,
		}
	}

	start := time.Now()
	errs := p.merkleClient.RegisterBatchWithResults(ctx, regs, p.merkleConcurrency)
	metrics.PropagationMerkleRegisterDuration.Observe(time.Since(start).Seconds())

	registered = make([]propagationMsg, 0, len(batch))
	failed = make([]propagationMsg, 0)
	successTxIDs := make([]string, 0, len(batch))
	var sampleErr, sampleAuthErr error
	authFailures := 0
	canceledFailures := 0
	for i, err := range errs {
		if err == nil {
			// A successful registration ends this episode. The network
			// budget (retryCount) is left as it was — /watch failures
			// never incremented it — and the merkle counter resets so a
			// later registration blip gets a full merkle budget.
			msg := batch[i]
			msg.merkleRetryCount = 0
			msg.merkleFailureReason = ""
			registered = append(registered, msg)
			successTxIDs = append(successTxIDs, msg.TXID)
			continue
		}
		// F-024: a tx we failed to register must NOT be broadcast, so every
		// failure goes to the requeue subset regardless of cause. Keeping
		// auth failures in the same requeue path preserves the dispatcher's
		// offset / in-flight bookkeeping (a diverted tx would strand its Kafka
		// offset and freeze the commit watermark — see handleAdmit). The
		// requeue itself debits the merkle budget, not the network budget.
		reason := merkleRegisterFailureReason(ctx, err)
		msg := batch[i]
		if reason != "claim_revoked" {
			msg.merkleFailureReason = clipRetryReason(err.Error())
		}
		failed = append(failed, msg)
		metrics.PropagationMerkleRegisterFailures.WithLabelValues(reason).Inc()
		switch reason {
		case "claim_revoked":
			// Context cancellation isn't a merkle-service failure at all:
			// the claim was revoked (consumer-group rebalance) or the
			// service is shutting down. One rebalance used to pump tens of
			// thousands of bogus "register_error" counts into the metric
			// (2026-08-10: 39,189 in one batch). Keep it out of sampleErr
			// so the partial-failure WARN below fires only for real /watch
			// errors, and do not treat it as a merkle retry — processBatch
			// aborts before requeue when the claim context is dead.
			canceledFailures++
		case "auth_error":
			// A 401/403 is an auth rejection (merkle_service.auth_token
			// missing or wrong — issue #269), not a transient blip. Its
			// own metric label + the WARN below keep an operator from
			// staring at a generic "register_error" while every
			// registration silently fails. The txs are still requeued on
			// the merkle budget (bounded, same delay) and self-heal once
			// the token is set; nothing is dropped and nothing spins.
			authFailures++
			if sampleAuthErr == nil {
				sampleAuthErr = err
			}
		default:
			if sampleErr == nil {
				sampleErr = err
			}
		}
	}

	switch {
	case len(failed) == 0:
		metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok").Inc()
	case len(registered) == 0:
		metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("all_failed").Inc()
	default:
		metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("partial").Inc()
	}
	// Cancellation-only failures don't warn here: nothing will be requeued
	// (processBatch aborts the batch at its post-register checkpoint) and the
	// caller's single "claim revoked mid-batch" Info line already covers the
	// event — a 39k-line-equivalent WARN claiming "requeueing failed subset"
	// during a rebalance was pure noise.
	if len(failed) > canceledFailures {
		p.logger.Warn(
			"merkle-service /watch partial/all failure; requeueing failed subset",
			zap.Int("batch_size", len(batch)),
			zap.Int("failed", len(failed)),
			zap.Int("registered", len(registered)),
			zap.Error(sampleErr),
		)
	}
	if authFailures > 0 {
		// Loud, distinct diagnosis (one line per batch, so naturally throttled):
		// registration is 401/403ing, which blocks broadcast (F-024) until the
		// operator sets merkle_service.auth_token. Txs are requeued and will
		// self-heal once configured; nothing is dropped.
		p.logger.Warn(
			"merkle-service /watch rejected with 401/403 — merkle_service.auth_token is missing or wrong; registration (and therefore broadcast) is blocked until it is set. Affected txs are requeued and self-heal once configured",
			zap.Int("auth_failed", authFailures),
			zap.Int("batch_size", len(batch)),
			zap.Error(sampleAuthErr),
		)
	}

	// Stamp merkle_registered_at on the successful subset so the startup
	// replay loop can skip them. A store-write failure here must not block
	// broadcast — the mark is a hint, not part of the F-024 invariant.
	if len(successTxIDs) > 0 {
		if err := p.store.MarkMerkleRegisteredByTxIDs(ctx, successTxIDs, time.Now()); err != nil {
			p.logger.Warn(
				"mark merkle-registered failed",
				zap.Int("count", len(successTxIDs)),
				zap.Error(err),
			)
		}
		// Debug, not Info: this isn't a status-lattice transition, just an
		// F-024 durability checkpoint. Keeps Info-level volume flat while
		// still making the registration searchable by txid when debug
		// logging is enabled.
		p.logger.Debug("registered with merkle-service", logfields.TxIDBatch(successTxIDs)...)
	}
	return registered, failed
}

// merkleRegisterFailureReason classifies a /watch error for the
// bounded-cardinality failure metric. claim_revoked is not a Merkle
// retry: the claim context died, and processBatch must not charge either
// budget for it. txids are not part of the label.
func merkleRegisterFailureReason(ctx context.Context, err error) string {
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return "claim_revoked"
	}
	var regErr *merkleservice.RegisterError
	if errors.As(err, &regErr) {
		switch {
		case regErr.StatusCode == http.StatusUnauthorized || regErr.StatusCode == http.StatusForbidden:
			return "auth_error"
		case regErr.StatusCode >= 500:
			return "http_5xx"
		default:
			return "register_error"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "transport"
}

func clipRetryReason(s string) string {
	const maxReason = 240
	if len(s) <= maxReason {
		return s
	}
	return s[:maxReason]
}

// txResult carries per-tx outcome of a broadcast. class is the
// authoritative bucket (accepted / rejected / requeue) that processBatch
// switches on; the other fields are diagnostic / status-write inputs.
//
// successEndpoint is the URL of the peer whose response drove the
// accepted status (empty when no peer accepted or the broadcast
// produced no verdict). errMsg carries a short Teranode code string
// like "TX_INVALID (31)" when the class is rejected.
type txResult struct {
	class           txResultClass
	status          *models.TransactionStatus
	errMsg          string
	rawTx           []byte
	successEndpoint string
}

// failureLinesForLog returns a stable, capped list of Teranode failure lines
// for structured logging. Keys are ignored; lines are ordered most-informative
// first (rejectionLineScore, ties broken lexicographically) so the cap keeps
// the concrete validator verdicts (UTXO_SPENT, TX_CONFLICTING, …) and drops
// PROCESSING catch-alls, not the reverse. Deterministic across map iteration.
func failureLinesForLog(failures *teranode.TxsFailures) []string {
	const maxLines = 8
	if failures.Len() == 0 {
		return nil
	}
	lines := make([]string, 0, failures.Len())
	for _, line := range failures.ByKey {
		lines = append(lines, line)
	}
	lines = append(lines, failures.Unkeyed...)
	sort.Slice(lines, func(i, j int) bool {
		si, sj := rejectionLineScore(lines[i]), rejectionLineScore(lines[j])
		if si != sj {
			return si > sj
		}
		return lines[i] < lines[j]
	})
	if len(lines) > maxLines {
		lines = lines[:maxLines]
	}
	return lines
}

// preferRejectionLine chooses which peer rejection line to keep when several
// endpoints reject the same tx. Higher score wins; equal score keeps the
// current (first-writer) line for stability. Ranking prefers concrete
// Teranode validator codes (UTXO_SPENT, TX_CONFLICTING, TX_INVALID, …) over
// the PROCESSING catch-all over opaque free text, so wallet-visible ExtraInfo
// carries the best available reason rather than whichever peer responded first.
func preferRejectionLine(current, candidate string) string {
	if candidate == "" {
		return current
	}
	if current == "" {
		return candidate
	}
	switch cand, cur := rejectionLineScore(candidate), rejectionLineScore(current); {
	case cand > cur:
		return candidate
	case cand == cur && len(competingSpenders(candidate)) > 0 && len(competingSpenders(current)) == 0:
		// Same verdict class, but only the candidate names the competing
		// spender (UTXO_SPENT does; TX_CONFLICTING does not). Keep the one
		// that can populate competingTxs, whichever peer answered first.
		return candidate
	default:
		return current
	}
}

// alienFailureLines scans one peer's parsed failure map for lines whose key
// does not name any tx in the submitted batch, returning how many such lines
// exist and the most informative one (preferRejectionLine ordering).
//
// Alien keys are expected, not a parser bug: Teranode's conflict-family
// verdicts (UTXO_SPENT / TX_CONFLICTING / TX_INVALID_DOUBLE_SPEND, HTTP 409)
// render the deepest public cause, whose message names the spent OUTPOINT's
// txid and the competing spender — never the submitted txid (no
// [ProcessTransaction][<txid>] wrapper; see teranode
// errors/error_data_utxo_spent.go + errors.UserMessage). The parse then keys
// the line under a hash that isn't in the batch. Any alien line voids the
// failure map's "absent ⇒ accepted" contract for that peer: exactly one
// submitted tx DID fail per line, we just can't tell which, so implicit
// acceptance votes would fabricate an ACCEPTED_BY_NETWORK for the very tx
// the peer rejected.
func alienFailureLines(failures map[string]string, inBatch map[string]bool) (count int, best string) {
	for key, line := range failures {
		if !inBatch[key] {
			count++
			best = preferRejectionLine(best, line)
		}
	}
	return count, best
}

// outpointRefPattern matches an "<txid>:<vout>" outpoint reference anywhere in
// a Teranode failure line. The conflict family renders the spent outpoint in
// exactly this form — "UTXO_SPENT (70): <outpoint_txid>:<vout> utxo already
// spent by tx <spender_txid>[n]" — and, crucially, renders the COMPETING
// SPENDER with a bracketed index instead ("...[0]"), so a colon-anchored match
// can never pick up the spender by accident. Case-insensitive because Teranode
// lowercases but the regex stays defensive, exactly like txsTxidPattern.
var outpointRefPattern = regexp.MustCompile(`[0-9a-fA-F]{64}:[0-9]{1,10}`)

// outpointOwnerAmbiguous marks an outpoint spent by more than one tx in the
// same submitted batch — an intra-batch double spend. Teranode's single
// conflict line cannot say which of them lost, so such an outpoint attributes
// to nobody (see attributeConflictLines).
const outpointOwnerAmbiguous = -1

// buildSubmittedOutpointIndex maps every outpoint spent by the batch to the
// index of the single tx that spends it, or outpointOwnerAmbiguous when
// several do. Keys are lowercase "<txid>:<vout>" so they compare directly
// against what outpointRefPattern lifts out of a Teranode failure line.
//
// The raw bytes are re-parsed here rather than read off propagationMsg —
// InputTXIDs carries parent txids only, and an outpoint needs the vout to be
// unambiguous (two inputs of the same batch can spend two different outputs of
// the same parent). Parsing is confined to the failure path: the caller only
// builds the index when a peer actually returned alien lines, and builds it at
// most once per batch no matter how many peers do.
//
// Unparseable raw bytes are skipped, not fatal — a tx we cannot parse simply
// owns no outpoints and therefore attracts no attribution.
func buildSubmittedOutpointIndex(batch []propagationMsg) map[string]int {
	owners := make(map[string]int, len(batch))
	for i, msg := range batch {
		tx, _, err := sdkTx.NewTransactionFromStream(msg.RawTx)
		if err != nil || tx == nil {
			continue
		}
		for _, in := range tx.Inputs {
			if in == nil || in.SourceTXID == nil {
				continue
			}
			key := strings.ToLower(in.SourceTXID.String()) + ":" + strconv.FormatUint(uint64(in.SourceTxOutIndex), 10)
			if prev, seen := owners[key]; seen && prev != i {
				owners[key] = outpointOwnerAmbiguous
				continue
			}
			owners[key] = i
		}
	}
	return owners
}

// attributeConflictLines resolves alien (outpoint-keyed) failure lines back to
// the submitted transactions they actually condemn, returning batch-index →
// line for every line it could place.
//
// This is the fix for the common case behind the 2026-08-11 wedge. Teranode's
// conflict verdicts name the spent outpoint rather than the submitted txid, so
// #292 could tell that SOMETHING in the batch had failed but not what — and a
// tx with neither an accept vote nor an attributable rejection falls to
// requeue, forever. BSV Broadcaster does hold the submitted raw transactions, so for
// the overwhelmingly common shape (one line per genuinely double-spent tx) it
// can simply look up which submitted tx spends that outpoint and terminalize
// it REJECTED with the real verdict.
//
// The safety property from #292 is preserved verbatim: attribution only ever
// ADDS rejection votes, and only on an exact outpoint match against a
// submitted input. A line whose outpoint matches nothing, or matches more than
// one submitted tx, is left alone — nothing is guessed, and no acceptance is
// ever fabricated. Callers keep withholding implicit accepts for the peer
// regardless of how many lines were attributed.
//
// Iteration is over sorted keys so a batch that draws two attributable lines
// for the same tx resolves to the same ExtraInfo on every run, independent of
// Go's map iteration order.
func attributeConflictLines(failures map[string]string, inBatch map[string]bool, owners map[string]int) map[int]string {
	if len(failures) == 0 || len(owners) == 0 {
		return nil
	}
	keys := make([]string, 0, len(failures))
	for key := range failures {
		if !inBatch[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var attributed map[int]string
	for _, key := range keys {
		line := failures[key]
		idx, ok := attributeOneConflictLine(line, owners)
		if !ok {
			metrics.PropagationConflictAttributionTotal.WithLabelValues("unattributable").Inc()
			continue
		}
		metrics.PropagationConflictAttributionTotal.WithLabelValues("attributed").Inc()
		if attributed == nil {
			attributed = make(map[int]string, len(keys))
		}
		attributed[idx] = preferRejectionLine(attributed[idx], line)
	}
	return attributed
}

// attributeOneConflictLine finds the submitted tx a single failure line
// condemns by matching every "<txid>:<vout>" the line mentions against the
// batch's spent outpoints. Returns the batch index and true only when the line
// resolves to exactly one submitted tx.
func attributeOneConflictLine(line string, owners map[string]int) (int, bool) {
	matches := outpointRefPattern.FindAllString(line, -1)
	found := -1
	for _, ref := range matches {
		idx, ok := owners[strings.ToLower(ref)]
		if !ok || idx == outpointOwnerAmbiguous {
			continue
		}
		if found >= 0 && found != idx {
			// The line names spent outpoints belonging to two different
			// submitted txs. Nothing about that shape tells us which one the
			// verdict is about, so decline rather than guess.
			return 0, false
		}
		found = idx
	}
	if found < 0 {
		return 0, false
	}
	return found, true
}

// rejectionLineScore ranks a Teranode failure line for wallet-facing quality.
// Higher is better. Unknown free text scores 0.
// lineIsMissingParentCondition reports whether a Teranode failure line is
// the missing-parent condition family: TX_MISSING_PARENT (34), or its
// un-wrapped store-layer form TX_NOT_FOUND (30) — both produced when a
// tx's parent isn't in the node's UTXO store yet, which on a
// multi-partition propagation topic is the signature of an out-of-order
// child rather than a verdict about the bytes. Substring match, not a
// prefix: Teranode nests codes ("PROCESSING (4): … TX_MISSING_PARENT
// (34): …") and doubles prefixes. Over-matching only routes a tx to the
// bounded requeue (safe, self-correcting); under-matching risks a false
// terminal REJECTED — so err on matching. The "(" suffix keeps free-text
// mentions of parents from matching.
func lineIsMissingParentCondition(line string) bool {
	return strings.Contains(line, "TX_MISSING_PARENT (") ||
		strings.Contains(line, "TX_NOT_FOUND (")
}

// teranodeNamedCode matches Teranode's "NAME (n)" tokens, including ones
// this build has not mapped yet. PROCESSING is the catch-all wrapper.
var teranodeNamedCode = regexp.MustCompile(`[A-Z][A-Z0-9_]* \(\d+\)`)

func lineHasNestedTeranodeVerdict(line string) bool {
	for _, m := range teranodeNamedCode.FindAllString(line, -1) {
		name, _, _ := strings.Cut(m, " (")
		if name != "PROCESSING" {
			return true
		}
	}
	return false
}

// lineIsOpaqueProcessing reports a PROCESSING (4) wrapper with no nested
// TX_*/UTXO_* (or other named) code — the code Teranode's public error
// boundary falls back to when no cause in the chain is on its client-safe
// allowlist. Such a line says only "this transaction did not make it": the
// actual cause (missing parent, frozen utxo, coinbase immature, a BDK CGO
// exception, a storage failure inside the validator) was stripped at the
// boundary.
//
// It is therefore the one line shape where the peer's HTTP status carries
// strictly more information than its message, and the only one for which
// BSV Broadcaster consults that status. A nested code disqualifies it: a line carrying
// any named code other than PROCESSING is carrying a verdict, whatever wrapper
// sits in front of it.
func lineIsOpaqueProcessing(line string) bool {
	return strings.Contains(line, "PROCESSING (") && !lineHasNestedTeranodeVerdict(line)
}

// lineIsInfraCondition reports whether a Teranode failure line names a fault
// in the node rather than a judgement about the submitted transaction: its
// blob store rejected the write, a downstream service was unreachable, an
// unclassified internal error escaped. Nothing about the bytes changed, so
// these must never terminalize — the same transaction sent to a healthy peer
// (or to this one a minute later) is expected to be accepted.
//
// Matched on the leading code name, which Teranode always emits, rather than
// on message text. STORAGE_ERROR and SERVICE_ERROR are the two the propagation
// path produces directly (failed blob save, no HTTP validator configured for
// an over-sized tx); ERROR is the generic fallback the gRPC boundary uses when
// it cannot resolve a code at all.
func lineIsInfraCondition(line string) bool {
	name, _, found := strings.Cut(line, " (")
	if !found {
		return false
	}
	switch name {
	case "STORAGE_ERROR", "SERVICE_ERROR", "ERROR":
		return true
	default:
		return false
	}
}

// bestUnplaceableLine picks the most informative line among those that named
// no submitted transaction — the best alien line and every unkeyed line. Used
// only once a chunk is down to a single transaction, where "named nobody"
// unambiguously means "named this one".
func bestUnplaceableLine(bestAlien string, unkeyed []string) string {
	best := bestAlien
	for _, line := range unkeyed {
		best = preferRejectionLine(best, line)
	}
	return best
}

func rejectionLineScore(line string) int {
	best := -1
	for _, m := range teranodeNamedCode.FindAllString(line, -1) {
		name, _, _ := strings.Cut(m, " (")
		best = max(best, codeNameScore(name))
	}
	if best >= 0 {
		return best
	}
	name, _, found := strings.Cut(line, " (")
	if !found {
		return 0
	}
	return codeNameScore(name)
}

// verdictCodeName returns the Teranode code a failure line's verdict should be
// read from: the most specific "NAME (n)" token anywhere in the line, not
// merely the leading one. Teranode wraps causes ("PROCESSING (4): … TX_INVALID
// (31): … UTXO_FROZEN (72): …"), and keying on an outer wrapper would report a
// double spend as an uncoded catch-all, or a frozen utxo as a generic 467 —
// losing the code a wallet branches on. Ranked by verdictSpecificity, not by
// the cross-peer codeNameScore: within one line the question is which code IS
// the cause, not which peer's line reads best. Ties keep the leftmost
// (outermost) token, so a line whose only code is PROCESSING still reads as
// PROCESSING. found is false only when the line carries no code token at all.
func verdictCodeName(line string) (name string, found bool) {
	best := -1
	for _, m := range teranodeNamedCode.FindAllString(line, -1) {
		candidate, _, _ := strings.Cut(m, " (")
		if rank := verdictSpecificity(candidate); rank > best {
			name, best = candidate, rank
		}
	}
	if best >= 0 {
		return name, true
	}
	// No well-formed token (e.g. a non-uppercase prefix): fall back to the
	// leading "<name> (" shape the parser has always accepted.
	name, _, found = strings.Cut(line, " (")
	return name, found
}

// verdictSpecificity ranks a code for verdictCodeName: codes classifyFailureLine
// maps to a specific ARC status beat TX_INVALID, the generic wrapper that maps
// to 467, which beats known codes with no mapping, then unknown codes, then
// the PROCESSING catch-all. A specific unmapped code (TX_POLICY under
// TX_INVALID) therefore never demotes a mapped line to uncoded.
func verdictSpecificity(name string) int {
	switch name {
	case "UTXO_SPENT", "TX_INVALID_DOUBLE_SPEND", "TX_CONFLICTING":
		return 50
	case "TX_LOCKED", "UTXO_FROZEN", "TX_LOCK_TIME", "UTXO_NON_FINAL":
		return 40
	case "TX_INVALID":
		return 30
	case "PROCESSING":
		return 10
	}
	if codeNameScore(name) == 30 {
		return 20 // known, specific, but unmapped
	}
	return 15 // a code this build does not know
}

// codeNameScore ranks one Teranode code name for wallet-facing quality; see
// rejectionLineScore.
func codeNameScore(name string) int {
	switch name {
	case "UTXO_SPENT", "TX_INVALID_DOUBLE_SPEND", "TX_CONFLICTING":
		return 40
	case "TX_INVALID", "TX_POLICY", "TX_LOCK_TIME", "TX_MISSING_PARENT",
		"TX_LOCKED", "TX_COINBASE_IMMATURE", "UTXO_FROZEN", "UTXO_NON_FINAL",
		"UTXO_INVALID_SIZE", "INVALID_ARGUMENT":
		return 30
	case "PROCESSING":
		// Catch-all "this tx is bad" wrapper — better than opaque infra text,
		// worse than any concrete validator code.
		return 20
	default:
		// Recognizable "<NAME> (n):" shape but not a code this list knows —
		// e.g. a Teranode code added after this build. Still a concrete
		// verdict, so it outranks the PROCESSING catch-all but never a
		// known code.
		return 25
	}
}

// classifyFailureLine maps one Teranode /txs failure line into a client-
// facing message and a machine-readable ARC status code (errors/errors.go
// taxonomy; 0 when the line has no confident ARC equivalent). The line is
// the full UserMessage produced by Teranode,
// e.g. "PROCESSING (4): [ProcessTransaction][<txid>] failed to validate
// transaction" or "TX_INVALID (31): [ProcessTransaction][<txid>] ...".
//
// This helper only maps lines the broadcast loop has already routed as
// terminal verdicts (rejectionLine). Opaque PROCESSING (4) with no nested
// named code never reaches here — it requeues. Nested wrappers such as
// "PROCESSING (4): … UTXO_SPENT (70)" arrive as the full line and are
// classified by their most informative code (verdictCodeName), not by the
// leading PROCESSING wrapper; ExtraInfo keeps the whole line.
//
// What the verdict's "NAME (n)" code DOES tell us confidently is mapped onto
// the ARC taxonomy so consumers can branch on a number instead of prose
// (issue #254 / external feedback item 2):
//
//	TX_LOCK_TIME (35), UTXO_NON_FINAL (71) → 476 StatusNotFinal. The peer's
//	  view says the tx isn't final yet — a condition of that view, not a
//	  verdict about the bytes; resubmitting after the lock expires (or the
//	  peer's tip catches up) is expected to succeed, so the message gains an
//	  explicit retryable hint. (Intake's finality gate catches most of these
//	  against BSV Broadcaster's own tip; a line here means the peer's view trails.)
//	TX_CONFLICTING (36), UTXO_SPENT (70), TX_INVALID_DOUBLE_SPEND →
//	  466 StatusConflict (double-spend attempt / input already spent).
//	  Teranode's own /txs handler groups exactly these under HTTP 409
//	  Conflict (httpStatusForTxError, services/propagation/Server.go), so
//	  the 466 mapping mirrors the upstream classification. Wallets branch
//	  on 466 to decide "do NOT blind-release these inputs" — the outpoint
//	  is owned by a competing/confirmed tx, not merely unaccepted.
//	TX_INVALID (31)     → 467 StatusGeneric (wraps fee/script/policy — the
//	  name alone can't recover which).
//	UTXO_FROZEN (72)    → 471 StatusFrozenPolicy.
//	PROCESSING (4) and everything else → 0, message verbatim (PROCESSING
//	  with a single-tx HTTP 403 → 471, see httpStatus below).
//
// errMsg preserves the Teranode line verbatim (plus the retryable suffix for
// the non-final family) so wallet rows surface the actual message.
//
// httpStatus is the peer's HTTP status when its response described exactly one
// transaction, and 0 otherwise. It is consulted only for the opaque PROCESSING
// catch-all, where the boundary stripped the cause and the status is the last
// remaining signal: a 403 there means the node refused the spend outright
// (frozen or otherwise administratively blocked utxo), which is a real,
// actionable verdict that would otherwise reach the submitter as "failed to
// validate transaction" and nothing more.
func classifyFailureLine(line string, httpStatus int) (errMsg string, arcCode int) {
	name, found := verdictCodeName(line)
	if !found {
		return line, 0
	}
	switch name {
	case "TX_LOCK_TIME", "UTXO_NON_FINAL":
		return line + " — retryable: the transaction is not final against this peer's chain view; resubmit after the locktime expires",
			int(arcerrors.StatusNotFinal)
	case "TX_CONFLICTING", "UTXO_SPENT", "TX_INVALID_DOUBLE_SPEND", "TX_LOCKED":
		// TX_LOCKED joins the conflict family: Teranode's own /txs handler
		// groups it under HTTP 409 (httpStatusForTxError), and a wallet must
		// treat "the utxo is not spendable" the same way it treats "the utxo
		// is already spent" — do not blind-release the inputs.
		return line, int(arcerrors.StatusConflict)
	case "TX_INVALID":
		return line, int(arcerrors.StatusGeneric)
	case "UTXO_FROZEN":
		// The named form of what the opaque-PROCESSING 403 branch below
		// infers from the status alone.
		return line, int(arcerrors.StatusFrozenPolicy)
	case "PROCESSING":
		if httpStatus == http.StatusForbidden {
			return line + " — the peer answered HTTP 403 (forbidden): the transaction spends a frozen or " +
					"administratively blocked utxo. The peer's error boundary does not surface the frozen-utxo " +
					"code itself, so this status is the whole of the available detail.",
				int(arcerrors.StatusFrozenPolicy)
		}
		return line, 0
	default:
		return line, 0
	}
}

// competingSpenderPattern lifts the competing spender's txid out of a Teranode
// conflict line, which renders it as "already spent by tx <txid>[<vin>]"
// (teranode errors.NewUtxoSpentError over spend.SpendingData.String()).
var competingSpenderPattern = regexp.MustCompile(`(?i)already spent by tx ([0-9a-f]{64})`)

// competingSpenders returns the distinct txids a conflict line names as the
// current owners of the outpoints the rejected transaction tried to spend,
// lowercased, in first-seen order — the value for
// TransactionStatus.CompetingTxs. nil when the line names none (TX_CONFLICTING
// and TX_INVALID_DOUBLE_SPEND do not carry the spender).
func competingSpenders(line string) []string {
	var out []string
	for _, m := range competingSpenderPattern.FindAllStringSubmatch(line, -1) {
		txid := strings.ToLower(m[1])
		if !slices.Contains(out, txid) {
			out = append(out, txid)
		}
	}
	return out
}

// startBroadcastSpan opens one "propagation.broadcast" span per flushed
// batch and links it back to up to batchSpanMaxLinks producer spans found on
// the batch's messages (propagationMsg.spanCtx, populated at decode/admit
// time by ExtractTraceContext — see runDispatcher and handleMessage). This
// is the OTel batch-messaging pattern of linking, not parenting, individual
// producer spans onto one span per batch, so a large flush never opens a
// span per message on the hot path.
//
// The returned ctx carries the new span and must be used for
// broadcastInChunks so the otelhttp client spans issued against every
// teranode endpoint nest under it. The returned func ends the span and must
// be called exactly once (callers should defer it).
//
// Building the link list and attributes is deferred behind
// span.IsRecording() — mirroring startProduceSpan/startConsumeSpan in
// package kafka — so the disabled/no-op path (default no-op TracerProvider)
// costs one no-op Start call and nothing else, independent of batch size.
func (p *Propagator) startBroadcastSpan(ctx context.Context, batch []propagationMsg) (context.Context, func()) {
	spanCtx, span := otel.Tracer(propagationTracerName).Start(ctx, "propagation.broadcast")
	if span.IsRecording() {
		links := 0
		for _, msg := range batch {
			if links >= batchSpanMaxLinks {
				break
			}
			if !msg.spanCtx.IsValid() {
				continue
			}
			span.AddLink(trace.Link{SpanContext: msg.spanCtx})
			links++
		}
		// Record batch size alongside the actual link count so operators
		// can see when the batchSpanMaxLinks cap truncated the links (i.e.
		// batch_size had >128 valid producer contexts but only 128 linked).
		span.SetAttributes(
			attribute.Int("broadcast.batch_size", len(batch)),
			attribute.Int("broadcast.linked_count", links),
		)
	}
	return spanCtx, func() { span.End() }
}

// abortBatchOnRevokedClaim reports whether the batch's claim context is
// already dead — the broker revoked the partition claim mid-batch (consumer-
// group rebalance) or the service is shutting down. When it is, the caller
// must stop the batch where it stands: every downstream stage (merkle /watch,
// teranode /txs, the delayed requeue) would run against a born-canceled
// context, instantly "failing" tens of thousands of txs into requeue
// goroutines that park 2s and drop. Observed live at >1,900 TPS (2026-08-10):
// 39,189 of a 53,269-tx batch "failed" registration with context canceled
// after a claim revocation, then a 14,080-tx retry batch logged "batch
// propagated" accepted=0 in 12ms because every SubmitTransactions returned
// instantly under the dead ctx.
//
// Stopping IS the durable path: nothing in the batch has been marked on the
// revoked claim's offsetTracker, so every tx's Kafka offset stays uncommitted
// and the NEW claim replays the whole batch (at-least-once). One Info line +
// one counter tick per aborted batch keeps the event visible without the log
// storm the dead-ctx spiral used to produce.
func (p *Propagator) abortBatchOnRevokedClaim(ctx context.Context, txCount int) bool {
	if ctx.Err() == nil {
		return false
	}
	metrics.PropagationClaimRevokedBatchesTotal.Inc()
	p.logger.Info(
		"claim revoked mid-batch; leaving txs uncommitted for replay",
		logfields.TxIDCount(txCount),
	)
	return true
}

// processBatch handles one drained batch:
//  1. Register every tx with merkle-service. Txs whose /watch failed are
//     requeued on the Merkle registration budget (not the network
//     propagation budget) after a short flat wait. They are not broadcast
//     until registration succeeds (F-024).
//  2. Broadcast the registered subset to teranode in /txs chunks.
//  3. For each per-tx result, apply the corresponding action:
//     - Accepted → terminal ACCEPTED row + dispatcher notify (releases waiters)
//     - Rejected → terminal REJECTED row + dispatcher notify (cascade)
//     - Requeue  → requeue through the dispatcher after a short flat wait
//     (transient infra failure: peer 5xx with no per-slot info, no peer
//     reachable, per-slot infra code). The Kafka offset stays pinned via
//     the existing inFlight entry.
//
// The claim context is checked between stages (abortBatchOnRevokedClaim): a
// batch whose claim was revoked stops immediately and leaves its txs
// uncommitted for the next claim to replay, rather than dragging a dead ctx
// through register/broadcast/requeue.
//
// Failure paths are absorbed internally — there's no caller that reacts
// to an aggregate error here, so the function returns void.
// io is the channel set of the dispatcher that flushed this batch; every
// downstream requeue and terminal notification routes back through it.
func (p *Propagator) processBatch(ctx context.Context, batch []propagationMsg, io *dispatcherIO) {
	// The dispatcher detaches this goroutine (see flushBatch / the flush
	// tick), so the claim can be revoked before the batch even starts.
	if p.abortBatchOnRevokedClaim(ctx, len(batch)) {
		return
	}
	registered, failedRegister := p.registerBatch(ctx, batch)
	// A claim revoked DURING registerBatch has already collapsed some or all
	// /watch calls with context canceled — those "failures" are not merkle-
	// service verdicts, and broadcasting the survivors under the dead ctx
	// would only produce born-canceled submits. Stop before touching the
	// requeue or broadcast stages.
	if p.abortBatchOnRevokedClaim(ctx, len(batch)) {
		return
	}
	if len(failedRegister) > 0 {
		// Merkle registration retries are a different failure domain from
		// Teranode propagation. F-024: these txs are not broadcast.
		p.requeueMerkleRegistration(ctx, failedRegister, io)
	}
	if len(registered) == 0 {
		return
	}
	batch = registered

	// Batch-size only — no txid list. The full accepted-txid set is logged
	// (chunked, searchable) on the "transactions accepted by network" line
	// after broadcast; shipping a 5-item preview here under the canonical
	// "txids" key would collide with that full-list semantic and mislead a
	// txid search.
	p.logger.Info(
		"processing batch",
		logfields.TxIDCount(len(batch)),
	)

	metrics.PropagationBatchSize.Observe(float64(len(batch)))

	rawTxs := make([][]byte, len(batch))
	for i, msg := range batch {
		rawTxs[i] = msg.RawTx
	}

	// One span per flushed batch (never per message — see batchSpanMaxLinks)
	// links back to each tx's producer span, and its ctx becomes the parent
	// for broadcastInChunks so the otelhttp client spans to every teranode
	// endpoint nest under it. No-op (and effectively free) when telemetry is
	// disabled: startBroadcastSpan defers all string/attribute/link
	// construction behind span.IsRecording(). defer (rather than ending
	// immediately after broadcastInChunks) guarantees the span still closes
	// if anything downstream in this function panics.
	broadcastCtx, endSpan := p.startBroadcastSpan(ctx, batch)
	defer endSpan()
	results := p.broadcastInChunks(broadcastCtx, batch, rawTxs)

	seenEndpoints := make(map[string]struct{})
	var successEndpoints []string
	var accepted, rejected int
	terminalStatuses := make([]*models.TransactionStatus, 0, len(results))
	var toRequeue []propagationMsg
	var missingParentMsgs []propagationMsg
	var missingParentLines []string
	for i, res := range results {
		if res.successEndpoint != "" {
			if _, ok := seenEndpoints[res.successEndpoint]; !ok {
				seenEndpoints[res.successEndpoint] = struct{}{}
				successEndpoints = append(successEndpoints, res.successEndpoint)
			}
		}
		switch res.class {
		case txResultClassAccepted:
			accepted++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassRejected:
			rejected++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassMissingParent:
			// Condition, not verdict (#254/#295): resolved below against
			// BSV Broadcaster's own store before anything terminalizes.
			missingParentMsgs = append(missingParentMsgs, batch[i])
			missingParentLines = append(missingParentLines, res.errMsg)
		default:
			// Requeue / Unknown: transient infra failure. Collect for
			// requeue after a short flat wait so the dispatcher re-runs
			// admission (dep-aware) and the tx flows through the next
			// flush. errMsg is set when the peer named the fault (a
			// STORAGE_ERROR / SERVICE_ERROR line, or an opaque PROCESSING
			// line its 5xx status marked as a node fault); carrying it means
			// a tx that eventually exhausts its budget can quote what went
			// wrong instead of being handed a generic story.
			msg := batch[i]
			if res.errMsg != "" {
				msg.retryReason = res.errMsg
			}
			toRequeue = append(toRequeue, msg)
		}
	}

	if len(missingParentMsgs) > 0 {
		mpTxids := make([]string, len(missingParentMsgs))
		for i, m := range missingParentMsgs {
			mpTxids[i] = m.TXID
		}
		mpFields := append([]zap.Field{logfields.Stage(logfields.StageNetwork)}, logfields.TxIDBatch(mpTxids)...)
		p.logger.Warn(
			"teranode reported missing parents; treating as retryable condition, not verdict",
			mpFields...,
		)
		cascaded, requeue := p.resolveMissingParents(ctx, missingParentMsgs, missingParentLines, missingParentOutcomeRequeued)
		rejected += len(cascaded)
		terminalStatuses = append(terminalStatuses, cascaded...)
		toRequeue = append(toRequeue, requeue...)
	}

	p.applyTerminalStatuses(ctx, terminalStatuses, accepted, rejected, io)
	if len(toRequeue) > 0 {
		// A claim revoked during broadcast classifies every no-verdict tx
		// as Requeue (born-canceled submitCtx → instant SubmitTransactions
		// returns). Requeueing them under the dead ctx would only park a
		// goroutine that drops them 2s later, and the "batch propagated"
		// line below would report accepted=0 requeued=N for a batch that
		// never really broadcast. Stop here instead; any verdicts that DID
		// land before the revocation were already applied above.
		if p.abortBatchOnRevokedClaim(ctx, len(toRequeue)) {
			return
		}
		p.requeueAfterDelay(ctx, toRequeue, io)
	}
	metrics.PropagationOutcomeTotal.WithLabelValues("accepted").Add(float64(accepted))
	metrics.PropagationOutcomeTotal.WithLabelValues("rejected").Add(float64(rejected))
	metrics.PropagationOutcomeTotal.WithLabelValues("skipped").Add(float64(len(toRequeue)))

	p.logger.Info(
		"batch propagated",
		logfields.TxIDCount(len(batch)),
		zap.Int("accepted", accepted),
		zap.Int("rejected", rejected),
		zap.Int("requeued", len(toRequeue)),
		zap.Strings("success_endpoints", successEndpoints),
	)
}

// defaultRequeueDelay is the flat wait before a requeue lands back on the
// dispatcher. Long enough to ride out a brief upstream blip; short
// enough that submitter-visible latency stays acceptable. Tunable if
// observed retry traffic suggests a different cadence works better; carried
// on the Propagator as requeueDelay so tests can compress it.
const defaultRequeueDelay = 2 * time.Second

// retryStage names which fast-path budget a requeue debits. The two
// stages are independent failure domains.
type retryStage int

const (
	retryStageNetwork retryStage = iota
	retryStageMerkle
)

func (s retryStage) String() string {
	if s == retryStageMerkle {
		return logfields.RetryStageMerkle
	}
	return logfields.RetryStageNetwork
}

// requeueAfterDelay schedules a delayed NETWORK requeue of msgs through
// the dispatcher. Spawns a goroutine that sleeps p.requeueDelay then sends
// each msg to requeueCh. The goroutine bails on ctx cancellation so
// claim revocation and shutdown don't hold txs in limbo.
//
// Every requeue costs the tx one unit of its network propagation budget
// (propagation.retry_max_attempts). Txs whose budget is spent do NOT go
// round again: they are parked at PENDING_RETRY by parkExhaustedRequeues,
// which releases their dispatcher in-flight entry so the Kafka commit
// watermark can advance. That ceiling is the whole reason this function
// splits its input — see the incident write-up on parkExhaustedRequeues.
// Merkle /watch failures must not call this; they use
// requeueMerkleRegistration so they cannot exhaust this budget.
//
// Bailing on ctx.Done is safe for at-least-once delivery: a requeue
// dropped here leaves the tx in the dispatcher's inFlight set with its
// Kafka offset still un-Done on the (per-claim) offsetTracker, so that
// offset is never marked for commit. When the claim is replaced the
// next claim re-reads the tx from Kafka and reprocesses it — the
// uncommitted offset IS the retry. The drop must not be silent though:
// it's logged so a teardown that strands a large requeue batch is
// visible in the operator's logs rather than looking like lost work.
func (p *Propagator) requeueAfterDelay(ctx context.Context, msgs []propagationMsg, io *dispatcherIO) {
	p.requeueAfterDelayStaged(ctx, msgs, io, retryStageNetwork)
}

// requeueMerkleRegistration schedules a delayed Merkle-/watch requeue.
// It debits merkleRetryCount only. The tx stays unbroadcast (F-024) until
// registration succeeds. Claim cancellation is not charged.
func (p *Propagator) requeueMerkleRegistration(ctx context.Context, msgs []propagationMsg, io *dispatcherIO) {
	p.requeueAfterDelayStaged(ctx, msgs, io, retryStageMerkle)
}

func (p *Propagator) requeueAfterDelayStaged(ctx context.Context, msgs []propagationMsg, io *dispatcherIO, stage retryStage) {
	if len(msgs) == 0 {
		return
	}
	// Already-dead ctx (claim revoked / shutdown): drop NOW rather than park
	// a goroutine for requeueDelay that can only ever hit the ctx.Done arm
	// below. Same at-least-once contract and same Debug line as that arm —
	// the txs' offsets stay uncommitted and the next claim replays them —
	// minus the 2s of parked goroutines per aborted batch a rebalance used
	// to accumulate. Also skips the "transactions requeued for retry" Info,
	// which would be a lie: nothing is being requeued.
	//
	// Checked BEFORE the budget is charged: a claim revocation is not an
	// attempt, and charging for it would let a rebalance storm park txs that
	// were never actually broadcast.
	if ctx.Err() != nil {
		p.logger.Debug(
			"requeue dropped before delay elapsed; txs left uncommitted for replay",
			zap.Int("dropped", len(msgs)),
		)
		return
	}

	var exhausted []propagationMsg
	switch stage {
	case retryStageMerkle:
		msgs, exhausted = p.chargeMerkleRetryBudget(msgs)
		if len(exhausted) > 0 {
			p.parkExhaustedMerkleRegistrations(ctx, exhausted, io)
		}
	default:
		msgs, exhausted = p.chargeRetryBudget(msgs)
		if len(exhausted) > 0 {
			p.parkExhaustedRequeues(ctx, exhausted, io)
		}
	}
	if len(msgs) == 0 {
		return
	}
	metrics.PropagationRetryTotal.WithLabelValues(stage.String()).Add(float64(len(msgs)))

	requeueTxIDs := make([]string, len(msgs))
	for i, m := range msgs {
		requeueTxIDs[i] = m.TXID
	}
	// Failure-path only, so volume is low and a bounded TxIDBatch line
	// suffices. Plain component logger, not telemetry.LoggerWith: a
	// requeued batch aggregates txs from many unrelated producer traces,
	// so there is no single trace_id to attach. retry_stage separates
	// Merkle registration retries from network propagation retries; txids
	// stay in the log line and never on a metric label.
	stageName := logfields.StageNetwork
	line := "transactions requeued for retry"
	if stage == retryStageMerkle {
		stageName = logfields.StageMerkle
		line = "merkle registration requeued for retry"
	}
	requeueFields := append(
		[]zap.Field{logfields.Stage(stageName), logfields.RetryStage(stage.String())},
		logfields.TxIDBatch(requeueTxIDs)...,
	)
	p.logger.Info(line, requeueFields...)

	// Track pending requeue goroutines on the metric so sustained
	// upstream pressure shows up in dashboards without needing to
	// introspect goroutines. Per-call goroutine + timer is intentional
	// (see the requeueDelay comment) — the gauge is the observability
	// hook for catching the failure mode where TPS × requeueDelay
	// fans out further than capacity tolerates.
	metrics.PropagationPendingRequeues.Inc()
	go func(msgs []propagationMsg) {
		defer metrics.PropagationPendingRequeues.Dec()
		select {
		case <-time.After(p.requeueDelay):
		case <-ctx.Done():
			p.logger.Debug(
				"requeue dropped before delay elapsed; txs left uncommitted for replay",
				zap.Int("dropped", len(msgs)),
			)
			return
		}
		for i, m := range msgs {
			// requeueToDispatcher selects on ctx, so a teardown mid-loop
			// unblocks the send instead of leaking this goroutine on a
			// requeueCh whose dispatcher has already exited.
			if !p.requeueToDispatcher(ctx, io, m) {
				p.logger.Debug(
					"requeue dropped mid-batch; txs left uncommitted for replay",
					zap.Int("dropped", len(msgs)-i),
				)
				return
			}
		}
	}(msgs)
}

// chargeRetryBudget debits one attempt from every msg and splits the batch
// into the txs that may go round again and the txs whose budget is spent.
// The increment is on the local copy of each msg, which is exactly right:
// requeueToDispatcher sends the value on to the dispatcher, which stores it
// verbatim in pendingMsgs/heldMsgs, so the count rides with the tx through
// every subsequent flush on this claim.
func (p *Propagator) chargeRetryBudget(msgs []propagationMsg) (retry, exhausted []propagationMsg) {
	retry = make([]propagationMsg, 0, len(msgs))
	for _, m := range msgs {
		m.retryCount++
		if m.retryCount > p.retryMaxAttempts {
			exhausted = append(exhausted, m)
			continue
		}
		retry = append(retry, m)
	}
	return retry, exhausted
}

// chargeMerkleRetryBudget debits merkleRetryCount only. retryCount, the
// network propagation budget, is not touched.
func (p *Propagator) chargeMerkleRetryBudget(msgs []propagationMsg) (retry, exhausted []propagationMsg) {
	retry = make([]propagationMsg, 0, len(msgs))
	for _, m := range msgs {
		m.merkleRetryCount++
		if m.merkleRetryCount > p.merkleRetryMaxAttempts {
			exhausted = append(exhausted, m)
			continue
		}
		retry = append(retry, m)
	}
	return retry, exhausted
}

// parkExhaustedMerkleRegistrations moves txs that spent the fast-path
// Merkle /watch budget to PENDING_RETRY. The network propagation budget
// is not consumed: retryCount is unchanged, and
// PropagationRequeueExhaustedTotal (the "no network verdict" alert) does
// not move. The same applyTerminalStatuses path releases the dispatcher
// in-flight entry so the Kafka watermark can advance. The reaper later
// retries registration and, only after /watch succeeds, broadcast (F-024).
//
// This is not a new terminal status. PENDING_RETRY is the existing
// durable-retry state. A Merkle outage must not become REJECTED just
// because registration has not succeeded yet.
func (p *Propagator) parkExhaustedMerkleRegistrations(ctx context.Context, msgs []propagationMsg, io *dispatcherIO) {
	now := time.Now()
	txids := make([]string, len(msgs))
	statuses := make([]*models.TransactionStatus, len(msgs))
	base := fmt.Sprintf(
		"merkle-service /watch failed after %d registration attempts: retryable — parked for durable rebroadcast by the propagation reaper; network propagation budget was not consumed",
		p.merkleRetryMaxAttempts,
	)
	for i, m := range msgs {
		txids[i] = m.TXID
		reason := base
		if m.merkleFailureReason != "" {
			reason = base + "; last registration error: " + m.merkleFailureReason
		}
		statuses[i] = &models.TransactionStatus{
			TxID:      m.TXID,
			Status:    models.StatusPendingRetry,
			Timestamp: now,
			ExtraInfo: reason,
		}
	}

	metrics.PropagationMerkleRetryExhaustedTotal.Add(float64(len(msgs)))
	parkFields := append(
		[]zap.Field{
			zap.Int("attempts", p.merkleRetryMaxAttempts),
			logfields.Stage(logfields.StageMerkle),
			logfields.RetryStage(logfields.RetryStageMerkle),
		},
		logfields.TxIDBatch(txids)...,
	)
	p.logger.Error(
		"merkle registration retry budget exhausted; parking txs at PENDING_RETRY without consuming the network propagation budget",
		parkFields...,
	)

	p.applyTerminalStatuses(ctx, statuses, 0, 0, io)
	for _, m := range msgs {
		p.schedulePendingRetry(ctx, m.TXID, m.RawTx, m.merkleFailureReason)
	}
}

// parkExhaustedRequeues is the poison-batch escape hatch: it moves txs that
// spent their whole requeue budget without ever drawing a network verdict to
// PENDING_RETRY, which releases their dispatcher in-flight entry and lets the
// Kafka commit watermark advance past them.
//
// WHY this has to exist. Before it, "no verdict" meant "requeue", full stop,
// and a batch that could never succeed simply went round forever. On
// 2026-08-11 (dev-ovh-1, BSV Broadcaster v0.11.5) Teranode answered a 21-tx batch of
// stale transactions with HTTP 409 and 21 conflict lines of the form
// "UTXO_SPENT (70): <outpoint>:<vout> utxo already spent by tx <spender>[n]".
// Those lines key under the spent outpoint, not the submitted txid, so
// alienFailureLines correctly withheld implicit accepts (#292) and — with no
// attributable rejection either — every tx fell to the requeue arm. Result:
// ~2,410 409s in two minutes, 42 of 43 batches requeued, 4,679 txs stuck in
// the dispatcher's inFlight map pinning offsets, and 337,247 messages of lag
// on the single-partition bsv-broadcaster.propagation topic frozen solid while the
// leader pod burned 4.2 CPU cores. Every newly submitted transaction sat at
// RECEIVED until operators hand-seeked the consumer group past the backlog.
//
// WHY PENDING_RETRY and not REJECTED. The network requeue arm covers
// genuine infra failure too (no healthy peer, body-less 5xx). Rejecting
// on a ~10s budget would turn a brief Teranode outage into a wave of
// false terminal REJECTEDs. Merkle /watch failures are not this arm —
// they have their own budget and their own park path. PENDING_RETRY is
// honest — "no verdict yet" — keeps the row's raw bytes, and hands
// ownership to the reaper, which rebroadcasts PENDING_RETRY rows on its
// own (much slower, offset-free) cadence. Nothing is dropped; only the
// fast path gives up.
//
// The write goes through applyTerminalStatuses precisely because that is the
// one path that both persists the status AND notifies the dispatcher. A park
// that skipped the notify would leave the in-flight entry — and therefore the
// wedge — exactly as it was.
func (p *Propagator) parkExhaustedRequeues(ctx context.Context, msgs []propagationMsg, io *dispatcherIO) {
	now := time.Now()
	txids := make([]string, len(msgs))
	statuses := make([]*models.TransactionStatus, len(msgs))
	genericReason := fmt.Sprintf(
		"no network verdict after %d propagation attempts: retryable — parked for durable rebroadcast by the propagation reaper",
		p.retryMaxAttempts,
	)
	for i, m := range msgs {
		txids[i] = m.TXID
		// A tx that cycled here on a named condition gets a reason stating
		// it, so GET /tx explains WHY the fast path gave up instead of the
		// generic no-verdict text. The missing-parent wording is reserved
		// for a missing-parent line, mirroring giveUpReason: a node fault
		// (STORAGE_ERROR) or a batch-shape refusal quotes the peer as-is
		// rather than inventing a story about the transaction's ancestry.
		reason := genericReason
		switch {
		case m.retryReason == "":
		case lineIsMissingParentCondition(m.retryReason):
			reason = fmt.Sprintf(
				"parent not yet accepted by the network after %d propagation attempts: retryable — parked for durable rebroadcast by the propagation reaper; last error: %s",
				p.retryMaxAttempts,
				m.retryReason,
			)
		default:
			reason = genericReason + "; last network response: " + m.retryReason
		}
		statuses[i] = &models.TransactionStatus{
			TxID:      m.TXID,
			Status:    models.StatusPendingRetry,
			Timestamp: now,
			ExtraInfo: reason,
		}
	}

	metrics.PropagationRequeueExhaustedTotal.Add(float64(len(msgs)))
	// ERROR, not Warn: reaching this line means transactions got no answer
	// from any peer across the entire fast-path budget. It is always worth
	// waking someone up, and the bounded txid list is what an operator needs
	// to reproduce the failing submit against Teranode by hand.
	parkFields := append(
		[]zap.Field{
			zap.Int("attempts", p.retryMaxAttempts),
			logfields.Stage(logfields.StageNetwork),
			logfields.RetryStage(logfields.RetryStageNetwork),
		},
		logfields.TxIDBatch(txids)...,
	)
	p.logger.Error(
		"propagation retry budget exhausted with no network verdict; parking txs at PENDING_RETRY so the Kafka commit watermark can advance",
		parkFields...,
	)

	p.applyTerminalStatuses(ctx, statuses, 0, 0, io)

	// Hand the txs to the durable retry queue. applyTerminalStatuses above
	// wrote status=PENDING_RETRY and released the offset; this records the
	// per-row retry bins (retry_count, next_retry_at) the reaper drains by.
	//
	// Without this the reaper can only find a parked row by scanning
	// timestamp_at, which Postgres serves newest-first — and because a
	// failed rebroadcast never rewrote the row, its timestamp stayed frozen
	// at park time and it sank below every newer eligible row until it fell
	// out of the 24h scan window entirely.
	for _, m := range msgs {
		p.schedulePendingRetry(ctx, m.TXID, m.RawTx, m.retryReason)
	}
}

// defaultPendingRetryBackoff / defaultPendingRetryMaxBackoff /
// defaultPendingRetryMaxAttempts back the propagation.pending_retry_* knobs.
// The max backoff equals the flat eligibility age the reaper used before
// #299, so the slowest cadence is unchanged while early attempts are much
// faster; the attempt bound is 24h at that cap, which is when a parked row
// used to silently fall out of the scan window.
const (
	defaultPendingRetryBackoff     = 5 * time.Second
	defaultPendingRetryMaxBackoff  = 5 * time.Minute
	defaultPendingRetryMaxAttempts = 288
)

// nextPendingRetryDelay is the wait before attempt n+1 of a parked tx:
// exponential in the attempt count, capped at pendingRetryMaxBackoff, with
// ±20% jitter.
//
// Jitter matters more here than it looks. Every child of one late parent
// parks in the same batch with the same attempt count, so an unjittered
// schedule re-broadcasts them in lockstep — a synchronized burst at teranode
// and merkle-service every cycle, forever, for as long as the parent is
// missing.
func (p *Propagator) nextPendingRetryDelay(retryCount int) time.Duration {
	base := p.pendingRetryBackoff
	if base <= 0 {
		base = defaultPendingRetryBackoff
	}
	maxBackoff := p.pendingRetryMaxBackoff
	if maxBackoff < base {
		maxBackoff = base
	}
	delay := base
	for i := 1; i < retryCount && delay < maxBackoff; i++ {
		delay *= 2
	}
	if delay > maxBackoff {
		delay = maxBackoff
	}
	// ±20% jitter. Sourced from crypto/rand only because it is already the
	// package's rand import; nothing here is a security decision.
	var b [2]byte
	if _, err := rand.Read(b[:]); err == nil {
		frac := float64(uint16(b[0])<<8|uint16(b[1])) / 65535.0
		delay += time.Duration((frac*0.4 - 0.2) * float64(delay))
	}
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	return delay
}

// giveUpReason renders the terminal reason for a tx whose durable-retry budget
// ran out. It quotes what the network last said rather than asserting a cause,
// because by construction nothing here is a verdict about the transaction: the
// tx exhausted its attempts precisely because no peer ever returned one.
//
// The missing-parent wording is used only when the last thing we heard really
// was a missing parent — the case the give-up escape was built for, and the
// one where naming the cause genuinely helps the submitter.
func giveUpReason(maxAttempts int, lastReason string) string {
	switch {
	case lastReason == "":
		return fmt.Sprintf(
			"no network verdict after %d durable retry attempts: giving up — no peer ever answered for this transaction",
			maxAttempts,
		)
	case lineIsMissingParentCondition(lastReason):
		return fmt.Sprintf(
			"no network verdict after %d durable retry attempts: giving up — a parent this transaction spends has never reached the network; last network response: %s",
			maxAttempts, lastReason,
		)
	default:
		return fmt.Sprintf(
			"no network verdict after %d durable retry attempts: giving up; last network response: %s",
			maxAttempts, lastReason,
		)
	}
}

// schedulePendingRetry books a parked tx's next durable attempt, or gives up
// on it.
//
// Give-up is the property the PENDING_RETRY path was missing entirely. A tx
// spending an outpoint that does not exist — the most common client mistake
// there is — draws TX_MISSING_PARENT forever: rejectedAncestor finds no
// REJECTED ancestor because BSV Broadcaster has never seen the parent at all, so the
// row can never be condemned and can never succeed. Before #299 those rows
// accumulated without bound, monopolized the reaper's per-tick batch, and
// were finally abandoned at 24h in a non-terminal state with no signal of
// any kind. Now they terminate with a verdict the submitter can read.
//
// lastReason is the last thing the network actually said about this tx (a
// Teranode failure line, or an empty string when no peer ever answered). It is
// quoted verbatim in the give-up reason. Without it every exhausted tx was
// told the same story — that a parent it spends had never reached the network
// — which is true only for the missing-parent case that motivated the escape.
// A tx parked by a node-side fault, or by a verdict BSV Broadcaster could not attribute,
// got a confident explanation of something that never happened.
func (p *Propagator) schedulePendingRetry(ctx context.Context, txid string, rawTx []byte, lastReason string) {
	if p.store == nil || len(rawTx) == 0 {
		return
	}
	count, err := p.store.BumpRetryCount(ctx, txid)
	if err != nil {
		// The row may not exist yet on the intake path; the next reaper tick
		// re-derives everything it needs, so this is not fatal.
		p.logger.Warn("bump retry count failed; tx keeps its previous retry schedule",
			logfields.TxID(txid), zap.Error(err))
		return
	}
	if count > p.pendingRetryMaxAttempts {
		reason := giveUpReason(p.pendingRetryMaxAttempts, lastReason)
		metrics.PropagationPendingRetryTotal.WithLabelValues("exhausted").Inc()
		p.logger.Warn("durable retry budget exhausted; terminalizing parked tx as REJECTED",
			logfields.TxID(txid), zap.Int("attempts", count))
		if err := p.store.ClearRetryState(ctx, txid, models.StatusRejected, reason); err != nil {
			p.logger.Error("clear retry state failed", logfields.TxID(txid), zap.Error(err))
			return
		}
		// Same reason string that just went onto the row. A give-up is the
		// least self-evident rejection BSV Broadcaster issues — no peer ever answered
		// for this tx, so there is no verdict to quote and nothing about the
		// bytes explains it — which makes it the one most worth carrying on
		// the event rather than leaving to a GET /tx.
		p.publishRejections(ctx, []*models.TransactionStatus{{
			TxID:      txid,
			Status:    models.StatusRejected,
			ExtraInfo: reason,
		}}, time.Now())
		return
	}
	next := time.Now().Add(p.nextPendingRetryDelay(count))
	if err := p.store.SetPendingRetryFields(ctx, txid, rawTx, next, lastReason); err != nil {
		p.logger.Error("set pending retry fields failed; tx may not be re-broadcast",
			logfields.TxID(txid), zap.Error(err))
		return
	}
	metrics.PropagationPendingRetryTotal.WithLabelValues("scheduled").Inc()
}

// Per-batch chunk parallelism is now config-driven via
// cfg.Propagation.MaxParallelChunks (see Propagator.maxParallelChunks);
// defaults to defaultMaxParallelChunks.

// broadcastInChunks splits a batch into chunks bounded by both
// p.teranodeBatchCap transactions and p.teranodeBatchBytesCap payload bytes
// (see planChunks) and broadcasts each via /txs. Chunks run in parallel
// bounded by p.maxParallelChunks so a large flush doesn't serialize behind
// one slow endpoint. Returns per-tx results in the same order as the input.
// Precondition: len(batch) == len(rawTxs), index-aligned.
func (p *Propagator) broadcastInChunks(ctx context.Context, batch []propagationMsg, rawTxs [][]byte) []txResult {
	results := make([]txResult, len(batch))
	chunks := planChunks(rawTxs, p.teranodeBatchCap, p.teranodeBatchBytesCap)

	if len(chunks) <= 1 {
		if len(chunks) == 1 {
			c := chunks[0]
			p.broadcastChunk(ctx, batch[c.start:c.end], rawTxs[c.start:c.end], results[c.start:c.end])
		}
		return results
	}

	sem := make(chan struct{}, p.maxParallelChunks)
	var wg sync.WaitGroup
	for _, c := range chunks {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			p.broadcastChunk(ctx, batch[c.start:c.end], rawTxs[c.start:c.end], results[c.start:c.end])
		}()
	}
	wg.Wait()
	return results
}

// broadcastChunk broadcasts a single chunk (≤ teranodeBatchCap txs and, unless
// it is one oversize tx travelling alone, ≤ teranodeBatchBytesCap bytes) via
// POST /txs and writes per-tx classifications into out. /txs handles any
// chunk size — a chunk of one is just one tx's bytes — so there's a single
// classification path regardless of count.
func (p *Propagator) broadcastChunk(ctx context.Context, chunk []propagationMsg, rawTxs [][]byte, out []txResult) {
	chunkBytes := rawTxsBytes(rawTxs)
	metrics.PropagationChunkTotal.WithLabelValues("none").Inc()
	metrics.PropagationChunkBytes.Observe(float64(chunkBytes))
	if len(chunk) == 1 && p.teranodeBatchBytesCap > 0 && chunkBytes > p.teranodeBatchBytesCap {
		// planChunks never pairs an oversize tx with another, so this fires
		// once per oversize tx per broadcast attempt. It is sent regardless:
		// Teranode, not BSV Broadcaster, rules on per-tx size policy.
		p.logger.Warn(
			"transaction exceeds teranode_max_batch_bytes; broadcasting it alone",
			logfields.TxID(chunk[0].TXID),
			zap.Int("tx_bytes", chunkBytes),
			zap.Int("max_batch_bytes", p.teranodeBatchBytesCap),
		)
	}
	results, _, needsNarrowing := p.broadcastBatchToEndpoints(ctx, rawTxs, chunk, chunkBytes)
	if needsNarrowing && len(chunk) > 1 {
		p.narrowChunk(ctx, chunk, rawTxs, out)
		return
	}
	copy(out, results)
}

// narrowChunk re-broadcasts a chunk as two halves after a peer returned a
// failure line BSV Broadcaster could not bind to any submitted transaction.
//
// Teranode's public error boundary surfaces only the deepest allowlisted
// cause, discarding the "[ProcessTransaction][<txid>]" wrapper — so for whole
// classes of rejection (every Go-side consensus check, every BDK script and
// policy failure, the over-size guard) the line names no transaction at all.
// A chunk that draws one of those knows a transaction failed but not which.
// Requeueing the whole chunk on that basis is what produced the observed
// shape: the accepted transactions were re-broadcast pointlessly, the failing
// one looped until the durable-retry budget gave up, and the submitter was
// told a parent had never reached the network — a reason with no relationship
// to the actual verdict.
//
// Halving instead settles it. Each pass either lands the line on a chunk of
// one — where "named nobody" can only mean "named this one" — or discards the
// half that answers 200. Re-submitting an already-accepted transaction is
// idempotent: teranode answers ErrTxExists, which its own handler classifies
// as success. Cost is bounded at roughly 2·log2(n) extra round trips and only
// on the failure path; once teranode carries the txid on every failure line,
// nothing reaches here at all.
func (p *Propagator) narrowChunk(ctx context.Context, chunk []propagationMsg, rawTxs [][]byte, out []txResult) {
	metrics.PropagationChunkTotal.WithLabelValues("narrowed").Inc()
	mid := len(chunk) / 2
	p.logger.Info(
		"narrowing chunk: peer response cannot be bound to individual transactions",
		zap.Int("chunk_size", len(chunk)),
		zap.Int("split_at", mid),
	)
	p.broadcastChunk(ctx, chunk[:mid], rawTxs[:mid], out[:mid])
	p.broadcastChunk(ctx, chunk[mid:], rawTxs[mid:], out[mid:])
}

// isCanceledByBroadcast reports whether err is a context.Canceled directly
// caused by the broadcast's own cancel signal (i.e. the winning race). A
// context.Canceled that wasn't triggered by the broadcast cancel still counts
// as a real failure — e.g. the outer submitCtx timing out.
func isCanceledByBroadcast(broadcastCtx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if broadcastCtx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled)
}

// endpointOutcome is one (endpoint, statusCode) tuple ready for batched
// circuit-breaker accounting. Carried as a slice so recordBroadcastOutcomes
// can reason about the whole broadcast attempt at once.
type endpointOutcome struct {
	endpoint   string
	statusCode int
	// shapeRejected marks a peer that refused the chunk by shape
	// (teranode.ErrBatchTooLarge): reachable, but it judged nothing.
	shapeRejected bool
}

// recordBroadcastOutcomes applies circuit-breaker accounting to a complete
// set of per-endpoint outcomes from one broadcast attempt:
//
//   - statusCode == 0 (no HTTP response): RecordFailure (per-peer reachability).
//   - At least one 2xx in the set: each non-2xx is RecordBroadcastFailure,
//     each 2xx is RecordSuccess.
//   - Zero 2xx but every responder returned non-2xx: unanimous network reject.
//     The peers responded and they all agree the tx is bad — don't penalize
//     them for being correct. RecordSuccess for responders, RecordFailure for
//     transport errors.
func recordBroadcastOutcomes(tc *teranode.Client, outcomes []endpointOutcome) {
	if len(outcomes) == 0 {
		return
	}
	// Consensus is computed over VOTING outcomes only. A shape rejection
	// (teranode.ErrBatchTooLarge) proves the peer is reachable and says
	// nothing about the transactions, so it counts neither toward
	// unanimity nor against it.
	any2xx := false
	anyResponded := false
	anyVote := false
	for _, o := range outcomes {
		if o.shapeRejected {
			continue
		}
		anyVote = true
		if o.statusCode >= 200 && o.statusCode < 300 {
			any2xx = true
		}
		if o.statusCode != 0 {
			anyResponded = true
		}
	}
	unanimousReject := !any2xx && anyResponded
	switch {
	case !anyVote:
		// Every responder refused the chunk by shape: no verdict was cast,
		// so there is no consensus to count. PropagationChunkTotal
		// {fallback="size_rejected"} already records the event.
	case any2xx:
		metrics.PropagationBroadcastConsensus.WithLabelValues("accepted").Inc()
	case unanimousReject:
		metrics.PropagationBroadcastConsensus.WithLabelValues("unanimous_reject").Inc()
	default:
		// no 2xx, no non-zero responses → everyone was unreachable
		metrics.PropagationBroadcastConsensus.WithLabelValues("unreachable").Inc()
	}
	for _, o := range outcomes {
		switch {
		case o.statusCode == 0:
			tc.RecordFailure(o.endpoint)
		case o.statusCode >= 200 && o.statusCode < 300:
			tc.RecordSuccess(o.endpoint)
		case o.shapeRejected:
			// Neutral. Charging the slow-track breaker would sideline a
			// healthy peer whose body limit is merely below BSV Broadcaster's chunk
			// size; resetting it (the unanimous arm) would launder real
			// failures the same peer produced earlier.
		case unanimousReject:
			// Network consensus — peer responded, did its job. Reset its
			// counters so a long rejection storm doesn't progressively
			// sideline the entire fleet.
			tc.RecordSuccess(o.endpoint)
		default:
			tc.RecordBroadcastFailure(o.endpoint)
		}
	}
}

// submitBroadcastJobs enqueues one broadcast job per endpoint to the
// persistent worker pool, returning the number of jobs actually queued.
// Each job POSTs the same rawTxs slice to its endpoint.
//
// Sends block on the worker pool's job channel — when the pool is
// saturated, backpressure flows back to the caller (and ultimately to
// the dispatcher's pendingMsgs cap). ctx cancellation unblocks a stuck
// send so shutdown and per-claim revocation observe the cancel.
//
// In tests that construct a Propagator without Start() (broadcastRunning
// stays false), the channel is unbuffered for delivery and falls through
// to a per-endpoint goroutine — preserves test ergonomics without
// affecting production behavior.
func (p *Propagator) submitBroadcastJobs(ctx context.Context, endpoints []string, rawTxs [][]byte, resultCh chan<- broadcastJobResult) int {
	submitted := 0
	useChannel := p.broadcastRunning.Load()
	for _, endpoint := range endpoints {
		job := broadcastJob{
			ctx:      ctx,
			endpoint: endpoint,
			rawTxs:   rawTxs,
			resultCh: resultCh,
		}
		if useChannel {
			select {
			case p.broadcastJobs <- job:
				submitted++
				continue
			case <-ctx.Done():
				return submitted
			}
		}
		submitted++
		go func(j broadcastJob) {
			statusCode, failures, err := p.teranodeClient.SubmitTransactions(j.ctx, j.endpoint, j.rawTxs)
			select {
			case j.resultCh <- broadcastJobResult{endpoint: j.endpoint, statusCode: statusCode, failures: failures, err: err}:
			default:
			}
		}(job)
	}
	return submitted
}

// broadcastBatchToEndpoints submits a batch to each healthy teranode
// endpoint via /txs and aggregates every peer's response into a per-tx
// verdict:
//
//   - HTTP 200 from a peer → that peer accepted the whole batch, so
//     every tx in the batch gets a sticky acceptance vote.
//   - Non-200 with a parseable "Failed to process transactions:" body
//     (HTTP status is NOT part of the contract — Teranode's own /txs
//     handler returns 409 for the conflict family, 400/422 for policy
//     failures, and 500 only for unclassified server faults, all with the
//     same body shape; see httpStatusForTxError in Teranode's
//     services/propagation/Server.go) →
//     that peer rejected the listed txids and accepted the rest. Each
//     accepted tx gets an acceptance vote; each rejected tx gets a
//     rejection vote carrying the verbatim Teranode error line (this is
//     what lands in the wallet-visible ExtraInfo / SSE payload).
//     Implicit acceptance is only trusted when every failure line named a
//     submitted tx; conflict-family lines key under alien hashes (spent
//     outpoint / competing spender — no [ProcessTransaction] wrapper), in
//     which case the peer grants no implicit accepts. Alien lines are then
//     re-attributed by OUTPOINT (attributeConflictLines): the spent
//     "<txid>:<vout>" the line names is looked up against the inputs of the
//     submitted raw transactions, and a unique match rejects that tx with the
//     real verdict. A single-tx batch attributes any leftover alien verdict to
//     its only tx.
//   - Non-200 with no parseable body (gateway 502/503 "no available
//     server", bare 500, truncated body, network error, unexpected
//     2xx-but-not-200), or no healthy endpoints → no per-tx information
//     from that peer. These must NEVER become the terminal ExtraInfo on
//     their own — they are infra noise, not a validator verdict.
//
// After all responses are in (or the 15s submitCtx timeout fires), each
// tx is classified:
//
//   - Any acceptance vote → ACCEPTED_BY_NETWORK.
//   - Otherwise, at least one rejection vote → REJECTED with the
//     best (most informative) peer rejection line.
//   - Otherwise (no information from any peer) → Requeue.
//
// We treat any per-tx rejection line as terminal because Teranode wraps
// real validator failures as PROCESSING (catch-all "this tx is bad"),
// so a returned line cannot be reliably distinguished from a transient
// infra issue by code.
//
// Per-endpoint outcomes are recorded into the circuit-breaker. A peer
// whose non-2xx carried a per-tx verdict is shielded from sidelining only
// when NO peer accepted (the unanimous-reject branch of
// recordBroadcastOutcomes resets its counters); when another peer
// 2xx-accepts the same batch, a rejecting peer still accrues
// RecordBroadcastFailure — acceptable, since a healthy peer's next 2xx
// resets the counter, and a peer that persistently rejects batches its
// siblings accept is exactly the slow-track breaker's target. The
// returned successEndpoint is the URL of the first peer that returned
// HTTP 200 (empty when none did).
func (p *Propagator) broadcastBatchToEndpoints(ctx context.Context, rawTxs [][]byte, batch []propagationMsg, chunkBytes int) (results []txResult, successEndpoint string, needsNarrowing bool) {
	start := time.Now()
	defer func() {
		metrics.PropagationBroadcastDuration.WithLabelValues("batch").Observe(time.Since(start).Seconds())
	}()
	now := time.Now()
	endpoints := p.teranodeClient.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		p.logger.Error("no healthy teranode endpoints")
		return makeRequeueResults(batch), "", false
	}

	submitCtx, cancelSubmit := context.WithTimeout(ctx, 15*time.Second)
	defer cancelSubmit()
	broadcastCtx, cancelBroadcast := context.WithCancel(submitCtx)
	defer cancelBroadcast()

	resultCh := make(chan broadcastJobResult, len(endpoints))
	submitted := p.submitBroadcastJobs(broadcastCtx, endpoints, rawTxs, resultCh)

	// Precompute lowercase txids so failure-map lookups against
	// Teranode's lower-cased keys don't allocate inside the per-result
	// loop below. inBatch answers the reverse lookup — "does this failure
	// key name a tx we actually submitted?" — for alien-line detection.
	lowerTxids := make([]string, len(batch))
	inBatch := make(map[string]bool, len(batch))
	for i, msg := range batch {
		lowerTxids[i] = strings.ToLower(msg.TXID)
		inBatch[lowerTxids[i]] = true
	}

	// Per-tx accumulation across every peer response. Acceptance is
	// sticky: any peer that accepts a tx wins for that tx, regardless
	// of what other peers say or which response arrived first. A tx is
	// only REJECTED when every peer that gave us a parseable response
	// explicitly listed it as failed.
	outcomes := make([]endpointOutcome, 0, submitted)
	acceptedByAny := make([]bool, len(batch))
	rejectionLine := make([]string, len(batch))
	// missingParentLine collects TX_MISSING_PARENT / TX_NOT_FOUND lines
	// separately from real verdicts: they are conditions (#254), not
	// rejections, and rank below any genuine verdict in the final
	// classification. See lineIsMissingParentCondition and
	// txResultClassMissingParent.
	missingParentLine := make([]string, len(batch))
	// infraLine collects lines whose Teranode code names a node-side fault
	// (STORAGE_ERROR, SERVICE_ERROR, …) rather than anything about the
	// submitted bytes. Like missing-parent these are conditions, not verdicts:
	// a peer whose blob store is down has not judged the transaction, and
	// terminalizing on it makes a transient outage permanent. It also carries
	// a batch-shape refusal (teranode.ErrBatchTooLarge) for a tx nobody voted
	// on, so a chunk of one requeues with the peer's words as its reason.
	infraLine := make([]string, len(batch))
	// rejectionStatus records the HTTP status that accompanied each kept
	// rejection line, but only when that peer reported a single failure (see
	// statusHint below). classifyFailureLine uses it to recover meaning the
	// public error boundary stripped out of the message.
	rejectionStatus := make([]int, len(batch))
	// needsNarrowing is set when some peer returned a failure line that could
	// not be bound to a submitted transaction. The chunk must then be
	// re-broadcast in smaller pieces until every line lands; see
	// broadcastChunk.
	needsNarrowing = false
	// outpointOwners is the batch's spent-outpoint → tx index map used to
	// attribute conflict-family lines back to submitted txs. Built lazily and
	// at most once: it costs a full parse of every raw tx in the batch, which
	// is pure waste on the (overwhelmingly common) path where no peer returns
	// an alien-keyed failure line. nil means "not built yet".
	var outpointOwners map[string]int
	anySuccess := false

	// route files one failure line into the bucket that matches what it
	// actually says: a condition of the peer's view (missing parent), a fault
	// on the peer itself (infra), or a verdict about the transaction. statusHint
	// is the peer's HTTP status when it described exactly one transaction, and
	// 0 otherwise; it is the only way to tell the three apart when the cause
	// collapsed to Teranode's opaque PROCESSING catch-all.
	//
	// An opaque line with NO status evidence never terminalizes (#313). Teranode
	// answers 422 only for a missing parent, 403 only for a frozen utxo, 5xx
	// only for its own faults — but that mapping is per-transaction, and a body
	// reporting several failures aggregates them into one status. With nothing
	// left to distinguish "your bytes are bad" from "my store is down", a
	// rejection here would be the reasonless REJECTED this whole change exists
	// to remove. It retries instead, and terminates through the durable-retry
	// budget quoting the line it actually saw.
	route := func(idx int, line string, statusHint int) {
		switch {
		case lineIsMissingParentCondition(line),
			statusHint == http.StatusUnprocessableEntity && lineIsOpaqueProcessing(line):
			missingParentLine[idx] = preferRejectionLine(missingParentLine[idx], line)
		case lineIsInfraCondition(line),
			statusHint >= http.StatusInternalServerError && lineIsOpaqueProcessing(line),
			statusHint == 0 && lineIsOpaqueProcessing(line):
			infraLine[idx] = preferRejectionLine(infraLine[idx], line)
		default:
			kept := preferRejectionLine(rejectionLine[idx], line)
			if kept == line {
				rejectionStatus[idx] = statusHint
			}
			rejectionLine[idx] = kept
		}
	}
	// sizeRejected records that at least one peer refused the chunk by shape
	// (teranode.ErrBatchTooLarge, issue #271): no per-tx information, and
	// Teranode has already dispatched whatever it read before the trip. Such
	// a peer votes for nobody; any tx left without a vote makes the caller
	// narrow the chunk, and a chunk of one requeues quoting the peer.
	sizeRejected := false
	var sizeReason string
	for i := 0; i < submitted; i++ {
		result := <-resultCh
		if isCanceledByBroadcast(broadcastCtx, result.err) {
			continue
		}
		shapeRejected := errors.Is(result.err, teranode.ErrBatchTooLarge)
		outcomes = append(outcomes, endpointOutcome{endpoint: result.endpoint, statusCode: result.statusCode, shapeRejected: shapeRejected})

		if result.err == nil {
			// HTTP 200 — peer accepted the entire batch.
			p.logger.Debug(
				"batch broadcast endpoint succeeded",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("batch_bytes", chunkBytes),
			)
			if !anySuccess {
				successEndpoint = result.endpoint
			}
			anySuccess = true
			for j := range batch {
				acceptedByAny[j] = true
			}
			continue
		}

		if shapeRejected {
			sizeRejected = true
			sizeReason = fmt.Sprintf("teranode rejected the batch as submitted (%d txs, %d bytes): %v", len(batch), chunkBytes, result.err)
			p.logger.Warn(
				"teranode rejected chunk by size or count; no per-tx verdict",
				zap.String("endpoint", result.endpoint),
				zap.Int("status_code", result.statusCode),
				zap.Int("chunk_size", len(batch)),
				zap.Int("chunk_bytes", chunkBytes),
				zap.Bool("narrowable", len(batch) > 1),
				zap.Error(result.err),
			)
			continue
		}

		// Always log the human-visible error. When the response carried a
		// parseable Teranode failure-list, also log the per-txid lines —
		// otherwise operators only see "unexpected status code 500" and
		// cannot tell that the peer actually returned UTXO_SPENT /
		// PROCESSING (the structured body is not on err in that case).
		// Opaque gateway 502/503 bodies stay on err alone.
		if n := result.failures.Len(); n > 0 {
			p.logger.Warn(
				"batch broadcast endpoint failed",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("batch_bytes", chunkBytes),
				zap.Int("status_code", result.statusCode),
				zap.Error(result.err),
				zap.Int("failure_count", n),
				zap.Strings("failure_lines", failureLinesForLog(result.failures)),
			)
		} else {
			p.logger.Warn(
				"batch broadcast endpoint failed",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("batch_bytes", chunkBytes),
				zap.Int("status_code", result.statusCode),
				zap.Error(result.err),
			)
		}

		if result.failures.Len() == 0 {
			// Non-parseable failure — this peer carried no per-tx info.
			continue
		}
		// statusHint is the peer's HTTP status when it describes exactly one
		// transaction. Teranode derives the /txs status from the per-tx error
		// class and then aggregates (a 5xx dominates, otherwise the first 4xx
		// wins) — so with a single failure line there is nothing to aggregate
		// and the status describes that one transaction exactly. That makes it
		// a sound tiebreaker for the opaque PROCESSING catch-all, which is
		// otherwise indistinguishable between a missing parent (422), a frozen
		// utxo (403) and a node-side fault (500). With several failure lines
		// the status is ambiguous, so no hint is recorded.
		statusHint := 0
		if result.failures.Len() == 1 {
			statusHint = result.statusCode
		}

		// Parseable failure-list body (any non-2xx status): peer accepted
		// every tx not in the failure map and rejected the listed ones. An
		// acceptance vote from any peer makes the tx ACCEPTED; a rejection
		// vote only matters if no peer ever accepts. When several peers
		// reject the same tx with different lines, keep the most
		// informative one (specific Teranode codes over PROCESSING over
		// opaque text) so GET /tx ExtraInfo is useful to wallets.
		//
		// The "absent from map ⇒ accepted" half of the contract holds exactly
		// when every failure line this peer returned has been PLACED on a
		// submitted tx. Conflict-family lines (UTXO_SPENT et al., HTTP 409)
		// name the spent outpoint / the competing spender instead of the
		// submitted txid, so they key under an alien hash — see
		// alienFailureLines — and allowlisted causes name no hash at all. An
		// unplaced line means one unidentifiable submitted tx DID fail;
		// granting implicit accepts would mark that very tx
		// ACCEPTED_BY_NETWORK. Once outpoint attribution has placed every
		// alien line the failure set is fully accounted for, and withholding
		// the remaining accepts would only requeue transactions this peer
		// demonstrably took — the requeue churn #292 set out to stop.
		alienCount, bestAlien := alienFailureLines(result.failures.Keyed(), inBatch)
		// Alien lines are not a dead end. BSV Broadcaster holds the submitted raw
		// transactions, so the spent "<outpoint>:<vout>" a conflict line names
		// can be matched against the batch's own inputs and the verdict handed
		// to the tx that actually spends it. This is what stops the incident
		// shape from looping: on 2026-08-11 a 21-tx batch drew 21 UTXO_SPENT
		// 409 lines and, with no attributable verdict, requeued forever
		// (2,410 failures in two minutes, 42 of 43 batches requeued, 337,247
		// messages of Kafka lag frozen behind 4,679 in-flight txs).
		//
		// Attribution only ever ADDS rejection votes on an exact outpoint
		// match, and declines whenever a line could name more than one
		// submitted tx — so a placed line is placed unambiguously.
		var attributed map[int]string
		if alienCount > 0 {
			if outpointOwners == nil {
				outpointOwners = buildSubmittedOutpointIndex(batch)
			}
			attributed = attributeConflictLines(result.failures.Keyed(), inBatch, outpointOwners)
			p.logger.Warn(
				"teranode failure lines name no submitted tx",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("alien_line_count", alienCount),
				zap.Int("attributed_by_outpoint", len(attributed)),
				zap.String("best_alien_line", bestAlien),
			)
		}
		// unplaceable counts the lines this peer returned that could not be
		// bound to a submitted tx: lines naming no hash at all, plus alien
		// lines outpoint attribution could not resolve. Each one means a
		// submitted tx failed for a reason we cannot yet name — so this peer
		// vouches for nobody, and the caller narrows the chunk until the line
		// lands on a single tx.
		unattributedAliens := alienCount - len(attributed)
		if unattributedAliens < 0 {
			unattributedAliens = 0
		}
		unplaceable := unattributedAliens + len(result.failures.Unkeyed)
		if len(result.failures.Unkeyed) > 0 {
			p.logger.Warn(
				"teranode failure lines name no transaction; withholding implicit accepts from this peer",
				zap.String("endpoint", result.endpoint),
				zap.Int("batch_size", len(batch)),
				zap.Int("unkeyed_line_count", len(result.failures.Unkeyed)),
				zap.Strings("unkeyed_lines", result.failures.Unkeyed),
			)
		}
		if unplaceable > 0 {
			needsNarrowing = true
		}

		keyed := result.failures.Keyed()
		for j, lower := range lowerTxids {
			if line, failed := keyed[lower]; failed {
				// A missing-parent line keyed to a submitted tx is NOT a
				// verdict about its bytes — the peer processed the batch
				// and told us the parent isn't there yet. Route it to the
				// condition bucket; the siblings' implicit accepts above
				// are untouched (the line is in-batch-keyed, not alien).
				route(j, line, statusHint)
			} else if _, condemned := attributed[j]; !condemned && unplaceable == 0 {
				// Every line this peer returned has been placed, so the
				// failure set is fully accounted for and absence really does
				// mean acceptance — except for the txs an attributed line
				// condemns, which are absent from the map only because the
				// line named their spent outpoint rather than them. Granting
				// those an implicit accept would beat their own verdict:
				// acceptance is sticky.
				acceptedByAny[j] = true
			}
		}
		for idx, line := range attributed {
			// Attributed lines are conflict-family by construction, but the
			// condition check inside route is kept for defense: a mis-shaped
			// line must never become a terminal verdict via this path.
			route(idx, line, statusHint)
		}
		// A single-tx chunk has exactly one possible owner for a line that
		// named nobody — the tx itself. This is what makes narrowing
		// terminate: whatever teranode said, once the chunk is down to one
		// transaction the verdict is unambiguous. Covers both the mainnet
		// conflict shape (batch_size=1, HTTP 409, wrapper-less "UTXO_SPENT
		// (70): <outpoint> utxo already spent by tx <spender>[0]") and every
		// allowlisted cause whose message carries no txid at all.
		if unplaceable > 0 && len(batch) == 1 {
			route(0, bestUnplaceableLine(bestAlien, result.failures.Unkeyed), statusHint)
		}
	}
	if sizeRejected {
		metrics.PropagationChunkTotal.WithLabelValues("size_rejected").Inc()
		for i := range batch {
			if acceptedByAny[i] || rejectionLine[i] != "" || missingParentLine[i] != "" || infraLine[i] != "" {
				continue
			}
			// Nobody voted on this tx and a peer refused the chunk by shape:
			// halve if there is anything to halve (a sibling's sticky accept
			// or verdict above already settled the rest), and for a chunk of
			// one requeue carrying the peer's own words as the reason.
			needsNarrowing = true
			infraLine[i] = sizeReason
		}
	}
	recordBroadcastOutcomes(p.teranodeClient, outcomes)

	p.logger.Debug(
		"batch broadcast complete",
		zap.Int("batch_size", len(batch)),
		zap.Bool("any_success", anySuccess),
		zap.Int("endpoint_count", len(endpoints)),
	)

	results = make([]txResult, len(batch))
	for i, msg := range batch {
		switch {
		case acceptedByAny[i]:
			results[i] = txResult{
				class: txResultClassAccepted,
				status: &models.TransactionStatus{
					TxID:      msg.TXID,
					Status:    models.StatusAcceptedByNetwork,
					Timestamp: now,
				},
				rawTx:           msg.RawTx,
				successEndpoint: successEndpoint,
			}
		case rejectionLine[i] != "":
			errMsg, arcCode := classifyFailureLine(rejectionLine[i], rejectionStatus[i])
			var competing []string
			if arcCode == int(arcerrors.StatusConflict) {
				competing = competingSpenders(rejectionLine[i])
			}
			results[i] = txResult{
				class:  txResultClassRejected,
				errMsg: errMsg,
				status: &models.TransactionStatus{
					TxID:         msg.TXID,
					Status:       models.StatusRejected,
					StatusCode:   arcCode,
					Timestamp:    now,
					ExtraInfo:    errMsg,
					CompetingTxs: competing,
				},
				rawTx: msg.RawTx,
			}
		case missingParentLine[i] != "":
			// Condition, not verdict: no status write here. processBatch
			// consults the store (rejected ancestor → cascade REJECTED)
			// and otherwise requeues with this line as the retry reason.
			results[i] = txResult{
				class:  txResultClassMissingParent,
				errMsg: missingParentLine[i],
				rawTx:  msg.RawTx,
			}
		case infraLine[i] != "":
			// The peer faulted rather than judged. Requeue carrying the line
			// so the park reason (and the operator log) names the node fault
			// instead of inventing a story about the transaction.
			results[i] = txResult{
				class:  txResultClassRequeue,
				errMsg: infraLine[i],
				rawTx:  msg.RawTx,
			}
		default:
			results[i] = txResult{
				class: txResultClassRequeue,
				rawTx: msg.RawTx,
			}
		}
	}
	return results, successEndpoint, needsNarrowing
}

// makeRequeueResults builds a per-tx requeue result list for a batch
// that hit a pure infra failure (no healthy endpoints, no parseable
// per-slot body, etc.).
func makeRequeueResults(batch []propagationMsg) []txResult {
	results := make([]txResult, len(batch))
	for i, msg := range batch {
		results[i] = txResult{
			class: txResultClassRequeue,
			rawTx: msg.RawTx,
		}
	}
	return results
}
