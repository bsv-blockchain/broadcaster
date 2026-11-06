package propagation

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
)

// The dispatcher is a single-goroutine engine that owns all dep-aware
// state: inFlight, waiters, heldMsgs, and pendingMsgs. Every read and
// write to that state happens on this one goroutine — no locks
// anywhere.
//
// External components communicate via three channels:
//
//   - admitCh: handleMessage sends a propagationMsg here and waits for
//     a synchronous reply describing what happened (admitted to the
//     pending batch, or held as a waiter on in-flight parents).
//
//   - terminalCh: applyTerminalStatuses sends a terminalEvent here per
//     terminalized txid and waits for a terminalResult naming the
//     cascaded descendants the caller has to write REJECTED rows for.
//     Released waiters (parent ACCEPTED) are re-entered into
//     pendingMsgs by the dispatcher itself — the caller never touches
//     that slice.
//
//   - drainCh: flushBatch sends a drainRequest here to pull the
//     current pendingMsgs as a batch; the dispatcher hands over the
//     slice and clears its local state.
//
// Backpressure: when pendingMsgs is at maxPending the dispatcher
// excludes admitCh from its select, so handleMessage's send blocks
// and the Kafka consumer goroutine pauses pulling — backpressure
// flows back to the broker naturally, no DLQ.

// admitResult tells handleMessage what the dispatcher did with the
// admitted tx. Exactly one of admitted / held / duplicate is true.
// duplicate means the same txid was already in flight: the original
// admission still owns the broadcast and waiter graph, and the new
// message's Kafka offset has been added-and-immediately-marked-done
// on the tracker so it does not pin the commit watermark.
type admitResult struct {
	admitted  bool // true: tx was added to pendingMsgs, broadcast pending
	held      bool // true: registered as a waiter on in-flight parents
	duplicate bool // true: txid already in flight; offset bookkept and dropped
}

// admitRequest is the protocol between handleMessage and the
// dispatcher goroutine. offset is the Kafka offset the message came
// from; the dispatcher tracks it on the offsetTracker so the Kafka
// commit watermark cannot advance past unfinished work.
type admitRequest struct {
	msg    propagationMsg
	offset int64
	reply  chan admitResult
}

// terminalEvent is the protocol between applyTerminalStatuses and the
// dispatcher goroutine.
type terminalEvent struct {
	txid   string
	status models.Status
	reply  chan terminalResult
}

// terminalResult names cascaded descendants the caller has to write
// terminal REJECTED rows for. Released waiters (those whose blocking
// parents have all cleared) are NOT returned — the dispatcher
// re-enters them into pendingMsgs directly, so the next flushBatch
// picks them up without any caller action.
type terminalResult struct {
	cascaded []cascadedTx
}

// drainRequest is the protocol between flushBatch and the dispatcher.
// The dispatcher replies with the current pendingMsgs and clears its
// local pendingMsgs state.
type drainRequest struct {
	reply chan []propagationMsg
}

// requeueRequest sends an already-in-flight tx back through admission
// after a transient infra failure (Teranode infra slot, merkle /watch
// error, etc.). The tx is already in inFlight and its offset is already
// on the tracker — the dispatcher just re-checks parents and routes to
// either heldMsgs or pendingMsgs. No reply: the caller doesn't need to
// know which bucket the tx landed in.
type requeueRequest struct {
	msg propagationMsg
}

// dispatcherConfig is the small subset of Propagator config the
// dispatcher needs at runtime.
type dispatcherConfig struct {
	maxPending int
}

// dispatcherIO is one dispatcher instance's channel set. Pre-#295 these
// channels lived directly on the Propagator; with a multi-partition
// topic Sarama runs one runDispatcher per assigned partition claim, and
// a SHARED channel set would let partition B's loop consume partition
// A's terminal event — B's handleTerminal would no-op the unknown txid,
// A's offset would never be Done'd, and A's commit watermark would
// freeze permanently. Every claim therefore gets its own dispatcherIO;
// the batch pipeline spawned by a dispatcher carries its io along so
// replies route back to the loop whose inFlight map owns the tx.
//
// The reaper terminalizes txs off any claim's Kafka path — it passes a
// nil io to applyTerminalStatuses, which fans the event out to every
// registered dispatcher (at most one owns the tx; the rest no-op).
type dispatcherIO struct {
	admitCh    chan admitRequest
	requeueCh  chan requeueRequest
	terminalCh chan terminalEvent
	drainCh    chan drainRequest
}

// newDispatcherIO builds a channel set for one dispatcher instance.
// drainCh is unbuffered (request/reply handshake); the rest use
// dispatcherChannelBuffer to absorb bursts.
func newDispatcherIO() *dispatcherIO {
	return &dispatcherIO{
		admitCh:    make(chan admitRequest, dispatcherChannelBuffer),
		requeueCh:  make(chan requeueRequest, dispatcherChannelBuffer),
		terminalCh: make(chan terminalEvent, dispatcherChannelBuffer),
		drainCh:    make(chan drainRequest),
	}
}

// runDispatcher is the single state-owning loop. ALL dep-aware state
// (inFlight, waiters, heldMsgs, pendingMsgs, the offsetTracker, and the
// per-offset *kafka.Message references used for marking) lives in this
// function's local variables. Nothing else touches any of it — every
// mutation arrives via the channels read in the select.
//
// Two modes:
//
//   - Production: invoked as the kafka.ClaimHandler with a non-nil claim.
//     The loop runs on Sarama's per-claim goroutine; claim.Messages() is
//     the message source and claim.MarkMessage is called inline when an
//     offset terminalizes. Returns when the claim ends.
//   - Test: invoked with a nil claim from a goroutine started by tests.
//     admitCh is the message source; no MarkMessage calls.
//
// One goroutine, no locks, no atomics (the registry and depth mirrors on
// the Propagator are the deliberate exceptions — see registerDispatcher
// and the delta accounting below).
func (p *Propagator) runDispatcher(ctx context.Context, claim kafka.Claim, cfg dispatcherConfig, io *dispatcherIO) error {
	// Registration makes this loop reachable by the reaper's nil-io
	// fan-out; deregistering on exit keeps a dead loop's channels from
	// absorbing (and eventually blocking) fan-out sends.
	p.registerDispatcher(io)
	defer p.deregisterDispatcher(io)

	inFlight := make(map[string]int64)
	waiters := make(map[string]map[string]struct{})
	heldMsgs := make(map[string]propagationMsg)
	var pendingMsgs []propagationMsg
	tracker := newOffsetTracker()
	// marks queues consumed Kafka messages until the commit watermark
	// passes their offset; the 50ms flush tick (and the exit paths)
	// advance it. Only populated when claim != nil; in test mode
	// admitRequests carry no Kafka message and the queue stays empty.
	// See markQueue for why this is a queue, not a map (#295).
	marks := &markQueue{}

	var claimMsgCh <-chan hydratedMsg
	if claim != nil {
		claimMsgCh = p.hydrateClaim(ctx, claim.Messages())
	}

	// In production the ticker drives periodic flushes (50ms). In test
	// mode (nil claim) tests drive flushes explicitly via flushBatch /
	// drainCh, so a live ticker would race with the explicit drain.
	// flushTickC is nil in test mode, which (per the nil-channel-in-
	// select trick) means the ticker case never fires.
	var flushTickC <-chan time.Time
	if claim != nil {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		flushTickC = t.C
	}

	// Depth accounting is delta-based (#295): with one dispatcher per
	// partition claim, absolute Set()s from several loops would fight
	// last-writer-wins. Each loop Adds only its own change, so the gauge
	// and the Propagator-level atomic mirrors read as the POD TOTAL
	// across owned partitions — which is what dashboards and the
	// reaper's backlog log line want. The deferred removal zeroes this
	// loop's contribution on exit so a rebalanced-away partition doesn't
	// leave phantom depth behind.
	//
	// inFlight is the REAL backlog: every admitted-or-held tx whose
	// Kafka offset is pinned below the commit watermark but which
	// hasn't reached a terminal verdict. The pending gauge alone is
	// misleading under load — admission backpressure caps it at
	// maxPending, so during the 2026-08-10 load test a ~466k-tx
	// uncommitted-offset backlog was invisible behind a flat pending
	// depth.
	var prevPending, prevInflight int
	defer func() {
		metrics.PropagationPendingDepth.Sub(float64(prevPending))
		p.pendingDepth.Add(int64(-prevPending))
		metrics.PropagationInflightDepth.Sub(float64(prevInflight))
		p.inflightDepth.Add(int64(-prevInflight))
	}()

	for {
		// Observe depth changes at the top of every iteration: every
		// branch below that mutates pendingMsgs or inFlight is seen
		// exactly once before the next select fires.
		metrics.PropagationPendingDepth.Add(float64(len(pendingMsgs) - prevPending))
		p.pendingDepth.Add(int64(len(pendingMsgs) - prevPending))
		prevPending = len(pendingMsgs)
		metrics.PropagationInflightDepth.Add(float64(len(inFlight) - prevInflight))
		p.inflightDepth.Add(int64(len(inFlight) - prevInflight))
		prevInflight = len(inFlight)

		// Backpressure: nil-channel trick excludes incoming-message
		// sources from the select when pendingMsgs is at cap. Both
		// claim.Messages() and admitCh participate, so the same cap
		// applies in either mode.
		var admitChIfRoom <-chan admitRequest
		var claimChIfRoom <-chan hydratedMsg
		if cfg.maxPending <= 0 || len(pendingMsgs) < cfg.maxPending {
			admitChIfRoom = io.admitCh
			claimChIfRoom = claimMsgCh
		}

		select {
		case <-ctx.Done():
			// Final drain: commit anything the watermark already passed
			// so a clean teardown doesn't strand up to one tick's worth
			// of finished work for replay. Still-unfinished offsets are
			// never marked, so a revoked claim's in-flight work replays
			// exactly as before.
			marks.advance(claim, tracker)
			return nil

		case h, ok := <-claimChIfRoom:
			if !ok {
				// Claim channel closed → claim ended; exit cleanly so
				// Sarama can move on. Same final drain as ctx.Done.
				marks.advance(claim, tracker)
				return nil
			}
			msg := h.kmsg
			if h.drop != "" {
				// Malformed, or no raw tx to load (see hydrate). Queue the
				// offset with no tracker entry so it is marked once the
				// watermark reaches it, not ahead of in-flight offsets
				// below it.
				marks.push(msg)
				continue
			}
			propMsg := h.msg
			// Stash the producer's trace context (invalid/zero-value, at no
			// extra cost, when msg.Headers is nil — telemetry disabled or
			// the producer never injected one). processBatch links the
			// batch's broadcast span back to it; see startBroadcastSpan. The
			// derived context from ExtractTraceContext is discarded
			// immediately — only the SpanContext value is kept.
			//
			// CAVEAT: the base ctx must NOT carry an ambient span. For a
			// header-less message ExtractTraceContext returns the base ctx
			// unchanged, so an ambient span on it would be mis-attributed as
			// that message's producer span and linked onto the batch. The
			// dispatcher's ctx (service ctx → claim ctx) carries no span
			// today — nothing starts a root span around the propagation
			// service (only otelgin/otelhttp and the per-message kafka
			// spans create spans, none of which wrap this loop) — so ctx is
			// safe to use here.
			propMsg.spanCtx = trace.SpanContextFromContext(kafka.ExtractTraceContext(ctx, msg.Headers))
			handleAdmit(propMsg, msg.Offset, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
			marks.push(msg)

		case req := <-admitChIfRoom:
			res := handleAdmit(req.msg, req.offset, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
			req.reply <- res

		case req := <-io.requeueCh:
			handleRequeue(req.msg, inFlight, waiters, heldMsgs, &pendingMsgs)

		case ev := <-io.terminalCh:
			result := handleTerminal(ev, inFlight, waiters, heldMsgs, &pendingMsgs, tracker)
			ev.reply <- result
			// Marking moved to the flush tick: running the queue advance
			// here per terminal event was the O(terminals × in-flight)
			// serial hot spot of issue #295. The 50ms tick amortizes it
			// (and the queue makes each advance O(newly-markable)).

		case req := <-io.drainCh:
			batch := pendingMsgs
			pendingMsgs = nil
			req.reply <- batch

		case <-flushTickC:
			// Commit-watermark maintenance rides the existing production
			// ticker: no extra timer, stays on the dispatcher goroutine
			// (no-locks invariant), and runs whether or not there is a
			// batch to flush.
			marks.advance(claim, tracker)
			if len(pendingMsgs) == 0 {
				continue
			}
			// Non-blocking semaphore acquire: if all processBatch
			// slots are busy, leave pendingMsgs alone and try next
			// tick. The reads from claim/admit pause naturally
			// because the cap path keeps appending; backpressure
			// flows the right direction.
			select {
			case p.processBatchSem <- struct{}{}:
			default:
				continue
			}
			batch := pendingMsgs
			pendingMsgs = nil
			p.inflightBatches.Add(1)
			metrics.PropagationInflightBatches.Set(float64(len(p.processBatchSem)))
			go func() {
				defer func() {
					<-p.processBatchSem
					p.inflightBatches.Done()
					metrics.PropagationInflightBatches.Set(float64(len(p.processBatchSem)))
				}()
				p.processBatch(ctx, batch, io)
			}()
		}
	}
}

// handleAdmit decides what to do with a new tx based on whether any
// of its inputs are currently in flight:
//
//   - Parent in inFlight → block this child. Teranode processes bulk
//     submissions in parallel so we can't trust ordering across
//     concurrent /txs calls; the child has to wait until every parent
//     has terminalized.
//   - Parent not in inFlight → already mined, never seen by BSV Broadcaster,
//     or otherwise out of scope. Doesn't block this admit.
//
// If ANY input requires holding, the whole tx is held as a waiter on
// every blocking parent. Otherwise the tx is admitted to inFlight
// and appended to pendingMsgs.
//
// The maxPending cap is enforced upstream by the select loop's
// nil-channel pattern; handleAdmit never sees a full-queue admit.
//
// The Kafka offset is recorded on tracker so the commit watermark
// can never advance past this in-flight tx.
func handleAdmit(
	msg propagationMsg,
	offset int64,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
	tracker *offsetTracker,
) admitResult {
	// Duplicate admission: the same txid is already in flight (Kafka
	// redelivery, or a publisher that slipped past the intake dedup
	// CAS). The original entry still owns the broadcast and waiter
	// graph — overwriting inFlight[msg.TXID] would strand its offset
	// on the tracker forever, freezing the commit watermark. Add the
	// new offset and immediately mark it done so advanceMarks can
	// flush it once the original terminalizes, and return without
	// touching pendingMsgs.
	//
	// Scope, since #295: inFlight is per-claim, so this suppresses
	// duplicates only on the SAME partition. It is no longer a process-wide
	// guard and never was a fleet-wide one. Intake's republish branches —
	// the REJECTED resubmit and the stranded-RECEIVED retry — can still
	// publish a txid that is in flight elsewhere (a store error sheds with
	// 503 and publishes nothing, since #329). Family keys are computed over the whole
	// submitted batch, so batch composition no longer moves a txid between
	// partitions, but a resubmission whose family genuinely differs still
	// can. The consequence is a duplicate broadcast and racing terminal
	// writes, which the status lattice absorbs — not something this function
	// guarantees against.
	if _, exists := inFlight[msg.TXID]; exists {
		tracker.Add(offset)
		tracker.Done(offset)
		return admitResult{duplicate: true}
	}

	// Identify blocking parents — any input that's currently in
	// flight, regardless of whether it's in pendingMsgs or already
	// broadcasting. Teranode's parallel bulk processing means we
	// can't trust same-batch ordering.
	var blocking map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, inFlt := inFlight[parent]; !inFlt {
			continue
		}
		if blocking == nil {
			blocking = make(map[string]struct{})
		}
		blocking[parent] = struct{}{}
	}

	// Record the offset on the tracker for either branch — both held
	// and admitted txs are in-flight from the consumer's perspective
	// and must pin the commit watermark below this offset.
	inFlight[msg.TXID] = offset
	tracker.Add(offset)

	if len(blocking) > 0 {
		// Hold as a waiter. Held txs DO go into inFlight (so
		// descendants can register on them) but NOT into pendingMsgs
		// (they're not on the broadcast path yet).
		for parent := range blocking {
			set, ok := waiters[parent]
			if !ok {
				set = make(map[string]struct{})
				waiters[parent] = set
			}
			set[msg.TXID] = struct{}{}
		}
		heldMsgs[msg.TXID] = msg
		return admitResult{held: true}
	}

	// Eligible for broadcast. Add to pendingMsgs.
	*pendingMsgs = append(*pendingMsgs, msg)
	return admitResult{admitted: true}
}

// handleRequeue re-routes an already-in-flight tx after a transient
// infra failure. The tx is already in inFlight (from the original
// handleAdmit) and its offset is already on the tracker, so this only
// has to re-check parents and place the msg into either heldMsgs or
// pendingMsgs.
//
// A tx whose own status flipped terminal (e.g. via a sibling's cascade)
// won't be in inFlight anymore; in that case the requeue is a no-op.
func handleRequeue(
	msg propagationMsg,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) {
	if _, ok := inFlight[msg.TXID]; !ok {
		// Terminalized between the failed broadcast and the requeue
		// (e.g. cascade-rejected by a parent). Drop on the floor.
		return
	}

	var blocking map[string]struct{}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == msg.TXID {
			continue
		}
		if _, inFlt := inFlight[parent]; !inFlt {
			continue
		}
		if blocking == nil {
			blocking = make(map[string]struct{})
		}
		blocking[parent] = struct{}{}
	}

	if len(blocking) > 0 {
		for parent := range blocking {
			set, ok := waiters[parent]
			if !ok {
				set = make(map[string]struct{})
				waiters[parent] = set
			}
			set[msg.TXID] = struct{}{}
		}
		heldMsgs[msg.TXID] = msg
		return
	}

	*pendingMsgs = append(*pendingMsgs, msg)
}

// handleTerminal processes a terminal status flip for txid. ACCEPTED
// releases direct waiters whose other-parent set has also cleared —
// each released waiter goes into pendingMsgs (and its own waiters
// stay held until IT terminalizes; no recursive cascade). REJECTED
// recursively cascade-rejects every descendant. PENDING_RETRY (the
// requeue-budget escape) behaves structurally like REJECTED.
//
// All three branches mark the txid's offset Done on the tracker (and every
// cascaded descendant's offset too) so the Kafka commit watermark can
// advance past them.
//
// PENDING_RETRY is included precisely BECAUSE it is not a verdict. A tx that
// burned its whole requeue budget without an answer from any peer must still
// stop pinning its Kafka offset — otherwise it keeps LowestUnfinished()
// frozen and, on a single-partition topic, blocks every message behind it.
// That is the 2026-08-11 dev-ovh-1 wedge in one sentence: 4,679 no-verdict
// txs held 337,247 messages of lag stationary because nothing ever released
// their in-flight entries. The tx is not abandoned — the reaper rebroadcasts
// PENDING_RETRY rows from the store, off the Kafka offset path entirely.
//
// Held descendants are cascaded (not released into pendingMsgs) for the same
// reason they are on the REJECTED branch: their parent never reached
// ACCEPTED, so broadcasting them now would just draw TX_MISSING_PARENT. They
// ride the same durable reaper retry as their parked ancestor — the CALLER
// writes PENDING_RETRY rows for them, not REJECTED (see persistCascade).
func handleTerminal(
	ev terminalEvent,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
	tracker *offsetTracker,
) terminalResult {
	switch ev.status {
	case models.StatusAcceptedByNetwork:
		if offset, ok := inFlight[ev.txid]; ok {
			tracker.Done(offset)
		}
		delete(inFlight, ev.txid)
		releaseWaiters(ev.txid, inFlight, waiters, heldMsgs, pendingMsgs)
		return terminalResult{}

	case models.StatusRejected, models.StatusPendingRetry:
		if offset, ok := inFlight[ev.txid]; ok {
			tracker.Done(offset)
		}
		delete(inFlight, ev.txid)
		return terminalResult{
			cascaded: cascadeReject(ev.txid, inFlight, waiters, heldMsgs, tracker),
		}

	default:
		// Intermediate statuses don't change dispatcher state and
		// don't advance the offset watermark.
		return terminalResult{}
	}
}

// releaseWaiters processes a parent that just terminalized ACCEPTED.
// Walks waiters[parent] one level deep. For each child, canRelease
// checks whether the child has any OTHER in-flight parents — if all
// are cleared, the child moves into pendingMsgs. No recursion: a
// released child's own waiters stay held until the child itself
// terminalizes (Teranode processes batches in parallel, so child and
// grandchild can't share a batch).
func releaseWaiters(
	parentTxID string,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	pendingMsgs *[]propagationMsg,
) {
	children, ok := waiters[parentTxID]
	if !ok {
		return
	}
	delete(waiters, parentTxID)
	for child := range children {
		msg, ready := canRelease(child, inFlight, heldMsgs)
		if !ready {
			continue
		}
		cleanupWaiterEntries(child, parentTxID, waiters, heldMsgs)
		delete(heldMsgs, child)
		*pendingMsgs = append(*pendingMsgs, msg)
	}
}

// canRelease asks: are all of child's blocking parents resolved?
// A parent is "blocking" if it's still in inFlight. Recomputes the
// answer from heldMsgs[child].InputTXIDs at call time — we don't
// maintain a per-child pending-parent count.
func canRelease(
	child string,
	inFlight map[string]int64,
	heldMsgs map[string]propagationMsg,
) (propagationMsg, bool) {
	msg, ok := heldMsgs[child]
	if !ok {
		return propagationMsg{}, false
	}
	for _, parent := range msg.InputTXIDs {
		if parent == "" || parent == child {
			continue
		}
		if _, inFlt := inFlight[parent]; inFlt {
			return propagationMsg{}, false
		}
	}
	return msg, true
}

// cascadeReject walks the dep graph from rejectedTxID forward and
// returns every descendant that should be terminally rejected. The
// caller writes a REJECTED row for each, with "parent rejected" as
// the ExtraInfo — the descendants didn't fail for any reason of
// their own, only because an ancestor did. Every cascaded descendant's
// Kafka offset is marked Done on the tracker so the commit watermark
// can advance past them once the caller writes the REJECTED rows.
// cascadedTx is one descendant terminalized by a cascade, carrying the raw
// bytes captured from its held message so a parked descendant can be booked
// into the durable retry queue.
type cascadedTx struct {
	txid  string
	rawTx []byte
}

func cascadeReject(
	rejectedTxID string,
	inFlight map[string]int64,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
	tracker *offsetTracker,
) []cascadedTx {
	var cascaded []cascadedTx
	queue := []string{rejectedTxID}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		children, ok := waiters[parent]
		if !ok {
			continue
		}
		delete(waiters, parent)
		for child := range children {
			cleanupWaiterEntries(child, parent, waiters, heldMsgs)
			// Capture the raw bytes BEFORE dropping the held message. A
			// descendant parked by this cascade has to enter the durable
			// retry queue, and that needs a body to rebroadcast — without it
			// the row would sit at PENDING_RETRY with no next_retry_at, which
			// GetReadyRetries cannot see.
			rawTx := heldMsgs[child].RawTx
			delete(heldMsgs, child)
			if offset, ok := inFlight[child]; ok {
				tracker.Done(offset)
			}
			delete(inFlight, child)
			cascaded = append(cascaded, cascadedTx{txid: child, rawTx: rawTx})
			queue = append(queue, child)
		}
	}
	return cascaded
}

// cleanupWaiterEntries removes child from every OTHER parent's
// waiters set, where "other" means parents in heldMsgs[child].InputTXIDs
// other than skipParent (the one being processed by the calling
// release / cascade walk). Prevents dangling waiter entries that
// would otherwise show up when those other parents terminalize
// later.
func cleanupWaiterEntries(
	child, skipParent string,
	waiters map[string]map[string]struct{},
	heldMsgs map[string]propagationMsg,
) {
	msg, ok := heldMsgs[child]
	if !ok {
		return
	}
	for _, otherParent := range msg.InputTXIDs {
		if otherParent == "" || otherParent == child || otherParent == skipParent {
			continue
		}
		set, ok := waiters[otherParent]
		if !ok {
			continue
		}
		delete(set, child)
		if len(set) == 0 {
			delete(waiters, otherParent)
		}
	}
}

// dispatcherChannelBuffer sizes the dispatcher's input channels. The
// reply protocol means handleMessage and applyTerminalStatuses block
// on a goroutine round-trip per call, but the buffer absorbs short
// bursts so a momentarily slow dispatcher (during a large cascade
// walk, say) doesn't immediately stall the consumer or broadcast
// workers.
const dispatcherChannelBuffer = 256

// registerDispatcher / deregisterDispatcher maintain the set of live
// dispatcher loops. The registry exists for exactly one caller: the
// reaper's nil-io fan-out in notifyTerminalToDispatcher. Everything on
// a claim's own pipeline carries its dispatcherIO explicitly.
func (p *Propagator) registerDispatcher(io *dispatcherIO) {
	p.dispatchersMu.Lock()
	p.dispatchers[io] = struct{}{}
	p.dispatchersMu.Unlock()
}

func (p *Propagator) deregisterDispatcher(io *dispatcherIO) {
	p.dispatchersMu.Lock()
	delete(p.dispatchers, io)
	p.dispatchersMu.Unlock()
}

// dispatcherTargets resolves which dispatcher(s) an event should reach:
// the explicit io when the caller runs on a claim's pipeline, or a
// snapshot of every live dispatcher for io == nil (reaper path — the
// owning dispatcher, if any, releases the tx; the rest no-op).
func (p *Propagator) dispatcherTargets(io *dispatcherIO) []*dispatcherIO {
	if io != nil {
		return []*dispatcherIO{io}
	}
	p.dispatchersMu.RLock()
	defer p.dispatchersMu.RUnlock()
	targets := make([]*dispatcherIO, 0, len(p.dispatchers))
	for d := range p.dispatchers {
		targets = append(targets, d)
	}
	return targets
}

// admitToDispatcher is the consumer-side helper. Sends the tx and its
// Kafka offset, waits for the dispatcher's verdict, returns it. Only
// used by the test-mode message path, so it always targets the default
// dispatcher.
func (p *Propagator) admitToDispatcher(msg propagationMsg, offset int64) admitResult {
	reply := make(chan admitResult, 1)
	p.defaultIO.admitCh <- admitRequest{msg: msg, offset: offset, reply: reply}
	return <-reply
}

// notifyTerminalToDispatcher is the post-broadcast helper. Tells the
// dispatcher the txid reached terminal status, returns the cascaded
// descendants (caller writes REJECTED rows for them).
//
// io names the dispatcher whose pipeline produced this terminal; a nil
// io (reaper path — no claim of its own) fans the event out to every
// registered dispatcher and merges the cascades: at most one dispatcher
// owns the tx, the rest no-op, preserving the pre-#295 behavior where a
// reaper verdict could release a tx wedged in a live dispatcher.
//
// Both the send and the reply receive are guarded by ctx. processBatch
// goroutines outlive the dispatcher on a claim teardown (rebalance /
// shutdown): the dispatcher loop returns as soon as its ctx is canceled,
// so an unguarded reply receive here would block this goroutine forever
// on a dispatcher that will never answer. On ctx cancellation the notify
// is abandoned and a zero terminalResult is returned — the tx's Kafka
// offset is simply left un-Done and uncommitted, and the next claim
// replays it (at-least-once). reply is buffered so a late dispatcher
// answer after we've stopped waiting never blocks the dispatcher.
func (p *Propagator) notifyTerminalToDispatcher(ctx context.Context, io *dispatcherIO, txid string, status models.Status) terminalResult {
	var merged terminalResult
	for _, target := range p.dispatcherTargets(io) {
		r, ok := p.notifyTerminalToOne(ctx, target, txid, status)
		if !ok {
			// ctx died mid-fan-out; whatever we already collected is
			// still valid cascade output.
			break
		}
		merged.cascaded = append(merged.cascaded, r.cascaded...)
	}
	return merged
}

// notifyTerminalToOne performs the single-dispatcher send/reply
// round-trip described on notifyTerminalToDispatcher. The bool is false
// when ctx was canceled before a reply was obtained.
func (p *Propagator) notifyTerminalToOne(ctx context.Context, io *dispatcherIO, txid string, status models.Status) (terminalResult, bool) {
	reply := make(chan terminalResult, 1)
	select {
	case io.terminalCh <- terminalEvent{txid: txid, status: status, reply: reply}:
	case <-ctx.Done():
		return terminalResult{}, false
	}
	select {
	case r := <-reply:
		return r, true
	case <-ctx.Done():
		// ctx and reply can both be ready at once, and select picks a
		// ready case at random — so a teardown could win even though the
		// dispatcher already processed the event and replied. That reply
		// carries cascaded txids the caller must persist; dropping it
		// leaves those REJECTED rows unwritten even though the dispatcher
		// already advanced the offset. Prefer a ready reply over the
		// cancellation with a final non-blocking read.
		select {
		case r := <-reply:
			return r, true
		default:
			return terminalResult{}, false
		}
	}
}

// drainPending asks the default dispatcher for the current pendingMsgs
// as a batch (test-mode path only — production flushes happen inside
// runDispatcher's own tick). The dispatcher clears its slice and hands
// the snapshot to the caller; the caller owns it fully and processBatch
// can mutate it as needed.
func (p *Propagator) drainPending() []propagationMsg {
	reply := make(chan []propagationMsg, 1)
	p.defaultIO.drainCh <- drainRequest{reply: reply}
	return <-reply
}

// requeueToDispatcher sends a tx back through admission after a
// transient infra failure. Fire-and-forget — the dispatcher's reply
// (held vs pending) doesn't change anything the caller can act on.
// The send is buffered (dispatcherChannelBuffer); a momentarily
// saturated buffer applies natural backpressure to the caller.
//
// The send is guarded by ctx: when the claim is torn down the
// dispatcher stops draining requeueCh, so an unguarded send would block
// (and leak) the requeue goroutine once the buffer fills. Returns true
// if the requeue was handed to the dispatcher, false if ctx was
// canceled first — in which case the tx is left uncommitted for the
// next claim to replay.
func (p *Propagator) requeueToDispatcher(ctx context.Context, io *dispatcherIO, msg propagationMsg) bool {
	select {
	case io.requeueCh <- requeueRequest{msg: msg}:
		return true
	case <-ctx.Done():
		return false
	}
}
