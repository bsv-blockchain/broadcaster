package propagation

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/metrics"
)

// Propagation messages carry the txid only (#329). The raw transaction lives
// on the store row intake wrote before publishing, so the Kafka message size
// no longer has to track the tx-size policy. Hydration loads it back before
// the dispatcher admits the message. Messages from a pre-#329 producer still
// carry raw_tx inline and skip the store read, so a rolling deploy that
// updates propagation before api-server loses nothing.

// hydrateBatchMax caps how many claim messages share one store read.
const hydrateBatchMax = 500

// Store-read backoff while hydrating. The batch is retried until it
// succeeds or the claim ends: skipping it would strand txs at RECEIVED
// until the reaper's staleReceivedAge, and the claim's commit watermark
// cannot pass an unhydrated offset anyway.
//
// After hydrateIsolateAfter failed batch reads each message is read on its
// own, so one row the store keeps failing on drops alone instead of
// stalling the claim. hydrateOutageProbe consecutive single reads failing
// with none succeeding reads as the store being down: back to retrying the
// batch, without paying one read per message on every attempt.
const (
	hydrateRetryInitial = 100 * time.Millisecond
	hydrateRetryMax     = 5 * time.Second
	hydrateIsolateAfter = 3
	hydrateOutageProbe  = 8
)

// Drop reasons, also the PropagationHydrateDroppedTotal labels.
const (
	dropMalformed    = "malformed"
	dropRowMissing   = "row_missing"
	dropTerminal     = "terminal"
	dropRawTxMissing = "raw_tx_missing"
	dropLookupFailed = "lookup_failed"
)

// hydratedMsg is one claim message after hydration. drop is non-empty when
// the message cannot be propagated; the dispatcher still queues its offset
// for marking so the watermark moves past it.
type hydratedMsg struct {
	kmsg *kafka.Message
	msg  propagationMsg
	drop string
}

// hydrateClaim decodes and hydrates claim messages on a goroutine of its
// own, so the store read stays off the dispatcher loop, and emits them in
// claim order: per-partition order is what delivers parents before
// children (#295). The returned channel closes when in closes or ctx ends.
func (p *Propagator) hydrateClaim(ctx context.Context, in <-chan *kafka.Message) <-chan hydratedMsg {
	out := make(chan hydratedMsg)
	go func() {
		defer close(out)
		batch := make([]*kafka.Message, 0, hydrateBatchMax)
		for {
			batch = batch[:0]
			select {
			case <-ctx.Done():
				return
			case m, ok := <-in:
				if !ok {
					return
				}
				batch = append(batch, m)
			}
			// Take whatever else is already buffered, without waiting: a
			// quiet partition pays no linger, a busy one shares the read.
		fill:
			for len(batch) < hydrateBatchMax {
				select {
				case m, ok := <-in:
					if !ok {
						break fill
					}
					batch = append(batch, m)
				default:
					break fill
				}
			}

			hs := make([]hydratedMsg, len(batch))
			msgs := make([]propagationMsg, len(batch))
			for i, m := range batch {
				hs[i].kmsg = m
				if err := json.Unmarshal(m.Value, &msgs[i]); err != nil {
					p.logger.Warn("decoding propagation message", zap.Int64("offset", m.Offset), zap.Error(err))
					hs[i].drop = dropMalformed
					metrics.PropagationHydrateDroppedTotal.WithLabelValues(dropMalformed).Inc()
				}
			}
			drops, ok := p.hydrateWithRetry(ctx, msgs, hs)
			if !ok {
				return
			}
			for i := range hs {
				if hs[i].drop == "" {
					hs[i].drop = drops[i]
				}
				hs[i].msg = msgs[i]
				select {
				case <-ctx.Done():
					return
				case out <- hs[i]:
				}
			}
		}
	}()
	return out
}

// hydrateWithRetry runs hydrate until the store read succeeds. Messages
// already marked malformed in hs are skipped. ok is false when ctx ended
// first.
func (p *Propagator) hydrateWithRetry(ctx context.Context, msgs []propagationMsg, hs []hydratedMsg) (drops []string, ok bool) {
	skip := make([]bool, len(msgs))
	for i := range hs {
		skip[i] = hs[i].drop != ""
	}
	backoff := hydrateRetryInitial
	for attempt := 1; ; attempt++ {
		drops, err := p.hydrate(ctx, msgs, skip)
		if err == nil {
			return drops, true
		}
		if ctx.Err() != nil {
			return nil, false
		}
		metrics.PropagationHydrateErrorsTotal.Inc()
		if attempt >= hydrateIsolateAfter {
			if drops, ok := p.hydrateEach(ctx, msgs, skip); ok {
				return drops, true
			}
			if ctx.Err() != nil {
				return nil, false
			}
		}
		p.logger.Warn("loading raw txs for propagation messages; retrying",
			zap.Int("count", len(msgs)), zap.Duration("backoff", backoff), zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, hydrateRetryMax)
	}
}

// hydrateEach reads each message's row on its own once the batch read keeps
// failing. A message whose read fails while others succeed is dropped as
// lookup_failed: its row stays RECEIVED with raw_tx, so the reaper
// rebroadcasts it after staleReceivedAge. ok is false when no single read
// succeeded, which looks like the store being down; the caller keeps
// retrying. A lone message the store always fails on still holds the claim,
// since it cannot be told apart from an outage.
func (p *Propagator) hydrateEach(ctx context.Context, msgs []propagationMsg, skip []bool) (drops []string, ok bool) {
	drops = make([]string, len(msgs))
	succeeded, failed := 0, 0
	for i := range msgs {
		if skip[i] {
			continue
		}
		if ctx.Err() != nil || (succeeded == 0 && failed >= hydrateOutageProbe) {
			return nil, false
		}
		d, err := p.hydrate(ctx, msgs[i:i+1], nil)
		if err != nil {
			drops[i] = dropLookupFailed
			failed++
			continue
		}
		drops[i] = d[0]
		succeeded++
	}
	if succeeded == 0 {
		return nil, false
	}
	for i := range msgs {
		if drops[i] == dropLookupFailed {
			metrics.PropagationHydrateDroppedTotal.WithLabelValues(dropLookupFailed).Inc()
			p.logger.Error("propagation message's store row cannot be read; dropping for the reaper",
				logfields.TxID(msgs[i].TXID))
		}
	}
	return drops, true
}

// hydrate fills RawTx and InputTXIDs on every txid-only message in msgs from
// one store read. drops[i] is the reason msgs[i] cannot be propagated, or ""
// when it can. Messages with skip[i] set (nil skip skips none), and messages
// that already carry raw_tx, are left alone. err is a store failure; nothing
// is dropped for it.
func (p *Propagator) hydrate(ctx context.Context, msgs []propagationMsg, skip []bool) (drops []string, err error) {
	drops = make([]string, len(msgs))
	var need []string
	for i := range msgs {
		switch {
		case skip != nil && skip[i]:
		case msgs[i].TXID == "":
			drops[i] = dropMalformed
		case len(msgs[i].RawTx) == 0:
			need = append(need, msgs[i].TXID)
		}
	}
	if len(need) > 0 {
		rows, err := p.store.LookupRawTxs(ctx, need)
		if err != nil {
			return nil, err
		}
		for i := range msgs {
			if (skip != nil && skip[i]) || drops[i] != "" || len(msgs[i].RawTx) > 0 {
				continue
			}
			row, found := rows[msgs[i].TXID]
			switch {
			case !found:
				drops[i] = dropRowMissing
				p.logger.Warn("propagation message has no store row; dropping", logfields.TxID(msgs[i].TXID))
			case len(row.RawTx) == 0 && row.Status.IsTerminal():
				// Redelivery of finished work: ClearRetryState dropped the
				// bytes when the row terminalized.
				drops[i] = dropTerminal
			case len(row.RawTx) == 0:
				drops[i] = dropRawTxMissing
				p.logger.Error("propagation message's store row has no raw_tx; dropping",
					logfields.TxID(msgs[i].TXID), logfields.Status(string(row.Status)))
			default:
				msgs[i].RawTx = row.RawTx
				// The dispatcher's dep-aware admission keys on parents;
				// the message no longer carries them (one txid per input
				// would put a big consolidation back over the cap).
				msgs[i].InputTXIDs = parentTxIDsOf(msgs[i])
			}
		}
	}
	for i := range msgs {
		if skip != nil && skip[i] {
			continue
		}
		if drops[i] != "" {
			metrics.PropagationHydrateDroppedTotal.WithLabelValues(drops[i]).Inc()
		}
	}
	return drops, nil
}
