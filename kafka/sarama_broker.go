package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/metrics"
)

// saramaBroker is the production Broker backed by IBM Sarama. It owns both a
// sync and async producer so Send/SendAsync/SendBatch can pick the appropriate
// one without the caller caring.
//
// The async producer is configured with Return.Successes=true and
// Return.Errors=true. Sarama routes every produced message's outcome onto
// those channels, and if no goroutine drains them they fill up and the
// producer blocks indefinitely on Input(). To keep SendAsync non-blocking,
// the broker spawns two drain goroutines for the producer's lifetime: one
// discards successes (the SendAsync caller already counted the produce in
// the Producer wrapper) and one logs/counts errors via metrics.
// Close() waits for both drainers to exit after closing the underlying
// async producer, which closes both channels in turn.
type saramaBroker struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	brokers       []string
	consumerGroup string

	logger     *zap.Logger
	drainersWG sync.WaitGroup
}

// NewSaramaBroker constructs a Sarama-backed Broker with sensible defaults
// (WaitForAll on sync, WaitForLocal on async, 5 retries). Errors from the
// async producer are logged via the package-global zap logger; callers that
// want a custom logger should use NewSaramaBrokerWithLogger.
func NewSaramaBroker(brokers []string, consumerGroup string) (Broker, error) {
	return NewSaramaBrokerWithLogger(brokers, consumerGroup, nil)
}

// NewSaramaBrokerWithLogger is like NewSaramaBroker but lets callers inject a
// zap logger for async-producer error logging. A nil logger falls back to
// zap.NewNop() so the broker is always safe to construct.
func NewSaramaBrokerWithLogger(brokers []string, consumerGroup string, logger *zap.Logger) (Broker, error) {
	syncProducer, err := sarama.NewSyncProducer(brokers, newSyncProducerConfig())
	if err != nil {
		return nil, fmt.Errorf("creating sync producer: %w", err)
	}

	asyncCfg := sarama.NewConfig()
	asyncCfg.Producer.RequiredAcks = sarama.WaitForLocal
	asyncCfg.Producer.Retry.Max = 5
	asyncCfg.Producer.Return.Successes = true
	asyncCfg.Producer.Return.Errors = true

	asyncProducer, err := sarama.NewAsyncProducer(brokers, asyncCfg)
	if err != nil {
		_ = syncProducer.Close()
		return nil, fmt.Errorf("creating async producer: %w", err)
	}

	return newSaramaBrokerFromProducers(syncProducer, asyncProducer, brokers, consumerGroup, logger), nil
}

// newSyncProducerConfig builds the sync-producer config used for Send /
// SendBatch — the path every ordering-sensitive topic goes through
// (TopicPropagation most of all). Idempotent + MaxOpenRequests=1 makes
// per-partition order survive broker-side retries: without it a failed
// produce can be retried behind a newer in-flight batch, reordering a
// dependency family's parent behind its child on the same partition
// (#295 family keying makes that order load-bearing). Idempotence
// requires WaitForAll acks (already the sync default here) and protocol
// >= 0.11; 2.6.0 is safely below anything Redpanda or a maintained
// Kafka speaks. Extracted so tests can pin these invariants.
func newSyncProducerConfig() *sarama.Config {
	syncCfg := sarama.NewConfig()
	syncCfg.Version = sarama.V2_6_0_0
	syncCfg.Producer.RequiredAcks = sarama.WaitForAll
	syncCfg.Producer.Retry.Max = 5
	syncCfg.Producer.Return.Successes = true
	syncCfg.Producer.Return.Errors = true
	syncCfg.Producer.Idempotent = true
	syncCfg.Net.MaxOpenRequests = 1
	return syncCfg
}

// newSaramaBrokerFromProducers wires the broker around already-constructed
// sync and async producers. Extracted so tests can substitute Sarama mocks
// without standing up a real Kafka cluster. It also starts the async-producer
// drainer goroutines, which is the only place those should be spawned —
// duplicating that elsewhere would race for ownership of Successes/Errors.
func newSaramaBrokerFromProducers(
	sync sarama.SyncProducer,
	async sarama.AsyncProducer,
	brokers []string,
	consumerGroup string,
	logger *zap.Logger,
) *saramaBroker {
	if logger == nil {
		logger = zap.NewNop()
	}
	b := &saramaBroker{
		syncProducer:  sync,
		asyncProducer: async,
		brokers:       brokers,
		consumerGroup: consumerGroup,
		logger:        logger,
	}
	b.startAsyncDrainers()
	return b
}

// startAsyncDrainers spawns the two goroutines that consume the async
// producer's Successes and Errors channels for the producer's lifetime.
// They return when the underlying channels close, which Sarama does as
// part of asyncProducer.Close(). Close() then waits on drainersWG so the
// broker does not return from Close until both goroutines have exited —
// otherwise a test or a process restart could observe partial shutdown.
func (b *saramaBroker) startAsyncDrainers() {
	b.drainersWG.Add(2)
	go func() {
		defer b.drainersWG.Done()
		// Successes are already accounted for by Producer.SendAsync at
		// enqueue time, so we just discard them here. Draining is the
		// whole point — a full Successes channel blocks Input().
		successes := b.asyncProducer.Successes()
		for {
			if _, ok := <-successes; !ok {
				return
			}
		}
	}()
	go func() {
		defer b.drainersWG.Done()
		for produceErr := range b.asyncProducer.Errors() {
			topic := ""
			if produceErr.Msg != nil {
				topic = produceErr.Msg.Topic
			}
			metrics.KafkaProduceErrors.WithLabelValues(topic).Inc()
			b.logger.Error(
				"async kafka produce failed",
				zap.String("topic", topic),
				zap.Error(produceErr.Err),
			)
		}
	}()
}

func (b *saramaBroker) Send(ctx context.Context, topic, key string, value []byte) error {
	ctx, span := startProduceSpan(ctx, topic)
	defer span.End()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	msg.Headers = buildRecordHeaders(InjectTraceContext(ctx))
	if _, _, err := b.syncProducer.SendMessage(msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "sync produce failed")
		return fmt.Errorf("sending message to %s: %w", topic, err)
	}
	return nil
}

func (b *saramaBroker) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	ctx, span := startProduceSpan(ctx, topic)
	// Fire-and-forget: the span ends at enqueue time (Input() <- msg), which
	// only hands the message to Sarama's async producer. A later delivery
	// failure therefore does NOT surface on this span — it arrives on the
	// producer's Errors() channel and is logged/counted by the error drainer
	// (see startAsyncDrainers). The span records only that the enqueue
	// happened; the enqueue itself cannot fail here.
	defer span.End()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	msg.Headers = buildRecordHeaders(InjectTraceContext(ctx))
	b.asyncProducer.Input() <- msg
	return nil
}

func (b *saramaBroker) SendBatch(ctx context.Context, topic string, msgs []KeyValue) error {
	if len(msgs) == 0 {
		return nil
	}
	ctx, span := startProduceSpan(ctx, topic)
	defer span.End()

	// Injected once and shared by reference across every message in the
	// batch — SendBatch propagates the same parent trace context to every
	// message, and Sarama only reads Headers to encode the wire record (it
	// never mutates the slice), so sharing is safe and avoids re-running
	// the propagator for every message in a large batch.
	headers := buildRecordHeaders(InjectTraceContext(ctx))

	saramaMsgs := make([]*sarama.ProducerMessage, 0, len(msgs))
	for _, m := range msgs {
		data, err := marshalValue(m.Value)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "marshaling batch message failed")
			return fmt.Errorf("marshaling batch message: %w", err)
		}
		pm := &sarama.ProducerMessage{
			Topic:   topic,
			Value:   sarama.ByteEncoder(data),
			Headers: headers,
		}
		if m.Key != "" {
			pm.Key = sarama.StringEncoder(m.Key)
		}
		saramaMsgs = append(saramaMsgs, pm)
	}
	if err := b.syncProducer.SendMessages(saramaMsgs); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "sync batch produce failed")
		return fmt.Errorf("sending batch to %s: %w", topic, err)
	}
	return nil
}

// startProduceSpan starts a Kafka producer span for topic, parented on
// ctx's existing span. The span name and messaging.* attributes are
// low-cardinality (topic only — never a txid or key) per the metrics-package
// rule.
//
// When ctx carries no valid span (telemetry disabled end-to-end, or simply
// no active span at this call site), span creation is skipped entirely —
// there is no useful parent to attach to, InjectTraceContext would no-op on
// the result anyway, and the internal/global placeholder tracer's Start
// still costs a context.WithValue + interface allocation for a span nobody
// will ever query. Returning ctx unchanged with trace.SpanFromContext(ctx)
// (a shared no-op singleton — zero allocation) keeps this path exactly as
// cheap as InjectTraceContext's own guard.
//
// When ctx does carry a valid parent span, the per-topic name string and
// attribute values are still only built once span.IsRecording() is true —
// covering the case where a real span exists but this trace wasn't sampled.
func startProduceSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, "kafka.produce")
	if span.IsRecording() {
		span.SetName("kafka.produce " + topic)
		span.SetAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
		)
	}
	return ctx, span
}

// buildRecordHeaders converts an injected trace-context header map into
// Sarama's []RecordHeader representation. Returns nil when hdrs is empty
// (InjectTraceContext already returned nil on the disabled/no-span path) so
// callers can assign it straight to ProducerMessage.Headers without an extra
// nil check.
func buildRecordHeaders(hdrs map[string][]byte) []sarama.RecordHeader {
	if len(hdrs) == 0 {
		return nil
	}
	rh := make([]sarama.RecordHeader, 0, len(hdrs))
	for k, v := range hdrs {
		rh = append(rh, sarama.RecordHeader{Key: []byte(k), Value: v})
	}
	return rh
}

func (b *saramaBroker) Subscribe(groupID string, topics []string, start StartOffset) (Subscription, error) {
	group, err := sarama.NewConsumerGroup(b.brokers, groupID, newSaramaConsumerConfig(start))
	if err != nil {
		return nil, fmt.Errorf("creating consumer group %s: %w", groupID, err)
	}
	return &saramaSubscription{group: group, topics: topics}, nil
}

// newSaramaConsumerConfig maps the broker-neutral StartOffset onto Sarama's
// Consumer.Offsets.Initial. Initial is only consulted when the group has no
// committed offsets, so durable stable-group consumers are unaffected by it
// after their first commit; for fresh random groups it decides between
// replaying the topic's entire retained backlog (OffsetOldest) and starting
// at the head (OffsetNewest).
func newSaramaConsumerConfig(start StartOffset) *sarama.Config {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	if start == StartLatest {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	return saramaCfg
}

// partitionCountClientConfig is the metadata client used by PartitionCount.
//
// AllowAutoTopicCreation is forced off. Sarama's default is true, so a
// metadata lookup for a missing topic would ask the broker to create it
// at the cluster default width. CheckPartitions and CheckMinPartitions
// both go through PartitionCount; a probe that created the topic would
// hide ErrTopicNotFound, and every later restart would fail on "has N
// partitions" against a topic the probe itself created.
func partitionCountClientConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Metadata.AllowAutoTopicCreation = false
	return cfg
}

func (b *saramaBroker) PartitionCount(topic string) (int, error) {
	client, err := sarama.NewClient(b.brokers, partitionCountClientConfig())
	if err != nil {
		return 0, fmt.Errorf("creating metadata client: %w", err)
	}
	defer func() { _ = client.Close() }()

	partitions, err := client.Partitions(topic)
	if err != nil {
		if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			return 0, ErrTopicNotFound
		}
		return 0, fmt.Errorf("listing partitions for %s: %w", topic, err)
	}
	return len(partitions), nil
}

func (b *saramaBroker) Close() error {
	var errs []error
	if err := b.syncProducer.Close(); err != nil {
		errs = append(errs, err)
	}
	// Closing the async producer closes its Successes/Errors channels once
	// in-flight messages have been flushed, which is what unblocks the
	// drain goroutines below.
	if err := b.asyncProducer.Close(); err != nil {
		errs = append(errs, err)
	}
	b.drainersWG.Wait()
	if len(errs) > 0 {
		return fmt.Errorf("closing producers: %v", errs)
	}
	return nil
}

// saramaSubscription adapts a sarama.ConsumerGroup to the neutral Subscription
// interface. Each call to Consume runs a fresh group.Consume loop that
// re-engages on rebalance, invoking handler once per claim via the adapter.
type saramaSubscription struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler func(Claim) error
}

func (s *saramaSubscription) Consume(ctx context.Context, handler func(Claim) error) error {
	s.handler = handler
	adapter := &saramaGroupHandler{sub: s}
	for {
		if err := s.group.Consume(ctx, s.topics, adapter); err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // ctx cancellation is a graceful shutdown, not an error to bubble
			}
			// transient rebalance errors surface here; the library reconnects
			// automatically on the next Consume call.
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil //nolint:nilerr // graceful shutdown
		}
	}
}

func (s *saramaSubscription) Close() error {
	return s.group.Close()
}

// saramaGroupHandler bridges sarama.ConsumerGroupHandler to the neutral Claim
// abstraction. Setup/Cleanup are no-ops; ConsumeClaim wraps the Sarama claim
// and invokes the user's handler.
type saramaGroupHandler struct {
	sub *saramaSubscription
}

func (h *saramaGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *saramaGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *saramaGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// out is allocated here, before the pump goroutine is scheduled, so the
	// handler's first call to Messages() never observes a nil channel. A nil
	// channel read blocks forever, so when sarama assigned multiple partitions
	// to one consumer the partitions whose handler ran before pump() left the
	// claim wedged at offset 0 with no log signal.
	neutral := &saramaClaim{
		session: session,
		claim:   claim,
		out:     make(chan *Message, 256),
	}
	go neutral.pump()
	return h.sub.handler(neutral)
}

// saramaClaim wraps a sarama.ConsumerGroupClaim, translating each
// sarama.ConsumerMessage into a neutral *Message on its own channel. A small
// goroutine does the translation so the neutral channel closes exactly when
// the underlying claim ends, giving handlers a clean loop-exit signal.
type saramaClaim struct {
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim
	out     chan *Message
}

func (c *saramaClaim) pump() {
	defer close(c.out)
	for msg := range c.claim.Messages() {
		headers := make(map[string][]byte, len(msg.Headers))
		for _, h := range msg.Headers {
			headers[string(h.Key)] = h.Value
		}
		c.out <- &Message{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
			Headers:   headers,
		}
	}
}

func (c *saramaClaim) Messages() <-chan *Message {
	return c.out
}

func (c *saramaClaim) Context() context.Context {
	return c.session.Context()
}

func (c *saramaClaim) MarkMessage(msg *Message) {
	if msg == nil {
		return
	}
	// Rebuild a minimal sarama.ConsumerMessage — MarkMessage only uses topic,
	// partition, and offset on the Sarama side.
	c.session.MarkMessage(&sarama.ConsumerMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, "")
}
