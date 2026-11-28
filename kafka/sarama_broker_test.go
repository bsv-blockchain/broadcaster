package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// fakeSyncProducer satisfies sarama.SyncProducer so the saramaBroker can be
// constructed without a real Kafka cluster. The async-channel-drainer fix
// only exercises the async producer side, but Close() drives both producers.
type fakeSyncProducer struct {
	closed atomic.Bool
}

func (f *fakeSyncProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, nil
}

func (f *fakeSyncProducer) SendMessages([]*sarama.ProducerMessage) error { return nil }

func (f *fakeSyncProducer) Close() error { f.closed.Store(true); return nil }

func (f *fakeSyncProducer) AbortTxn() error { return nil }

func (f *fakeSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (f *fakeSyncProducer) AddMessageToTxnWithGroupMetadata(*sarama.ConsumerMessage, *sarama.ConsumerGroupMetadata, *string) error {
	return nil
}

func (f *fakeSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (f *fakeSyncProducer) AddOffsetsToTxnWithGroupMetadata(map[string][]*sarama.PartitionOffsetMetadata, *sarama.ConsumerGroupMetadata) error {
	return nil
}

func (f *fakeSyncProducer) BeginTxn() error { return nil }

func (f *fakeSyncProducer) CommitTxn() error { return nil }

func (f *fakeSyncProducer) IsTransactional() bool { return false }

func (f *fakeSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }

func (f *fakeSyncProducer) TxnAddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

// newAsyncProducerMock builds a Sarama mock async producer with the same
// Return.Successes/Return.Errors flags the production broker uses, so the
// drainer goroutines see the same channel behavior they would in a real
// deployment.
func newAsyncProducerMock(t testing.TB) *mocks.AsyncProducer {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return mocks.NewAsyncProducer(t, cfg)
}

// TestSaramaBroker_AsyncDrainer_DoesNotBlockOnSuccesses sends N messages
// through SendAsync. Without a Successes drainer the mock's success channel
// (capacity = ChannelBufferSize, default 256) would fill and the next send
// would block on Input(). N is chosen well above that buffer to make the
// regression mode unambiguous.
func TestSaramaBroker_AsyncDrainer_DoesNotBlockOnSuccesses(t *testing.T) {
	mp := newAsyncProducerMock(t)
	const n = 1024
	for range n {
		mp.ExpectInputAndSucceed()
	}

	b := newSaramaBrokerFromProducers(&fakeSyncProducer{}, mp, nil, "", zaptest.NewLogger(t))

	done := make(chan error, 1)
	go func() {
		ctx := context.Background()
		for i := 0; i < n; i++ {
			if err := b.SendAsync(ctx, "tx.validated", "k", []byte("v")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendAsync returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendAsync blocked — Successes channel was not drained")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

// TestSaramaBroker_AsyncDrainer_HandlesErrors verifies that messages routed
// to the Errors channel are drained (not blocking the producer) and logged
// without panicking. The mock fails every input, so without an Errors
// drainer the channel would fill and SendAsync would deadlock.
func TestSaramaBroker_AsyncDrainer_HandlesErrors(t *testing.T) {
	mp := newAsyncProducerMock(t)
	const n = 512
	produceErr := errors.New("simulated produce failure")
	for range n {
		mp.ExpectInputAndFail(produceErr)
	}

	b := newSaramaBrokerFromProducers(&fakeSyncProducer{}, mp, nil, "", zaptest.NewLogger(t))

	done := make(chan error, 1)
	go func() {
		ctx := context.Background()
		for i := 0; i < n; i++ {
			if err := b.SendAsync(ctx, "tx.validated", "k", []byte("v")); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendAsync returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendAsync blocked — Errors channel was not drained")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

// TestSaramaBroker_Close_WaitsForDrainers asserts that Close() blocks until
// the drainer goroutines exit. After Close returns, no goroutine should
// still be reading from Successes/Errors, which we approximate by checking
// that the broker's WaitGroup has zero counter (drainersWG.Wait() returns
// immediately on a second call).
func TestSaramaBroker_Close_WaitsForDrainers(t *testing.T) {
	mp := newAsyncProducerMock(t)
	mp.ExpectInputAndSucceed()

	b := newSaramaBrokerFromProducers(&fakeSyncProducer{}, mp, nil, "", zap.NewNop())

	if err := b.SendAsync(context.Background(), "t", "k", []byte("v")); err != nil {
		t.Fatalf("SendAsync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A second Wait must be a no-op; if it blocks the drainers leaked.
	waitDone := make(chan struct{})
	go func() {
		b.drainersWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("drainer goroutines leaked past Close()")
	}
}

// TestNewSaramaConsumerConfig_StartOffset pins the StartOffset →
// Consumer.Offsets.Initial mapping. This is the primary guard for the
// live-only events-subscriber contract: sarama consumer groups aren't
// mockable, so the config seam is the closest unit-testable point. A
// regression back to OffsetOldest for StartLatest callers silently
// reintroduces the full-backlog replay that OOM-crashlooped the sse pods.
func TestNewSaramaConsumerConfig_StartOffset(t *testing.T) {
	tests := []struct {
		name  string
		start StartOffset
		want  int64
	}{
		{name: "zero value defaults to oldest", start: StartOldest, want: sarama.OffsetOldest},
		{name: "latest maps to newest", start: StartLatest, want: sarama.OffsetNewest},
		{name: "out-of-range values fall back to oldest", start: StartOffset(42), want: sarama.OffsetOldest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newSaramaConsumerConfig(tt.start)
			if got := cfg.Consumer.Offsets.Initial; got != tt.want {
				t.Errorf("Consumer.Offsets.Initial = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestNewSyncProducerConfig_OrderingInvariants pins the produce-side
// ordering contract that family partition keying (#295) relies on: with
// idempotence on and one open request, a retried produce cannot be
// reordered behind a newer message on the same partition, so a family's
// parent-before-child submission order survives broker hiccups. Version
// must be >= 0.11 for idempotent produce; WaitForAll is required by
// idempotence (and was already the sync default here).
func TestNewSyncProducerConfig_OrderingInvariants(t *testing.T) {
	cfg := newSyncProducerConfig()
	if !cfg.Producer.Idempotent {
		t.Error("sync producer must be idempotent — retries may otherwise reorder a family's messages within a partition")
	}
	if cfg.Net.MaxOpenRequests != 1 {
		t.Errorf("Net.MaxOpenRequests = %d, want 1 (required by idempotent produce, guarantees per-partition order)", cfg.Net.MaxOpenRequests)
	}
	if cfg.Producer.RequiredAcks != sarama.WaitForAll {
		t.Errorf("RequiredAcks = %v, want WaitForAll", cfg.Producer.RequiredAcks)
	}
	if !cfg.Version.IsAtLeast(sarama.V0_11_0_0) {
		t.Errorf("Version = %v, want >= 0.11.0.0 for idempotent produce", cfg.Version)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sarama config invalid: %v", err)
	}
}

// TestPartitionCountClientConfig_DisablesAutoTopicCreation pins the flag
// the metadata probe must clear. Sarama's default is true; leaving it
// would make a missing-topic lookup ask the broker to create the topic.
func TestPartitionCountClientConfig_DisablesAutoTopicCreation(t *testing.T) {
	if !sarama.NewConfig().Metadata.AllowAutoTopicCreation {
		t.Fatal("sarama default AllowAutoTopicCreation changed; the override below may no longer be load-bearing")
	}
	cfg := partitionCountClientConfig()
	if cfg.Metadata.AllowAutoTopicCreation {
		t.Fatal("partition-count client must not allow auto topic creation")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sarama config invalid: %v", err)
	}
}

// TestSaramaBroker_PartitionCount_DoesNotAskToCreateMissingTopic drives
// PartitionCount against a mock broker. The metadata request for the
// missing topic must carry AllowAutoTopicCreation=false, and the result
// must be ErrTopicNotFound rather than a freshly created partition list.
func TestSaramaBroker_PartitionCount_DoesNotAskToCreateMissingTopic(t *testing.T) {
	const missing = "bsv-broadcaster.block_processed"
	const wide = "bsv-broadcaster.tx_status"

	seed := sarama.NewMockBroker(t, 1)
	defer seed.Close()

	seed.SetHandlerByMap(map[string]sarama.MockResponse{
		// Advertise Metadata (3) and ApiVersions (18). The stock mock only
		// advertises Produce and Fetch, which makes the real client drop
		// the broker before it can ask about the topic.
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: 3, MinVersion: 0, MaxVersion: 12},
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
		}),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seed.BrokerID()).
			SetBroker(seed.Addr(), seed.BrokerID()).
			SetLeader(wide, 0, seed.BrokerID()).
			SetLeader(wide, 1, seed.BrokerID()).
			SetLeader(wide, 2, seed.BrokerID()),
	})

	b := newSaramaBrokerFromProducers(
		&fakeSyncProducer{},
		newAsyncProducerMock(t),
		[]string{seed.Addr()},
		"bsv-broadcaster",
		zap.NewNop(),
	)
	defer func() { _ = b.Close() }()

	n, err := b.PartitionCount(missing)
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("PartitionCount(%s) = %d, %v; want ErrTopicNotFound", missing, n, err)
	}

	count, err := b.PartitionCount(wide)
	if err != nil {
		t.Fatalf("PartitionCount(%s): %v", wide, err)
	}
	if count != 3 {
		t.Fatalf("PartitionCount(%s) = %d, want 3", wide, count)
	}

	sawMissing := false
	for _, rr := range seed.History() {
		req, ok := rr.Request.(*sarama.MetadataRequest)
		if !ok {
			continue
		}
		for _, topic := range req.Topics {
			if topic != missing {
				continue
			}
			sawMissing = true
			if req.AllowAutoTopicCreation {
				t.Fatal("metadata lookup for a missing topic set AllowAutoTopicCreation")
			}
		}
	}
	if !sawMissing {
		t.Fatal("expected a metadata request naming the missing topic")
	}
}
