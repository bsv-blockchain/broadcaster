//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/bsv-blockchain/broadcaster/kafka"
)

// TestKafkaStartOffset proves the StartOffset contract against real Kafka
// semantics (redpanda), which the memory broker cannot express:
//
//   - StartLatest: a fresh random consumer group never receives messages
//     published before it subscribed — the exact property that stops sse
//     pods from replaying the topic's entire retained backlog (and OOMing)
//     on every start.
//   - StartOldest (zero value): a fresh group receives the full retained
//     history, which durable stable-group consumers (propagation,
//     bump-builder) rely on for their pre-first-commit deployment.
//
// Only the redpanda container is started — not the full harness — so the
// test stays cheap enough to run alongside the smoke tests.
func TestKafkaStartOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("no container runtime reachable (NewDockerProvider): %v", err)
	}
	if err := provider.Health(context.Background()); err != nil {
		t.Skipf("container runtime not healthy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rp, err := redpanda.Run(
		ctx,
		"docker.redpanda.com/redpandadata/redpanda:v26.1.9@sha256:8cb52078690728e2b3d1507fc84eda4c07d4d59bd8fa241e9b1cc7eb7de096c9",
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() { _ = rp.Terminate(context.Background()) })

	seed, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}

	broker, err := kafka.NewSaramaBroker([]string{seed}, "e2e-offsets")
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	t.Run("StartLatest skips pre-subscription history", func(t *testing.T) {
		const topic = "offsets.latest"
		const history = 25

		for i := 0; i < history; i++ {
			if err := broker.Send(ctx, topic, "k", []byte(fmt.Sprintf("history-%d", i))); err != nil {
				t.Fatalf("send history: %v", err)
			}
		}

		first := consumeFirst(ctx, t, broker, topic, "latest-group", kafka.StartLatest, func(stop <-chan struct{}) {
			// Publish sentinels until the consumer observes one. The loop
			// (rather than a single send) absorbs the group-join window:
			// OffsetNewest pins the group to the head at join time, and any
			// sentinel published after that is delivered. If ANY history
			// message were delivered it would necessarily arrive before the
			// first sentinel, so checking the first message is sufficient.
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-ticker.C:
					_ = broker.Send(ctx, topic, "k", []byte(fmt.Sprintf("sentinel-%d", i)))
				}
			}
		})

		if len(first) < len("sentinel-") || string(first[:len("sentinel-")]) != "sentinel-" {
			t.Fatalf("first delivered message = %q — pre-subscription history replayed under StartLatest", first)
		}
	})

	t.Run("StartOldest replays full history", func(t *testing.T) {
		const topic = "offsets.oldest"

		if err := broker.Send(ctx, topic, "k", []byte("history-0")); err != nil {
			t.Fatalf("send history: %v", err)
		}

		first := consumeFirst(ctx, t, broker, topic, "oldest-group", kafka.StartOldest, nil)
		if string(first) != "history-0" {
			t.Fatalf("first delivered message = %q, want history-0 — StartOldest lost pre-subscription history", first)
		}
	})
}

// consumeFirst subscribes with the given StartOffset and returns the value of
// the first message delivered. If background is non-nil it runs concurrently
// (e.g. a sentinel publisher) and is stopped once the first message lands.
func consumeFirst(ctx context.Context, t *testing.T, broker kafka.Broker, topic, group string, start kafka.StartOffset, background func(stop <-chan struct{})) []byte {
	t.Helper()

	sub, err := broker.Subscribe(group, []string{topic}, start)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	consumeCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	received := make(chan []byte, 1)
	go func() {
		_ = sub.Consume(consumeCtx, func(claim kafka.Claim) error {
			for {
				select {
				case msg, ok := <-claim.Messages():
					if !ok {
						return nil
					}
					select {
					case received <- msg.Value:
					default: // only the first message matters
					}
					return nil
				case <-claim.Context().Done():
					return nil
				}
			}
		})
	}()

	if background != nil {
		stop := make(chan struct{})
		defer close(stop)
		go background(stop)
	}

	select {
	case v := <-received:
		return v
	case <-consumeCtx.Done():
		t.Fatalf("timed out waiting for first message on %s (group %s)", topic, group)
		return nil
	}
}
