package kafka

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// CheckPartitions verifies that every topic in topics exists on the broker
// and has at least minPartitions partitions. Startup calls it so a
// horizontally scaled deployment sees a bad topic before traffic arrives.
//
// Parameters:
//   - broker: answers PartitionCount. A topic the broker does not have is
//     ErrTopicNotFound, and that sentinel is wrapped on the error returned
//     here. The in-memory broker never returns that error (unknown topics
//     report their configured width, default 1), so the missing-topic
//     failure below does not fire on the standalone path.
//   - topics: topics to inspect. Nil or empty checks nothing.
//   - minPartitions: required floor. Values <= 1 skip the check. That is
//     the standalone and single-replica setting (kafka.min_partitions left
//     at 0 or 1), where topics may be created later and a partition floor
//     is not meaningful.
//   - logger: receives an info line for each topic that meets the floor.
//
// Returns an error when minPartitions > 1 and a topic is missing or has
// fewer partitions than minPartitions. A missing topic is a hard error for
// the same reason a narrow topic is: auto-creation on first publish uses
// the broker's default partition count, which is often lower than the pod
// count. The Sarama PartitionCount client disables auto-topic creation so
// the probe itself cannot create the topic and then fail on that width.
// Operators create the topic with at least minPartitions partitions before
// startup. Other broker errors are wrapped and returned.
//
// Returns nil when minPartitions <= 1, when topics is empty, or when every
// listed topic meets the floor.
func CheckPartitions(broker Broker, topics []string, minPartitions int, logger *zap.Logger) error {
	if minPartitions <= 1 {
		return nil
	}
	for _, topic := range topics {
		count, err := broker.PartitionCount(topic)
		if errors.Is(err, ErrTopicNotFound) {
			return fmt.Errorf(
				"topic %s not found on broker; create it with at least %d partitions before startup (horizontal scaling): %w",
				topic,
				minPartitions,
				err,
			)
		}
		if err != nil {
			return fmt.Errorf("querying partition count for %s: %w", topic, err)
		}
		if count < minPartitions {
			return fmt.Errorf("topic %s has %d partitions, need at least %d for horizontal scaling", topic, count, minPartitions)
		}
		logger.Info(
			"topic partition count ok",
			zap.String("topic", topic),
			zap.Int("partitions", count),
		)
	}
	return nil
}

// CheckMinPartitions verifies that `topic` exists on the broker with at
// least `want` partitions. Used for topics whose partition count is a
// correctness-adjacent deployment constraint: producers key
// TopicPropagation by dependency family, so any partition count
// preserves parent-before-child order per partition, but a topic with
// fewer partitions than the deployment was sized for leaves consumers
// idle and means config and topology have drifted apart. `want < 1` is
// treated as 1 — the check is never a no-op.
//
// Missing topics are hard errors regardless of `want`: allowing
// auto-creation on first publish would let the broker default decide
// the partition count (and therefore the key→partition mapping) instead
// of the operator.
func CheckMinPartitions(broker Broker, topic string, want int, logger *zap.Logger) error {
	if want < 1 {
		want = 1
	}
	count, err := broker.PartitionCount(topic)
	if errors.Is(err, ErrTopicNotFound) {
		return fmt.Errorf(
			"topic %s not found on broker; create it before startup with at least %d partitions (correctness requirement): %w",
			topic,
			want,
			err,
		)
	}
	if err != nil {
		return fmt.Errorf("querying partition count for %s: %w", topic, err)
	}
	if count < want {
		return fmt.Errorf("topic %s has %d partition(s), want at least %d (propagation.partitions)", topic, count, want)
	}
	logger.Info(
		"topic partition count meets required minimum",
		zap.String("topic", topic),
		zap.Int("partitions", count),
		zap.Int("min_required", want),
	)
	return nil
}
