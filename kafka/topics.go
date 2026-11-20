package kafka

// DefaultTopicPrefix namespaces every topic unless kafka.topic_prefix says
// otherwise. Deployments that predate the BSV Broadcaster rebrand run with
// prefix "arcade" so they keep consuming their existing topics.
const DefaultTopicPrefix = "bsv-broadcaster"

// Topic name suffixes; every topic is "<prefix><suffix>".
const (
	suffixBlock          = ".block"
	suffixBlockProcessed = ".block_processed"
	suffixTransaction    = ".transaction"
	suffixPropagation    = ".propagation"
	suffixStatusUpdate   = ".tx_status"
	suffixDLQ            = ".dlq"
)

// Topic names. They are variables only so SetTopicPrefix can apply
// kafka.topic_prefix at startup; treat them as constants otherwise.
var (
	// TopicBlock is retained only for the p2p_client scaffold (which is not
	// registered in main). It is not created by AllTopics() and has no consumers.
	TopicBlock          = DefaultTopicPrefix + suffixBlock
	TopicBlockProcessed = DefaultTopicPrefix + suffixBlockProcessed
	TopicTransaction    = DefaultTopicPrefix + suffixTransaction
	TopicPropagation    = DefaultTopicPrefix + suffixPropagation
	// TopicStatusUpdate carries transaction status mutations from every
	// service that writes to the store (validator, propagation, bump-builder,
	// api-server). It exists so the api-server SSE handler and webhook
	// service can fan out updates to clients without depending on which pod
	// the mutation originated from.
	TopicStatusUpdate = DefaultTopicPrefix + suffixStatusUpdate

	// Dead-letter queue topics
	TopicBlockProcessedDLQ = TopicBlockProcessed + suffixDLQ
	TopicTransactionDLQ    = TopicTransaction + suffixDLQ
	TopicPropagationDLQ    = TopicPropagation + suffixDLQ
	TopicStatusUpdateDLQ   = TopicStatusUpdate + suffixDLQ
)

// SetTopicPrefix renames every topic to "<prefix>.<name>"; an empty prefix
// means DefaultTopicPrefix. It is not safe for concurrent use: app.Bootstrap
// calls it once, before any broker, producer or consumer exists.
func SetTopicPrefix(prefix string) {
	if prefix == "" {
		prefix = DefaultTopicPrefix
	}
	TopicBlock = prefix + suffixBlock
	TopicBlockProcessed = prefix + suffixBlockProcessed
	TopicTransaction = prefix + suffixTransaction
	TopicPropagation = prefix + suffixPropagation
	TopicStatusUpdate = prefix + suffixStatusUpdate

	TopicBlockProcessedDLQ = DLQTopic(TopicBlockProcessed)
	TopicTransactionDLQ = DLQTopic(TopicTransaction)
	TopicPropagationDLQ = DLQTopic(TopicPropagation)
	TopicStatusUpdateDLQ = DLQTopic(TopicStatusUpdate)
}

// HorizontalScaleTopics returns the fan-out topics checked against
// kafka.min_partitions when that floor is greater than 1.
//
// TopicPropagation is not included: CheckMinPartitions already enforces
// propagation.partitions, a different knob. The retired TopicTransaction
// and the DLQ topics are one partition by design. Names follow the prefix
// applied by SetTopicPrefix, so call this after that.
func HorizontalScaleTopics() []string {
	return []string{TopicBlockProcessed, TopicStatusUpdate}
}

// AllTopics returns all primary topics created/managed by BSV Broadcaster.
func AllTopics() []string {
	return []string{
		TopicBlockProcessed,
		TopicTransaction,
		TopicPropagation,
		TopicStatusUpdate,
	}
}

// DLQTopic returns the dead-letter topic name for a given primary topic.
func DLQTopic(topic string) string {
	return topic + suffixDLQ
}
