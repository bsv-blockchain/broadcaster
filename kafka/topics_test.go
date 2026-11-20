package kafka

import "testing"

func TestSetTopicPrefix(t *testing.T) {
	t.Cleanup(func() { SetTopicPrefix(DefaultTopicPrefix) })

	SetTopicPrefix("arcade")
	if TopicPropagation != "arcade.propagation" {
		t.Errorf("TopicPropagation = %q, want %q", TopicPropagation, "arcade.propagation")
	}
	if TopicStatusUpdateDLQ != "arcade.tx_status.dlq" {
		t.Errorf("TopicStatusUpdateDLQ = %q, want %q", TopicStatusUpdateDLQ, "arcade.tx_status.dlq")
	}

	SetTopicPrefix("")
	if TopicBlockProcessed != "bsv-broadcaster.block_processed" {
		t.Errorf("empty prefix: TopicBlockProcessed = %q, want the default-prefixed name", TopicBlockProcessed)
	}
	if TopicBlockProcessedDLQ != "bsv-broadcaster.block_processed.dlq" {
		t.Errorf("empty prefix: TopicBlockProcessedDLQ = %q, want the default-prefixed name", TopicBlockProcessedDLQ)
	}
}

func TestHorizontalScaleTopics(t *testing.T) {
	t.Cleanup(func() { SetTopicPrefix(DefaultTopicPrefix) })
	SetTopicPrefix("arcade")

	got := HorizontalScaleTopics()
	want := []string{"arcade.block_processed", "arcade.tx_status"}
	if len(got) != len(want) {
		t.Fatalf("HorizontalScaleTopics() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("HorizontalScaleTopics()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	for _, excluded := range []string{
		TopicPropagation,
		TopicTransaction,
		TopicBlockProcessedDLQ,
		TopicStatusUpdateDLQ,
	} {
		for _, topic := range got {
			if topic == excluded {
				t.Errorf("HorizontalScaleTopics() includes %q", topic)
			}
		}
	}
}
