package cg

import (
	"context"

	"github.com/Shopify/sarama"
)

// Seek returns an offset for the SeekConsumer to begin reading from. Seek is provided
// a topic, partition. This can be
// used for an application that needs a history of messages for context before
// the application can begin reading at the last committed offset.
type Seek func(topic string, partition int32) (int64, error)

// SeekConsumerConfig is needed to create a new SeekConsumer.
type SeekConsumerConfig struct {
	Client    sarama.Client
	Committer *CachingCommitter
	Context   context.Context
	Partition int32
	Seek      Seek
	Topic     string
}

// Ensure that we're implementing the Consumer interface.
var _ Consumer = &SeekConsumer{}

// SeekConsumer consumes given topic-partition starting at an offset determined
// by the provided Seek function.
type SeekConsumer struct {
	oc *OffsetConsumer
}

// NewSeekConsumer creates a new SeekConsumer that immediately starts consuming and whose
// messages are available on the Messages() channel.
func NewSeekConsumer(cfg *SeekConsumerConfig) (*SeekConsumer, error) {
	offset, err := cfg.Seek(cfg.Topic, cfg.Partition)
	if err != nil {
		return nil, err
	}
	oc, err := NewOffsetConsumer(&OffsetConsumerConfig{
		Client:    cfg.Client,
		Committer: cfg.Committer,
		Context:   cfg.Context,
		Offset:    offset,
		Partition: cfg.Partition,
		Topic:     cfg.Topic,
	})
	if err != nil {
		return nil, err
	}
	return &SeekConsumer{
		oc: oc,
	}, nil
}

// CommitOffset writes the provided offset to kafka.
func (sk *SeekConsumer) CommitOffset(offset int64) error {
	return sk.oc.CommitOffset(offset)
}

// Consume returns a channel of Kafka messages on this topic-partition starting
// at the provided offset. This channel will close when there is a non-recoverable error, or
// the context provided at creation time closes.
func (sk *SeekConsumer) Consume() <-chan *sarama.ConsumerMessage {
	return sk.oc.Consume()
}

// Err should be called after the Messages() channel closes to determine if there was an
// error during processing.
func (sk *SeekConsumer) Err() error {
	return sk.oc.Err()
}
