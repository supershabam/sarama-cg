package consumer

import (
	"context"
	"time"

	"github.com/Shopify/sarama"

	"github.com/supershabam/sarama-cg"
)

// SeekFn returns an offset for the Seek to begin reading from. Seek is provided
// a topic, partition. This can be
// used for an application that needs a history of messages for context before
// the application can begin reading at the last committed offset.
type SeekFn func(topic string, partition int32) (int64, error)

// SeekConfig is needed to create a new Seek.
type SeekConfig struct {
	CacheDuration time.Duration
	Client        sarama.Client
	Context       context.Context
	Coordinator   *cg.Coordinator
	Partition     int32
	SeekFn        SeekFn
	Topic         string
}

// Ensure that we're implementing the Consumer interface.
var _ cg.Consumer = &Seek{}

// Seek consumes given topic-partition starting at an offset determined
// by the provided Seek function.
type Seek struct {
	oc *Offset
}

// NewSeek creates a new Seek that immediately starts consuming and whose
// messages are available on the Messages() channel.
func NewSeek(cfg *SeekConfig) (*Seek, error) {
	offset, err := cfg.SeekFn(cfg.Topic, cfg.Partition)
	if err != nil {
		return nil, err
	}
	oc, err := NewOffset(&OffsetConfig{
		CacheDuration: cfg.CacheDuration,
		Client:        cfg.Client,
		Context:       cfg.Context,
		Coordinator:   cfg.Coordinator,
		Offset:        offset,
		Partition:     cfg.Partition,
		Topic:         cfg.Topic,
	})
	if err != nil {
		return nil, err
	}
	return &Seek{
		oc: oc,
	}, nil
}

// CommitOffset writes the provided offset to kafka.
func (sk *Seek) CommitOffset(offset int64) error {
	return sk.oc.CommitOffset(offset)
}

// Consume returns a channel of Kafka messages on this topic-partition starting
// at the provided offset. This channel will close when there is a non-recoverable error, or
// the context provided at creation time closes.
func (sk *Seek) Consume() <-chan *sarama.ConsumerMessage {
	return sk.oc.Consume()
}

// Err should be called after the Messages() channel closes to determine if there was an
// error during processing.
func (sk *Seek) Err() error {
	return sk.oc.Err()
}
