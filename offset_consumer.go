package cg

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// OffsetConsumerConfig is all the instantiated dependencies needed to run an OffsetConsumer.
type OffsetConsumerConfig struct {
	CacheDuration time.Duration
	Client        sarama.Client
	Context       context.Context
	Coordinator   *Coordinator
	Offset        int64
	Partition     int32
	Topic         string
}

// OffsetConsumer consumes given topic-partition starting at an offset provided.
type OffsetConsumer struct {
	cfg       *OffsetConsumerConfig
	ch        chan *sarama.ConsumerMessage
	committer *CachingCommitter
	err       error
}

// NewOffsetConsumer creates a new OffsetConsumer that immediately starts consuming and whose
// messages are available on the Messages() channel.
func NewOffsetConsumer(cfg *OffsetConsumerConfig) (*OffsetConsumer, error) {
	if cfg.Offset < 0 {
		return nil, fmt.Errorf("special <0 offsets should be resolved to actual offsets before instantiating NewOffsetConsumer")
	}
	committer, err := NewCachingCommitter(&CachingCommitterConfig{
		Coordinator: cfg.Coordinator,
		Duration:    cfg.CacheDuration,
		Partition:   cfg.Partition,
		Topic:       cfg.Topic,
	})
	if err != nil {
		return nil, err
	}
	oc := &OffsetConsumer{
		cfg:       cfg,
		ch:        make(chan *sarama.ConsumerMessage),
		committer: committer,
	}
	go func() {
		err := oc.run()
		if err != nil {
			oc.err = err
		}
		close(oc.ch)
	}()
	return oc, nil
}

// CommitOffset writes the provided offset to kafka.
func (oc *OffsetConsumer) CommitOffset(offset int64) error {
	return oc.committer.CommitOffset(offset)
}

// Consume returns a channel of Kafka messages on this topic-partition starting
// at the provided offset. This channel will close when there is a non-recoverable error, or
// the context provided at creation time closes.
func (oc *OffsetConsumer) Consume() <-chan *sarama.ConsumerMessage {
	return oc.ch
}

// Err should be called after the Messages() channel closes to determine if there was an
// error during processing.
func (oc *OffsetConsumer) Err() error {
	return oc.err
}

func (oc *OffsetConsumer) run() error {
	c, err := sarama.NewConsumerFromClient(oc.cfg.Client)
	if err != nil {
		return err
	}
	defer c.Close()
	pc, err := c.ConsumePartition(oc.cfg.Topic, oc.cfg.Partition, oc.cfg.Offset)
	if err != nil {
		return err
	}
	defer pc.Close()
	ch := pc.Messages()
	for {
		select {
		case <-oc.cfg.Context.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			select {
			case <-oc.cfg.Context.Done():
				return nil
			case oc.ch <- msg:
			}
		}
	}
}
