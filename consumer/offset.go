package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"

	"github.com/supershabam/sarama-cg"
)

// OffsetConfig is all the instantiated dependencies needed to run an Offset.
type OffsetConfig struct {
	CacheDuration time.Duration
	Client        sarama.Client
	Context       context.Context
	Coordinator   *cg.Coordinator
	Offset        int64
	Partition     int32
	Topic         string
}

// Offset consumes given topic-partition starting at an offset provided.
type Offset struct {
	cfg       *OffsetConfig
	ch        chan *sarama.ConsumerMessage
	committer *cg.CachingCommitter
	err       error
	pc        sarama.PartitionConsumer
}

// NewOffset creates a new Offset that immediately starts consuming and whose
// messages are available on the Messages() channel.
func NewOffset(cfg *OffsetConfig) (*Offset, error) {
	if cfg.Offset < 0 {
		return nil, fmt.Errorf("special <0 offsets should be resolved to actual offsets before instantiating NewOffset")
	}
	committer, err := cg.NewCachingCommitter(&cg.CachingCommitterConfig{
		Coordinator: cfg.Coordinator,
		Duration:    cfg.CacheDuration,
		Partition:   cfg.Partition,
		Topic:       cfg.Topic,
	})
	if err != nil {
		return nil, err
	}
	c, err := sarama.NewConsumerFromClient(cfg.Client)
	if err != nil {
		return nil, err
	}
	pc, err := c.ConsumePartition(cfg.Topic, cfg.Partition, cfg.Offset)
	if err != nil {
		return nil, err
	}
	oc := &Offset{
		cfg:       cfg,
		ch:        make(chan *sarama.ConsumerMessage),
		committer: committer,
		pc:        pc,
	}
	go func() {
		defer c.Close()
		defer pc.Close()
		defer close(oc.ch)
		err := oc.run()
		if err != nil {
			oc.err = err
		}
	}()
	return oc, nil
}

// CommitOffset writes the provided offset to kafka.
func (oc *Offset) CommitOffset(offset int64) error {
	return oc.committer.CommitOffset(offset)
}

// Consume returns a channel of Kafka messages on this topic-partition starting
// at the provided offset. This channel will close when there is a non-recoverable error, or
// the context provided at creation time closes.
func (oc *Offset) Consume() <-chan *sarama.ConsumerMessage {
	return oc.ch
}

// Err should be called after the Messages() channel closes to determine if there was an
// error during processing.
func (oc *Offset) Err() error {
	return oc.err
}

// HighWaterMarkOffset returns the last reported highwatermark offset for the partition this
// consumer is reading.
func (oc *Offset) HighWaterMarkOffset() int64 {
	return oc.pc.HighWaterMarkOffset()
}

func (oc *Offset) run() error {
	ch := oc.pc.Messages()
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
