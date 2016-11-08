package cg

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// StartPosition is where the TimeWindowConsumer should start at to seek back
// the Window duration.
type StartPosition int

const (
	// OffsetGroup starts at the committed offset, if no offset is commited or the offset is out of range
	// of what is readable, the consumer will error upon reading.
	OffsetGroup StartPosition = iota
	// OffsetNewest starts at the newest message in Kafka and then seeks back Window amount.
	OffsetNewest
)

// TimeWindowConsumerConfig is used to create a new TimeWindowConsumer.
type TimeWindowConsumerConfig struct {
	CacheDuration time.Duration
	Client        sarama.Client
	Context       context.Context
	Coordinator   *Coordinator
	Start         StartPosition
	Partition     int32
	Topic         string
	Window        time.Duration
}

// Ensures that TimeWindowConsumer fulfils Consumer interface.
var _ Consumer = &TimeWindowConsumer{}

// TimeWindowConsumer is a consumer that finds the current offset in the group
// for the given partition-topic, discovers what time that message happened, and
// then rewinds to past offsets until the provided Window of time is acheived.
type TimeWindowConsumer struct {
	client sarama.Client
	coord  *Coordinator
	sc     *SeekConsumer
	start  StartPosition
	window time.Duration
}

// NewTimeWindowConsumer creates a new consumer that is ready to begin reading.
func NewTimeWindowConsumer(cfg *TimeWindowConsumerConfig) (*TimeWindowConsumer, error) {
	twc := &TimeWindowConsumer{
		client: cfg.Client,
		coord:  cfg.Coordinator,
		start:  cfg.Start,
		window: cfg.Window,
	}
	sc, err := NewSeekConsumer(&SeekConsumerConfig{
		CacheDuration: cfg.CacheDuration,
		Client:        cfg.Client,
		Context:       cfg.Context,
		Coordinator:   cfg.Coordinator,
		Partition:     cfg.Partition,
		Seek:          twc.seek,
		Topic:         cfg.Topic,
	})
	if err != nil {
		return nil, err
	}
	twc.sc = sc
	return twc, nil
}

// CommitOffset writes the provided offset to kafka.
func (twc *TimeWindowConsumer) CommitOffset(offset int64) error {
	return twc.sc.CommitOffset(offset)
}

// Consume returns a channel of Kafka messages on this topic-partition starting
// at the provided offset. This channel will close when there is a non-recoverable error, or
// the context provided at creation time closes.
func (twc *TimeWindowConsumer) Consume() <-chan *sarama.ConsumerMessage {
	return twc.sc.Consume()
}

// Err should be called after the Messages() channel closes to determine if there was an
// error during processing.
func (twc *TimeWindowConsumer) Err() error {
	return twc.sc.Err()
}

func (twc *TimeWindowConsumer) seek(topic string, partition int32) (int64, error) {
	var offset int64
	switch twc.start {
	case OffsetGroup:
		o, err := twc.coord.GetOffset(topic, partition)
		if err != nil {
			return 0, err
		}
		offset = o
	case OffsetNewest:
		offset = sarama.OffsetNewest
	default:
		panic("unknown start type provided")
	}
	t, err := twc.timeAt(topic, partition, offset)
	if err != nil {
		return 0, err
	}
	target := t.Add(-twc.window)
	return twc.binarySearch(topic, partition, target)
}

func (twc *TimeWindowConsumer) binarySearch(topic string, partition int32, target time.Time) (int64, error) {
	lower, upper, err := twc.bounds(topic, partition)
	if err != nil {
		return 0, err
	}
	// GetOffset can at best return the segment the desired time starts in; it doesn't return an accurate offset.
	offset, err := twc.client.GetOffset(topic, partition, target.UnixNano()/int64(time.Millisecond))
	if err == sarama.ErrOffsetOutOfRange {
		// could not get time offset, falling back to mid offset.
		offset = (lower + upper) / 2
	}
	for offset != lower && offset != upper {
		t, err := twc.timeAt(topic, partition, offset)
		if err != nil {
			return 0, err
		}
		if t.After(target) {
			upper = offset
			offset = (lower + offset) / 2
			continue
		}
		lower = offset
		offset = (offset + upper) / 2
	}
	return offset, nil
}

func (twc *TimeWindowConsumer) bounds(topic string, partition int32) (lower, upper int64, err error) {
	lower, err = twc.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}
	upper, err = twc.client.GetOffset(topic, partition, sarama.OffsetNewest)
	return
}

func (twc *TimeWindowConsumer) timeAt(topic string, partition int32, offset int64) (time.Time, error) {
	c, err := sarama.NewConsumerFromClient(twc.client)
	if err != nil {
		return time.Time{}, err
	}
	defer c.Close()
	pc, err := c.ConsumePartition(topic, partition, offset)
	if err != nil {
		return time.Time{}, err
	}
	defer pc.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("deadline exceeded for getting time at offset")
		case msg := <-pc.Messages():
			return msg.Timestamp, nil
		}
	}
}
