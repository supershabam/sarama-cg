package cg

import (
	"context"

	"github.com/Shopify/sarama"
)

// MakeConsumer is a function the Coordinator utilizes to create Consumers for
// topic-partitions. ctx is provided so that the Coordinator can signal when
// the consumer should close.
type MakeConsumer func(ctx context.Context, topic string, partition int32) (Consumer, error)

// Consumer consumes a single partition from a topic in Kafka. The caller should
// read the Messages() channel until completion. Once the Messages() channel is
// closed, the caller should call Err() to determine if there was an error causing
// the Messages() channel to cease.
type Consumer interface {
	Err() error
	Messages() <-chan *sarama.ConsumerMessage
}
