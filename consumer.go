package cg

import "github.com/Shopify/sarama"

// Consumer can read from Kafka on the returned Consume() channel and
// commit offsets for the topic-partition it represents.
type Consumer interface {
	CommitOffset(offset int64) error
	Consume() <-chan *sarama.ConsumerMessage
	Err() error
	HighWaterMarkOffset() int64
}
