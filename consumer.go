package cg

import "github.com/Shopify/sarama"

// Consumer can read from Kafka on the returned Consume() channel.
type Consumer interface {
	Consume() <-chan *sarama.ConsumerMessage
	Err() error
}
