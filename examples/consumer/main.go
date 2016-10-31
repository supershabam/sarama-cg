package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	cg "github.com/supershabam/sarama-cg"
)

func main() {
	// create sarama client which is needed by sarama-cg
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0
	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}

	// set up ctx, and cancel it on interrupt.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-done
		cancel()
		<-done // if we receive a second signal, let's exit hard.
		os.Exit(1)
	}()

	// create coordinator.
	coord := cg.NewCoordinator(&cg.CoordinatorConfig{
		Client:  client,
		Context: ctx,
		GroupID: "test",
		// Protocols are how we agree on how to assign topic-partitions to consumers.
		// As long as every consumer in the group has at least 1 common protocol (determined by the key),
		// then the group will function.
		// A protocol is an interface, so you can implement your own.
		Protocols: []cg.ProtocolKey{
			{
				Protocol: &cg.HashRing{},
				Key:      "hashring",
			},
		},
		SessionTimeout: 30 * time.Second,
		Heartbeat:      3 * time.Second,
		Topics:         []string{"test"},
	})
	// consume is called when we become responsible for a topic-partition. Ctx cancels when we are
	// no longer responsible for the topic-partition. Offset is the last committed offset for the
	// topic-partition in your consumer group.
	// TODO be able to return an error from this function to bubble a fatal error into the coordinator.
	consume := func(ctx context.Context, topic string, partition int32) {
		if partition != 0 {
			return
		}
		log := logrus.WithFields(logrus.Fields{
			"topic":     topic,
			"partition": partition,
		})
		log.Info("creating consumer")
		sc, err := cg.NewTimeWindowConsumer(&cg.TimeWindowConsumerConfig{
			CacheDuration: time.Second * 10,
			Client:        client,
			Context:       ctx,
			Coordinator:   coord,
			Partition:     partition,
			Topic:         topic,
			Window:        time.Minute * 5,
		})
		if err != nil {
			log.WithError(err).Error("could not create consumer")
			return
		}
		for {
			select {
			case <-ctx.Done():
				log.Info("our parent context canceled")
				return
			case msg, ok := <-sc.Consume():
				if !ok {
					log.Info("consumer channel closed")
					err = sc.Err()
					if err != nil {
						log.WithError(err).Error("ending consume because of error")
					}
					return
				}
				log.WithFields(logrus.Fields{
					"offset": msg.Offset,
					"at":     msg.Timestamp,
				}).Info("read message")
				err := sc.CommitOffset(msg.Offset)
				if err != nil {
					log.WithField("offset", msg.Offset).WithError(err).Error("could not commit offset")
					return
				}
			}
		}

	}
	err = coord.Run(consume)
	if err != nil {
		panic(err)
	}
	os.Exit(0)
}
