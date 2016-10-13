package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cg "github.com/supershabam/sarama-cg"
)

func main() {
	// create sarama client which is needed by sarama-cg
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_9_0_0
	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}

	// create coordinator
	coord := cg.NewCoordinator(&cg.Config{
		Client:  client,
		GroupID: "test",
		// Protocols are how we agree on how to assign topic-partitions to consumers.
		// As long as every consumer in the group has at least 1 common protocol (determined by the key),
		// then the group will function.
		// A protocol is an interface, so I can implement my own.
		Protocols: []cg.ProtocolKey{
			{
				Protocol: &cg.HashRing{},
				Key:      "hashring",
			},
		},
		SessionTimeout: 30 * time.Second,
		Heartbeat:      3 * time.Second,
		Topics:         []string{"test"},
		// Consume is called every time we become responsible for a topic-partition.
		// This let's us implement our own logic of how to consume a partition.
		Consume: func(ctx context.Context, topic string, partition int32) {
			fmt.Printf("creating consumer %s-%d\n", topic, partition)
			go func() {
				<-ctx.Done()
				fmt.Printf("closing consumer %s-%d\n", topic, partition)
			}()
		},
	})

	// set up ctx, and cancel it on interrupt.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-done
		cancel()
		<-done // if we receive a second signal, let's exit hard.
	}()
	err = coord.Run(ctx)
	if err != nil {
		panic(err)
	}
	os.Exit(0)
}
