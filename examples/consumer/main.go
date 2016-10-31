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

	// declare ahead so we can reference in our consume function before
	// instantiating and starting the coordinator.
	var coord *cg.Coordinator
	consume := func(ctx context.Context, topic string, partition int32, offset int64) {
		fmt.Printf("creating consumer %s-%d@%d\n", topic, partition, offset)
		// let's pretent like we actually read something...
		// and commit our new offset.
		coord.CommitOffset(topic, partition, offset+1)
		<-ctx.Done()
		fmt.Printf("closing consumer %s-%d\n", topic, partition)
	}

	// create coordinator
	coord = cg.NewCoordinator(&cg.Config{
		Client:  client,
		Context: ctx,
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
		Consume: consume,
	})
	err = coord.Run()
	if err != nil {
		panic(err)
	}
	os.Exit(0)
}
