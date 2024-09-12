package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

var consumer sarama.Consumer

// InitConsumer initializes Kafka consumer
func InitConsumer() {
	var err error
	consumer, err = sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}
}

func consumeMessages(topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to start consumer for partition: %v", err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Println("Consumer started. Listening for messages...")

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Consumed message: %s\n", string(msg.Value))
		case <-signals:
			fmt.Println("Interrupt received, shutting down...")
			return
		}
	}
}

func main() {
	InitConsumer()
	defer consumer.Close()

	topic := "test_topic"
	consumeMessages(topic)
}
