package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

var producer sarama.SyncProducer

// InitProducer initializes Kafka producer
func InitProducer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	var err error
	producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
}

func produceMessage(topic, message string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(msg)
	return err
}

func producerHandler(w http.ResponseWriter, r *http.Request) {
	topic := "test_topic"
	message := r.URL.Query().Get("message")

	if message == "" {
		http.Error(w, "Message is required", http.StatusBadRequest)
		return
	}

	err := produceMessage(topic, message)
	if err != nil {
		http.Error(w, "Failed to produce message", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Message sent: %s", message)
}

func main() {
	InitProducer()
	defer producer.Close()

	http.HandleFunc("/produce", producerHandler)
	log.Println("Producer API is running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
