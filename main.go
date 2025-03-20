package main

import (
	"fmt"
	"log"

	"demo/benchmark"
	"demo/kafka"
	"demo/rabbitmq"
	"demo/redis"
)

func main() {
	messageCount := 10000

	fmt.Println("=== RabbitMQ ===")
	rabbit, err := rabbitmq.NewRabbitMQTester()
	if err != nil {
		log.Fatalf("RabbitMQ init error: %v", err)
	}
	if err := benchmark.TestBrokerPerformance(rabbit, messageCount); err != nil {
		log.Fatalf("RabbitMQ test error: %v", err)
	}
	rabbit.Close()
	fmt.Println("=== Redis ===")
	redisTester, err := redis.NewRedisTester()
	if err != nil {
		log.Fatalf("Redis init error: %v", err)
	}
	if err := benchmark.TestPubSubPerformance(redisTester, messageCount); err != nil {
		log.Fatalf("Redis test error: %v", err)
	}
	redisTester.Close()

	fmt.Println("=== Kafka ===")
	kafka, err := kafka.NewKafkaTester([]string{"localhost:9092"}, "test_topic")
	if err != nil {
		log.Fatalf("Kafka init error: %v", err)
	}
	if err := benchmark.TestBrokerPerformance(kafka, messageCount); err != nil {
		log.Fatalf("Kafka test error: %v", err)
	}
	kafka.Close()

}
