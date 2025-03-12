package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"demo/kafka"
	"demo/rabbitmq"
	"demo/redis"
)

// Dành cho Kafka và RabbitMQ (broker lưu trữ message)
func TestBrokerPerformance(b interface {
	Produce([]byte) error
	Consume(int) error
	Close() error
}, messageCount int) error {
	start := time.Now()
	for i := 0; i < messageCount; i++ {
		if err := b.Produce([]byte("Hello Broker")); err != nil {
			return err
		}
	}
	if err := b.Consume(messageCount); err != nil {
		return err
	}
	duration := time.Since(start)
	throughput := float64(messageCount) / duration.Seconds()
	fmt.Printf("Processed %d messages in %.2f seconds => Throughput: %.2f msg/s\n",
		messageCount, duration.Seconds(), throughput)
	return nil
}

// Dành cho Redis Pub/Sub, cần chạy consumption song song với production
func TestPubSubPerformance(b interface {
	Produce([]byte) error
	Consume(int) error
	Close() error
}, messageCount int) error {
	var wg sync.WaitGroup
	wg.Add(1)
	var duration time.Duration
	go func() {
		// Đảm bảo subscriber đã sẵn sàng
		time.Sleep(100 * time.Millisecond)
		start := time.Now()
		for i := 0; i < messageCount; i++ {
			if err := b.Produce([]byte("Hello Broker")); err != nil {
				log.Fatalf("Produce error: %v", err)
			}
		}
		if err := b.Consume(messageCount); err != nil {
			log.Fatalf("Consume error: %v", err)
		}
		duration = time.Since(start)
		wg.Done()
	}()
	wg.Wait()
	throughput := float64(messageCount) / duration.Seconds()
	fmt.Printf("Processed %d messages in %.2f seconds => Throughput: %.2f msg/s\n",
		messageCount, duration.Seconds(), throughput)
	return nil
}

// ==================== Main Function ====================

func main() {
	messageCount := 10000

	fmt.Println("=== RabbitMQ ===")
	rabbit, err := rabbitmq.NewRabbitMQTester()
	if err != nil {
		log.Fatalf("RabbitMQ init error: %v", err)
	}
	if err := TestBrokerPerformance(rabbit, messageCount); err != nil {
		log.Fatalf("RabbitMQ test error: %v", err)
	}
	rabbit.Close()
	fmt.Println("=== Redis ===")
	redisTester, err := redis.NewRedisTester()
	if err != nil {
		log.Fatalf("Redis init error: %v", err)
	}
	if err := TestPubSubPerformance(redisTester, messageCount); err != nil {
		log.Fatalf("Redis test error: %v", err)
	}
	redisTester.Close()

	fmt.Println("=== Kafka ===")
	kafka, err := kafka.NewKafkaTester([]string{"localhost:9092"}, "test_topic")
	if err != nil {
		log.Fatalf("Kafka init error: %v", err)
	}
	if err := TestBrokerPerformance(kafka, messageCount); err != nil {
		log.Fatalf("Kafka test error: %v", err)
	}
	kafka.Close()

}
