package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
)

var ctx = context.Background()

// ==================== Kafka Implementation ====================

type KafkaTester struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
	topic    string
}

func NewKafkaTester(brokers []string, topic string) (*KafkaTester, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaTester{producer: producer, consumer: consumer, topic: topic}, nil
}

func (k *KafkaTester) Produce(msg []byte) error {
	message := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(msg),
	}
	_, _, err := k.producer.SendMessage(message)
	return err
}

func (k *KafkaTester) Consume(messageCount int) error {
	partitionConsumer, err := k.consumer.ConsumePartition(k.topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	count := 0
	for range partitionConsumer.Messages() {
		count++
		if count >= messageCount {
			break
		}
	}
	return nil
}

func (k *KafkaTester) Close() error {
	if err := k.producer.Close(); err != nil {
		return err
	}
	if err := k.consumer.Close(); err != nil {
		return err
	}
	return nil
}

// ==================== RabbitMQ Implementation ====================

type RabbitMQTester struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
}

func NewRabbitMQTester() (*RabbitMQTester, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare("test_queue", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return &RabbitMQTester{conn: conn, ch: ch, queue: q}, nil
}

func (r *RabbitMQTester) Produce(msg []byte) error {
	return r.ch.Publish(
		"",           // exchange
		r.queue.Name, // routing key
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		},
	)
}

func (r *RabbitMQTester) Consume(messageCount int) error {
	msgs, err := r.ch.Consume(
		r.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	count := 0
	for range msgs {
		count++
		if count >= messageCount {
			break
		}
	}
	return nil
}

func (r *RabbitMQTester) Close() error {
	if err := r.ch.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}

// ==================== Redis Implementation ====================

type RedisTester struct {
	client  *redis.Client
	pubsub  *redis.PubSub
	channel string
}

func NewRedisTester() (*RedisTester, error) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6380", // sử dụng port 6380 theo ánh xạ từ docker-compose
	})
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	// Đăng ký Pub/Sub ngay từ lúc khởi tạo
	pubsub := client.Subscribe(ctx, "test_channel")
	// Chờ xác nhận đăng ký
	_, err = pubsub.Receive(ctx)
	if err != nil {
		return nil, err
	}
	return &RedisTester{client: client, pubsub: pubsub, channel: "test_channel"}, nil
}

func (r *RedisTester) Produce(msg []byte) error {
	return r.client.Publish(ctx, r.channel, msg).Err()
}

// Với Redis Pub/Sub, subscriber đã được đăng ký từ trước,
// nên chúng ta nhận message song song trong khi publish.
func (r *RedisTester) Consume(messageCount int) error {
	ch := r.pubsub.Channel()
	var wg sync.WaitGroup
	wg.Add(messageCount)
	// Lắng nghe message trên channel
	go func() {
		count := 0
		for range ch {
			count++
			wg.Done()
			if count >= messageCount {
				break
			}
		}
	}()
	wg.Wait()
	return nil
}

func (r *RedisTester) Close() error {
	if err := r.pubsub.Close(); err != nil {
		return err
	}
	return r.client.Close()
}

// ==================== Benchmark Functions ====================

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
	rabbit, err := NewRabbitMQTester()
	if err != nil {
		log.Fatalf("RabbitMQ init error: %v", err)
	}
	if err := TestBrokerPerformance(rabbit, messageCount); err != nil {
		log.Fatalf("RabbitMQ test error: %v", err)
	}
	rabbit.Close()
	fmt.Println("=== Redis ===")
	redisTester, err := NewRedisTester()
	if err != nil {
		log.Fatalf("Redis init error: %v", err)
	}
	if err := TestPubSubPerformance(redisTester, messageCount); err != nil {
		log.Fatalf("Redis test error: %v", err)
	}
	redisTester.Close()

	fmt.Println("=== Kafka ===")
	kafka, err := NewKafkaTester([]string{"localhost:9092"}, "test_topic")
	if err != nil {
		log.Fatalf("Kafka init error: %v", err)
	}
	if err := TestBrokerPerformance(kafka, messageCount); err != nil {
		log.Fatalf("Kafka test error: %v", err)
	}
	kafka.Close()

}
