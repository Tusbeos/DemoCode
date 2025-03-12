package redis

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

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
