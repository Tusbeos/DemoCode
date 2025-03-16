package redis

import (
	"context"
	"sync"

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
		Addr: "localhost:6380",
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

func (r *RedisTester) Consume(messageCount int) error {
	ch := r.pubsub.Channel()
	var wg sync.WaitGroup
	wg.Add(messageCount)
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
