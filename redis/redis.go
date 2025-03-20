package redis

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type RedisTester struct {
	client *redis.Client
	queue  string
}

// Khởi tạo Redis client
func NewRedisTester() (*RedisTester, error) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6380",
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	return &RedisTester{
		client: client,
		queue:  "test_queue",
	}, nil
}

// Đẩy message vào queue
func (r *RedisTester) Produce(msg []byte) error {
	return r.client.LPush(ctx, r.queue, msg).Err()
}

// Lấy message từ queue mà không in ra màn hình
func (r *RedisTester) Consume(messageCount int) error {
	var wg sync.WaitGroup
	wg.Add(messageCount)

	go func() {
		count := 0
		for count < messageCount {
			_, err := r.client.RPop(ctx, r.queue).Result()
			if err == redis.Nil {
				time.Sleep(10 * time.Millisecond) // Nếu queue rỗng, đợi một chút
				continue
			} else if err != nil {
				break
			}
			count++
			wg.Done()
		}
	}()

	wg.Wait()
	return nil
}

// Đóng kết nối Redis
func (r *RedisTester) Close() error {
	return r.client.Close()
}
