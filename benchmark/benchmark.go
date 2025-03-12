package benchmark

import (
	"fmt"
	"log"
	"sync"
	"time"
)

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
