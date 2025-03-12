package kafka

import "github.com/IBM/sarama"

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
