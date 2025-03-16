package rabbitmq

import "github.com/streadway/amqp"

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
		"",   
		r.queue.Name,
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
