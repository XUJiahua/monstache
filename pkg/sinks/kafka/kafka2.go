package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"strings"
)

type KafkaProducer struct {
	w *kafka.Writer
}

func (k KafkaProducer) Produce(topic string, key, data []byte) error {
	return k.w.WriteMessages(context.TODO(), kafka.Message{
		Topic: topic,
		Key:   key,
		Value: data,
	})
}

func (k KafkaProducer) Close() error {
	return k.w.Close()
}

func NewKafkaProducer(brokers string) (*KafkaProducer, error) {
	brokerList := strings.Split(brokers, ",")
	conn := &kafka.Writer{
		Addr:                   kafka.TCP(brokerList...),
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}

	return &KafkaProducer{
		w: conn,
	}, nil
}
