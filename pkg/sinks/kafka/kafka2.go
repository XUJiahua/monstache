package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"strings"
)

type KafkaProducerV2 struct {
	conn *kafka.Writer
}

func (k KafkaProducerV2) Produce(topic string, key, data []byte) error {
	return k.conn.WriteMessages(context.TODO(), kafka.Message{
		Topic: topic,
		Key:   key,
		Value: data,
	})
}

func NewKafkaProducerV2(brokers string) (*KafkaProducerV2, error) {
	brokerList := strings.Split(brokers, ",")
	conn := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaProducerV2{
		conn: conn,
	}, nil
}
