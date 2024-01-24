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

type LoggerFunc func(string, ...interface{})

func NewKafkaProducer(brokers string, infoL LoggerFunc, errorL LoggerFunc) (*KafkaProducer, error) {
	brokerList := strings.Split(brokers, ",")
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokerList...),
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.Hash{},
	}
	if infoL != nil {
		w.Logger = kafka.LoggerFunc(infoL)
	}
	if errorL != nil {
		w.ErrorLogger = kafka.LoggerFunc(errorL)
	}

	return &KafkaProducer{
		w: w,
	}, nil
}
