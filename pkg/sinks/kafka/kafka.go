package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/segmentio/kafka-go"
	"strings"
)

type Config struct {
	Enabled          bool   `toml:"enabled"`
	KafkaBrokers     string `toml:"kafka-brokers"`
	KafkaTopicPrefix string `toml:"kafka-topic-prefix"`
}

type KafkaProducer struct {
	w     *kafka.Writer
	infoL LoggerFunc
}

func (k KafkaProducer) Name() string {
	return "kafka"
}

func (k KafkaProducer) Commit(ctx context.Context, requests []bulk.BulkableRequest) error {
	topicPrefix := "monstache."

	messages := make([]kafka.Message, len(requests))
	for i, request := range requests {
		byteData, err := json.Marshal(request.GetDoc())
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%v", request.GetId())
		message := kafka.Message{
			Topic: topicPrefix + request.GetNamespace(),
			Key:   []byte(key),
			Value: byteData,
		}
		messages[i] = message
	}

	return k.ProduceBatch(ctx, messages...)
}

func (k KafkaProducer) Produce(topic string, key, data []byte) error {
	return k.w.WriteMessages(context.TODO(), kafka.Message{
		Topic: topic,
		Key:   key,
		Value: data,
	})
}

func (k KafkaProducer) ProduceBatch(ctx context.Context, msgs ...kafka.Message) error {
	return k.w.WriteMessages(ctx, msgs...)
}

func (k KafkaProducer) Close() error {
	k.infoL("Closing kafka producer ...")
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
		w:     w,
		infoL: infoL,
	}, nil
}
