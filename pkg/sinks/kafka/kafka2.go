package kafka

import (
	"context"
	"github.com/rwynn/monstache/v6/pkg/metrics"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

type KafkaProducer struct {
	w     *kafka.Writer
	infoL LoggerFunc
}

func (k KafkaProducer) Produce(topic string, key, data []byte) error {
	start := time.Now()
	defer func() {
		elapsed := float64(time.Since(start).Milliseconds())
		metrics.OpsProcessedLatencyHistogram.WithLabelValues("kafka").Observe(elapsed)
		metrics.OpsProcessed.WithLabelValues("kafka").Inc()
	}()

	return k.w.WriteMessages(context.TODO(), kafka.Message{
		Topic: topic,
		Key:   key,
		Value: data,
	})
}

func (k KafkaProducer) ProduceBatch(ctx context.Context, msgs ...kafka.Message) error {
	start := time.Now()
	defer func() {
		elapsed := float64(time.Since(start).Milliseconds())
		metrics.OpsProcessedLatencyHistogram.WithLabelValues("kafka").Observe(elapsed)
		metrics.OpsProcessed.WithLabelValues("kafka").Add(float64(len(msgs)))
	}()

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
