package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	p *kafka.Producer
}

func NewKafkaProducer(brokers string) (*KafkaProducer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logrus.Errorf("Failed to deliver message: %v", ev.TopicPartition)
				} else {
					logrus.Debugf("Produced event to topic %s: key = %-10s value = %s",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()
	return &KafkaProducer{
		p: p,
	}, nil
}

func (k KafkaProducer) Produce(topic string, key, data []byte) error {
	return k.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          data,
	}, nil)
}

func (k KafkaProducer) Close() error {
	// Wait for all messages to be delivered
	k.p.Flush(15 * 1000)
	k.p.Close()
	return nil
}
