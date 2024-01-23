package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/rwynn/gtm/v2"
)

type Sink struct {
	virtualDeleteFieldName string
	producer               *KafkaProducerV2
	topicPrefix            string
}

func New(brokers, virtualDeleteFieldName, topicPrefix string) (*Sink, error) {
	producer, err := NewKafkaProducerV2(brokers)
	if err != nil {
		return nil, err
	}

	if virtualDeleteFieldName == "" {
		virtualDeleteFieldName = "is_deleted"
	}
	if topicPrefix == "" {
		topicPrefix = "monstache."
	}

	return &Sink{
		producer:               producer,
		virtualDeleteFieldName: virtualDeleteFieldName,
		topicPrefix:            topicPrefix,
	}, nil
}

func (s Sink) process(op *gtm.Op, isDelete bool) error {
	if s.virtualDeleteFieldName != "" {
		if isDelete {
			op.Data[s.virtualDeleteFieldName] = 1
		} else {
			op.Data[s.virtualDeleteFieldName] = 0
		}
	}
	byteData, err := json.Marshal(op.Data)
	if err != nil {
		return err
	}
	topic := fmt.Sprintf("%s%s", s.topicPrefix, op.Namespace)
	key := fmt.Sprintf("%v", op.Id)
	return s.producer.Produce(topic, []byte(key), byteData)
}

func (s Sink) RouteData(op *gtm.Op) (err error) {
	return s.process(op, false)
}

func (s Sink) RouteDelete(op *gtm.Op) (err error) {
	return s.process(op, true)
}

func (s Sink) RouteDrop(op *gtm.Op) (err error) {
	return nil
}
