package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/rwynn/gtm/v2"
)

type Producer interface {
	Produce(topic string, key, data []byte) error
}

type Sink struct {
	virtualDeleteFieldName string
	opTimeFieldName        string
	producer               Producer
	topicPrefix            string
}

func New(producer Producer, virtualDeleteFieldName, opTimeFieldName, topicPrefix string) (*Sink, error) {
	if virtualDeleteFieldName == "" {
		virtualDeleteFieldName = "is_deleted"
	}
	if opTimeFieldName == "" {
		opTimeFieldName = "op_time"
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

func (s Sink) process(op *gtm.Op, isDeleteOp bool) error {
	if isDeleteOp && s.virtualDeleteFieldName != "" {
		op.Data[s.virtualDeleteFieldName] = 1
	}
	if op.IsSourceOplog() && s.opTimeFieldName != "" {
		// add new column op_time for tracing/debugging
		op.Data[s.opTimeFieldName] = op.Timestamp.T
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
	if op.IsSourceOplog() && s.opTimeFieldName != "" {
		// add new column op_time for tracing/debugging
		op.Data[s.opTimeFieldName] = op.Timestamp.T
	}
	return s.process(op, true)
}

func (s Sink) RouteDrop(op *gtm.Op) (err error) {
	return nil
}
