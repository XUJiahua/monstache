package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rwynn/gtm/v2"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Producer interface {
	Produce(topic string, key, data []byte) error
	ProduceBatch(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Sink struct {
	virtualDeleteFieldName string
	opTimeFieldName        string
	producer               Producer
	topicPrefix            string
	rwlock                 sync.RWMutex
	messages               []kafka.Message
	doneC                  chan struct{}
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

	doneC := make(chan struct{})

	sink := &Sink{
		producer:               producer,
		virtualDeleteFieldName: virtualDeleteFieldName,
		topicPrefix:            topicPrefix,
		doneC:                  doneC,
	}

	go func() {
		logrus.Infof("flusher started")
		timestampTicker := time.NewTicker(5 * time.Second)
		defer timestampTicker.Stop()
		for {
			select {
			case <-doneC:
				logrus.Infof("flusher stopped")
				return
			case <-timestampTicker.C:
				logrus.Debug("tick...")
				if err := sink.Flush(); err != nil {
					logrus.Errorf("kafka flush failed: %v", err)
				}
			}
		}

	}()

	return sink, nil
}

func (s *Sink) Flush() error {
	var err error
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	if len(s.messages) != 0 {
		err = s.producer.ProduceBatch(context.TODO(), s.messages...)
		if err == nil {
			s.messages = nil
		}
	}

	return err
}

func (s *Sink) process(op *gtm.Op, isDeleteOp bool) error {
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

	message := kafka.Message{Topic: topic, Key: []byte(key), Value: byteData}
	s.rwlock.Lock()
	s.messages = append(s.messages, message)
	s.rwlock.Unlock()

	return nil
}

func (s *Sink) RouteData(op *gtm.Op) (err error) {
	return s.process(op, false)
}

func (s *Sink) RouteDelete(op *gtm.Op) (err error) {
	return s.process(op, true)
}

func (s *Sink) RouteDrop(op *gtm.Op) (err error) {
	return nil
}

func (s *Sink) Close() error {
	s.doneC <- struct{}{}
	return s.producer.Close()
}
