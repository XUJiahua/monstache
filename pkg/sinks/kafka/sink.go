package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"time"
)

type Request struct {
	topic string
	key   []byte
	value []byte
}

func (r Request) GetTopic() string {
	return r.topic
}

func (r Request) GetKey() []byte {
	return r.key
}

func (r Request) GetValue() []byte {
	return r.value
}

type Sink struct {
	virtualDeleteFieldName string
	opTimeFieldName        string
	topicPrefix            string
	bulkProcessor          *bulk.BulkProcessor
}

func (s *Sink) Flush() error {
	return s.bulkProcessor.Flush()
}

func New(client bulk.Client, afterBulk bulk.BulkAfterFunc, virtualDeleteFieldName, opTimeFieldName, topicPrefix string) (*Sink, error) {
	bulkProcessorService := bulk.NewBulkProcessorService(client)
	bulkProcessorService.Workers(1)
	bulkProcessorService.BulkActions(1000)
	bulkProcessorService.FlushInterval(5 * time.Second)
	bulkProcessorService.After(afterBulk)
	bulkProcessor, err := bulkProcessorService.Do(context.TODO())
	if err != nil {
		return nil, err
	}

	if virtualDeleteFieldName == "" {
		virtualDeleteFieldName = "is_deleted"
	}
	if opTimeFieldName == "" {
		opTimeFieldName = "op_time"
	}
	if topicPrefix == "" {
		topicPrefix = "monstache."
	}

	sink := &Sink{
		virtualDeleteFieldName: virtualDeleteFieldName,
		topicPrefix:            topicPrefix,
		opTimeFieldName:        opTimeFieldName,
		bulkProcessor:          bulkProcessor,
	}

	return sink, nil
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

	request := Request{
		topic: topic,
		key:   []byte(key),
		value: byteData,
	}
	s.bulkProcessor.Add(request)

	return nil
}

// RouteData json document for op insert or update
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
	return s.bulkProcessor.Close()
}
