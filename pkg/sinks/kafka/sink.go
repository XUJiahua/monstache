package kafka

import (
	"context"
	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"time"
)

type Request struct {
	namespace string
	id        interface{}
	doc       interface{}
}

func (r Request) GetNamespace() string {
	return r.namespace
}

func (r Request) GetId() interface{} {
	return r.id
}

func (r Request) GetDoc() interface{} {
	return r.doc
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
		virtualDeleteFieldName = "__is_deleted"
	}
	if opTimeFieldName == "" {
		opTimeFieldName = "__op_time"
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

	request := Request{
		namespace: op.Namespace,
		id:        op.Id,
		doc:       op.Data,
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
