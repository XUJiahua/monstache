package common

import (
	"context"
	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"time"
)

// Sink it's a common Sink, all you need is injecting bulk.Client
type Sink struct {
	virtualDeleteFieldName string
	opTimeFieldName        string
	bulkProcessor          *bulk.BulkProcessor
	updateTimeFieldName    string
}

func (s *Sink) Flush() error {
	return s.bulkProcessor.Flush()
}

func New(client bulk.Client, afterBulk bulk.BulkAfterFunc, virtualDeleteFieldName, opTimeFieldName string) (*Sink, error) {
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

	sink := &Sink{
		virtualDeleteFieldName: virtualDeleteFieldName,
		opTimeFieldName:        opTimeFieldName,
		updateTimeFieldName:    "__update_time",
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
	// __update_time derived from updateTime
	if s.updateTimeFieldName != "" {
		// make it configurable
		if updateTime, ok := op.Data["updateTime"]; ok {
			if tStr, ok := updateTime.(string); ok {
				if t, ok := parseTime(tStr); ok {
					op.Data[s.updateTimeFieldName] = t
				}
			}
		}
	}

	request := Request{
		Namespace: op.Namespace,
		Id:        op.Id,
		Doc:       op.Data,
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
