package common

import (
	"context"
	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"time"
)

type SinkConfig struct {
	Transform TransformConfig
	Bulk      BulkConfig
}

type BulkConfig struct {
	Workers   int
	BatchSize int
}

type TransformConfig struct {
	VirtualDeleteFieldName string
	OpTimeFieldName        string
	UpdateTimeFieldName    string
}

// Sink it's a common Sink, all you need is injecting bulk.Client
type Sink struct {
	bulkProcessor *bulk.BulkProcessor
	Transform     TransformConfig
}

func (s *Sink) Flush() error {
	return s.bulkProcessor.Flush()
}

const defaultBulkWorkers = 1
const defaultBulkBatchSize = 1000

func New(client bulk.Client, afterBulk bulk.BulkAfterFunc, sinkConfig SinkConfig) (*Sink, error) {
	if sinkConfig.Bulk.BatchSize == 0 {
		sinkConfig.Bulk.BatchSize = defaultBulkBatchSize
	}
	if sinkConfig.Bulk.Workers == 0 {
		sinkConfig.Bulk.Workers = defaultBulkWorkers
	}
	if sinkConfig.Transform.VirtualDeleteFieldName == "" {
		sinkConfig.Transform.VirtualDeleteFieldName = "__is_deleted"
	}
	if sinkConfig.Transform.OpTimeFieldName == "" {
		sinkConfig.Transform.OpTimeFieldName = "__op_time"
	}
	if sinkConfig.Transform.UpdateTimeFieldName == "" {
		sinkConfig.Transform.UpdateTimeFieldName = "__update_time"
	}

	bulkProcessorService := bulk.NewBulkProcessorService(client)
	bulkProcessorService.Workers(sinkConfig.Bulk.Workers)
	bulkProcessorService.BulkActions(sinkConfig.Bulk.BatchSize)
	bulkProcessorService.FlushInterval(5 * time.Second)
	bulkProcessorService.After(afterBulk)
	bulkProcessor, err := bulkProcessorService.Do(context.TODO())
	if err != nil {
		return nil, err
	}

	sink := &Sink{
		bulkProcessor: bulkProcessor,
		Transform:     sinkConfig.Transform,
	}

	return sink, nil
}

func (s *Sink) process(op *gtm.Op, isDeleteOp bool) error {
	if isDeleteOp && s.Transform.VirtualDeleteFieldName != "" {
		op.Data[s.Transform.VirtualDeleteFieldName] = 1
	}
	if op.IsSourceOplog() && s.Transform.OpTimeFieldName != "" {
		// add new column op_time for tracing/debugging
		op.Data[s.Transform.OpTimeFieldName] = op.Timestamp.T
	}
	// __update_time derived from updateTime
	if s.Transform.UpdateTimeFieldName != "" {
		// make it configurable
		if updateTime, ok := op.Data["updateTime"]; ok {
			if tStr, ok := updateTime.(string); ok {
				if t, ok := parseTime(tStr); ok {
					op.Data[s.Transform.UpdateTimeFieldName] = t
				}
			} else if t, ok := updateTime.(time.Time); ok {
				op.Data[s.Transform.UpdateTimeFieldName] = t.UnixMilli()
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
