package common

import (
	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"time"
)

type TransformConfig struct {
	VirtualDeleteFieldName string
	OpTimeFieldName        string
	UpdateTimeFieldName    string
}

// Sink it's a common Sink, all you need is injecting bulk.Client
type Sink struct {
	bulkProcessor *bulk.BulkProcessor
	transform     TransformConfig
}

func (s *Sink) Flush() error {
	return s.bulkProcessor.Flush()
}

func New(transformConfig TransformConfig, bulkProcessor *bulk.BulkProcessor) (*Sink, error) {
	if transformConfig.VirtualDeleteFieldName == "" {
		transformConfig.VirtualDeleteFieldName = "__is_deleted"
	}
	if transformConfig.OpTimeFieldName == "" {
		transformConfig.OpTimeFieldName = "__op_time"
	}
	if transformConfig.UpdateTimeFieldName == "" {
		transformConfig.UpdateTimeFieldName = "__update_time"
	}

	sink := &Sink{
		bulkProcessor: bulkProcessor,
		transform:     transformConfig,
	}

	return sink, nil
}

// transform doc and save to bulk processor
func (s *Sink) process(op *gtm.Op, isDeleteOp bool) error {
	if isDeleteOp && s.transform.VirtualDeleteFieldName != "" {
		op.Data[s.transform.VirtualDeleteFieldName] = 1
	}
	if op.IsSourceOplog() && s.transform.OpTimeFieldName != "" {
		// add new column op_time for tracing/debugging
		op.Data[s.transform.OpTimeFieldName] = op.Timestamp.T
	}
	// __update_time derived from updateTime
	if s.transform.UpdateTimeFieldName != "" {
		// make it configurable
		if updateTime, ok := op.Data["updateTime"]; ok {
			if tStr, ok := updateTime.(string); ok {
				if t, ok := parseTime(tStr); ok {
					op.Data[s.transform.UpdateTimeFieldName] = t
				}
			} else if t, ok := updateTime.(time.Time); ok {
				op.Data[s.transform.UpdateTimeFieldName] = t.UnixMilli()
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
