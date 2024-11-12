package common

import (
	"time"

	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type TransformConfig struct {
	// field names are configurable
	// ReplacingMergeTree engine uses these fields:
	VersionFieldName       string `toml:"system-field-version"`
	VirtualDeleteFieldName string `toml:"system-field-virtual-delete"`

	// Debugging fields
	NamespaceFieldName string `toml:"system-field-namespace"`
	OpTimeFieldName    string `toml:"system-field-op-time"`
	SyncTimeFieldName  string `toml:"system-field-sync-time"`
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
	if transformConfig.VersionFieldName == "" {
		transformConfig.VersionFieldName = "__ver"
	}
	if transformConfig.VirtualDeleteFieldName == "" {
		transformConfig.VirtualDeleteFieldName = "__is_deleted"
	}
	if transformConfig.NamespaceFieldName == "" {
		transformConfig.NamespaceFieldName = "__ns"
	}
	if transformConfig.OpTimeFieldName == "" {
		transformConfig.OpTimeFieldName = "__op_time"
	}
	if transformConfig.SyncTimeFieldName == "" {
		transformConfig.SyncTimeFieldName = "__sync_time"
	}

	sink := &Sink{
		bulkProcessor: bulkProcessor,
		transform:     transformConfig,
	}

	return sink, nil
}

func TimeStampToInt64(ts primitive.Timestamp) int64 {
	return int64(ts.T)<<32 + int64(ts.I)
}

// transform doc and save to bulk processor
func (s *Sink) process(op *gtm.Op, isDeleteOp bool) error {
	now := time.Now().Unix()
	data := make(map[string]interface{})
	data[s.transform.NamespaceFieldName] = op.Namespace
	data["_id"] = op.Id
	// doc is the original data, unmodified
	data["doc"] = op.Data
	data[s.transform.SyncTimeFieldName] = now

	if isDeleteOp {
		data[s.transform.VirtualDeleteFieldName] = 1
	}
	if op.IsSourceOplog() {
		data[s.transform.OpTimeFieldName] = op.Timestamp.T
		data[s.transform.VersionFieldName] = TimeStampToInt64(op.Timestamp)
	} else {
		// parse _id ObjectId, get timestamp
		if id, ok := op.Id.(primitive.ObjectID); ok {
			data[s.transform.VersionFieldName] = id.Timestamp().Unix() << 32
		} else {
			logrus.Warnf("invalid _id type: %T, namespace: %s . Expecting ObjectId. Skip this op.", op.Id, op.Namespace)
			return nil
		}
	}

	request := Request{
		Namespace: op.Namespace,
		Id:        op.Id,
		Doc:       data,
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
