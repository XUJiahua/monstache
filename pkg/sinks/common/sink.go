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

	// partition by date
	DateFieldName string `toml:"system-field-date"`

	// Debugging fields
	NamespaceFieldName string `toml:"system-field-namespace"`
	OpTimeFieldName    string `toml:"system-field-op-time"`
	SyncTimeFieldName  string `toml:"system-field-sync-time"`

	EmbedDocFieldName string `toml:"system-field-embed-doc"`

	// embed original data, unmodified
	EmbedDoc bool `toml:"-"`
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
	if transformConfig.DateFieldName == "" {
		transformConfig.DateFieldName = "__date"
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
	if transformConfig.EmbedDocFieldName == "" {
		transformConfig.EmbedDocFieldName = "__doc"
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
	var objectID primitive.ObjectID
	var ok bool
	if objectID, ok = op.Id.(primitive.ObjectID); !ok {
		logrus.Warnf("invalid _id type: %T, namespace: %s . Expecting ObjectId. Skip this op.", op.Id, op.Namespace)
		return nil
	}

	data := op.Data
	if s.transform.EmbedDoc {
		data = make(map[string]interface{})
		data["_id"] = op.Id
		// embed original data, unmodified
		data[s.transform.EmbedDocFieldName] = op.Data
	}

	data[s.transform.NamespaceFieldName] = op.Namespace
	data[s.transform.SyncTimeFieldName] = time.Now().Unix()
	if isDeleteOp {
		data[s.transform.VirtualDeleteFieldName] = 1
	}
	if op.IsSourceOplog() {
		data[s.transform.OpTimeFieldName] = op.Timestamp.T
		data[s.transform.VersionFieldName] = TimeStampToInt64(op.Timestamp)
	} else {
		data[s.transform.VersionFieldName] = objectID.Timestamp().Unix() << 32
	}
	data[s.transform.DateFieldName] = objectID.Timestamp().Format("2006-01-02")

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
