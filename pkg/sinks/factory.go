package sinks

import (
	"context"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse/view"
	"time"

	"github.com/pkg/errors"
	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse"
	"github.com/rwynn/monstache/v6/pkg/sinks/common"
	"github.com/rwynn/monstache/v6/pkg/sinks/console"
	"github.com/rwynn/monstache/v6/pkg/sinks/file"
	"github.com/rwynn/monstache/v6/pkg/sinks/kafka"
	"github.com/sirupsen/logrus"
)

type SinkConnector interface {
	// RouteData expect op contain full document
	RouteData(op *gtm.Op) (err error)
	// RouteDelete _id is expected
	RouteDelete(op *gtm.Op) (err error)
	// RouteDrop drop database/collection
	RouteDrop(op *gtm.Op) (err error)
	// Flush the batch messages
	Flush() error
}

type Closer interface {
	Close() error
}

const defaultBulkWorkers = 1
const defaultBulkBatchSize = 1000
const defaultBulkFlushIntervalSeconds = 5

func CreateSink(sinkConfig SinkConfig, afterBulk bulk.BulkAfterFunc) (SinkConnector, view.Manager, []Closer, error) {
	if sinkConfig.Bulk.BatchSize == 0 {
		sinkConfig.Bulk.BatchSize = defaultBulkBatchSize
	}
	if sinkConfig.Bulk.Workers == 0 {
		sinkConfig.Bulk.Workers = defaultBulkWorkers
	}
	if sinkConfig.Bulk.FlushIntervalSeconds == 0 {
		sinkConfig.Bulk.FlushIntervalSeconds = defaultBulkFlushIntervalSeconds
	}

	var err error
	var closers []Closer

	// clickhouse, kafka, file, use bulk processor + common sink
	var client bulk.Client
	var viewManager view.Manager
	if sinkConfig.ClickHouseConfig.Enabled {
		client, viewManager = clickhouse.NewClient(sinkConfig.ClickHouseConfig)
	} else if sinkConfig.KafkaConfig.Enabled {
		client, err = kafka.NewKafkaProducer(sinkConfig.KafkaConfig.KafkaBrokers, func(s string, i ...interface{}) {
			logrus.Debugf(s, i...)
		}, func(s string, i ...interface{}) {
			logrus.Errorf(s, i...)
		})
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "unable to create kafka producer")
		}

	} else if sinkConfig.FileConfig.Enabled {
		client = &file.Client{}
	} else if sinkConfig.ConsoleConfig.Enabled {
		return &console.Client{}, nil, nil, nil
	} else {
		return nil, nil, nil, nil
	}

	bulkProcessorService := bulk.NewBulkProcessorService(client)
	bulkProcessorService.Workers(sinkConfig.Bulk.Workers)
	bulkProcessorService.BulkActions(sinkConfig.Bulk.BatchSize)
	bulkProcessorService.FlushInterval(time.Duration(sinkConfig.Bulk.FlushIntervalSeconds) * time.Second)
	bulkProcessorService.After(afterBulk)
	bulkProcessor, err := bulkProcessorService.Do(context.TODO())
	if err != nil {
		return nil, nil, nil, err
	}

	sinkConfig.Transform.EmbedDoc = client.EmbedDoc()
	sink, err := common.New(sinkConfig.Transform, bulkProcessor)
	if err != nil {
		return nil, nil, nil, err
	}
	closers = append(closers, sink)

	return sink, viewManager, closers, nil
}
