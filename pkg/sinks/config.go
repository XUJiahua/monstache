package sinks

import (
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse"
	"github.com/rwynn/monstache/v6/pkg/sinks/common"
	"github.com/rwynn/monstache/v6/pkg/sinks/console"
	"github.com/rwynn/monstache/v6/pkg/sinks/file"
	"github.com/rwynn/monstache/v6/pkg/sinks/kafka"
)

type SinkConfig struct {
	ClickHouseConfig clickhouse.Config
	KafkaConfig      kafka.Config
	FileConfig       file.Config
	ConsoleConfig    console.Config
	Transform        common.TransformConfig
	Bulk             BulkConfig
}

type BulkConfig struct {
	Workers              int
	BatchSize            int
	FlushIntervalSeconds int
}
