package sinks

import (
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse"
	"github.com/rwynn/monstache/v6/pkg/sinks/common"
	"github.com/rwynn/monstache/v6/pkg/sinks/console"
	"github.com/rwynn/monstache/v6/pkg/sinks/file"
	"github.com/rwynn/monstache/v6/pkg/sinks/kafka"
)

type SinkConfig struct {
	ClickHouseConfig clickhouse.Config      `toml:"clickhouse"`
	KafkaConfig      kafka.Config           `toml:"kafka"`
	FileConfig       file.Config            `toml:"file"`
	ConsoleConfig    console.Config         `toml:"console"`
	Transform        common.TransformConfig `toml:"transform"`
	Bulk             BulkConfig             `toml:"bulk"`
}

type BulkConfig struct {
	Workers              int `toml:"workers"`
	BatchSize            int `toml:"batch-size"`
	FlushIntervalSeconds int `toml:"flush-interval-seconds"`
}
