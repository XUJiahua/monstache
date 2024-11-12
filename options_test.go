package main

import (
	"fmt"
	"github.com/rwynn/monstache/v6/pkg/sinks"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse"
	"github.com/rwynn/monstache/v6/pkg/sinks/common"
	"github.com/rwynn/monstache/v6/pkg/sinks/console"
	"github.com/rwynn/monstache/v6/pkg/sinks/file"
	"github.com/rwynn/monstache/v6/pkg/sinks/kafka"
	"os"
	"testing"
)

func TestToTomlString(t *testing.T) {
	clickhouseConfig := clickhouse.Config{
		Enabled:            true,
		Endpoint:           "http://localhost:8123",
		SkipUnknownFields:  true,
		DateTimeBestEffort: true,
		Auth: clickhouse.Auth{
			User:     "default",
			Password: "",
		},
		Sinks: map[string]clickhouse.Namespace{
			"db.col": {
				Database: "db",
				Table:    "table",
			},
		},
	}
	tomlStr := ToTomlString(&configOptions{SinkConfig: sinks.SinkConfig{
		ClickHouseConfig: clickhouseConfig,
		KafkaConfig: kafka.Config{
			Enabled:          false,
			KafkaBrokers:     "",
			KafkaTopicPrefix: "",
		},
		FileConfig: file.Config{
			Enabled: false,
		},
		ConsoleConfig: console.Config{
			Enabled: false,
		},
		Transform: common.TransformConfig{
			VirtualDeleteFieldName: "",
			OpTimeFieldName:        "",
			VersionFieldName:       "",
		},
		Bulk: sinks.BulkConfig{
			Workers:              1,
			BatchSize:            1000,
			FlushIntervalSeconds: 5,
		},
	}})
	fmt.Println(tomlStr)
	os.WriteFile("config.toml", []byte(tomlStr), 0644)
}
