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
			MongoKeepFields: []common.MongoKeepFields{
				{
					Ns:         "config.abc",
					IsDrop:     false,
					KeepFields: []string{"_id"},
				},
				{
					Ns:         "config.abc",
					IsDrop:     false,
					KeepFields: []string{"_id"},
				},
			},
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
