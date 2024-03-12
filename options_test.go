package main

import (
	"fmt"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse"
	"os"
	"testing"
)

func TestToTomlString(t *testing.T) {
	tomlStr := ToTomlString(&configOptions{ClickHouseConfig: clickhouse.ClickHouseConfig{
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
	}})
	fmt.Println(tomlStr)
	os.WriteFile("config.toml", []byte(tomlStr), 0644)
}
