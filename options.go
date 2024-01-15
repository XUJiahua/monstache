package main

import (
	"github.com/BurntSushi/toml"
	"strings"
)

func ToTomlString(config *configOptions) string {
	var sb strings.Builder
	err := toml.NewEncoder(&sb).Encode(config)
	if err != nil {
		errorLog.Fatalf("Unable to encode options config")
	}

	return sb.String()
}
