package main

import (
	"fmt"
	"os"
	"testing"
)

func TestToTomlString(t *testing.T) {
	tomlStr := ToTomlString(&configOptions{})
	fmt.Println(tomlStr)
	os.WriteFile("config.toml", []byte(tomlStr), 0644)
}
