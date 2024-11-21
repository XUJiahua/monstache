package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse/view"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestClient_BatchInsert(t *testing.T) {
	data, err := os.ReadFile("messages.json")
	require.NoError(t, err)
	var twoRows []interface{}
	err = json.Unmarshal(data, &twoRows)
	require.NoError(t, err)

	var rows []interface{}
	// try 20 objects in one call
	for i := 0; i < 10; i++ {
		rows = append(rows, twoRows...)
	}

	client, _ := NewClient(Config{
		Endpoint:           "http://10.30.11.112:8123/",
		SkipUnknownFields:  true,
		DateTimeBestEffort: true,
		Auth: Auth{
			User:     "default",
			Password: "",
		},
	})
	err = client.BatchInsert(context.TODO(), "evocloud", "settle_evo_trans", rows)
	require.NoError(t, err)
}

func TestClient_BatchInsert2(t *testing.T) {
	data, err := os.ReadFile("messages2.json")
	require.NoError(t, err)
	var twoRows []interface{}
	err = json.Unmarshal(data, &twoRows)
	require.NoError(t, err)

	var rows []interface{}
	// try 20 objects in one call
	for i := 0; i < 10; i++ {
		rows = append(rows, twoRows...)
	}

	client, _ := NewClient(Config{
		Endpoint:           "http://10.30.11.112:8123/",
		EndpointTCP:        "10.30.11.112:9000",
		SkipUnknownFields:  true,
		DateTimeBestEffort: true,
		Auth: Auth{
			User:     "default",
			Password: "",
		},
		Database: "evocloud",
	})
	err = client.EnsureTableExists(context.TODO(), []string{"test_123"})
	require.NoError(t, err)
	err = client.BatchInsert(context.TODO(), "evocloud", "test_123", rows)
	require.NoError(t, err)
}

func Test2(t *testing.T) {
	data, err := os.ReadFile("NUMBER_OF_DIMENSIONS_MISMATCHED.ndjson")
	require.NoError(t, err)
	lines := strings.Split(string(data), "\n")

	docs := lo.Map(lines, func(item string, index int) interface{} {
		var doc map[string]interface{}
		err := json.Unmarshal([]byte(item), &doc)
		require.NoError(t, err)

		// channel 有问题啊
		//delete(doc, "channels")

		m := make(map[string]interface{})
		m["__doc"] = doc
		uid, _ := uuid.NewUUID()
		m["_id"] = uid.String()
		return m
	})

	client, _ := NewClient(Config{
		Endpoint:           "http://10.30.11.112:8123/",
		EndpointTCP:        "10.30.11.112:9000",
		SkipUnknownFields:  true,
		DateTimeBestEffort: true,
		Auth: Auth{
			User:     "default",
			Password: "",
		},
		Database: "evocloud",
	})
	err = client.EnsureTableExists(context.TODO(), []string{"test_1234"})
	require.NoError(t, err)
	err = client.BatchInsertWithPreprocess(context.TODO(), "evocloud", "test_1234", docs)
	require.NoError(t, err)
}

func TestAssignDefaultValues(t *testing.T) {
	data, err := os.ReadFile("NUMBER_OF_DIMENSIONS_MISMATCHED.ndjson")
	require.NoError(t, err)
	lines := strings.Split(string(data), "\n")

	// collect fields
	traveler := view.NewMapTraveler()
	lo.Map(lines, func(item string, index int) int {
		var doc map[string]interface{}
		err := json.Unmarshal([]byte(item), &doc)
		require.NoError(t, err)

		traveler.Collect(doc)
		fmt.Printf("handled types: %v\n", traveler.HandledTypes())
		fmt.Printf("unhandled types: %v\n", traveler.UnhandledTypes())

		return 0
	})

	// assign default values
	lo.Map(lines, func(item string, index int) int {
		var doc map[string]interface{}
		err := json.Unmarshal([]byte(item), &doc)
		require.NoError(t, err)

		traveler.AssignDefaultValues(doc)
		spew.Dump(doc)

		return 0
	})

}
