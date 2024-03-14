package clickhouse

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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

	client := NewClient(Config{
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
