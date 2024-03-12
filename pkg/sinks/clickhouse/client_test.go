package clickhouse

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestClient_BatchInsert(t *testing.T) {
	data, err := os.ReadFile("messages.json")
	require.NoError(t, err)
	var rows []interface{}
	err = json.Unmarshal(data, &rows)
	require.NoError(t, err)

	client := NewClient(ClickHouseConfig{
		Endpoint:           "http://10.30.11.112:8123/",
		SkipUnknownFields:  true,
		DateTimeBestEffort: true,
		Auth: Auth{
			User:     "default",
			Password: "",
		},
	})
	err = client.BatchInsert("evocloud", "settle_evo_trans", rows)
	require.NoError(t, err)
}
