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

	client := NewClient("http://10.30.11.112:8123/")
	err = client.BatchInsert("evocloud", "settle_evo_trans", rows)
	require.NoError(t, err)
}
