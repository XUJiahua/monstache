package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse/view"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestConfig_NamespaceDatabaseMap_TOMLParsing(t *testing.T) {
	tomlStr := `
[sink.clickhouse]
  enabled = true
  endpoint = "http://localhost:8123"
  database = "test"
  [sink.clickhouse.namespace-database-map]
    "newdb" = "olddb"
    "new-settle" = "settle"
`

	type sinkWrapper struct {
		Clickhouse Config `toml:"clickhouse"`
	}
	type configWrapper struct {
		Sink sinkWrapper `toml:"sink"`
	}

	var cfg configWrapper
	_, err := toml.Decode(tomlStr, &cfg)
	require.NoError(t, err)

	assert.True(t, cfg.Sink.Clickhouse.Enabled)
	assert.Equal(t, "http://localhost:8123", cfg.Sink.Clickhouse.Endpoint)
	assert.Equal(t, "test", cfg.Sink.Clickhouse.Database)

	require.Len(t, cfg.Sink.Clickhouse.NamespaceDatabaseMap, 2)
	assert.Equal(t, "olddb", cfg.Sink.Clickhouse.NamespaceDatabaseMap["newdb"])
	assert.Equal(t, "settle", cfg.Sink.Clickhouse.NamespaceDatabaseMap["new-settle"])
}

// mockBulkRequest implements bulk.BulkableRequest for unit testing.
type mockBulkRequest struct {
	namespace string
	id        interface{}
	doc       interface{}
	date      string
}

func (r *mockBulkRequest) GetNamespace() string { return r.namespace }
func (r *mockBulkRequest) GetId() interface{}   { return r.id }
func (r *mockBulkRequest) GetDoc() interface{}  { return r.doc }
func (r *mockBulkRequest) GetDate() string      { return r.date }

var tableFromQueryRE = regexp.MustCompile("`[^`]+`\\.`([^`]+)`")

func TestCommit_ParallelInsert(t *testing.T) {
	const perRequestDelay = 100 * time.Millisecond
	const numTables = 5

	var mu sync.Mutex
	insertedTables := make(map[string]int)

	// mock ClickHouse HTTP endpoint with deliberate delay
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		matches := tableFromQueryRE.FindStringSubmatch(query)
		if len(matches) < 2 {
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}
		table := matches[1]

		time.Sleep(perRequestDelay)

		mu.Lock()
		insertedTables[table]++
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &Client{
		httpClient: server.Client(),
		config: Config{
			Endpoint: server.URL,
			Database: "testdb",
		},
		tablesCache: make(map[string]struct{}),
		viewManager: &view.MockManager{},
		remapper:    NewNsDatabaseRemapper(nil),
	}

	// pre-populate cache so EnsureTableExists skips DB calls
	var requests []bulk.BulkableRequest
	for i := 0; i < numTables; i++ {
		ns := fmt.Sprintf("db.table_%d", i)
		table := view.ConvertToClickhouseTable(ns, "", "")
		client.tablesCache[table] = struct{}{}
		requests = append(requests, &mockBulkRequest{
			namespace: ns,
			id:        fmt.Sprintf("id_%d", i),
			doc:       map[string]interface{}{"field": "value"},
			date:      "2024-01-01",
		})
	}

	start := time.Now()
	err := client.Commit(context.Background(), requests)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Len(t, insertedTables, numTables)

	// parallel: elapsed ~= 1 * delay; sequential would be >= numTables * delay
	maxExpected := time.Duration(numTables) * perRequestDelay
	assert.Less(t, elapsed, maxExpected,
		"inserts should run in parallel, expected < %v but took %v", maxExpected, elapsed)
	t.Logf("parallel commit of %d tables took %v (sequential would be >= %v)", numTables, elapsed, maxExpected)
}

func Test3(t *testing.T) {
	data, err := os.ReadFile("/Users/jiahua/Downloads/复现问题/settle.evo.pspOutgoing_evocloud_hkg_settle_evo_pspOutgoing_testv1_20241122054405.ndjson")
	require.NoError(t, err)
	lines := strings.Split(string(data), "\n")

	docs := lo.Map(lines, func(item string, index int) interface{} {
		var doc map[string]interface{}
		err := json.Unmarshal([]byte(item), &doc)
		require.NoError(t, err)

		return doc
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
		Database:             "evocloud",
		PreprocessStringOnly: true,
	})
	err = client.EnsureTableExists(context.TODO(), []string{"test_12345"})
	require.NoError(t, err)
	err = client.BatchInsertWithPreprocess(context.TODO(), "evocloud", "test_12345", docs)
	require.NoError(t, err)
}
