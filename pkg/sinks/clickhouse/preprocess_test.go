package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPreprocessBatch_LargeInt64Precision verifies that preprocessBatch preserves
// int64 precision for large numbers like __ver (oplog timestamp << 32 + increment).
// This was the root cause: json.Unmarshal decodes numbers as float64, which only
// has 53-bit mantissa — any integer > 2^53 loses trailing digits.
func TestPreprocessBatch_LargeInt64Precision(t *testing.T) {
	const ver = int64(7604288878022754304)
	logger := logrus.WithField("test", t.Name())

	rows := []interface{}{
		map[string]interface{}{
			"_id":          "abc123",
			"__ver":        ver,
			"__is_deleted": 0,
			"__ns":         "db.collection",
			"__doc": map[string]interface{}{
				"name":  "test",
				"count": int64(9007199254740993), // 2^53 + 1, also exceeds float64 precision
			},
		},
	}

	result, err := preprocessBatch(rows, logger, false)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// The critical check: re-marshal the preprocessed doc (this is what gets sent to ClickHouse)
	jsonData, err := json.Marshal(result[0])
	require.NoError(t, err)
	jsonStr := string(jsonData)

	// __ver must appear with exact digits, not truncated to ...754000
	assert.Contains(t, jsonStr, "7604288878022754304",
		"__ver should preserve full int64 precision after preprocess, got: %s", jsonStr)

	// nested large int should also be preserved
	assert.Contains(t, jsonStr, "9007199254740993",
		"nested large int64 should preserve precision, got: %s", jsonStr)

	// must NOT contain the truncated value
	assert.NotContains(t, jsonStr, "7604288878022754000",
		"__ver must not be truncated to float64 precision")
}

// TestPreprocessBatch_TypeInference verifies that json.Number values are correctly
// recognized by MapTraveler's type inference (inferGoType handles json.Number).
func TestPreprocessBatch_TypeInference(t *testing.T) {
	logger := logrus.WithField("test", t.Name())

	rows := []interface{}{
		map[string]interface{}{
			"_id":    "doc1",
			"__ver":  int64(7604288878022754304),
			"score":  3.14,
			"count":  42,
			"active": true,
			"name":   "hello",
		},
	}

	result, err := preprocessBatch(rows, logger, false)
	require.NoError(t, err)
	require.Len(t, result, 1)

	doc := result[0]

	// After UseNumber(), integers become json.Number, floats become json.Number
	// MapTraveler.inferGoType distinguishes them by checking for "."
	// Verify the values are json.Number and marshal correctly
	verVal := doc["__ver"]
	_, isJsonNumber := verVal.(json.Number)
	assert.True(t, isJsonNumber, "__ver should be json.Number, got %T", verVal)

	scoreVal := doc["score"]
	_, isJsonNumber = scoreVal.(json.Number)
	assert.True(t, isJsonNumber, "score should be json.Number, got %T", scoreVal)

	// Verify json.Number string representations
	assert.Equal(t, "7604288878022754304", fmt.Sprintf("%v", doc["__ver"]))
	assert.Equal(t, "3.14", fmt.Sprintf("%v", doc["score"]))
}

// TestPreprocessBatch_AssignDefaultValues verifies that when one doc has a field
// and another doesn't, the default value is correctly assigned even with json.Number types.
func TestPreprocessBatch_AssignDefaultValues(t *testing.T) {
	logger := logrus.WithField("test", t.Name())

	rows := []interface{}{
		map[string]interface{}{
			"_id":   "doc1",
			"__ver": int64(100),
			"__doc": map[string]interface{}{
				"name":  "alice",
				"score": 99.5,
				"level": 10,
			},
		},
		map[string]interface{}{
			"_id":   "doc2",
			"__ver": int64(200),
			"__doc": map[string]interface{}{
				"name": "bob",
				// missing "score" and "level" — should get default values
			},
		},
	}

	result, err := preprocessBatch(rows, logger, false)
	require.NoError(t, err)
	require.Len(t, result, 2)

	doc2 := result[1]
	innerDoc, ok := doc2["__doc"].(map[string]interface{})
	require.True(t, ok)

	// "score" was float64 (json.Number with "."), default should be float64(0)
	assert.Contains(t, innerDoc, "score", "missing field 'score' should be filled with default")
	// "level" was int64 (json.Number without "."), default should be int64(0)
	assert.Contains(t, innerDoc, "level", "missing field 'level' should be filled with default")
}

// TestPreprocessBatch_StringOnlyMode verifies that stringOnly=true only assigns
// default values for string fields, leaving other types unset.
func TestPreprocessBatch_StringOnlyMode(t *testing.T) {
	logger := logrus.WithField("test", t.Name())

	rows := []interface{}{
		map[string]interface{}{
			"_id": "doc1",
			"__doc": map[string]interface{}{
				"name":  "alice",
				"score": 99.5,
			},
		},
		map[string]interface{}{
			"_id":   "doc2",
			"__doc": map[string]interface{}{
				// missing both "name" (string) and "score" (float)
			},
		},
	}

	result, err := preprocessBatch(rows, logger, true) // stringOnly = true
	require.NoError(t, err)
	require.Len(t, result, 2)

	doc2Inner, ok := result[1]["__doc"].(map[string]interface{})
	require.True(t, ok)

	// string field should get default
	assert.Contains(t, doc2Inner, "name", "string field should get default in stringOnly mode")
	assert.Equal(t, "", doc2Inner["name"])

	// numeric field should NOT get default in stringOnly mode
	assert.NotContains(t, doc2Inner, "score", "numeric field should not get default in stringOnly mode")
}

// TestPreprocessBatch_EndToEnd_JSONOutput simulates the full pipeline:
// original map → preprocessBatch → json.Marshal (what gets sent to ClickHouse).
// Verifies the final JSON payload has exact numeric values.
func TestPreprocessBatch_EndToEnd_JSONOutput(t *testing.T) {
	logger := logrus.WithField("test", t.Name())

	ver1 := int64(7604288878022754304)
	ver2 := int64(7604288878022754305)

	rows := []interface{}{
		map[string]interface{}{
			"_id":          "id1",
			"__ver":        ver1,
			"__is_deleted": 0,
			"__ns":         "db.orders",
			"__op_time":    int64(1770511474),
			"__sync_time":  int64(1770511475),
			"__date":       "2026-02-08",
			"__doc": map[string]interface{}{
				"amount": 12345.67,
				"qty":    int64(9999999999999999), // 16 digits, near float64 boundary
			},
		},
		map[string]interface{}{
			"_id":          "id2",
			"__ver":        ver2,
			"__is_deleted": 1,
			"__ns":         "db.orders",
			"__op_time":    int64(1770511474),
			"__sync_time":  int64(1770511476),
			"__date":       "2026-02-08",
			"__doc": map[string]interface{}{
				"amount": 0.01,
				// missing "qty" — should get default
			},
		},
	}

	result, err := preprocessBatch(rows, logger, false)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Marshal each row as JSONEachRow (same as BatchInsert does)
	var lines []string
	for _, doc := range result {
		data, err := json.Marshal(doc)
		require.NoError(t, err)
		lines = append(lines, string(data))
	}

	payload := strings.Join(lines, "\n")

	// Exact __ver values must survive the full round-trip
	assert.Contains(t, payload, "7604288878022754304", "ver1 must be exact in final JSON")
	assert.Contains(t, payload, "7604288878022754305", "ver2 must be exact in final JSON")
	assert.Contains(t, payload, "9999999999999999", "large qty must be exact in final JSON")

	// Truncated values must NOT appear
	assert.NotContains(t, payload, "7604288878022754000", "ver1 must not be truncated")
	assert.NotContains(t, payload, "10000000000000000", "qty must not be rounded up to 1e16")

	t.Logf("Final JSON payload:\n%s", payload)
}

// TestPreprocessBatch_WithoutUseNumber_PrecisionLoss reproduces the original bug:
// when useNumber=false, json.Unmarshal decodes all numbers as float64, and
// re-marshaling truncates large int64 values.
func TestPreprocessBatch_WithoutUseNumber_PrecisionLoss(t *testing.T) {
	const ver = int64(7604288878022754304)
	logger := logrus.WithField("test", t.Name())

	rows := []interface{}{
		map[string]interface{}{
			"_id":          "abc123",
			"__ver":        ver,
			"__is_deleted": 0,
			"__ns":         "db.collection",
			"__doc": map[string]interface{}{
				"name":  "test",
				"count": int64(9007199254740993), // 2^53 + 1
			},
		},
	}

	// useNumber=false → reproduces the bug
	result, err := preprocessBatch(rows, logger, false, false)
	require.NoError(t, err)
	require.Len(t, result, 1)

	doc := result[0]

	// Without UseNumber, numbers become float64
	verVal := doc["__ver"]
	_, isFloat := verVal.(float64)
	assert.True(t, isFloat, "__ver should be float64 without UseNumber, got %T", verVal)

	// Re-marshal — this is what gets sent to ClickHouse
	jsonData, err := json.Marshal(result[0])
	require.NoError(t, err)
	jsonStr := string(jsonData)

	t.Logf("JSON without UseNumber: %s", jsonStr)

	// BUG: __ver is truncated from 7604288878022754304 → 7604288878022754000
	assert.Contains(t, jsonStr, "7604288878022754000",
		"without UseNumber, __ver should be truncated to float64 precision")
	assert.NotContains(t, jsonStr, "7604288878022754304",
		"without UseNumber, exact __ver value should NOT survive")

	// BUG: 2^53+1 is truncated to 2^53
	assert.NotContains(t, jsonStr, "9007199254740993",
		"without UseNumber, 2^53+1 should lose precision")
}

// TestPreprocessBatch_WithUseNumber_PrecisionPreserved is the mirror of the above:
// same data, but useNumber=true (default) — precision is preserved.
func TestPreprocessBatch_WithUseNumber_PrecisionPreserved(t *testing.T) {
	const ver = int64(7604288878022754304)
	logger := logrus.WithField("test", t.Name())

	rows := []interface{}{
		map[string]interface{}{
			"_id":          "abc123",
			"__ver":        ver,
			"__is_deleted": 0,
			"__ns":         "db.collection",
			"__doc": map[string]interface{}{
				"name":  "test",
				"count": int64(9007199254740993),
			},
		},
	}

	// useNumber=true (default) → precision preserved
	result, err := preprocessBatch(rows, logger, false, true)
	require.NoError(t, err)
	require.Len(t, result, 1)

	doc := result[0]

	// With UseNumber, numbers become json.Number
	verVal := doc["__ver"]
	_, isJsonNumber := verVal.(json.Number)
	assert.True(t, isJsonNumber, "__ver should be json.Number with UseNumber, got %T", verVal)

	// Re-marshal
	jsonData, err := json.Marshal(result[0])
	require.NoError(t, err)
	jsonStr := string(jsonData)

	t.Logf("JSON with UseNumber: %s", jsonStr)

	// FIX: exact values survive
	assert.Contains(t, jsonStr, "7604288878022754304",
		"with UseNumber, __ver should preserve full precision")
	assert.NotContains(t, jsonStr, "7604288878022754000",
		"with UseNumber, __ver must NOT be truncated")
	assert.Contains(t, jsonStr, "9007199254740993",
		"with UseNumber, 2^53+1 should preserve precision")
}

// TestPreprocessBatch_Integration_ClickHouse inserts two rows into a real ClickHouse:
//   - row 1: useNumber=false → __ver precision lost (7604288878022754304 → 7604288878022754000)
//   - row 2: useNumber=true  → __ver precision preserved (7604288878022754304)
//
// After inserting, it queries the table and prints both rows for visual comparison.
// Run with: go test -v -run TestPreprocessBatch_Integration_ClickHouse ./pkg/sinks/clickhouse/
func TestPreprocessBatch_Integration_ClickHouse(t *testing.T) {
	const (
		endpoint = "http://10.30.11.112:8123/"
		tcp      = "10.30.11.112:9000"
		database = "evocloud"
		table    = "test_use_number"
		ver      = int64(7604288878022754304)
	)

	logger := logrus.WithField("test", t.Name())

	client, _ := NewClient(Config{
		Endpoint:           endpoint,
		EndpointTCP:        tcp,
		SkipUnknownFields:  true,
		DateTimeBestEffort: true,
		Auth:               Auth{User: "default", Password: ""},
		Database:           database,
	})

	// ensure table exists
	err := client.EnsureTableExists(context.TODO(), []string{table})
	require.NoError(t, err)

	// --- row 1: useNumber=false (BUG path) ---
	rowBug := []interface{}{
		map[string]interface{}{
			"_id":          "precision_lost",
			"__ver":        ver,
			"__is_deleted": 0,
			"__ns":         "test.precision",
			"__op_time":    int64(1770511474),
			"__sync_time":  time.Now().Unix(),
			"__date":       "2026-04-03",
			"__doc": map[string]interface{}{
				"label": "without UseNumber",
			},
		},
	}
	preprocessedBug, err := preprocessBatch(rowBug, logger, false, false) // useNumber=false
	require.NoError(t, err)
	var docsBug []interface{}
	for _, d := range preprocessedBug {
		docsBug = append(docsBug, d)
	}
	err = client.BatchInsert(context.TODO(), database, table, docsBug)
	require.NoError(t, err)

	// --- row 2: useNumber=true (FIX path) ---
	rowFix := []interface{}{
		map[string]interface{}{
			"_id":          "precision_kept",
			"__ver":        ver,
			"__is_deleted": 0,
			"__ns":         "test.precision",
			"__op_time":    int64(1770511474),
			"__sync_time":  time.Now().Unix(),
			"__date":       "2026-04-03",
			"__doc": map[string]interface{}{
				"label": "with UseNumber",
			},
		},
	}
	preprocessedFix, err := preprocessBatch(rowFix, logger, false, true) // useNumber=true
	require.NoError(t, err)
	var docsFix []interface{}
	for _, d := range preprocessedFix {
		docsFix = append(docsFix, d)
	}
	err = client.BatchInsert(context.TODO(), database, table, docsFix)
	require.NoError(t, err)

	// --- query and compare ---
	querySQL := fmt.Sprintf(
		"SELECT _id, __ver FROM `%s`.`%s` WHERE _id IN ('precision_lost','precision_kept') ORDER BY _id",
		database, table,
	)
	rows, err := client.db.QueryContext(context.TODO(), querySQL)
	require.NoError(t, err)
	defer rows.Close()

	t.Logf("%-20s %s", "_id", "__ver")
	t.Logf("%-20s %s", "---", "---")
	for rows.Next() {
		var id string
		var v uint64
		require.NoError(t, rows.Scan(&id, &v))
		t.Logf("%-20s %d", id, v)

		if id == "precision_kept" {
			assert.Equal(t, uint64(ver), v, "useNumber=true should preserve exact __ver")
		}
		if id == "precision_lost" {
			assert.Equal(t, uint64(7604288878022754000), v, "useNumber=false should show truncated __ver")
		}
	}
	require.NoError(t, rows.Err())
}
