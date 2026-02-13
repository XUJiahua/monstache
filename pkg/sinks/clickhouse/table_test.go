package clickhouse

import (
	"context"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestClient_EnsureTableExists(t *testing.T) {
	c, _ := NewClient(Config{
		Enabled:            false,
		EndpointTCP:        "10.30.11.112:9000",
		SkipUnknownFields:  false,
		DateTimeBestEffort: false,
		Auth: Auth{
			User:     "default",
			Password: "",
		},
		Database:    "evocloud",
		TablePrefix: "hk_",
		TableSuffix: "_v1",
	})

	tables := []string{"hk_aaa_v1", "hk_bbb_v1"}

	err := c.EnsureTableExists(context.TODO(), tables)
	require.NoError(t, err)

	var cacheTables []string
	for table := range c.tablesCache {
		cacheTables = append(cacheTables, table)
	}
	sort.Strings(tables)
	sort.Strings(cacheTables)
	require.EqualValues(t, tables, cacheTables)

	err = c.EnsureTableExists(context.TODO(), tables)
	require.NoError(t, err)
}

func TestRenderCreateSQL_ObjectJSON(t *testing.T) {
	sql, err := renderCreateSQL("mydb", "mytable", "Object('json')")
	require.NoError(t, err)

	assert.Contains(t, sql, "IF NOT EXISTS mydb.mytable")
	assert.Contains(t, sql, "__doc Object('json')")
	assert.Contains(t, sql, "_id String")
	assert.Contains(t, sql, "ENGINE = ReplacingMergeTree")
}

func TestRenderCreateSQL_String(t *testing.T) {
	sql, err := renderCreateSQL("mydb", "mytable", "String")
	require.NoError(t, err)

	assert.Contains(t, sql, "IF NOT EXISTS mydb.mytable")
	assert.Contains(t, sql, "__doc String")
	assert.NotContains(t, sql, "Object('json')")
}

func TestRenderCreateSQL_DifferentDatabaseAndTable(t *testing.T) {
	sql, err := renderCreateSQL("prod_db", "user_events", "Object('json')")
	require.NoError(t, err)

	assert.Contains(t, sql, "IF NOT EXISTS prod_db.user_events")
}
