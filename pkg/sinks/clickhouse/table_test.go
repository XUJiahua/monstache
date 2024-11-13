package clickhouse

import (
	"context"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestClient_EnsureTableExists(t *testing.T) {
	c := NewClient(Config{
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
