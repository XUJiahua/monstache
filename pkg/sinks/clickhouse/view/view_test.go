package view

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

func TestViewBuilder_Build(t *testing.T) {
	vb := NewViewBuilder("test_table", "test_table_view", NewMockTableFieldCollector("test_table", []string{"id", "name"}))
	sql, err := vb.Build()
	assert.Nil(t, err)
	fmt.Println(sql)
	assert.Equal(t, "CREATE VIEW IF NOT EXISTS test_table_view AS\nSELECT\n`id`,\n`name`\nFROM test_table", strings.TrimSpace(sql))
}
