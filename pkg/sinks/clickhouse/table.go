package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

var createTpl *template.Template

func init() {
	createTemplate := `
CREATE TABLE
    IF NOT EXISTS {{.Database}}.{{.Table}} (
        _id String,
        __doc JSON,
        __date Date,
        __ver UInt64 DEFAULT 0, -- version, derived from oplog timestamp or _id timestamp
        __is_deleted UInt8 DEFAULT 0, -- 0:未删除 1:已删除 默认值为 0
        __ns String DEFAULT '', -- namespace
        __op_time UInt64 DEFAULT 0, -- for tracing oplog
        __sync_time UInt64 DEFAULT 0 -- for tracing
    ) ENGINE = ReplacingMergeTree (__ver, __is_deleted)
PARTITION BY
    __date
ORDER BY
    _id;
`
	var err error
	createTpl, err = template.New("create").Parse(createTemplate)
	if err != nil {
		panic(err)
	}
}

func (c *Client) EnsureTableExists(ctx context.Context, tables []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	database := c.config.Database
	logrus.Debugf("ensure tables exists: %v", tables)
	tableNames := lo.Filter(tables, func(table string, _ int) bool {
		_, ok := c.tablesCache[table]
		return !ok
	})
	logrus.Debugf("after filtering out existing tables: %v", tableNames)

	checkSQL := fmt.Sprintf(`
		SELECT name 
		FROM system.tables 
		WHERE database = '%s' AND name IN ('%s')
	`, database, strings.Join(tableNames, "','"))

	rows, err := c.db.QueryContext(ctx, checkSQL)
	if err != nil {
		return fmt.Errorf("check tables existence failed: %w", err)
	}
	defer rows.Close()

	existingTables := make(map[string]struct{})
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("scan table name failed: %w", err)
		}
		existingTables[table] = struct{}{}
		c.tablesCache[table] = struct{}{}
	}

	if _, err := c.db.ExecContext(ctx, "SET allow_experimental_object_type = 1;"); err != nil {
		return fmt.Errorf("set allow_experimental_object_type failed: %w", err)
	}
	for _, table := range tableNames {
		if _, ok := existingTables[table]; ok {
			continue
		}

		logrus.Infof("Creating Clickhouse table %s.%s ...", database, table)

		var buf bytes.Buffer
		if err := createTpl.Execute(&buf, map[string]string{
			"Database": database,
			"Table":    table,
		}); err != nil {
			return fmt.Errorf("execute create template failed: %w", err)
		}
		createSQL := buf.String()

		if _, err := c.db.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("create table %s failed: %w", table, err)
		}

		c.tablesCache[table] = struct{}{}
		logrus.Infof("Created Clickhouse table %s.%s", database, table)
	}

	return nil
}
