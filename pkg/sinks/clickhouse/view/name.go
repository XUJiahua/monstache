package view

import (
	"fmt"
	"strings"
)

func ConvertToClickhouseTable(ns string, prefix string, suffix string) string {
	// ns 包含 database 和 collection，使用 . 连接
	// . 不能在 Clickhouse 中使用，所以需要替换为 _
	// - 不能在 Clickhouse 中使用，所以需要替换为 _
	return fmt.Sprintf("%s%s%s", prefix, strings.ReplaceAll(strings.ReplaceAll(ns, ".", "_"), "-", "_"), suffix)
}
