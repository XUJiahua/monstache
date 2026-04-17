package clickhouse

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// NsDatabaseRemapper 负责将 namespace 的 database 部分进行重映射
type NsDatabaseRemapper struct {
	// mapping: 新 database → 旧 database
	mapping map[string]string
}

// NewNsDatabaseRemapper 根据配置创建 Remapper 实例
// 当 mapping 为 nil 或空时，Remap 方法将直接返回原始 namespace
func NewNsDatabaseRemapper(mapping map[string]string) *NsDatabaseRemapper {
	return &NsDatabaseRemapper{mapping: mapping}
}

// Remap 将 namespace 的 database 部分替换为映射中的旧名称
// 如果 namespace 不包含 "." 或 database 部分不在映射中，返回原始 namespace
func (r *NsDatabaseRemapper) Remap(ns string) string {
	if len(r.mapping) == 0 {
		return ns
	}

	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return ns
	}

	db := parts[0]
	coll := parts[1]

	if mappedDb, ok := r.mapping[db]; ok {
		remapped := mappedDb + "." + coll
		logrus.Debugf("namespace remapped: %s → %s", ns, remapped)
		return remapped
	}

	return ns
}
