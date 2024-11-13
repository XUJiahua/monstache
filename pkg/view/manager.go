package view

import (
	"sync"
)

type Manager struct {
	mu         sync.Mutex
	collectors map[string]*NSKeyCollector
	prefix     string
	suffix     string
}

// NewManager
// prefix 和 suffix 用于 Clickhouse 表名，在重命名 ns 后，补充 prefix 和 suffix
func NewManager(prefix string, suffix string) *Manager {
	return &Manager{collectors: make(map[string]*NSKeyCollector), prefix: prefix, suffix: suffix}
}

func (m *Manager) Collect(ns string, doc interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.collectors[ns]; !ok {
		m.collectors[ns] = NewNSKeyCollector(ns)
	}

	m.collectors[ns].CollectAny(doc)
}

func (m *Manager) Views() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	views := make([]string, 0, len(m.collectors))
	for ns, collector := range m.collectors {
		table := ConvertToClickhouseTable(ns, m.prefix, m.suffix)
		vb := NewViewBuilder(table, table+"_view", collector)
		sql, err := vb.Build()
		if err != nil {
			return nil, err
		}
		views = append(views, sql)
	}
	return views, nil
}
