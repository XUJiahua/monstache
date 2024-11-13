package view

import (
	"sync"
)

type Manager struct {
	mu         sync.Mutex
	collectors map[string]*TableFieldCollector
}

// NewManager
func NewManager() *Manager {
	return &Manager{collectors: make(map[string]*TableFieldCollector)}
}

func (m *Manager) Collect(table string, doc interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.collectors[table]; !ok {
		m.collectors[table] = NewTableFieldCollector(table)
	}

	m.collectors[table].CollectAny(doc)
}

func (m *Manager) Views() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	views := make([]string, 0, len(m.collectors))
	for table, collector := range m.collectors {
		vb := NewViewBuilder(table, table+"_view", collector)
		sql, err := vb.Build()
		if err != nil {
			return nil, err
		}
		views = append(views, sql)
	}
	return views, nil
}
