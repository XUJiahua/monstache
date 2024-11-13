package view

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type Element struct {
	table string
	doc   interface{}
}

type Manager struct {
	mu         sync.Mutex
	collectors map[string]*TableFieldCollector
	queue      chan Element
}

// NewManager
func NewManager() *Manager {
	return &Manager{collectors: make(map[string]*TableFieldCollector), queue: make(chan Element, 1024)}
}

func (m *Manager) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logrus.Errorf("panic in view manager: %v", r)
			}
		}()

		for element := range m.queue {
			m.collect(element.table, element.doc)
		}
	}()
}

func (m *Manager) Collect(table string, doc interface{}) {
	element := Element{table: table, doc: doc}
	m.queue <- element
}

func (m *Manager) collect(table string, doc interface{}) {
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
