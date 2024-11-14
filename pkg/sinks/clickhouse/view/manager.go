package view

import (
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

type Element struct {
	table string
	doc   interface{}
}

// MockManager do nothing
type MockManager struct{}

func (m *MockManager) Start()                                {}
func (m *MockManager) Collect(table string, doc interface{}) {}
func (m *MockManager) BuildRoutes(mux *http.ServeMux)        {}

type Manager interface {
	Start()
	Collect(table string, doc interface{})
	BuildRoutes(mux *http.ServeMux)
}

type ViewManager struct {
	mu         sync.Mutex
	collectors map[string]*TableFieldCollector
	queue      chan Element
}

// NewViewManager
func NewViewManager() *ViewManager {
	return &ViewManager{collectors: make(map[string]*TableFieldCollector), queue: make(chan Element, 1024)}
}

func (m *ViewManager) Start() {
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

func (m *ViewManager) Collect(table string, doc interface{}) {
	element := Element{table: table, doc: doc}
	m.queue <- element
}

func (m *ViewManager) collect(table string, doc interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.collectors[table]; !ok {
		m.collectors[table] = NewTableFieldCollector(table)
	}

	m.collectors[table].CollectAny(doc)
}

func (m *ViewManager) views() ([]string, error) {
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

func (m *ViewManager) BuildRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/views", func(w http.ResponseWriter, r *http.Request) {
		views, err := m.views()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(strings.Join(views, "\n\n\n")))
	})
}
