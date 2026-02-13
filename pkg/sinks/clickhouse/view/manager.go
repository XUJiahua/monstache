package view

import (
	"encoding/json"
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
	return &ViewManager{collectors: make(map[string]*TableFieldCollector), queue: make(chan Element, 1024*10)}
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
	select {
	case m.queue <- element:
	default:
		logrus.Debugf("view manager queue is full, dropping data for table: %s", table)
	}
}

func (m *ViewManager) collect(table string, doc interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.collectors[table]; !ok {
		m.collectors[table] = NewTableFieldCollectorWithOptions(table, WithLeafArray(true))
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

// TableFieldInfo 表示表和字段信息的结构
type TableFieldInfo struct {
	Table      string      `json:"table"`
	Fields     []string    `json:"fields"`
	FieldInfos []FieldInfo `json:"field_infos"`
}

func (m *ViewManager) fields() ([]TableFieldInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fields := make([]TableFieldInfo, 0, len(m.collectors))
	for table, collector := range m.collectors {
		info := TableFieldInfo{
			Table:      table,
			Fields:     collector.GetKeys(),
			FieldInfos: collector.GetFieldInfos(),
		}
		fields = append(fields, info)
	}
	return fields, nil
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

	mux.HandleFunc("/fields", func(w http.ResponseWriter, r *http.Request) {
		fields, err := m.fields()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(fields); err != nil {
			logrus.Errorf("failed to encode fields response: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})
}
