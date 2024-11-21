package view

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

type TableFieldCollector struct {
	table    string
	mu       sync.Mutex
	traveler *MapTraveler
	logger   *logrus.Entry
}

func NewTableFieldCollector(table string) *TableFieldCollector {
	logger := logrus.WithField("table", table).WithField("component", "TableFieldCollector")
	traveler := NewMapTraveler()
	return &TableFieldCollector{traveler: traveler, table: table, logger: logger}
}

// CollectAny any that can be converted to JSON
func (kc *TableFieldCollector) CollectAny(doc interface{}) {
	jsonStr, err := json.Marshal(doc)
	if err != nil {
		kc.logger.Errorf("[NSKeyCollector] CollectAny: %s", err)
		return
	}
	kc.CollectJSON(string(jsonStr))
}

func (kc *TableFieldCollector) CollectJSON(jsonStr string) {
	doc := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &doc)
	if err != nil {
		kc.logger.Errorf("[NSKeyCollector] CollectJSON: %s", err)
		return
	}
	kc.Collect(doc)
}

func (kc *TableFieldCollector) Collect(doc map[string]interface{}) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	kc.traveler.Collect(doc)
}

func (kc *TableFieldCollector) GetKeys() []string {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	var keys []string
	for key, _ := range kc.traveler.result {
		// not include array type
		if strings.Contains(key, "[]") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (kc *TableFieldCollector) GetTable() string {
	return kc.table
}
