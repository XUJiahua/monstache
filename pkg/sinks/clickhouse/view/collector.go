package view

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
)

type TableFieldCollector struct {
	table string
	mu    sync.Mutex
	keys  map[string]struct{}
}

func NewMockTableFieldCollector(table string, keys []string) *TableFieldCollector {
	keysMap := make(map[string]struct{})
	for _, k := range keys {
		keysMap[k] = struct{}{}
	}
	return &TableFieldCollector{table: table, keys: keysMap}
}

func NewTableFieldCollector(table string) *TableFieldCollector {
	keys := make(map[string]struct{})
	return &TableFieldCollector{keys: keys, table: table}
}

func (kc *TableFieldCollector) CollectAny(doc interface{}) {
	jsonStr, err := json.Marshal(doc)
	if err != nil {
		logrus.Errorf("[NSKeyCollector] CollectAny: %s", err)
		return
	}
	kc.CollectJSON(string(jsonStr))
}

func (kc *TableFieldCollector) CollectJSON(jsonStr string) {
	doc := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &doc)
	if err != nil {
		logrus.Errorf("[NSKeyCollector] CollectJSON: %s", err)
		return
	}
	kc.Collect(doc)
}

func (kc *TableFieldCollector) Collect(doc map[string]interface{}) {
	keys := GetAllKeys(doc, false)

	kc.mu.Lock()
	defer kc.mu.Unlock()
	for _, k := range keys {
		// just overwrite if exists
		kc.keys[k] = struct{}{}
	}
}

func (kc *TableFieldCollector) GetKeys() []string {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	keys := make([]string, 0, len(kc.keys))
	for k := range kc.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (kc *TableFieldCollector) GetTable() string {
	return kc.table
}

func GetAllKeysFromJSON(jsonStr string) []string {
	var doc map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &doc)
	if err != nil {
		return nil
	}
	return GetAllKeys(doc, true)
}

func GetAllKeys(doc map[string]interface{}, sorting bool) []string {
	traveler := NewMapTraveler(nil)
	traveler.Collect(doc)
	var keys []string
	for k, _ := range traveler.result {
		keys = append(keys, k)
	}
	if sorting {
		sort.Strings(keys)
	}
	return keys
}
