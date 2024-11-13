package view

import (
	"encoding/json"
	"fmt"
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
	keys := getAllKeys(doc, "")
	if sorting {
		sort.Strings(keys)
	}
	return keys
}

func getAllKeys(doc map[string]interface{}, prefix string) []string {
	var keys []string

	for k, v := range doc {
		k = fmt.Sprintf("%s%s", prefix, k)
		switch v := v.(type) {
		case string, int, int32, int64, float32, float64, bool:
			// nil is not needed
			keys = append(keys, k)
		case map[string]interface{}:
			nestedDoc := v
			nestedKeys := getAllKeys(nestedDoc, fmt.Sprintf("%s.", k))
			keys = append(keys, nestedKeys...)
		default:
			// currently do not support array
			logrus.Debugf("[getAllKeys] Unsupported type: %T", v)
		}
	}

	return keys
}
