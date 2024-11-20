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
	traveler := NewMapTraveler()
	traveler.getAllKeys(doc, "")
	var keys []string
	for k, _ := range traveler.result {
		keys = append(keys, k)
	}
	if sorting {
		sort.Strings(keys)
	}
	return keys
}

type MapTraveler struct {
	// key and it's type
	result       map[string]string
	notCollected map[string]string
}

func NewMapTraveler() *MapTraveler {
	return &MapTraveler{
		result:       make(map[string]string),
		notCollected: make(map[string]string),
	}
}

func getUniqueValues(m map[string]string) []string {
	valueSet := make(map[string]struct{})
	for _, v := range m {
		valueSet[v] = struct{}{}
	}

	var values []string
	for k := range valueSet {
		values = append(values, k)
	}

	sort.Strings(values)

	return values
}

// HandledTypes not including array, object but their children
func (t *MapTraveler) HandledTypes() []string {
	return getUniqueValues(t.result)
}

// UnhandledTypes expect only nil
func (t *MapTraveler) UnhandledTypes() []string {
	return getUniqueValues(t.notCollected)
}

func (t *MapTraveler) handleArray(array []interface{}, prefix string) {
	for _, elem := range array {
		// sample every element, collect every fields
		k := prefix
		switch elem := elem.(type) {
		case string, int, int32, int64, float32, float64, bool:
			t.result[k] = fmt.Sprintf("%T", elem)
		case map[string]interface{}:
			t.getAllKeys(elem, fmt.Sprintf("%s.", k))
		default:
			t.notCollected[k] = fmt.Sprintf("%T", elem)
		}
	}
}

func (t *MapTraveler) getAllKeys(doc map[string]interface{}, prefix string) {
	for k, elem := range doc {
		k = fmt.Sprintf("%s%s", prefix, k)
		switch elem := elem.(type) {
		case string, int, int32, int64, float32, float64, bool:
			t.result[k] = fmt.Sprintf("%T", elem)
		case map[string]interface{}:
			t.getAllKeys(elem, fmt.Sprintf("%s.", k))
		case []interface{}:
			t.handleArray(elem, fmt.Sprintf("%s[]", k))
		default:
			t.notCollected[k] = fmt.Sprintf("%T", elem)
		}
	}
}

func (t *MapTraveler) GetAllKeys(doc map[string]interface{}) {
	t.getAllKeys(doc, "")
}
