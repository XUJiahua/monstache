package view

import (
	"encoding/json"
	"fmt"
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

// NewTableFieldCollectorWithOptions creates a collector with custom MapTraveler options.
func NewTableFieldCollectorWithOptions(table string, opts ...MapTravelerOption) *TableFieldCollector {
	logger := logrus.WithField("table", table).WithField("component", "TableFieldCollector")
	traveler := NewMapTraveler(opts...)
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
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	var doc map[string]interface{}
	if err := dec.Decode(&doc); err != nil {
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
	for key := range kc.traveler.result {
		// not include array type
		if strings.Contains(key, "[]") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// FieldInfo represents a field with its name and type information.
type FieldInfo struct {
	Name           string `json:"name"`
	GoType         string `json:"go_type"`
	ClickHouseType string `json:"clickhouse_type"`
}

func (kc *TableFieldCollector) GetFieldInfos() []FieldInfo {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	result := kc.traveler.GetResult()
	var infos []FieldInfo
	for key, goType := range result {
		chType := GoTypeToClickHouseType(goType)
		// field name ends with "[]" means it's an array of primitives
		if strings.HasSuffix(key, "[]") {
			chType = fmt.Sprintf("Array(%s)", chType)
		}
		infos = append(infos, FieldInfo{
			Name:           key,
			GoType:         goType,
			ClickHouseType: chType,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}

// GetNotCollectedKeys returns field paths where all observed values were nil/unrecognized.
func (kc *TableFieldCollector) GetNotCollectedKeys() []string {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	result := kc.traveler.GetResult()
	notCollected := kc.traveler.GetNotCollected()

	var keys []string
	for key := range notCollected {
		// only return fields that never appeared in result (truly null-only)
		if _, inResult := result[key]; !inResult {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func (kc *TableFieldCollector) GetTable() string {
	return kc.table
}
