package view

import "sort"

type MockTableFieldCollector struct {
	table string
	keys  []string
}

func (m MockTableFieldCollector) GetKeys() []string {
	return m.keys
}

func NewMockTableFieldCollector(table string, keys []string) *MockTableFieldCollector {
	sort.Strings(keys)
	return &MockTableFieldCollector{table: table, keys: keys}
}
