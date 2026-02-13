package view

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func DumpSlice(slice []string) {
	fmt.Printf("[]string{\n")
	for _, s := range slice {
		fmt.Printf("\"%s\",\n", s)
	}
	fmt.Printf("}\n")
}

func TestGetAllKeysFromJSON(t *testing.T) {
	type args struct {
		jsonStr string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			args: args{
				jsonStr: `
{
    "glossary": {
        "title": "example glossary",
		"GlossDiv": {
            "title": "S",
			"GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
					"SortAs": "SGML",
					"GlossTerm": "Standard Generalized Markup Language",
					"Acronym": "SGML",
					"Abbrev": "ISO 8879:1986",
					"GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
						"GlossSeeAlso": ["GML", "XML"]
                    },
					"GlossSee": "markup"
                }
            }
        }
    }
}
`,
			},
			want: []string{
				"glossary.GlossDiv.GlossList.GlossEntry.Abbrev",
				"glossary.GlossDiv.GlossList.GlossEntry.Acronym",
				"glossary.GlossDiv.GlossList.GlossEntry.GlossDef.para",
				"glossary.GlossDiv.GlossList.GlossEntry.GlossSee",
				"glossary.GlossDiv.GlossList.GlossEntry.GlossTerm",
				"glossary.GlossDiv.GlossList.GlossEntry.ID",
				"glossary.GlossDiv.GlossList.GlossEntry.SortAs",
				"glossary.GlossDiv.title",
				"glossary.title",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := NewTableFieldCollector("mock_table")
			collector.CollectJSON(tt.args.jsonStr)
			if got := collector.GetKeys(); !reflect.DeepEqual(got, tt.want) {
				DumpSlice(got)
				t.Errorf("GetAllKeysFromJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFieldInfos(t *testing.T) {
	collector := NewTableFieldCollector("mock_table")
	collector.CollectJSON(`{"name": "alice", "age": 30, "active": true}`)

	infos := collector.GetFieldInfos()
	expected := []FieldInfo{
		{Name: "active", GoType: "bool", ClickHouseType: "Bool"},
		{Name: "age", GoType: "int64", ClickHouseType: "Int64"},
		{Name: "name", GoType: "string", ClickHouseType: "String"},
	}
	if !reflect.DeepEqual(infos, expected) {
		t.Errorf("GetFieldInfos() = %v, want %v", infos, expected)
	}
}

func TestTypePriorityConflict(t *testing.T) {
	collector := NewTableFieldCollector("mock_table")

	// First doc: "score" is float64
	collector.CollectJSON(`{"score": 1.5}`)
	// Second doc: "score" is bool (lower priority) — should NOT override
	collector.CollectJSON(`{"score": true}`)

	infos := collector.GetFieldInfos()
	if len(infos) != 1 {
		t.Fatalf("expected 1 field, got %d", len(infos))
	}
	if infos[0].GoType != "float64" {
		t.Errorf("expected float64 (higher priority), got %s", infos[0].GoType)
	}
}

func TestTypePriorityUpgrade(t *testing.T) {
	collector := NewTableFieldCollector("mock_table")

	// First doc: "value" is bool (priority 1)
	collector.CollectJSON(`{"value": true}`)
	// Second doc: "value" is string (priority 7) — should upgrade
	collector.CollectJSON(`{"value": "hello"}`)

	infos := collector.GetFieldInfos()
	if len(infos) != 1 {
		t.Fatalf("expected 1 field, got %d", len(infos))
	}
	if infos[0].GoType != "string" {
		t.Errorf("expected string (wider type), got %s", infos[0].GoType)
	}
	if infos[0].ClickHouseType != "String" {
		t.Errorf("expected ClickHouseType String, got %s", infos[0].ClickHouseType)
	}
}

func TestGoTypeToClickHouseType(t *testing.T) {
	tests := []struct {
		goType string
		want   string
	}{
		{"string", "String"},
		{"float64", "Float64"},
		{"float32", "Float32"},
		{"int", "Int64"},
		{"int32", "Int32"},
		{"int64", "Int64"},
		{"bool", "Bool"},
		{"unknown", "String"}, // fallback
	}
	for _, tt := range tests {
		t.Run(tt.goType, func(t *testing.T) {
			if got := GoTypeToClickHouseType(tt.goType); got != tt.want {
				t.Errorf("GoTypeToClickHouseType(%s) = %s, want %s", tt.goType, got, tt.want)
			}
		})
	}
}

func TestFieldInfosIncludeArrayFields(t *testing.T) {
	collector := NewTableFieldCollector("mock_table")
	collector.CollectJSON(`{"name": "alice", "tags": ["a", "b"]}`)

	infos := collector.GetFieldInfos()
	expected := []FieldInfo{
		{Name: "name", GoType: "string", ClickHouseType: "String"},
		{Name: "tags[]", GoType: "string", ClickHouseType: "Array(String)"},
	}
	if !reflect.DeepEqual(infos, expected) {
		t.Errorf("GetFieldInfos() = %v, want %v", infos, expected)
	}
}

func TestFieldInfosArrayOfNumbers(t *testing.T) {
	collector := NewTableFieldCollector("mock_table")
	collector.CollectJSON(`{"scores": [1.5, 2.3], "ids": [1, 2, 3]}`)

	infos := collector.GetFieldInfos()
	// JSON integers (no decimal) decode as int64 via UseNumber, floats stay float64
	expected := []FieldInfo{
		{Name: "ids[]", GoType: "int64", ClickHouseType: "Array(Int64)"},
		{Name: "scores[]", GoType: "float64", ClickHouseType: "Array(Float64)"},
	}
	if !reflect.DeepEqual(infos, expected) {
		t.Errorf("GetFieldInfos() = %v, want %v", infos, expected)
	}
}

func TestFieldInfosNestedArrayObjects(t *testing.T) {
	collector := NewTableFieldCollector("mock_table")
	collector.CollectJSON(`{"items": [{"name": "a", "price": 1.5}, {"name": "b", "price": 2.0}]}`)

	infos := collector.GetFieldInfos()
	// Fields inside array objects: items[].name, items[].price — contain "[]" but don't end with "[]"
	expected := []FieldInfo{
		{Name: "items[].name", GoType: "string", ClickHouseType: "String"},
		{Name: "items[].price", GoType: "float64", ClickHouseType: "Float64"},
	}
	if !reflect.DeepEqual(infos, expected) {
		t.Errorf("GetFieldInfos() = %v, want %v", infos, expected)
	}
}
