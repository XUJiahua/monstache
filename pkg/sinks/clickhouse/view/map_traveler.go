package view

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
)

// typePriority defines the priority for type conflict resolution.
// Higher value = wider type. When the same field is observed with
// different Go types across documents, the higher-priority type wins.
// "string" is the widest because any value can be stored as String.
var typePriority = map[string]int{
	"bool":    1,
	"int32":   2,
	"int":     3,
	"int64":   4,
	"float32": 5,
	"float64": 6,
	"string":  7,
	// array element types follow the same priority within arrays
	"[]bool":    1,
	"[]int32":   2,
	"[]int":     3,
	"[]int64":   4,
	"[]float32": 5,
	"[]float64": 6,
	"[]string":  7,
}

// goTypeToClickHouseType maps Go type names to ClickHouse column types.
var goTypeToClickHouseType = map[string]string{
	"string":  "String",
	"float64": "Float64",
	"float32": "Float32",
	"int":     "Int64",
	"int32":   "Int32",
	"int64":   "Int64",
	"bool":    "Bool",
	// array types
	"[]string":  "Array(String)",
	"[]float64": "Array(Float64)",
	"[]float32": "Array(Float32)",
	"[]int":     "Array(Int64)",
	"[]int32":   "Array(Int32)",
	"[]int64":   "Array(Int64)",
	"[]bool":    "Array(Bool)",
}

// inferGoType returns a normalized Go type name for any value.
// For json.Number it distinguishes int vs float by checking for ".".
// For all other types it falls back to fmt.Sprintf("%T", v).
func inferGoType(v interface{}) string {
	switch n := v.(type) {
	case json.Number:
		if strings.Contains(n.String(), ".") {
			return "float64"
		}
		return "int64"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// inferArrayGoType inspects array elements and returns a composite type
// like "[]string", "[]int64", etc. Defaults to "[]string" for empty/nil-only arrays.
// Map (object) elements are skipped — they cannot be represented as a single
// ClickHouse primitive type, matching Python's infer_type_from_value behavior.
func inferArrayGoType(arr []interface{}) string {
	for _, elem := range arr {
		if elem == nil {
			continue
		}
		if _, isMap := elem.(map[string]interface{}); isMap {
			continue
		}
		return "[]" + inferGoType(elem)
	}
	return "[]string"
}

type MapTraveler struct {
	// key and it's type
	result       map[string]string
	notCollected map[string]string

	objectTypeMap map[string]map[string]string

	defaultValues map[string]interface{}
	logger        *logrus.Entry
	// only replace string type with default value
	stringOnly bool
	// when true, arrays are treated as leaf nodes (inferred as Array(X))
	// instead of being recursively walked into
	leafArray bool
	// when true, the first observed type for a field wins (no priority upgrade).
	// This matches Python's "first non-null value" behavior.
	firstWins bool
}

// setResult records a field's type, resolving conflicts by keeping the wider type.
// When firstWins is true, the first observed type is never overridden.
func (t *MapTraveler) setResult(key, newType string) {
	if _, ok := t.result[key]; ok {
		if t.firstWins {
			return
		}
		existingPri := typePriority[t.result[key]]
		newPri := typePriority[newType]
		if newPri > existingPri {
			t.result[key] = newType
		}
		return
	}
	t.result[key] = newType
}

func (t *MapTraveler) recordObject(prefix, key, value string) {
	if obj, ok := t.objectTypeMap[prefix]; ok {
		obj[key] = value
		t.objectTypeMap[prefix] = obj
	} else {
		obj := make(map[string]string)
		obj[key] = value
		t.objectTypeMap[prefix] = obj
	}
}

func (t *MapTraveler) getObject(prefix string) map[string]string {
	if obj, ok := t.objectTypeMap[prefix]; ok {
		return obj
	}

	return nil
}

type MapTravelerOption func(*MapTraveler)

func WithLogger(logger *logrus.Entry) MapTravelerOption {
	return func(t *MapTraveler) {
		t.logger = logger
	}
}

func WithStringOnly(stringOnly bool) MapTravelerOption {
	return func(t *MapTraveler) {
		t.stringOnly = stringOnly
	}
}

// WithLeafArray sets whether arrays should be treated as leaf nodes.
// When true, arrays are not recursively walked; instead the whole array
// is recorded with a composite type like "[]string", "[]int64", etc.
func WithLeafArray(leafArray bool) MapTravelerOption {
	return func(t *MapTraveler) {
		t.leafArray = leafArray
	}
}

// WithFirstWins sets whether the first observed type for a field wins.
// When true, subsequent documents cannot change a field's type (matches
// Python's "first non-null value" strategy). When false (default), the
// wider type wins via priority.
func WithFirstWins(firstWins bool) MapTravelerOption {
	return func(t *MapTraveler) {
		t.firstWins = firstWins
	}
}

func NewMapTraveler(opts ...MapTravelerOption) *MapTraveler {
	// string, int, int32, int64, float32, float64, bool
	defaultValues := map[string]interface{}{
		// string
		"string":  "",
		"int":     0,
		"int32":   int32(0),
		"int64":   int64(0),
		"float32": float32(0),
		// number
		"float64": float64(0),
		// true/false
		"bool": false,
	}
	traveler := &MapTraveler{
		result:        make(map[string]string),
		notCollected:  make(map[string]string),
		objectTypeMap: make(map[string]map[string]string),
		defaultValues: defaultValues,
		logger:        nil,
		stringOnly:    false,
		leafArray:     false,
	}
	for _, opt := range opts {
		opt(traveler)
	}

	if traveler.logger == nil {
		traveler.logger = logrus.WithField("component", "MapTraveler")
	} else {
		traveler.logger = traveler.logger.WithField("component", "MapTraveler")
	}

	return traveler
}

func (t *MapTraveler) travelArray(array []interface{}, prefix string, collect bool, level int) {
	for _, elem := range array {
		// sample every element, collect every fields
		k := prefix
		ty := inferGoType(elem)
		switch elem := elem.(type) {
		case string, int, int32, int64, float32, float64, bool, json.Number:
			if collect {
				t.setResult(k, ty)
			}
		case map[string]interface{}:
			t.travelObject(elem, fmt.Sprintf("%s.", k), collect, level+1)
		default:
			if collect {
				t.notCollected[k] = ty
			}
		}
	}
}

func (t *MapTraveler) travelObject(doc map[string]interface{}, prefix string, collect bool, level int) {
	for k, elem := range doc {
		globalKey := fmt.Sprintf("%s%s", prefix, k)
		ty := inferGoType(elem)
		t.recordObject(prefix, k, ty)

		switch elem := elem.(type) {
		case string, int, int32, int64, float32, float64, bool, json.Number:
			if collect {
				t.setResult(globalKey, ty)
			}
		case map[string]interface{}:
			t.travelObject(elem, fmt.Sprintf("%s.", globalKey), collect, level+1)
		case []interface{}:
			if t.leafArray {
				if collect {
					t.setResult(globalKey, inferArrayGoType(elem))
				}
			} else {
				t.travelArray(elem, fmt.Sprintf("%s[]", globalKey), collect, level+1)
			}
		default:
			if collect {
				t.notCollected[globalKey] = ty
			} else if level != 0 {
				// assign except top level
				if globalTy, ok := t.result[globalKey]; ok {
					if defaultValue, ok := t.defaultValues[globalTy]; ok {
						if t.stringOnly && globalTy != "string" {
							continue
						}
						t.logger.Debugf("assign default value to key %s(%s->%s)", globalKey, ty, globalTy)
						doc[k] = defaultValue
					}
				}
			}
		}
	}

	if !collect {
		obj := t.getObject(prefix)
		for key, ty := range obj {
			if _, ok := doc[key]; !ok {
				// doc lacks of key, add default value
				if defaultValue, ok := t.defaultValues[ty]; ok {
					if t.stringOnly && ty != "string" {
						continue
					}
					t.logger.Debugf("assign default value to key %s(%s)", key, ty)
					doc[key] = defaultValue
				} else {
					t.logger.Debugf("no default value for key %s(%s)", key, ty)
				}
			}
		}
	}
}

// Collect assume top level is object, not array
func (t *MapTraveler) Collect(doc map[string]interface{}) {
	t.travelObject(doc, "", true, 0)
}

// AssignDefaultValues the same batch of messages to Clickhouse should have same structure
func (t *MapTraveler) AssignDefaultValues(doc map[string]interface{}) {
	// 也需要遍历一遍，但是这一次，应该不是收集，而是找到未设置的位置。。。
	t.travelObject(doc, "", false, 0)
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

func (t *MapTraveler) GetKeys() []string {
	var keys []string
	for k := range t.result {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// GetResult returns the internal result map (field_path -> Go type name).
func (t *MapTraveler) GetResult() map[string]string {
	return t.result
}

// GetNotCollected returns the internal notCollected map (field_path -> Go type name)
// for fields that had unrecognized types (typically nil).
func (t *MapTraveler) GetNotCollected() map[string]string {
	return t.notCollected
}

// GoTypeToClickHouseType converts a Go type name to the corresponding ClickHouse type.
// Returns "String" as fallback for unknown types.
func GoTypeToClickHouseType(goType string) string {
	if chType, ok := goTypeToClickHouseType[goType]; ok {
		return chType
	}
	return "String"
}
