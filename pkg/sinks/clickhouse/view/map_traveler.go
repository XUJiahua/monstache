package view

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"sort"
)

type MapTraveler struct {
	// key and it's type
	result        map[string]string
	notCollected  map[string]string
	defaultValues map[string]interface{}
	logger        *logrus.Entry
}

func NewMapTraveler(logger *logrus.Entry) *MapTraveler {
	if logger == nil {
		logger = logrus.WithField("component", "MapTraveler")
	} else {
		logger = logger.WithField("component", "MapTraveler")
	}
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
	return &MapTraveler{
		result:        make(map[string]string),
		notCollected:  make(map[string]string),
		defaultValues: defaultValues,
		logger:        logger,
	}
}

func (t *MapTraveler) travelArray(array []interface{}, prefix string, collect bool, level int) {
	for _, elem := range array {
		// sample every element, collect every fields
		k := prefix
		ty := fmt.Sprintf("%T", elem)
		switch elem := elem.(type) {
		case string, int, int32, int64, float32, float64, bool:
			if collect {
				t.result[k] = ty
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
		ty := fmt.Sprintf("%T", elem)
		switch elem := elem.(type) {
		case string, int, int32, int64, float32, float64, bool:
			if collect {
				t.result[globalKey] = ty
			}
		case map[string]interface{}:
			t.travelObject(elem, fmt.Sprintf("%s.", globalKey), collect, level+1)
		case []interface{}:
			t.travelArray(elem, fmt.Sprintf("%s[]", globalKey), collect, level+1)
		default:
			if collect {
				t.notCollected[globalKey] = ty
			} else if level != 0 {
				// assign except top level
				if globalTy, ok := t.result[globalKey]; ok {
					if defaultValue, ok := t.defaultValues[globalTy]; ok {
						t.logger.Warnf("assign default value to key %s(%s->%s)", globalKey, ty, globalTy)
						doc[k] = defaultValue
					}
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
