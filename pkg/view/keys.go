package view

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
)

type NSKeyCollector struct {
	ns string
}

func NewNSKeyCollector(ns string) *NSKeyCollector {
	return &NSKeyCollector{ns: ns}
}

func (kc *NSKeyCollector) Collect(doc map[string]interface{}) {
}

func GetAllKeysFromJSON(jsonStr string) []string {
	var doc map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &doc)
	if err != nil {
		return nil
	}
	return GetAllKeys(doc)
}

func GetAllKeys(doc map[string]interface{}) []string {
	keys := getAllKeys(doc, "")
	sort.Strings(keys)
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
