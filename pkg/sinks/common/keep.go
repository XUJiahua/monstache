package common

import (
	"strings"
)

// todo: 需要支持数组，新增数组语法，而不仅仅支持点语法

type Field struct {
	Name  string
	Parts []string
}

type Keeper struct {
	ns     string
	fields []Field
}

// NewKeeper
// ns mongo 的 namespace
// fields 需要保留的字段，支持点语法
// 使用 key1 可以覆盖 key1.subkey1, key1.subkey2
func NewKeeper(ns string, fields ...string) *Keeper {
	fieldsMap := make(map[string]struct{}, len(fields))
	newFields := make([]Field, 0, len(fields))

	for _, field := range fields {
		if field == "" {
			continue
		}
		if _, ok := fieldsMap[field]; ok {
			continue
		}

		newFields = append(newFields, Field{
			Name:  field,
			Parts: strings.Split(field, "."),
		})
		fieldsMap[field] = struct{}{}
	}
	return &Keeper{
		ns:     ns,
		fields: newFields,
	}
}

func (k *Keeper) Keep(doc map[string]interface{}) map[string]interface{} {
	if len(doc) == 0 {
		return nil
	}

	result := make(map[string]interface{}, len(k.fields))
	for _, field := range k.fields {
		if value := getNestedValue(doc, field.Parts); value != nil {
			setNestedValue(result, field.Parts, value)
		}
	}
	return result
}

func getNestedValue(doc map[string]interface{}, parts []string) interface{} {
	if len(parts) == 0 {
		return nil
	}

	current := doc
	for i := 0; i < len(parts)-1; i++ {
		v, ok := current[parts[i]]
		if !ok {
			return nil
		}

		current, ok = v.(map[string]interface{})
		if !ok {
			return nil
		}
	}

	lastKey := parts[len(parts)-1]
	return current[lastKey]
}

func setNestedValue(doc map[string]interface{}, parts []string, value interface{}) {
	if len(parts) == 0 {
		return
	}

	current := doc
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		next, ok := current[part].(map[string]interface{})
		if !ok {
			next = make(map[string]interface{})
			current[part] = next
		}
		current = next
	}

	current[parts[len(parts)-1]] = value
}
