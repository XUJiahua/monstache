package common

import (
	"strings"
)

type Field struct {
	Name  string
	Parts []string
}

type Keeper struct {
	Ns        string
	NewFields []Field
}

func NewKeeper(ns string, fields ...string) *Keeper {
	fieldsMap := make(map[string]struct{})
	var newFields []Field
	for _, field := range fields {
		if _, ok := fieldsMap[field]; ok {
			continue
		}
		// 按点号分割字段路径
		parts := strings.Split(field, ".")
		newField := Field{
			Name:  field,
			Parts: parts,
		}
		newFields = append(newFields, newField)

		fieldsMap[field] = struct{}{}
	}
	return &Keeper{Ns: ns, NewFields: newFields}
}

func (k *Keeper) Keep(doc map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// 遍历所有需要保留的字段
	for _, field := range k.NewFields {
		// 获取嵌套值
		value := getNestedValue(doc, field.Parts)
		if value != nil {
			// 设置嵌套值到结果中
			setNestedValue(result, field.Parts, value)
		}
	}

	return result
}

// 获取嵌套值的辅助函数
func getNestedValue(doc map[string]interface{}, parts []string) interface{} {
	current := doc
	for _, part := range parts[:len(parts)-1] {
		v, ok := current[part]
		if !ok {
			return nil
		}

		// 检查中间节点是否为 map
		if m, ok := v.(map[string]interface{}); ok {
			current = m
		} else {
			return nil
		}
	}

	// 返回最终值
	return current[parts[len(parts)-1]]
}

// 设置嵌套值的辅助函数
func setNestedValue(doc map[string]interface{}, parts []string, value interface{}) {
	current := doc
	for _, part := range parts[:len(parts)-1] {
		// 如果中间节点不存在，创建新的 map
		if _, ok := current[part]; !ok {
			current[part] = make(map[string]interface{})
		}
		current = current[part].(map[string]interface{})
	}

	// 设置最终值
	current[parts[len(parts)-1]] = value
}
