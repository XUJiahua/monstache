package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// 复现问题：
// 保存到 Clickhouse 的 __ver 丢失了精度 https://ciloa.feishu.cn/wiki/WKmYwchczinbDtkLyoFc6rWtnNb
func main() {
	ver := int64(7604288878022754304)

	// 模拟 preprocessBatch 中的 marshal → unmarshal 流程
	original := map[string]interface{}{
		"_id":   "abc",
		"__ver": ver,
	}

	fmt.Println("=== 原始值 ===")
	fmt.Printf("__ver = %d\n\n", ver)

	// Step 1: json.Marshal (这一步没问题，int64 会被正确序列化为数字字面量)
	data, _ := json.Marshal(original)
	fmt.Println("=== json.Marshal 输出 ===")
	fmt.Printf("%s\n\n", data)

	// Step 2 (BUG): json.Unmarshal → map[string]interface{} 会把数字解析为 float64
	var doc1 map[string]interface{}
	json.Unmarshal(data, &doc1)
	fmt.Println("=== json.Unmarshal (float64 精度丢失) ===")
	fmt.Printf("__ver type  = %T\n", doc1["__ver"])
	fmt.Printf("__ver value = %.0f\n", doc1["__ver"])
	// 关键：再次 Marshal 后发送给 ClickHouse 的实际 JSON
	reMarshaled, _ := json.Marshal(doc1)
	fmt.Printf("re-marshal  = %s\n\n", reMarshaled)

	// Step 3 (FIX): json.Decoder + UseNumber() 保留精度
	var doc2 map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	dec.Decode(&doc2)
	fmt.Println("=== json.Decoder + UseNumber() (精度保留) ===")
	fmt.Printf("__ver type  = %T\n", doc2["__ver"])
	fmt.Printf("__ver value = %v\n", doc2["__ver"])
	reMarshaled2, _ := json.Marshal(doc2)
	fmt.Printf("re-marshal  = %s\n", reMarshaled2)
}
