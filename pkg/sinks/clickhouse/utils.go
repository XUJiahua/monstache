package clickhouse

import "fmt"

func diff(a, b []string) []string {
	m := make(map[string]bool)
	var diff []string

	// 将 a 中的元素加入 map
	for _, item := range a {
		m[item] = true
	}

	// 检查 b 中的元素是否在 map 中
	for _, item := range b {
		if !m[item] {
			diff = append(diff, item)
		}
	}

	return diff
}

func DiffPrint(a, b []string) {
	diff := diff(a, b)
	fmt.Println(diff)
}
