package clickhouse

import "testing"

func TestDump(t *testing.T) {
	type testStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	rows := []interface{}{
		testStruct{Name: "test1", Age: 1},
		testStruct{Name: "test2", Age: 2},
	}
	Dump("test", "test", "test", rows)
}
