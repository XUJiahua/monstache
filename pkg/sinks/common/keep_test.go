package common

import (
	"github.com/davecgh/go-spew/spew"
	"testing"
)

func TestKeeper(t *testing.T) {
	keeper := NewKeeper("test", "a.b.c", "x.y")
	doc := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": 1,
			},
		},
		"x": map[string]interface{}{
			"y": 2,
		},
		"z": 3, // 这个字段会被忽略
	}

	result := keeper.Keep(doc)
	spew.Dump(result)
}
