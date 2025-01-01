package common

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestKeeper(t *testing.T) {
	keeper := NewKeeper("test", "a", "x.y")
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
