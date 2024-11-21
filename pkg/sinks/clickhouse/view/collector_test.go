package view

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"reflect"
	"testing"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func DumpSlice(slice []string) {
	fmt.Printf("[]string{\n")
	for _, s := range slice {
		fmt.Printf("\"%s\",\n", s)
	}
	fmt.Printf("}\n")
}

func TestGetAllKeysFromJSON(t *testing.T) {
	type args struct {
		jsonStr string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			args: args{
				jsonStr: `
{
    "glossary": {
        "title": "example glossary",
		"GlossDiv": {
            "title": "S",
			"GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
					"SortAs": "SGML",
					"GlossTerm": "Standard Generalized Markup Language",
					"Acronym": "SGML",
					"Abbrev": "ISO 8879:1986",
					"GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
						"GlossSeeAlso": ["GML", "XML"]
                    },
					"GlossSee": "markup"
                }
            }
        }
    }
}
`,
			},
			want: []string{
				"glossary.GlossDiv.GlossList.GlossEntry.Abbrev",
				"glossary.GlossDiv.GlossList.GlossEntry.Acronym",
				"glossary.GlossDiv.GlossList.GlossEntry.GlossDef.para",
				"glossary.GlossDiv.GlossList.GlossEntry.GlossSee",
				"glossary.GlossDiv.GlossList.GlossEntry.GlossTerm",
				"glossary.GlossDiv.GlossList.GlossEntry.ID",
				"glossary.GlossDiv.GlossList.GlossEntry.SortAs",
				"glossary.GlossDiv.title",
				"glossary.title",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := NewTableFieldCollector("mock_table")
			collector.CollectJSON(tt.args.jsonStr)
			if got := collector.GetKeys(); !reflect.DeepEqual(got, tt.want) {
				DumpSlice(got)
				t.Errorf("GetAllKeysFromJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}
