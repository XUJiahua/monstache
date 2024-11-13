package view

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"

	"github.com/samber/lo"
)

type FieldCollector interface {
	GetKeys() []string
}

type ViewBuilder struct {
	databaseTable     string
	databaseTableView string
	// keyCollector collect keys from documents constantly
	keyCollector FieldCollector
}

// NewViewBuilder creates a new ViewBuilder
// databaseTableView is the table name of the view, if it's empty, it will be set to databaseTable + "_view"
func NewViewBuilder(databaseTable, databaseTableView string, keyCollector FieldCollector) *ViewBuilder {
	if databaseTableView == "" {
		databaseTableView = fmt.Sprintf("%s_view", databaseTable)
	}

	return &ViewBuilder{databaseTable: databaseTable, databaseTableView: databaseTableView, keyCollector: keyCollector}
}

var viewTpl *template.Template

func init() {
	viewTemplate := `
CREATE VIEW IF NOT EXISTS {{.DatabaseTableView}} AS
SELECT
{{.Keys}}
FROM {{.DatabaseTable}}
	`
	tpl, err := template.New("view").Parse(viewTemplate)
	if err != nil {
		panic(err)
	}
	viewTpl = tpl
}

func (vb *ViewBuilder) Build() (string, error) {
	keys := vb.keyCollector.GetKeys()
	newKeys := lo.Map(keys, func(key string, _ int) string {
		return fmt.Sprintf("`%s`", key)
	})
	keysStr := strings.Join(newKeys, ",\n")

	var buf bytes.Buffer
	err := viewTpl.Execute(&buf, map[string]interface{}{
		"DatabaseTable":     vb.databaseTable,
		"DatabaseTableView": vb.databaseTableView,
		"Keys":              keysStr,
	})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
