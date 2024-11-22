package clickhouse

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

func Dump(ns, database, table string, rows []interface{}) {
	if filename, err := dump(ns, database, table, rows); err != nil {
		logrus.Errorf("dump data to file failed: %v", err)
	} else {
		logrus.Infof("data has been dumped to file: %s", filename)
	}
}

func dump(ns, database, table string, rows []interface{}) (string, error) {
	timestamp := time.Now().Format("20060102150405")
	filename := fmt.Sprintf("%s_%s_%s_%s.ndjson", ns, database, table, timestamp)

	file, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("create file %s failed: %w", filename, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return "", fmt.Errorf("write row to file %s failed: %w", filename, err)
		}
	}
	absPath, err := filepath.Abs(filename)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path of %s: %w", filename, err)
	}

	return absPath, nil
}
