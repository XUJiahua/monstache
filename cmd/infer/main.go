package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rwynn/monstache/v6/pkg/sinks/clickhouse/view"
)

type fieldEntry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Column string `json:"column"`
}

func main() {
	samplesDir := flag.String("samples-dir", "", "directory containing .jsonl files")
	file := flag.String("file", "", "single .jsonl file to process (alternative to --samples-dir)")
	output := flag.String("output", "", "output file path (default: stdout)")
	sourceTable := flag.String("source-table", "", "source table name for config")
	docField := flag.String("field", "", "JSON field to extract for sampling (default: entire document)")
	flag.Parse()

	if *samplesDir == "" && *file == "" {
		fmt.Fprintln(os.Stderr, "Error: provide --samples-dir or --file")
		flag.Usage()
		os.Exit(1)
	}

	// Collect input files
	var files []string
	if *file != "" {
		files = append(files, *file)
	} else {
		entries, err := os.ReadDir(*samplesDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading directory: %v\n", err)
			os.Exit(1)
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
				files = append(files, filepath.Join(*samplesDir, e.Name()))
			}
		}
		sort.Strings(files)
		if len(files) == 0 {
			fmt.Fprintf(os.Stderr, "Warning: no .jsonl files found in %s\n", *samplesDir)
			os.Exit(1)
		}
	}

	// Create collector matching Python's sampling behavior:
	// - leafArray: arrays treated as leaf nodes, not recursively walked
	collector := view.NewTableFieldCollectorWithOptions("infer",
		view.WithLeafArray(true),
	)

	totalDocs := 0

	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening %s: %v\n", f, err)
			continue
		}

		scanner := bufio.NewScanner(fh)
		scanner.Buffer(make([]byte, 0, 10*1024*1024), 10*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			// Parse with UseNumber to distinguish int vs float
			dec := json.NewDecoder(strings.NewReader(line))
			dec.UseNumber()
			var raw map[string]interface{}
			if err := dec.Decode(&raw); err != nil {
				continue
			}

			// Extract target field if specified
			var target map[string]interface{}
			if *docField != "" {
				val, exists := raw[*docField]
				if !exists {
					continue
				}
				switch v := val.(type) {
				case string:
					// __doc stored as string — parse it
					dec2 := json.NewDecoder(strings.NewReader(v))
					dec2.UseNumber()
					if err := dec2.Decode(&target); err != nil {
						continue
					}
				case map[string]interface{}:
					target = v
				default:
					continue
				}
			} else {
				target = raw
			}

			// Feed into the view package collector
			collector.Collect(target)
			totalDocs++
		}
		fh.Close()

		fmt.Fprintf(os.Stderr, "  loaded %s\n", filepath.Base(f))
	}

	fmt.Fprintf(os.Stderr, "Loaded %d sample document(s)\n", totalDocs)

	// Get results from the view package
	fieldInfos := collector.GetFieldInfos()
	nullFields := collector.GetNotCollectedKeys()

	// Build output in Python-compatible format
	var fields []fieldEntry
	typeStats := make(map[string]int)

	for _, info := range fieldInfos {
		typeStats[info.ClickHouseType]++
		fields = append(fields, fieldEntry{
			Path:   info.Name,
			Type:   info.ClickHouseType,
			Column: "__doc." + info.Name,
		})
	}

	// null-only fields default to String
	for _, path := range nullFields {
		typeStats["String"]++
		fields = append(fields, fieldEntry{
			Path:   path,
			Type:   "String",
			Column: "__doc." + path,
		})
	}

	// Sort all fields by path
	sort.Slice(fields, func(i, j int) bool { return fields[i].Path < fields[j].Path })

	// Build config output
	outputData := map[string]interface{}{
		"config": map[string]interface{}{
			"source_table": *sourceTable,
			"target_table": *sourceTable + "_mv_target_v1",
			"mv_name":      *sourceTable + "_mv_v1",
			"partition_by":  "toYYYYMM(__date)",
			"order_by":      []string{"__date", "_id"},
		},
		"indexes": []interface{}{},
		"fields":  fields,
	}

	outputJSON, err := json.MarshalIndent(outputData, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	if *output != "" {
		if err := os.WriteFile(*output, outputJSON, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "\nOutput: %s\n", *output)
	} else {
		fmt.Println(string(outputJSON))
	}

	// Summary
	fmt.Fprintf(os.Stderr, "\nInferred %d fields:\n", len(fields))
	type typeStat struct {
		name  string
		count int
	}
	var stats []typeStat
	for name, count := range typeStats {
		stats = append(stats, typeStat{name, count})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].count > stats[j].count })
	for _, s := range stats {
		fmt.Fprintf(os.Stderr, "  %-20s %d\n", s.name, s.count)
	}

	if len(nullFields) > 0 {
		fmt.Fprintf(os.Stderr, "\nWarning: %d fields were null/missing in all samples, defaulted to String:\n", len(nullFields))
		for _, f := range nullFields {
			fmt.Fprintf(os.Stderr, "  - %s\n", f)
		}
	}
}
