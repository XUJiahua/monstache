package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"context"
	"regexp"

	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const recoverBatchSize = 1000
const recoverProgressLogInterval = 10000
const recoverProgressFilename = ".recover_progress.json"

type recoverProgress struct {
	File      string   `json:"file"`
	Line      int64    `json:"line"`
	Completed []string `json:"completed"`
}

type recoverOp struct {
	Ts string `json:"ts"`
	Op string `json:"op"`
	Ns string `json:"ns"`
	Id string `json:"_id"`
}

// dedupKey is ns + _id
type dedupKey struct {
	ns string
	id string
}

type dedupEntry struct {
	op recoverOp
	ts uint64
}

// DocFetcher abstracts MongoDB document lookup so it can be mocked in tests.
type DocFetcher interface {
	// FetchByIDs returns documents from the given namespace whose _id is in oids.
	// The returned map is keyed by _id value.
	FetchByIDs(ctx context.Context, namespace string, oids []primitive.ObjectID) (map[interface{}]map[string]interface{}, error)
}

// mongoDocFetcher is the real implementation using a mongo.Client.
type mongoDocFetcher struct {
	client *mongo.Client
}

func (f *mongoDocFetcher) FetchByIDs(ctx context.Context, namespace string, oids []primitive.ObjectID) (map[interface{}]map[string]interface{}, error) {
	parts := strings.SplitN(namespace, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid namespace: %s", namespace)
	}
	collection := f.client.Database(parts[0]).Collection(parts[1])
	filter := bson.M{"_id": bson.M{"$in": oids}}
	cursor, err := collection.Find(ctx, filter, options.Find())
	if err != nil {
		return nil, fmt.Errorf("find docs in %s: %w", namespace, err)
	}
	defer cursor.Close(ctx)

	result := make(map[interface{}]map[string]interface{})
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			errorLog.Printf("decode error in %s: %v", namespace, err)
			continue
		}
		result[doc["_id"]] = doc
	}
	return result, nil
}

// buildRecoverNsFilter builds a namespace filter from config, reusing the same
// regex options as the normal sync flow (namespace-regex, namespace-exclude-regex).
func buildRecoverNsFilter(config *ConfigOptions) func(ns string) bool {
	var include *regexp.Regexp
	var exclude *regexp.Regexp
	if config.NsRegex != "" {
		include = regexp.MustCompile(config.NsRegex)
	}
	if config.NsExcludeRegex != "" {
		exclude = regexp.MustCompile(config.NsExcludeRegex)
	}
	return func(ns string) bool {
		if include != nil && !include.MatchString(ns) {
			return false
		}
		if exclude != nil && exclude.MatchString(ns) {
			return false
		}
		return true
	}
}

func Recover(client *mongo.Client, config *ConfigOptions, sinkConnector sinks.SinkConnector, closers []sinks.Closer) {
	fetcher := &mongoDocFetcher{client: client}
	doRecover(fetcher, config, sinkConnector, closers)
}

func doRecover(fetcher DocFetcher, config *ConfigOptions, sinkConnector sinks.SinkConnector, closers []sinks.Closer) {
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	path := config.OplogRecoverFilepath
	info, err := os.Stat(path)
	if err != nil {
		errorLog.Fatalf("failed to stat oplog recover path: %v", err)
	}

	var files []string
	var progressDir string
	if info.IsDir() {
		progressDir = path
		entries, err := os.ReadDir(path)
		if err != nil {
			errorLog.Fatalf("failed to read directory: %v", err)
		}
		for _, e := range entries {
			name := e.Name()
			if strings.HasSuffix(name, ".ndjson.gz") || strings.HasSuffix(name, ".ndjson") {
				files = append(files, filepath.Join(path, name))
			}
		}
		sort.Strings(files)
	} else {
		progressDir = filepath.Dir(path)
		files = []string{path}
	}

	if len(files) == 0 {
		infoLog.Println("no oplog files found")
		return
	}

	progressPath := filepath.Join(progressDir, recoverProgressFilename)
	progress := loadProgress(progressPath)

	completedSet := make(map[string]bool)
	for _, f := range progress.Completed {
		completedSet[f] = true
	}

	nsFilter := buildRecoverNsFilter(config)

	var totalProcessed int64
	var totalSkipped int64
	var totalFiltered int64

	for _, filePath := range files {
		baseName := filepath.Base(filePath)
		if completedSet[baseName] {
			infoLog.Printf("skipping completed file: %s", baseName)
			continue
		}

		var startLine int64
		if progress.File == baseName {
			startLine = progress.Line
			infoLog.Printf("resuming file %s from line %d", baseName, startLine)
		}

		processed, skipped, filtered, err := recoverFile(fetcher, sinkConnector, nsFilter, filePath, baseName, startLine, progress, progressPath)
		if err != nil {
			errorLog.Fatalf("failed to recover file %s: %v", baseName, err)
		}
		totalProcessed += processed
		totalSkipped += skipped
		totalFiltered += filtered

		// mark file completed
		progress.Completed = append(progress.Completed, baseName)
		progress.File = ""
		progress.Line = 0
		saveProgress(progressPath, progress)

		infoLog.Printf("completed file %s: processed=%d skipped=%d filtered=%d", baseName, processed, skipped, filtered)
	}

	infoLog.Printf("recovery complete: total_processed=%d total_skipped=%d total_filtered=%d", totalProcessed, totalSkipped, totalFiltered)

	// optionally remove progress file
	os.Remove(progressPath)
}

func recoverFile(
	fetcher DocFetcher,
	sinkConnector sinks.SinkConnector,
	nsFilter func(string) bool,
	filePath, baseName string,
	startLine int64,
	progress *recoverProgress,
	progressPath string,
) (processed, skipped, filtered int64, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(filePath, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("gzip reader: %w", err)
		}
		defer gr.Close()
		reader = gr
	}

	scanner := bufio.NewScanner(reader)
	// increase buffer for long lines
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var lineNum int64
	// batch accumulator: dedup by ns+_id, keep latest ts
	batch := make(map[dedupKey]dedupEntry)
	var batchCount int

	for scanner.Scan() {
		lineNum++

		if lineNum <= startLine {
			continue
		}

		line := scanner.Bytes()
		var op recoverOp
		if err := json.Unmarshal(line, &op); err != nil {
			errorLog.Printf("skipping malformed line %d in %s: %v", lineNum, baseName, err)
			continue
		}

		if !nsFilter(op.Ns) {
			filtered++
			continue
		}

		ts, err := strconv.ParseUint(op.Ts, 10, 64)
		if err != nil {
			errorLog.Printf("skipping invalid ts at line %d in %s: %v", lineNum, baseName, err)
			continue
		}

		key := dedupKey{ns: op.Ns, id: op.Id}
		if existing, ok := batch[key]; ok {
			// keep the one with larger ts (newer)
			if ts > existing.ts {
				batch[key] = dedupEntry{op: op, ts: ts}
			}
		} else {
			batch[key] = dedupEntry{op: op, ts: ts}
			batchCount++
		}

		if batchCount >= recoverBatchSize {
			p, s, err := flushBatch(fetcher, sinkConnector, batch)
			if err != nil {
				return processed, skipped, filtered, fmt.Errorf("flush batch at line %d: %w", lineNum, err)
			}
			processed += p
			skipped += s
			batch = make(map[dedupKey]dedupEntry)
			batchCount = 0

			// update progress after successful flush
			progress.File = baseName
			progress.Line = lineNum
			saveProgress(progressPath, progress)
		}

		if lineNum%recoverProgressLogInterval == 0 {
			infoLog.Printf("file=%s line=%d processed=%d skipped=%d filtered=%d", baseName, lineNum, processed, skipped, filtered)
		}
	}

	if err := scanner.Err(); err != nil {
		return processed, skipped, filtered, fmt.Errorf("scanner error: %w", err)
	}

	// flush remaining
	if batchCount > 0 {
		p, s, err := flushBatch(fetcher, sinkConnector, batch)
		if err != nil {
			return processed, skipped, filtered, fmt.Errorf("flush remaining batch: %w", err)
		}
		processed += p
		skipped += s

		progress.File = baseName
		progress.Line = lineNum
		saveProgress(progressPath, progress)
	}

	return processed, skipped, filtered, nil
}

func flushBatch(
	fetcher DocFetcher,
	sinkConnector sinks.SinkConnector,
	batch map[dedupKey]dedupEntry,
) (processed, skipped int64, err error) {
	// group by namespace and op type
	type nsOps struct {
		upsertIDs []string
		deleteIDs []string
		// map from _id hex to the entry (for ts)
		entries map[string]dedupEntry
	}

	grouped := make(map[string]*nsOps)
	for _, entry := range batch {
		ns := entry.op.Ns
		if _, ok := grouped[ns]; !ok {
			grouped[ns] = &nsOps{entries: make(map[string]dedupEntry)}
		}
		g := grouped[ns]
		g.entries[entry.op.Id] = entry

		switch entry.op.Op {
		case "i", "u":
			g.upsertIDs = append(g.upsertIDs, entry.op.Id)
		case "d":
			g.deleteIDs = append(g.deleteIDs, entry.op.Id)
		}
	}

	ctx := context.Background()

	for ns, ops := range grouped {
		// handle deletes
		for _, idHex := range ops.deleteIDs {
			oid, err := primitive.ObjectIDFromHex(idHex)
			if err != nil {
				errorLog.Printf("skipping invalid delete _id %s: %v", idHex, err)
				skipped++
				continue
			}

			entry := ops.entries[idHex]
			ts := uint32(entry.ts)

			deleteOp := &gtm.Op{
				Id:        oid,
				Operation: "d",
				Namespace: ns,
				Source:    gtm.OplogQuerySource,
				Timestamp: primitive.Timestamp{T: ts, I: 0},
				Data:      map[string]interface{}{"_id": oid},
			}
			if err := sinkConnector.RouteDelete(deleteOp); err != nil {
				return processed, skipped, fmt.Errorf("route delete %s/%s: %w", ns, idHex, err)
			}
			processed++
		}

		// handle upserts: batch fetch from MongoDB
		if len(ops.upsertIDs) > 0 {
			oids := make([]primitive.ObjectID, 0, len(ops.upsertIDs))
			for _, idHex := range ops.upsertIDs {
				oid, err := primitive.ObjectIDFromHex(idHex)
				if err != nil {
					errorLog.Printf("skipping invalid upsert _id %s: %v", idHex, err)
					skipped++
					continue
				}
				oids = append(oids, oid)
			}

			foundDocs, err := fetcher.FetchByIDs(ctx, ns, oids)
			if err != nil {
				return processed, skipped, fmt.Errorf("fetch docs in %s: %w", ns, err)
			}

			for _, idHex := range ops.upsertIDs {
				oid, err := primitive.ObjectIDFromHex(idHex)
				if err != nil {
					continue // already counted above
				}

				doc, found := foundDocs[oid]
				if !found {
					skipped++
					continue
				}

				entry := ops.entries[idHex]
				ts := uint32(entry.ts)

				dataOp := &gtm.Op{
					Id:        oid,
					Operation: entry.op.Op,
					Namespace: ns,
					Source:    gtm.OplogQuerySource,
					Timestamp: primitive.Timestamp{T: ts, I: 0},
					Data:      doc,
				}
				if err := sinkConnector.RouteData(dataOp); err != nil {
					return processed, skipped, fmt.Errorf("route data %s/%s: %w", ns, idHex, err)
				}
				processed++
			}
		}
	}

	// flush sink after batch
	if err := sinkConnector.Flush(); err != nil {
		return processed, skipped, fmt.Errorf("sink flush: %w", err)
	}

	return processed, skipped, nil
}

func loadProgress(path string) *recoverProgress {
	data, err := os.ReadFile(path)
	if err != nil {
		return &recoverProgress{}
	}
	var p recoverProgress
	if err := json.Unmarshal(data, &p); err != nil {
		errorLog.Printf("failed to parse progress file, starting fresh: %v", err)
		return &recoverProgress{}
	}
	infoLog.Printf("loaded recovery progress: file=%s line=%d completed=%d files",
		p.File, p.Line, len(p.Completed))
	return &p
}

func saveProgress(path string, p *recoverProgress) {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		errorLog.Printf("failed to marshal progress: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		errorLog.Printf("failed to write progress file: %v", err)
	}
}
