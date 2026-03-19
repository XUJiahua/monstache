package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// mockDocFetcher returns pre-configured documents for any FetchByIDs call.
type mockDocFetcher struct {
	// docs keyed by namespace then _id hex string
	docs map[string]map[string]map[string]interface{}
	mu   sync.Mutex
	// record all fetch calls for assertions
	fetchCalls []fetchCall
}

type fetchCall struct {
	Namespace string
	IDs       []primitive.ObjectID
}

func (m *mockDocFetcher) FetchByIDs(ctx context.Context, namespace string, oids []primitive.ObjectID) (map[interface{}]map[string]interface{}, error) {
	m.mu.Lock()
	m.fetchCalls = append(m.fetchCalls, fetchCall{Namespace: namespace, IDs: oids})
	m.mu.Unlock()

	result := make(map[interface{}]map[string]interface{})
	nsDocs := m.docs[namespace]
	if nsDocs == nil {
		return result, nil
	}
	for _, oid := range oids {
		if doc, ok := nsDocs[oid.Hex()]; ok {
			result[oid] = doc
		}
	}
	return result, nil
}

// mockSinkConnector records all routed ops.
type mockSinkConnector struct {
	mu         sync.Mutex
	dataOps    []*gtm.Op
	deleteOps  []*gtm.Op
	flushCount int
}

func (m *mockSinkConnector) RouteData(op *gtm.Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataOps = append(m.dataOps, op)
	return nil
}

func (m *mockSinkConnector) RouteDelete(op *gtm.Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteOps = append(m.deleteOps, op)
	return nil
}

func (m *mockSinkConnector) RouteDrop(op *gtm.Op) error {
	return nil
}

func (m *mockSinkConnector) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCount++
	return nil
}

// mockCloser tracks whether Close was called.
type mockCloser struct {
	closed bool
}

func (m *mockCloser) Close() error {
	m.closed = true
	return nil
}

// writeNDJSON creates a .ndjson file with the given ops.
func writeNDJSON(t *testing.T, dir, filename string, ops []recoverOp) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, op := range ops {
		if err := enc.Encode(op); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

// writeNDJSONGz creates a .ndjson.gz file with the given ops.
func writeNDJSONGz(t *testing.T, dir, filename string, ops []recoverOp) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gw := gzip.NewWriter(f)
	enc := json.NewEncoder(gw)
	for _, op := range ops {
		if err := enc.Encode(op); err != nil {
			t.Fatal(err)
		}
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func generateObjectID(t *testing.T) primitive.ObjectID {
	t.Helper()
	return primitive.NewObjectID()
}

func TestRecover_BasicUpsertAndDelete(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)
	id2 := generateObjectID(t)
	id3 := generateObjectID(t)

	ops := []recoverOp{
		{Ts: "1000", Op: "i", Ns: "testdb.coll1", Id: id1.Hex()},
		{Ts: "1001", Op: "u", Ns: "testdb.coll1", Id: id2.Hex()},
		{Ts: "1002", Op: "d", Ns: "testdb.coll1", Id: id3.Hex()},
	}
	writeNDJSON(t, dir, "part_1.ndjson", ops)

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"testdb.coll1": {
				id1.Hex(): {"_id": id1, "name": "doc1"},
				id2.Hex(): {"_id": id2, "name": "doc2"},
			},
		},
	}
	sink := &mockSinkConnector{}
	closer := &mockCloser{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, []sinks.Closer{closer})

	if len(sink.dataOps) != 2 {
		t.Fatalf("expected 2 data ops, got %d", len(sink.dataOps))
	}
	if len(sink.deleteOps) != 1 {
		t.Fatalf("expected 1 delete op, got %d", len(sink.deleteOps))
	}
	if !closer.closed {
		t.Fatal("expected closer to be called")
	}
	// progress file should be cleaned up
	if _, err := os.Stat(filepath.Join(dir, recoverProgressFilename)); !os.IsNotExist(err) {
		t.Fatal("expected progress file to be removed after completion")
	}
}

func TestRecover_GzipFile(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)

	ops := []recoverOp{
		{Ts: "2000", Op: "u", Ns: "mydb.mycol", Id: id1.Hex()},
	}
	writeNDJSONGz(t, dir, "part_1.ndjson.gz", ops)

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"mydb.mycol": {
				id1.Hex(): {"_id": id1, "value": 42},
			},
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != 1 {
		t.Fatalf("expected 1 data op, got %d", len(sink.dataOps))
	}
	if sink.dataOps[0].Timestamp.T != 2000 {
		t.Fatalf("expected timestamp T=2000, got %d", sink.dataOps[0].Timestamp.T)
	}
}

func TestRecover_DedupKeepsLatestTs(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)

	// same _id: first update, then delete with higher ts → delete should win
	ops := []recoverOp{
		{Ts: "1000", Op: "u", Ns: "testdb.coll", Id: id1.Hex()},
		{Ts: "2000", Op: "d", Ns: "testdb.coll", Id: id1.Hex()},
	}
	writeNDJSON(t, dir, "part_1.ndjson", ops)

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"testdb.coll": {
				id1.Hex(): {"_id": id1, "name": "doc1"},
			},
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != 0 {
		t.Fatalf("expected 0 data ops (dedup should keep delete), got %d", len(sink.dataOps))
	}
	if len(sink.deleteOps) != 1 {
		t.Fatalf("expected 1 delete op, got %d", len(sink.deleteOps))
	}
}

func TestRecover_DedupKeepsUpdateOverOlderDelete(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)

	// same _id: first delete, then update with higher ts → update should win
	ops := []recoverOp{
		{Ts: "1000", Op: "d", Ns: "testdb.coll", Id: id1.Hex()},
		{Ts: "2000", Op: "u", Ns: "testdb.coll", Id: id1.Hex()},
	}
	writeNDJSON(t, dir, "part_1.ndjson", ops)

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"testdb.coll": {
				id1.Hex(): {"_id": id1, "name": "doc1"},
			},
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.deleteOps) != 0 {
		t.Fatalf("expected 0 delete ops (dedup should keep update), got %d", len(sink.deleteOps))
	}
	if len(sink.dataOps) != 1 {
		t.Fatalf("expected 1 data op, got %d", len(sink.dataOps))
	}
}

func TestRecover_CheckpointResume(t *testing.T) {
	dir := t.TempDir()

	// create 3 files with 2 ops each
	ids := make([]primitive.ObjectID, 6)
	for i := range ids {
		ids[i] = generateObjectID(t)
	}

	writeNDJSON(t, dir, "part_1.ndjson", []recoverOp{
		{Ts: "100", Op: "u", Ns: "db.col", Id: ids[0].Hex()},
		{Ts: "101", Op: "u", Ns: "db.col", Id: ids[1].Hex()},
	})
	writeNDJSON(t, dir, "part_2.ndjson", []recoverOp{
		{Ts: "200", Op: "u", Ns: "db.col", Id: ids[2].Hex()},
		{Ts: "201", Op: "u", Ns: "db.col", Id: ids[3].Hex()},
	})
	writeNDJSON(t, dir, "part_3.ndjson", []recoverOp{
		{Ts: "300", Op: "u", Ns: "db.col", Id: ids[4].Hex()},
		{Ts: "301", Op: "u", Ns: "db.col", Id: ids[5].Hex()},
	})

	// simulate: part_1 completed, part_2 processed line 1
	progress := &recoverProgress{
		File:      "part_2.ndjson",
		Line:      1,
		Completed: []string{"part_1.ndjson"},
	}
	saveProgress(filepath.Join(dir, recoverProgressFilename), progress)

	allDocs := make(map[string]map[string]interface{})
	for _, id := range ids {
		allDocs[id.Hex()] = map[string]interface{}{"_id": id, "x": 1}
	}
	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"db.col": allDocs,
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	// should process: part_2 line 2 (ids[3]) + part_3 lines 1-2 (ids[4], ids[5])
	// part_1 skipped (completed), part_2 line 1 skipped (resume)
	if len(sink.dataOps) != 3 {
		t.Fatalf("expected 3 data ops after resume, got %d", len(sink.dataOps))
	}
}

func TestRecover_SkipMissingDocs(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)
	id2 := generateObjectID(t)

	ops := []recoverOp{
		{Ts: "1000", Op: "u", Ns: "db.col", Id: id1.Hex()},
		{Ts: "1001", Op: "u", Ns: "db.col", Id: id2.Hex()},
	}
	writeNDJSON(t, dir, "part_1.ndjson", ops)

	// only id1 exists in "MongoDB"
	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"db.col": {
				id1.Hex(): {"_id": id1, "val": "exists"},
			},
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != 1 {
		t.Fatalf("expected 1 data op (missing doc skipped), got %d", len(sink.dataOps))
	}
}

func TestRecover_SingleFile(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)
	filePath := writeNDJSON(t, dir, "single.ndjson", []recoverOp{
		{Ts: "500", Op: "i", Ns: "db.col", Id: id1.Hex()},
	})

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"db.col": {
				id1.Hex(): {"_id": id1, "v": 1},
			},
		},
	}
	sink := &mockSinkConnector{}

	// point to file directly, not directory
	config := &ConfigOptions{
		OplogRecoverFilepath: filePath,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != 1 {
		t.Fatalf("expected 1 data op, got %d", len(sink.dataOps))
	}
}

func TestRecover_MultipleNamespaces(t *testing.T) {
	dir := t.TempDir()

	id1 := generateObjectID(t)
	id2 := generateObjectID(t)

	ops := []recoverOp{
		{Ts: "1000", Op: "u", Ns: "db1.col1", Id: id1.Hex()},
		{Ts: "1001", Op: "u", Ns: "db2.col2", Id: id2.Hex()},
	}
	writeNDJSON(t, dir, "part_1.ndjson", ops)

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"db1.col1": {
				id1.Hex(): {"_id": id1, "src": "db1"},
			},
			"db2.col2": {
				id2.Hex(): {"_id": id2, "src": "db2"},
			},
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != 2 {
		t.Fatalf("expected 2 data ops across namespaces, got %d", len(sink.dataOps))
	}
}

func TestRecover_LargeBatchFlush(t *testing.T) {
	dir := t.TempDir()

	// generate more ops than recoverBatchSize to trigger mid-file flush
	n := recoverBatchSize + 50
	ops := make([]recoverOp, n)
	docs := make(map[string]map[string]interface{})
	for i := 0; i < n; i++ {
		id := generateObjectID(t)
		ops[i] = recoverOp{
			Ts: fmt.Sprintf("%d", 1000+i),
			Op: "u",
			Ns: "db.col",
			Id: id.Hex(),
		}
		docs[id.Hex()] = map[string]interface{}{"_id": id, "i": i}
	}
	writeNDJSON(t, dir, "part_1.ndjson", ops)

	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			"db.col": docs,
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: dir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != n {
		t.Fatalf("expected %d data ops, got %d", n, len(sink.dataOps))
	}
	// at least 2 flushes: one at batch boundary, one for remainder
	if sink.flushCount < 2 {
		t.Fatalf("expected at least 2 flushes, got %d", sink.flushCount)
	}
}

func TestRecover_RealOplogDir(t *testing.T) {
	const oplogDir = "/Users/administrator/Downloads/oplog/split_output"
	if _, err := os.Stat(oplogDir); os.IsNotExist(err) {
		t.Skipf("oplog dir not found: %s", oplogDir)
	}

	// 30 ops, all "u" on config-bkk.evo.mccExtraData, _id from c11c to c139
	ns := "config-bkk.evo.mccExtraData"
	idHexes := []string{
		"68f3459d65a5f9b7c875c11c", "68f3459d65a5f9b7c875c11d",
		"68f3459d65a5f9b7c875c11e", "68f3459d65a5f9b7c875c11f",
		"68f3459d65a5f9b7c875c120", "68f3459d65a5f9b7c875c121",
		"68f3459d65a5f9b7c875c122", "68f3459d65a5f9b7c875c123",
		"68f3459d65a5f9b7c875c124", "68f3459d65a5f9b7c875c125",
		"68f3459d65a5f9b7c875c126", "68f3459d65a5f9b7c875c127",
		"68f3459d65a5f9b7c875c128", "68f3459d65a5f9b7c875c129",
		"68f3459d65a5f9b7c875c12a", "68f3459d65a5f9b7c875c12b",
		"68f3459d65a5f9b7c875c12c", "68f3459d65a5f9b7c875c12d",
		"68f3459d65a5f9b7c875c12e", "68f3459d65a5f9b7c875c12f",
		"68f3459d65a5f9b7c875c130", "68f3459d65a5f9b7c875c131",
		"68f3459d65a5f9b7c875c132", "68f3459d65a5f9b7c875c133",
		"68f3459d65a5f9b7c875c134", "68f3459d65a5f9b7c875c135",
		"68f3459d65a5f9b7c875c136", "68f3459d65a5f9b7c875c137",
		"68f3459d65a5f9b7c875c138", "68f3459d65a5f9b7c875c139",
	}

	// mock: all 30 docs exist in "MongoDB"
	docs := make(map[string]map[string]interface{})
	for _, hex := range idHexes {
		oid, _ := primitive.ObjectIDFromHex(hex)
		docs[hex] = map[string]interface{}{"_id": oid, "mock": true}
	}
	fetcher := &mockDocFetcher{
		docs: map[string]map[string]map[string]interface{}{
			ns: docs,
		},
	}
	sink := &mockSinkConnector{}

	config := &ConfigOptions{
		OplogRecoverFilepath: oplogDir,
	}

	doRecover(fetcher, config, sink, nil)

	if len(sink.dataOps) != 30 {
		t.Fatalf("expected 30 data ops, got %d", len(sink.dataOps))
	}
	if len(sink.deleteOps) != 0 {
		t.Fatalf("expected 0 delete ops, got %d", len(sink.deleteOps))
	}

	// verify all ops have correct namespace and timestamp
	for i, op := range sink.dataOps {
		if op.Namespace != ns {
			t.Errorf("op[%d] namespace = %s, want %s", i, op.Namespace, ns)
		}
		if op.Timestamp.T != 1769419115 {
			t.Errorf("op[%d] timestamp = %d, want 1769419115", i, op.Timestamp.T)
		}
		if op.Operation != "u" {
			t.Errorf("op[%d] operation = %s, want u", i, op.Operation)
		}
		if op.Data == nil {
			t.Errorf("op[%d] data is nil", i)
		}
	}

	// verify fetcher was called with correct namespace
	if len(fetcher.fetchCalls) == 0 {
		t.Fatal("expected at least 1 fetch call")
	}
	for _, call := range fetcher.fetchCalls {
		if call.Namespace != ns {
			t.Errorf("fetch namespace = %s, want %s", call.Namespace, ns)
		}
	}

	// verify progress file cleaned up
	progressPath := filepath.Join(oplogDir, recoverProgressFilename)
	if _, err := os.Stat(progressPath); !os.IsNotExist(err) {
		t.Fatalf("progress file should be removed after completion")
	}

	t.Logf("OK: 30 ops routed, %d flushes, %d fetch calls",
		sink.flushCount, len(fetcher.fetchCalls))
}
