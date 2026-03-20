package common

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

type mockClient struct {
	mu       sync.Mutex
	requests int
}

func (m *mockClient) Name() string  { return "mock" }
func (m *mockClient) EmbedDoc() bool { return false }
func (m *mockClient) Commit(_ context.Context, reqs []bulk.BulkableRequest) error {
	m.mu.Lock()
	m.requests += len(reqs)
	m.mu.Unlock()
	return nil
}
func (m *mockClient) getRequests() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func newTestSink(t *testing.T, noDeleteBeforeDays int) (*Sink, *mockClient) {
	t.Helper()
	mc := &mockClient{}
	bp, err := bulk.NewBulkProcessorService(mc).
		BulkActions(1).
		Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := New(TransformConfig{
		NoDeleteBeforeDays: noDeleteBeforeDays,
	}, bp)
	if err != nil {
		t.Fatal(err)
	}
	return sink, mc
}

func makeDeleteOp(docCreateTime time.Time) *gtm.Op {
	id := primitive.NewObjectIDFromTimestamp(docCreateTime)
	return &gtm.Op{
		Id:        id,
		Namespace: "test.col",
		Data:      map[string]interface{}{"_id": id},
	}
}

func TestRouteDelete_ZeroDisabled(t *testing.T) {
	sink, mc := newTestSink(t, 0)
	defer sink.Close()

	// no-delete-before-days=0 不限制，老文档删除也正常处理
	op := makeDeleteOp(time.Now().AddDate(-2, 0, 0))
	if err := sink.RouteDelete(op); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if mc.getRequests() != 1 {
		t.Fatalf("expected 1 request, got %d", mc.getRequests())
	}
}

func TestRouteDelete_OldDocIgnored(t *testing.T) {
	sink, mc := newTestSink(t, 30)
	defer sink.Close()

	// 60 天前的文档 > 30 天阈值，删除应被忽略
	op := makeDeleteOp(time.Now().AddDate(0, 0, -60))
	if err := sink.RouteDelete(op); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if mc.getRequests() != 0 {
		t.Fatalf("expected 0 requests, got %d", mc.getRequests())
	}
}

func TestRouteDelete_RecentDocProcessed(t *testing.T) {
	sink, mc := newTestSink(t, 30)
	defer sink.Close()

	// 10 天前的文档 < 30 天阈值，删除应正常处理
	op := makeDeleteOp(time.Now().AddDate(0, 0, -10))
	if err := sink.RouteDelete(op); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if mc.getRequests() != 1 {
		t.Fatalf("expected 1 request, got %d", mc.getRequests())
	}
}

func TestRouteDelete_BoundaryInside(t *testing.T) {
	sink, mc := newTestSink(t, 30)
	defer sink.Close()

	// 29 天，未超阈值，应处理
	op := makeDeleteOp(time.Now().AddDate(0, 0, -29))
	if err := sink.RouteDelete(op); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if mc.getRequests() != 1 {
		t.Fatalf("expected 1 request, got %d", mc.getRequests())
	}
}

func TestRouteDelete_BoundaryOutside(t *testing.T) {
	sink, mc := newTestSink(t, 30)
	defer sink.Close()

	// 31 天，超出阈值，应忽略
	op := makeDeleteOp(time.Now().AddDate(0, 0, -31))
	if err := sink.RouteDelete(op); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if mc.getRequests() != 0 {
		t.Fatalf("expected 0 requests, got %d", mc.getRequests())
	}
}
