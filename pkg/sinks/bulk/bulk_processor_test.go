package bulk

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/rwynn/gtm/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type MockClient struct {
	i int
}

func (m *MockClient) Commit(ctx context.Context, requests []BulkableRequest) error {
	m.i++
	fmt.Printf("[%d]commit %d requests\n", m.i, len(requests))
	if m.i%2 == 0 {
		return fmt.Errorf("even err")
	}

	return nil
}

func TestNewBulkService(t *testing.T) {
	bulkService := NewBulkService(&MockClient{})
	{
		for i := 0; i < 3; i++ {
			bulkService.Add(&gtm.Op{})
		}
		err := bulkService.Do(context.TODO())
		require.NoError(t, err)
	}
	{
		for i := 0; i < 3; i++ {
			bulkService.Add(&gtm.Op{})
		}
		err := bulkService.Do(context.TODO())
		require.Error(t, err)
	}
}

type MockClient2 struct {
	i        int
	received int
	mu       sync.Mutex
}

func (m *MockClient2) Commit(ctx context.Context, requests []BulkableRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.i++
	if m.i%2 == 0 {
		return fmt.Errorf("even err")
	}
	m.received += len(requests)

	fmt.Printf("[%d]commit %d requests\n", m.i, len(requests))
	return nil
}

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func Test_NewBulkProcessorService(t *testing.T) {
	client := &MockClient2{}
	// 2 worker and
	// commit every 100 requests and
	// commit every 1 second
	service := NewBulkProcessorService(client)
	service.BulkActions(100)
	service.FlushInterval(time.Second)
	service.Workers(2)
	// start bulk processor which will start bulkWorkers and flusher
	bulkProcessor, err := service.Do(context.TODO())
	require.NoError(t, err)

	closeC := make(chan struct{})
	sentRequests := 0
	go func() {
		// produce message
		ticker := time.NewTicker(time.Millisecond * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				bulkProcessor.Add(&gtm.Op{})
				sentRequests++
			case <-closeC:
				fmt.Println("exit message producer")
				fmt.Printf("total requests sent: %d\n", sentRequests)
				return
			}
		}
	}()

	time.Sleep(time.Second * 10)

	close(closeC)
	err = bulkProcessor.Close()
	require.NoError(t, err)
	assert.Equal(t, sentRequests, client.received)
}

func TestExponentialBackOff(t *testing.T) {
	b := backoff.NewExponentialBackOff()
	for i := 0; i < 100; i++ {
		d := b.NextBackOff()
		fmt.Printf("backoff, %v\n", d)
	}
	b.Reset()
	fmt.Printf("after reset: %v\n", b.NextBackOff())
}
