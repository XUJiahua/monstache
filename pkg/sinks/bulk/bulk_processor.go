// Package bulk
// it's a fork of elastic bulk processor but for general purpose
package bulk

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

type BulkableRequest interface {
	GetTopic() string
	GetKey() []byte
	GetValue() []byte
}

type Client interface {
	Commit(ctx context.Context, requests []BulkableRequest) error
}

// BulkProcessorService allows to easily process bulk requests. It allows setting
// policies when to flush new bulk requests, e.g. based on a number of actions,
// on the size of the actions, and/or to flush periodically. It also allows
// to control the number of concurrent bulk requests allowed to be executed
// in parallel.
//
// BulkProcessorService, by default, commits either every 1000 requests or when the
// (estimated) size of the bulk requests exceeds 5 MB. However, it does not
// commit periodically. BulkProcessorService also does retry by default, using
// an exponential backoff algorithm.
type BulkProcessorService struct {
	c             Client
	beforeFn      BulkBeforeFunc
	afterFn       BulkAfterFunc
	name          string        // name of processor
	numWorkers    int           // # of workers (>= 1)
	bulkActions   int           // # of requests after which to commit
	bulkSize      int           // # of bytes after which to commit
	flushInterval time.Duration // periodic flush interval
	wantStats     bool          // indicates whether to gather statistics
}

// NewBulkProcessorService creates a new BulkProcessorService.
func NewBulkProcessorService(client Client) *BulkProcessorService {
	return &BulkProcessorService{
		c:           client,
		numWorkers:  1,
		bulkActions: 1000,
		bulkSize:    -1, // fixme: not implement bulkSize internally
	}
}

// Before specifies a function to be executed before bulk requests get committed
// to Elasticsearch.
func (s *BulkProcessorService) Before(fn BulkBeforeFunc) *BulkProcessorService {
	s.beforeFn = fn
	return s
}

// After specifies a function to be executed when bulk requests have been
// committed to Elasticsearch. The After callback executes both when the
// commit was successful as well as on failures.
func (s *BulkProcessorService) After(fn BulkAfterFunc) *BulkProcessorService {
	s.afterFn = fn
	return s
}

// Name is an optional name to identify this bulk processor.
func (s *BulkProcessorService) Name(name string) *BulkProcessorService {
	s.name = name
	return s
}

// Workers is the number of concurrent workers allowed to be
// executed. Defaults to 1 and must be greater or equal to 1.
func (s *BulkProcessorService) Workers(num int) *BulkProcessorService {
	s.numWorkers = num
	return s
}

// BulkActions specifies when to flush based on the number of actions
// currently added. Defaults to 1000 and can be set to -1 to be disabled.
func (s *BulkProcessorService) BulkActions(bulkActions int) *BulkProcessorService {
	s.bulkActions = bulkActions
	return s
}

// BulkSize specifies when to flush based on the size (in bytes) of the actions
// currently added. Defaults to 5 MB and can be set to -1 to be disabled.
func (s *BulkProcessorService) BulkSize(bulkSize int) *BulkProcessorService {
	s.bulkSize = bulkSize
	return s
}

// FlushInterval specifies when to flush at the end of the given interval.
// This is disabled by default. If you want the bulk processor to
// operate completely asynchronously, set both BulkActions and BulkSize to
// -1 and set the FlushInterval to a meaningful interval.
func (s *BulkProcessorService) FlushInterval(interval time.Duration) *BulkProcessorService {
	s.flushInterval = interval
	return s
}

// Stats tells bulk processor to gather stats while running.
// Use Stats to return the stats. This is disabled by default.
func (s *BulkProcessorService) Stats(wantStats bool) *BulkProcessorService {
	s.wantStats = wantStats
	return s
}

// Do creates a new BulkProcessor and starts it.
// Consider the BulkProcessor as a running instance that accepts bulk requests
// and commits them to Elasticsearch, spreading the work across one or more
// workers.
//
// You can interoperate with the BulkProcessor returned by Do, e.g. Start and
// Stop (or Close) it.
//
// Context is an optional context that is passed into the bulk request
// service calls. In contrast to other operations, this context is used in
// a long running process. You could use it to pass e.g. loggers, but you
// shouldn't use it for cancellation.
//
// Calling Do several times returns new BulkProcessors. You probably don't
// want to do this. BulkProcessorService implements just a builder pattern.
func (s *BulkProcessorService) Do(ctx context.Context) (*BulkProcessor, error) {
	p := newBulkProcessor(
		s.c,
		s.beforeFn,
		s.afterFn,
		s.numWorkers,
		s.bulkActions,
		s.bulkSize,
		s.flushInterval,
	)

	err := p.Start(ctx)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// BulkProcessor bulk commit for improving sink performance
type BulkProcessor struct {
	numWorkers int
	workerWg   sync.WaitGroup
	workers    []*bulkWorker

	client      Client
	beforeFn    BulkBeforeFunc
	afterFn     BulkAfterFunc
	executionId int64

	requestsC     chan BulkableRequest
	flushInterval time.Duration
	flusherStopC  chan struct{}
	bulkActions   int
	bulkSize      int

	startedMu sync.Mutex // guards the following block
	started   bool
}

// BulkBeforeFunc defines the signature of callbacks that are executed
// before a commit to Elasticsearch.
type BulkBeforeFunc func(executionId int64, requests []BulkableRequest)

// BulkAfterFunc defines the signature of callbacks that are executed
// after a commit to Elasticsearch. The err parameter signals an error.
type BulkAfterFunc func(executionId int64, requests []BulkableRequest, err error)

func newBulkProcessor(
	client Client,
	beforeFn BulkBeforeFunc,
	afterFn BulkAfterFunc,
	numWorkers int,
	bulkActions int,
	bulkSize int,
	flushInterval time.Duration,
) *BulkProcessor {
	return &BulkProcessor{
		client:        client,
		beforeFn:      beforeFn,
		afterFn:       afterFn,
		numWorkers:    numWorkers,
		bulkActions:   bulkActions,
		bulkSize:      bulkSize,
		flushInterval: flushInterval,
	}
}

func (p *BulkProcessor) Start(ctx context.Context) error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()

	if p.started {
		return nil
	}

	if p.numWorkers < 1 {
		p.numWorkers = 1
	}

	p.requestsC = make(chan BulkableRequest)
	p.workers = make([]*bulkWorker, p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		p.workerWg.Add(1)
		p.workers[i] = newBulkWorker(p, i)
		go p.workers[i].work(ctx)
	}

	// Start the ticker for flush (if enabled)
	if int64(p.flushInterval) > 0 {
		p.flusherStopC = make(chan struct{})
		go p.flusher(p.flushInterval)
	}

	p.started = true

	return nil
}

func (p *BulkProcessor) Close() error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()

	// Already stopped? Do nothing.
	if !p.started {
		return nil
	}

	// Stop flusher (if enabled)
	if p.flusherStopC != nil {
		p.flusherStopC <- struct{}{}
		<-p.flusherStopC
		close(p.flusherStopC)
		p.flusherStopC = nil
	}

	// Stop all workers.
	close(p.requestsC)
	p.workerWg.Wait()

	p.started = false

	return nil
}

// Flush manually asks all workers to commit their outstanding requests.
// It returns only when all workers acknowledge completion.
func (p *BulkProcessor) Flush() error {
	for _, w := range p.workers {
		w.flushC <- struct{}{}
		<-w.flushAckC // wait for completion
	}
	return nil
}

// flusher is a single goroutine that periodically asks all workers to
// commit their outstanding bulk requests. It is only started if
// FlushInterval is greater than 0.
func (p *BulkProcessor) flusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // Periodic flush
			p.Flush()

		case <-p.flusherStopC:
			p.flusherStopC <- struct{}{}
			logrus.Debugf("flusher will stop itself")
			return
		}
	}
}

// Add a new op, underlying sinker will handle it
func (p *BulkProcessor) Add(request BulkableRequest) {
	p.requestsC <- request
}

type bulkWorker struct {
	p           *BulkProcessor
	index       int
	flushC      chan struct{}
	flushAckC   chan struct{}
	bulkActions int
	bulkSize    int

	service *BulkService
}

func newBulkWorker(p *BulkProcessor, index int) *bulkWorker {
	return &bulkWorker{
		p:           p,
		index:       index,
		service:     NewBulkService(p.client),
		flushC:      make(chan struct{}),
		flushAckC:   make(chan struct{}),
		bulkActions: p.bulkActions,
		bulkSize:    p.bulkSize,
	}
}

func (w *bulkWorker) work(ctx context.Context) {
	defer func() {
		w.p.workerWg.Done()
		close(w.flushAckC)
		close(w.flushC)
	}()

	stop := false
	for !stop {
		var err error
		select {
		case req, open := <-w.p.requestsC:
			if open {
				w.service.Add(req)
				if w.commitRequired() {
					logrus.Debugf("commit the requests")
					err = w.commit(ctx)
				}

			} else {
				logrus.Debugf("worker will stop itself as requestsC closed")
				stop = true
				if w.service.NumberOfActions() > 0 {
					logrus.Debugf("commit the remaining requests")
					err = w.commit(ctx)
				}
			}
		case <-w.flushC:
			if w.service.NumberOfActions() > 0 {
				logrus.Debugf("commit the request as flusher requeted")
				err = w.commit(ctx)
			}
			w.flushAckC <- struct{}{}
		}

		if err != nil {
			logrus.Errorf("commit failed: %v", err)
			// todo: wait for client back online
		}
	}
}

func (w *bulkWorker) commitRequired() bool {
	if w.bulkActions >= 0 && w.service.NumberOfActions() >= w.bulkActions {
		return true
	}
	if w.bulkSize >= 0 && w.service.EstimatedSizeInBytes() >= int64(w.bulkSize) {
		return true
	}
	return false
}

func (w *bulkWorker) commit(ctx context.Context) error {

	// Save requests because they will be reset in commitFunc
	reqs := w.service.requests

	id := atomic.AddInt64(&w.p.executionId, 1)

	// commit using client
	commitFunc := func() error {
		return w.service.Do(ctx)
	}
	notifyFunc := func(err error, duration time.Duration) {
		logrus.Warnf("bulk processor [%d] failed with err: %v, but may retry in %v", id, err, duration)
	}
	// Invoke before callback
	if w.p.beforeFn != nil {
		w.p.beforeFn(id, reqs)
	}

	// backoff.NewExponentialBackOff() is stateful, new one for every Retry
	err := backoff.RetryNotify(commitFunc, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), notifyFunc)
	if err != nil {
		logrus.Errorf("bulk processor failed with err: %v", err)
	}

	// Invoke after callback
	if w.p.afterFn != nil {
		w.p.afterFn(id, reqs, err)
	}

	return err
}

// BulkService bulk service which use client to commit request
type BulkService struct {
	requests []BulkableRequest
	client   Client
}

func NewBulkService(client Client) *BulkService {
	return &BulkService{
		client: client,
	}
}

func (s *BulkService) Add(req ...BulkableRequest) {
	s.requests = append(s.requests, req...)
}

func (s *BulkService) NumberOfActions() int {
	return len(s.requests)
}

func (s *BulkService) EstimatedSizeInBytes() int64 {
	return 0
}

// Reset cleans up the request queue
func (s *BulkService) Reset() {
	s.requests = make([]BulkableRequest, 0)
}

// Do commit the requests and reset request queue
func (s *BulkService) Do(ctx context.Context) error {
	if len(s.requests) == 0 {
		return nil
	}
	if err := s.client.Commit(ctx, s.requests); err != nil {
		return err
	}

	// commit successful, reset the request queue
	s.Reset()

	return nil
}
