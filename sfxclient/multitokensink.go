package sfxclient

import (
	"fmt"
	"hash"
	"hash/fnv"
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/pointer"
	"golang.org/x/net/context"
)

// ContextKey is a custom key type for context values
type ContextKey string

const (
	// TokenCtxKey is a context key for tokens
	TokenCtxKey ContextKey = TokenHeaderName
)

// dpMsg is the message object for datapoints
type dpMsg struct {
	token string
	data  []*datapoint.Datapoint
}

// evMsg is the message object for events
type evMsg struct {
	token string
	data  []*event.Event
}

// TODO: (GO 1.9) use the builtin sync.Map as an underlying data structure
// atomictokenStatusCounterMap is an atomic map for storing counters on a per http status per token basis
type atomicTokenStatusCounterMap struct {
	lock *sync.Mutex
	m    map[string]map[string]*int64
	name string
}

// Load returns a pointer to the int64 value for the supplied token and status and a boolean indicating if the load was successful or not
func (a *atomicTokenStatusCounterMap) Load(token string, status string) (val *int64, ok bool) {
	a.lock.Lock()
	defer a.lock.Unlock()
	var sub map[string]*int64
	if sub, ok = a.m[token]; ok {
		val, ok = sub[status]
	}
	return
}

// Store stores an int64 value in the atomic counter map for the supplied token and status
func (a *atomicTokenStatusCounterMap) Store(token string, status string, val int64) {
	a.lock.Lock()
	defer a.lock.Unlock()
	var ok bool
	if _, ok = a.m[token]; !ok {
		a.m[token] = map[string]*int64{}
	}
	a.m[token][status] = pointer.Int64(val)
}

// FetchDatapoints returns cumulative counters for all combinations of tokens and statuses
func (a *atomicTokenStatusCounterMap) FetchDatapoints() (counters []*datapoint.Datapoint) {
	a.lock.Lock()
	defer a.lock.Unlock()
	for token, statuses := range a.m {
		for status, counter := range statuses {
			counters = append(counters, Cumulative(a.name, map[string]string{"token": token, "status": status}, atomic.LoadInt64(counter)))
		}
	}
	return
}

// newAtomicTokenStatusCounterMap returns a map keyed by token containing a map keyed by http status containing a pointer to an int64 value
func newAtomicTokenStatusCounterMap(name string) *atomicTokenStatusCounterMap {
	return &atomicTokenStatusCounterMap{
		lock: &sync.Mutex{},
		m:    map[string]map[string]*int64{},
		name: name,
	}
}

type atomicBool struct {
	val  bool
	lock *sync.Mutex
}

func (a *atomicBool) Get() bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.val
}

func (a *atomicBool) Set(in bool) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.val = in
}

func newAtomicBool() *atomicBool {
	return &atomicBool{
		lock: &sync.Mutex{},
	}
}

// worker manages a pipeline for emitting metrics
type worker struct {
	lock                *sync.Mutex       // lock to control concurrent access to the worker
	errorHandler        func(error) error // error handler for handling error emitting datapoints
	sink                *HTTPSink         // sink is an HTTPSink for emitting datapoints to Signal Fx
	closing             chan bool         // channel to signal that the worker is stopping
	stopped             *atomicBool       // indicates that the worker has completely shutdown
	invalidStatusRegexp *regexp.Regexp    // regex to identify an http error from the sink
	statusCodeRegexp    *regexp.Regexp    // regex to identify the status code returned in an error from the sink
}

// returns a new instance of worker with an configured emission pipeline
func newWorker(maxBuffer int, errorHandler func(error) error) *worker {
	w := &worker{
		lock:                &sync.Mutex{},
		sink:                NewHTTPSink(),
		errorHandler:        errorHandler,
		closing:             make(chan bool),
		stopped:             newAtomicBool(),
		invalidStatusRegexp: regexp.MustCompile(`^invalid status code [\d]+$`),
		statusCodeRegexp:    regexp.MustCompile(`\d+`),
	}

	return w
}

// worker for handling datapoints
type datapointWorker struct {
	*worker
	input        chan *dpMsg // channel for inputing datapoints into a worker
	buffer       []*datapoint.Datapoint
	batchSize    int
	dataBuffered int64                     // atomic Int64 that maintains count of data in the sink
	stats        *asyncMultiTokenSinkStats // stats about
}

// emits a series of datapoints
func (w *datapointWorker) emit(token string, datapoints []*datapoint.Datapoint) {
	// set the token on the HTTPSink
	w.sink.AuthToken = token
	// emit datapoints and handle any errors
	err := w.sink.AddDatapoints(context.Background(), datapoints)
	w.handleError(err, token, datapoints)
	// account for the emitted datapoints
	_ = atomic.AddInt64(&w.dataBuffered, int64(len(datapoints)*-1))
}

func (w *datapointWorker) handleError(err error, token string, datapoints []*datapoint.Datapoint) {
	if err != nil {
		_ = atomic.AddInt64(&w.stats.TotalDatapointsDropped, int64(len(datapoints)))
		// TODO: improve httpSinks handle response to return the response code
		// parse the http status code from the error returned by the httpsink
		statusCode := "unknown"
		if w.invalidStatusRegexp.MatchString(err.Error()) {
			statusCode = w.statusCodeRegexp.FindStringSubmatch(err.Error())[0]
		}
		if counter, ok := w.stats.TotalDatapointsDroppedByToken.Load(token, statusCode); ok {
			_ = atomic.AddInt64(counter, int64(len(datapoints)))
		} else {
			w.stats.TotalDatapointsDroppedByToken.Store(token, statusCode, int64(len(datapoints)))
		}
		_ = w.errorHandler(err)
	} else {
		_ = atomic.AddInt64(&w.stats.TotalDatapointsEmitted, int64(len(datapoints)))
	}
}

// processMsg is
func (w *datapointWorker) processMsg(msg *dpMsg) {
	for len(msg.data) > 0 {
		msgLength := len(msg.data)
		remainingBuffer := (w.batchSize - len(w.buffer))
		if msgLength > remainingBuffer {
			msgLength = remainingBuffer
		}
		w.buffer = append(w.buffer, msg.data[:msgLength]...)
		msg.data = msg.data[msgLength:]
		if len(w.buffer) >= w.batchSize {
			w.emit(msg.token, w.buffer)
			w.buffer = w.buffer[:0]
		}
	}
}

// bufferDatapoints is responsible for batching incoming datapoints into a buffer
func (w *datapointWorker) bufferFunc(msg *dpMsg) (stop bool) {
	var lastTokenSeen = msg.token
	w.processMsg(msg)
outer:
	for len(w.buffer) < w.batchSize {
		select {
		case msg = <-w.input:
			if msg.token != lastTokenSeen {
				// if the token changes, then emit what ever is in the buffer before proceeding
				w.emit(msg.token, w.buffer)
				w.buffer = w.buffer[:0]
				lastTokenSeen = msg.token
			}
			w.processMsg(msg)
		default:
			break outer // emit what ever is in the buffer if there are no more datapoints to read
		}
	}
	// emit the data in the buffer
	w.emit(msg.token, w.buffer)
	w.buffer = w.buffer[:0]
	return
}

// newBuffer buffers datapoints and events in the pipeline for the duration specified during Startup
func (w *datapointWorker) newBuffer() {
	go func() {
		for {
			select {
			case <-w.closing: // check if the worker is in a closing state
				w.stopped.Set(true)
				return
			case msg := <-w.input:
				// process the Datapoint Message
				w.bufferFunc(msg)
			}
		}
	}()
}

// Stop will stop the pipeline
func (w *datapointWorker) Stop(timeout time.Duration) error {
	timer := time.After(timeout)
	for {
		select {
		case <-timer: // timeout and generate error message indicating what may be dropped
			return fmt.Errorf("worker timed out while stopping")
		default:
			select {
			case <-w.closing:
				if w.stopped.Get() {
					return nil
				}
			default:
				close(w.closing)
			}
		}
	}
}

func newDatapointWorker(maxBuffer int, batchSize int, errorHandler func(error) error, stats *asyncMultiTokenSinkStats) *datapointWorker {
	w := &datapointWorker{
		newWorker(maxBuffer, errorHandler),
		make(chan *dpMsg, maxBuffer),
		make([]*datapoint.Datapoint, 0, batchSize),
		batchSize,
		int64(0),
		stats,
	}
	w.newBuffer()
	return w
}

// worker for handling events
type eventWorker struct {
	*worker
	input        chan *evMsg // channel for inputing datapoints into a worker
	buffer       []*event.Event
	batchSize    int
	dataBuffered int64                     // atomic Int64 that maintains count of data in the sink
	stats        *asyncMultiTokenSinkStats // stats about
}

// emits a series of datapoints
func (w *eventWorker) emit(token string, events []*event.Event) {
	// set the token on the HTTPSink
	w.sink.AuthToken = token

	// emit datapoints and handle any errors
	err := w.sink.AddEvents(context.Background(), events)
	w.handleError(err, token, events)
	// account for the emitted datapoints
	_ = atomic.AddInt64(&w.dataBuffered, int64(len(events)*-1))
}

func (w *eventWorker) handleError(err error, token string, events []*event.Event) {
	if err != nil {
		_ = atomic.AddInt64(&w.stats.TotalEventsDropped, int64(len(events)))
		// TODO: improve httpSinks handle response to return the response code
		// parse the http status code from the error returned by the httpsink
		statusCode := "unknown"
		if w.invalidStatusRegexp.MatchString(err.Error()) {
			statusCode = w.statusCodeRegexp.FindStringSubmatch(err.Error())[0]
		}
		if counter, ok := w.stats.TotalEventsDroppedByToken.Load(token, statusCode); ok {
			_ = atomic.AddInt64(counter, int64(len(events)))
		} else {
			w.stats.TotalEventsDroppedByToken.Store(token, statusCode, int64(len(events)))
		}
		_ = w.errorHandler(err)
	} else {
		_ = atomic.AddInt64(&w.stats.TotalEventsEmitted, int64(len(events)))
	}
}

// processMsg is
func (w *eventWorker) processMsg(msg *evMsg) {
	for len(msg.data) > 0 {
		msgLength := len(msg.data)
		remainingBuffer := (w.batchSize - len(w.buffer))
		if msgLength > remainingBuffer {
			msgLength = remainingBuffer
		}
		w.buffer = append(w.buffer, msg.data[:msgLength]...)
		msg.data = msg.data[msgLength:]
		if len(w.buffer) >= w.batchSize {
			w.emit(msg.token, w.buffer)
			w.buffer = w.buffer[:0]
		}
	}
}

// bufferDatapoints is responsible for batching incoming datapoints into a buffer
func (w *eventWorker) bufferFunc(msg *evMsg) (stop bool) {
	var lastTokenSeen = msg.token
	w.processMsg(msg)
outer:
	for len(w.buffer) < w.batchSize {
		select {
		case msg = <-w.input:
			if msg.token != lastTokenSeen {
				// if the token changes, then emit what ever is in the buffer before proceeding
				w.emit(msg.token, w.buffer)
				w.buffer = w.buffer[:0]
				lastTokenSeen = msg.token
			}
			w.processMsg(msg)
		default:
			break outer // emit what ever is in the buffer if there are no more datapoints to read
		}
	}
	// emit the data in the buffer
	w.emit(msg.token, w.buffer)
	w.buffer = w.buffer[:0]
	return
}

// newBuffer buffers datapoints and events in the pipeline for the duration specified during Startup
func (w *eventWorker) newBuffer() {
	go func() {
		for {
			select {
			case <-w.closing: // check if the worker is in a closing state
				w.stopped.Set(true)
				return
			case msg := <-w.input:
				// process the Datapoint Message
				w.bufferFunc(msg)
			}
		}
	}()
}

// Stop will stop the pipeline
func (w *eventWorker) Stop(timeout time.Duration) error {
	timer := time.After(timeout)
	for {
		select {
		case <-timer: // timeout and generate error message indicating what may be dropped
			return fmt.Errorf("worker timed out while stopping")
		default:
			select {
			case <-w.closing:
				if w.stopped.Get() {
					return nil
				}
			default:
				close(w.closing)
			}
		}
	}
}

func newEventWorker(maxBuffer int, batchSize int, errorHandler func(error) error, stats *asyncMultiTokenSinkStats) *eventWorker {
	w := &eventWorker{
		newWorker(maxBuffer, errorHandler),
		make(chan *evMsg, maxBuffer),
		make([]*event.Event, 0, batchSize),
		batchSize,
		int64(0),
		stats,
	}
	w.newBuffer()
	return w
}

//asyncMultiTokenSinkStats - holds stats about the sink
type asyncMultiTokenSinkStats struct {
	TotalDatapointsDropped        int64
	TotalDatapointsDroppedByToken *atomicTokenStatusCounterMap
	TotalDatapointsEmitted        int64
	TotalDatapointsEmittedByToken *atomicTokenStatusCounterMap
	TotalEventsDropped            int64
	TotalEventsDroppedByToken     *atomicTokenStatusCounterMap
	TotalEventsEmitted            int64
	TotalEventsEmittedByToken     *atomicTokenStatusCounterMap
	BufferSize                    int64
	BatchSize                     int64
	NumberOfDatapointWorkers      int64
	NumberOfEventWorkers          int64
}

func newAsyncMultiTokenSinkStats() *asyncMultiTokenSinkStats {
	return &asyncMultiTokenSinkStats{
		TotalDatapointsDropped:        0,
		TotalDatapointsDroppedByToken: newAtomicTokenStatusCounterMap("total_datapoints_dropped_by_token"),
		TotalDatapointsEmitted:        0,
		TotalDatapointsEmittedByToken: newAtomicTokenStatusCounterMap("total_datapoints_emitted_by_token"),
		TotalEventsDropped:            0,
		TotalEventsDroppedByToken:     newAtomicTokenStatusCounterMap("total_events_dropped_by_token"),
		TotalEventsEmitted:            0,
		TotalEventsEmittedByToken:     newAtomicTokenStatusCounterMap("total_events_emitted_by_token"),
		BufferSize:                    0,
		BatchSize:                     0,
		NumberOfDatapointWorkers:      0,
		NumberOfEventWorkers:          0,
	}
}

// AsyncMultiTokenSink asynchronously sends datapoints for multiple tokens
type AsyncMultiTokenSink struct {
	ShutdownTimeout time.Duration             // ShutdownTimeout is how long the sink should wait before timing out after Close() is called
	errorHandler    func(error) error         // error handler is a handler for errors encountered while emitting metrics
	Hasher          hash.Hash32               // Hasher is used to hash access tokens to a worker
	lock            sync.RWMutex              // lock is a mutex preventing concurrent access to getWorker
	stopped         *atomicBool               // stopped is a boolean indicating if the sink has been stopped or not
	dpWorkers       []*datapointWorker        // dpWorkers is an array of workers used to emit datapoints asynchronously
	evWorkers       []*eventWorker            // evWorkers is an array of workers dedicated to sending events
	NewHTTPClient   func() http.Client        // function used to create an http client for the underlying sinks
	stats           *asyncMultiTokenSinkStats //stats are stats about that sink that can be collected from the Datapoitns() method
}

// Datapoints returns a set of datapoints about the sink
func (a *AsyncMultiTokenSink) Datapoints() []*datapoint.Datapoint {
	var dps = []*datapoint.Datapoint{
		Cumulative("total_datapoints_emitted", nil, atomic.LoadInt64(&a.stats.TotalDatapointsEmitted)),
		Cumulative("total_datapoints_dropped", nil, atomic.LoadInt64(&a.stats.TotalDatapointsDropped)),
		Cumulative("total_events_emitted", nil, atomic.LoadInt64(&a.stats.TotalEventsEmitted)),
		Cumulative("total_events_dropped", nil, atomic.LoadInt64(&a.stats.TotalEventsDropped)),
		Gauge("configured_buffer_size", nil, atomic.LoadInt64(&a.stats.BufferSize)),
		Gauge("configured_batch_size", nil, atomic.LoadInt64(&a.stats.BatchSize)),
		Gauge("configured_number_of_datapoint_workers", nil, atomic.LoadInt64(&a.stats.NumberOfDatapointWorkers)),
		Gauge("configured_number_of_event_workers", nil, atomic.LoadInt64(&a.stats.NumberOfEventWorkers)),
	}
	dps = append(dps, a.stats.TotalDatapointsEmittedByToken.FetchDatapoints()...)
	dps = append(dps, a.stats.TotalDatapointsDroppedByToken.FetchDatapoints()...)
	dps = append(dps, a.stats.TotalEventsEmittedByToken.FetchDatapoints()...)
	dps = append(dps, a.stats.TotalEventsDroppedByToken.FetchDatapoints()...)
	return dps
}

// getWorker hashes the string to one of the workers and returns the integer position of the worker
func (a *AsyncMultiTokenSink) getWorker(input string, size int) (workerID int64, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.Hasher != nil {
		a.Hasher.Reset()
		_, _ = a.Hasher.Write([]byte(input))
		if size > 0 {
			workerID = int64(a.Hasher.Sum32()) % int64(size)
		} else {
			err = fmt.Errorf("no available workers")
		}
	} else {
		err = fmt.Errorf("hasher is nil")
	}
	return
}

// AddDatapointsWithToken emits a list of datapoints using a supplied token
func (a *AsyncMultiTokenSink) AddDatapointsWithToken(token string, datapoints []*datapoint.Datapoint) (err error) {
	var workerID int64
	if workerID, err = a.getWorker(token, len(a.dpWorkers)); err == nil {
		var worker = a.dpWorkers[workerID]
		_ = atomic.AddInt64(&worker.dataBuffered, int64(len(datapoints)))
		var m = &dpMsg{
			token: token,
			data:  datapoints,
		}
		select {
		case <-worker.closing: // check if the worker is closing and return if so
			err = fmt.Errorf("unable to add datapoints: the worker has been stopped")
		default:
			select {
			case worker.input <- m:
			default:
				err = fmt.Errorf("unable to add datapoints: the input buffer is full")
			}
		}
	} else {
		err = fmt.Errorf("unable to add datapoints: there was an error while hashing the token to a worker. %v", err)
	}

	return
}

// AddDatapoints add datepoints to the multitoken sync using a context that has the TokenCtxKey
func (a *AsyncMultiTokenSink) AddDatapoints(ctx context.Context, datapoints []*datapoint.Datapoint) (err error) {
	if token := ctx.Value(TokenCtxKey); token != nil {
		err = a.AddDatapointsWithToken(token.(string), datapoints)
	} else {
		err = fmt.Errorf("no value was found on the context with key '%s'", TokenCtxKey)
	}
	return
}

// AddEventsWithToken emits a list of events using a supplied token
func (a *AsyncMultiTokenSink) AddEventsWithToken(token string, events []*event.Event) (err error) {
	var workerID int64
	if workerID, err = a.getWorker(token, len(a.evWorkers)); err == nil {
		var worker = a.evWorkers[workerID]
		_ = atomic.AddInt64(&worker.dataBuffered, int64(len(events)))
		var m = &evMsg{
			token: token,
			data:  events,
		}
		select {
		case <-worker.closing:
			err = fmt.Errorf("unable to add events: the worker has been stopped")
		default:
			select {
			case worker.input <- m:
			default:
				err = fmt.Errorf("unable to add events: the input buffer is full")
			}
		}

	} else {
		err = fmt.Errorf("unable to add events: there was an error while hashing the token to a worker. %v", err)
	}

	return
}

// AddEvents add datepoints to the multitoken sync using a context that has the TokenCtxKey
func (a *AsyncMultiTokenSink) AddEvents(ctx context.Context, events []*event.Event) (err error) {
	if token := ctx.Value(TokenCtxKey); token != nil {
		err = a.AddEventsWithToken(token.(string), events)
	} else {
		err = fmt.Errorf("no value was found on the context with key '%s'", TokenCtxKey)
	}
	return
}

// Startup starts up the sink with the specified number of workers time buffer.
// If the bufferDuration is 0, then a time buffer will not be included in the worker pipeline.
func (a *AsyncMultiTokenSink) Startup(numWorkers int64, buffer int, batchSize int, DatapointEndpoint string, EventEndpoint string, userAgent string) (err error) {
	if a.stopped.Get() {
		a.dpWorkers = make([]*datapointWorker, numWorkers)
		a.evWorkers = make([]*eventWorker, numWorkers)
		for i := int64(0); i < numWorkers; i++ {
			dpWorker := newDatapointWorker(buffer, batchSize, a.errorHandler, a.stats)
			if DatapointEndpoint != "" {
				dpWorker.sink.DatapointEndpoint = DatapointEndpoint
			}
			evWorker := newEventWorker(buffer, batchSize, a.errorHandler, a.stats)
			if EventEndpoint != "" {
				evWorker.sink.EventEndpoint = EventEndpoint
			}
			if userAgent != "" {
				dpWorker.sink.UserAgent = userAgent
				evWorker.sink.UserAgent = userAgent
			}
			if a.NewHTTPClient != nil {
				dpWorker.sink.Client = a.NewHTTPClient()
				evWorker.sink.Client = a.NewHTTPClient()
			}
			a.dpWorkers[i] = dpWorker
			a.evWorkers[i] = evWorker
		}
		_ = atomic.SwapInt64(&a.stats.BufferSize, int64(buffer))
		_ = atomic.SwapInt64(&a.stats.BatchSize, int64(batchSize))
		_ = atomic.SwapInt64(&a.stats.NumberOfDatapointWorkers, numWorkers)
		_ = atomic.SwapInt64(&a.stats.NumberOfEventWorkers, numWorkers)
		a.stopped.Set(false)
	} else {
		err = fmt.Errorf("unable to start up the sink because it has already started")
	}
	return
}

// Close stops the existing workers and prevents additional datapoints from being added
// if a ShutdownTimeout is set on the sink, it will be used as a timeout for closing the sink
// the default timeout is 5 seconds
func (a *AsyncMultiTokenSink) Close() (err error) {
	var datapointsDropped = int64(0)
	var eventsDropped = int64(0)
	if !a.stopped.Get() {
		// stop the sink
		a.stopped.Set(true)
		// send stop message to each worker and wait for done statement
		var wg sync.WaitGroup
		wg.Add(len(a.evWorkers) + len(a.dpWorkers))
		for _, worker := range a.dpWorkers {
			go func(timeout time.Duration, worker *datapointWorker) {
				defer wg.Done()
				if stopErr := worker.Stop(timeout); stopErr == nil {
					_ = atomic.AddInt64(&a.stats.NumberOfDatapointWorkers, -1)
				} else {
					_ = atomic.AddInt64(&datapointsDropped, atomic.LoadInt64(&worker.dataBuffered))
				}
			}(a.ShutdownTimeout, worker)
		}
		for _, worker := range a.evWorkers {
			go func(timeout time.Duration, worker *eventWorker) {
				defer wg.Done()
				if stopErr := worker.Stop(timeout); stopErr == nil {
					_ = atomic.AddInt64(&a.stats.NumberOfEventWorkers, -1)
				} else {
					_ = atomic.AddInt64(&eventsDropped, atomic.LoadInt64(&worker.dataBuffered))
				}
			}(a.ShutdownTimeout, worker)
		}
		wg.Wait()
		if atomic.LoadInt64(&a.stats.NumberOfDatapointWorkers) > 0 || atomic.LoadInt64(&a.stats.NumberOfEventWorkers) > 0 || datapointsDropped > 0 || eventsDropped > 0 {
			err = fmt.Errorf("some workers (%d) timedout while stopping the sink approximately %d datapoints and %d events may have been dropped",
				(atomic.LoadInt64(&a.stats.NumberOfDatapointWorkers) + atomic.LoadInt64(&a.stats.NumberOfEventWorkers)), datapointsDropped, eventsDropped)
		}
	} else {
		err = fmt.Errorf("unable to stop the sink because it has already stopped")
	}
	return
}

// newDefaultHTTPClient returns a default http client for the sink
func newDefaultHTTPClient() http.Client {
	return http.Client{
		Timeout: DefaultTimeout,
	}
}

// NewAsyncMultiTokenSink returns a sink that asynchronously emits datapoints with different tokens
func NewAsyncMultiTokenSink() *AsyncMultiTokenSink {
	a := &AsyncMultiTokenSink{
		ShutdownTimeout: time.Second * 5,
		errorHandler:    DefaultErrorHandler,
		dpWorkers:       []*datapointWorker{},
		evWorkers:       []*eventWorker{},
		Hasher:          fnv.New32(),
		stopped:         newAtomicBool(),
		lock:            sync.RWMutex{},
		NewHTTPClient:   newDefaultHTTPClient,
		stats:           newAsyncMultiTokenSinkStats(),
	}
	a.stopped.Set(true)
	return a
}
