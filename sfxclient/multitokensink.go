package sfxclient

import (
	"fmt"
	"hash"
	"hash/fnv"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"golang.org/x/net/context"
)

// ContextKey is a custom key type for context values
type ContextKey string

const (
	// TokenCtxKey is a context key for tokens
	TokenCtxKey ContextKey = TokenHeaderName
)

type msg struct {
	token string
	data  interface{}
}

// stopPMsg is a "poison pill" used to stop the pipeline
var stopMsg = &msg{
	token: "STOPITNOW",
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
	lock         *sync.Mutex       // lock to control concurrent access to the worker
	input        chan *msg         // channel for inputing datapoints into a worker
	maxBuffer    int               // maxBuffer is the maximum size of the buffer for data
	errorHandler func(error) error // error handler for handling error emitting datapoints
	dataBuffered int64             // atomic Int64 that maintains count of data in the sink
	sink         *HTTPSink         // sink is an HTTPSink for emitting datapoints to Signal Fx
	done         chan bool         // done is a boolean channel for signaling that the worker has stopped
}

// emits a series of datapoints
func (w *worker) emit(token string, datapoints []*datapoint.Datapoint, events []*event.Event) {
	// set the token on the HTTPSink
	w.sink.AuthToken = token

	if len(datapoints) > 0 {
		// emit datapoints and handle any errors
		if err := w.sink.AddDatapoints(context.Background(), datapoints); err != nil {
			_ = w.errorHandler(err)
		}
		// account for the emitted datapoints
		_ = atomic.AddInt64(&w.dataBuffered, int64(len(datapoints)*-1))
	}
	if len(events) > 0 {
		// emit datapoints and handle any errors
		if err := w.sink.AddEvents(context.Background(), events); err != nil {
			_ = w.errorHandler(err)
		}

		// account for the emitted events
		_ = atomic.AddInt64(&w.dataBuffered, int64(len(events)*-1))
	}
}

// Stop will stop the pipeline
func (w *worker) Stop(timeout time.Duration) bool {
	for {
		select {
		case <-time.After(timeout): // timeout and generate error message indicating what may be dropped
			return false
		case w.input <- stopMsg:
			// stop datapoint emission
		case <-w.done:
			defer close(w.done)
			return true
		}
	}
}

// returns a new instance of worker with an configured emission pipeline
func newWorker(maxBuffer int, errorHandler func(error) error) *worker {
	w := &worker{
		lock:         &sync.Mutex{},
		sink:         NewHTTPSink(),
		maxBuffer:    maxBuffer,
		errorHandler: errorHandler,
		done:         make(chan bool),
	}

	return w
}

// worker for handling datapoints
type datapointWorker struct {
	*worker
	buffer []*datapoint.Datapoint
}

// processDatapointMsg buffers datapoints attached to datapoint pipeline messages and emits them when necessary
func (w *datapointWorker) processMsg(msg *msg) (stop bool) {
	if msg == stopMsg {
		stop = true
	} else {
		var remainingBuffer int
		var msgLength int
		var data = msg.data.([]*datapoint.Datapoint) // cast msg data to an array of points (avoiding reflection)
		// iterate over the data in the message until it's empty, breaking it up into batches
		for len(data) > 0 {
			remainingBuffer = (w.maxBuffer - len(w.buffer))
			msgLength = len(data)
			if msgLength >= w.maxBuffer && remainingBuffer >= w.maxBuffer {
				w.emit(msg.token, data[:w.maxBuffer], nil)
				data = data[w.maxBuffer:]
			} else if msgLength > remainingBuffer {
				w.buffer = append(w.buffer, data[:remainingBuffer]...)
				data = data[remainingBuffer:]
			} else if remainingBuffer >= msgLength {
				w.buffer = append(w.buffer, data[:msgLength]...)
				data = data[msgLength:]
			}
			if len(w.buffer) >= w.maxBuffer {
				// if the buffer is full emit the data
				w.emit(msg.token, w.buffer, nil)
				w.buffer = w.buffer[:0] // reset the buffer
			}
		}
	}
	return
}

// bufferDatapoints is responsible for batching incoming datapoints into a buffer
func (w *datapointWorker) bufferFunc(msg *msg) (stop bool) {
	var lastTokenSeen = msg.token
	_ = w.processMsg(msg)
outer:
	for len(w.buffer) < w.maxBuffer {
		select {
		case msg = <-w.input:
			if msg.token != lastTokenSeen {
				// if the token changes, then emit what ever is in the buffer before proceeding
				w.emit(msg.token, w.buffer, nil)
				w.buffer = w.buffer[:0]
				lastTokenSeen = msg.token
			}
			if stop = w.processMsg(msg); stop { // will continually read and buffer until there are no more datapoints to read
				break outer // emit what ever is in the buffer and return true to stop
			}
		default:
			break outer // emit what ever is in the buffer if there are no more datapoints to read
		}
	}
	// emit the data in the buffer
	w.emit(msg.token, w.buffer, nil)
	w.buffer = w.buffer[:0]
	return
}

// newBuffer buffers datapoints and events in the pipeline for the duration specified during Startup
func (w *datapointWorker) newBuffer() (chan *msg, chan bool) {
	w.input = make(chan *msg)

	go func() {
		var stop bool
		for msg := range w.input {
			if msg == stopMsg {
				stop = true
			} else {
				// process the Datapoint Message
				stop = w.bufferFunc(msg)
			}
			if stop {
				w.done <- true
				return
			}
		}
	}()
	return w.input, w.done
}

func newDatapointWorker(maxBuffer int, errorHandler func(error) error) *datapointWorker {
	w := &datapointWorker{
		newWorker(maxBuffer, errorHandler),
		make([]*datapoint.Datapoint, 0, maxBuffer),
	}
	_, _ = w.newBuffer()
	return w
}

// worker for handling events
type eventWorker struct {
	*worker
	buffer []*event.Event
}

// processDatapointMsg buffers datapoints attached to datapoint pipeline messages and emits them when necessary
func (w *eventWorker) processMsg(msg *msg) (stop bool) {
	if msg == stopMsg {
		stop = true
	} else {
		var remainingBuffer int
		var msgLength int
		var data = msg.data.([]*event.Event) // cast msg data to an array of points (avoiding reflection)
		// iterate over the data in the message until it's empty, breaking it up into batches
		for len(data) > 0 {
			remainingBuffer = (w.maxBuffer - len(w.buffer))
			msgLength = len(data)
			if msgLength >= w.maxBuffer && remainingBuffer >= w.maxBuffer {
				w.emit(msg.token, nil, data[:w.maxBuffer])
				data = data[w.maxBuffer:]
			} else if msgLength > remainingBuffer {
				w.buffer = append(w.buffer, data[:remainingBuffer]...)
				data = data[remainingBuffer:]
			} else if remainingBuffer >= msgLength {
				w.buffer = append(w.buffer, data[:msgLength]...)
				data = data[msgLength:]
			}
			if len(w.buffer) >= w.maxBuffer {
				// if the buffer is full emit the data
				w.emit(msg.token, nil, w.buffer)
				w.buffer = w.buffer[:0] // reset the buffer
			}
		}
	}
	return
}

// bufferDatapoints is responsible for batching incoming datapoints into a buffer
func (w *eventWorker) bufferFunc(msg *msg) (stop bool) {
	var lastTokenSeen = msg.token
	_ = w.processMsg(msg)
outer:
	for len(w.buffer) < w.maxBuffer {
		select {
		case msg = <-w.input:
			if msg.token != lastTokenSeen {
				// if the token changes, then emit what ever is in the buffer before proceeding
				w.emit(msg.token, nil, w.buffer)
				w.buffer = w.buffer[:0]
				lastTokenSeen = msg.token
			}
			if stop = w.processMsg(msg); stop { // will continually read and buffer until there are no more datapoints to read
				break outer // emit what ever is in the buffer and return true to stop
			}
		default:
			break outer // emit what ever is in the buffer if there are no more datapoints to read
		}
	}
	// emit the data in the buffer
	w.emit(msg.token, nil, w.buffer)
	w.buffer = w.buffer[:0]
	return
}

// newBuffer buffers datapoints and events in the pipeline for the duration specified during Startup
func (w *eventWorker) newBuffer() (chan *msg, chan bool) {
	w.input = make(chan *msg)

	go func() {
		var stop bool
		for msg := range w.input {
			if msg == stopMsg {
				stop = true
			} else {
				// process the Datapoint Message
				stop = w.bufferFunc(msg)
			}
			if stop {
				w.done <- true
				return
			}
		}
	}()
	return w.input, w.done
}

func newEventWorker(maxBuffer int, errorHandler func(error) error) *eventWorker {
	w := &eventWorker{
		newWorker(maxBuffer, errorHandler),
		make([]*event.Event, 0, maxBuffer),
	}

	_, _ = w.newBuffer()
	return w
}

// AsyncMultiTokenSink asynchronously sends datapoints for multiple tokens
type AsyncMultiTokenSink struct {
	ShutdownTimeout time.Duration      // ShutdownTimeout is how long the sink should wait before timing out after Close() is called
	errorHandler    func(error) error  // error handler is a handler for errors encountered while emitting metrics
	Hasher          hash.Hash32        // Hasher is used to hash access tokens to a worker
	numOfWorkers    int64              // numOfWorkers maintains a count of the number of workers in the sink
	lock            sync.RWMutex       // lock is a mutex preventing concurrent access to getWorker
	stopped         *atomicBool        // stopped is a boolean indicating if the sink has been stopped or not
	dpWorkers       []*datapointWorker // dpWorkers is an array of workers used to emit datapoints asynchronously
	evWorkers       []*eventWorker     // evWorkers is an array of workers dedicated to sending events
	NewHTTPClient   func() http.Client // function used to create an http client for the underlying sinks
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
		var m = &msg{
			token: token,
			data:  datapoints,
		}
		for {
			select {
			case <-worker.done: // check if worker is done and return if so
				err = fmt.Errorf("unable to add datapoints: the worker has been stopped")
				return
			case worker.input <- m:
				return
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
		var m = &msg{
			token: token,
			data:  events,
		}
		for {
			select {
			case <-worker.done: // check if worker is done and return if so
				err = fmt.Errorf("unable to add events: the worker has been stopped")
				return
			case worker.input <- m:
				return
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
func (a *AsyncMultiTokenSink) Startup(numWorkers int64, buffer int, DatapointEndpoint string, EventEndpoint string, userAgent string) (err error) {
	if a.stopped.Get() {
		atomic.AddInt64(&a.numOfWorkers, (numWorkers * 2))
		a.dpWorkers = make([]*datapointWorker, numWorkers)
		a.evWorkers = make([]*eventWorker, numWorkers)
		for i := int64(0); i < numWorkers; i++ {
			dpWorker := newDatapointWorker(buffer, a.errorHandler)
			if DatapointEndpoint != "" {
				dpWorker.sink.DatapointEndpoint = DatapointEndpoint
			}
			evWorker := newEventWorker(buffer, a.errorHandler)
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
				if worker.Stop(timeout) {
					_ = atomic.AddInt64(&a.numOfWorkers, -1)
				} else {
					_ = atomic.AddInt64(&datapointsDropped, atomic.LoadInt64(&worker.dataBuffered))
				}
			}(a.ShutdownTimeout, worker)
		}
		for _, worker := range a.evWorkers {
			go func(timeout time.Duration, worker *eventWorker) {
				defer wg.Done()
				if worker.Stop(timeout) {
					_ = atomic.AddInt64(&a.numOfWorkers, -1)
				} else {
					_ = atomic.AddInt64(&eventsDropped, atomic.LoadInt64(&worker.dataBuffered))
				}
			}(a.ShutdownTimeout, worker)
		}
		wg.Wait()
		if atomic.LoadInt64(&a.numOfWorkers) > 0 || (datapointsDropped > 0 || eventsDropped > 0) {
			err = fmt.Errorf("some workers (%d) timedout while stopping the sink approximately %d datapoints and %d events may have been dropped", atomic.LoadInt64(&a.numOfWorkers), datapointsDropped, eventsDropped)
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
		numOfWorkers:    0,
		Hasher:          fnv.New32(),
		stopped:         newAtomicBool(),
		lock:            sync.RWMutex{},
		NewHTTPClient:   newDefaultHTTPClient,
	}
	a.stopped.Set(true)
	return a
}
