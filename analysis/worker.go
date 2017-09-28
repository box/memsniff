package analysis

import (
	"errors"
	"github.com/box/memsniff/analysis/aggregate"
	"github.com/box/memsniff/protocol/model"
	"sync"
)

var (
	aggregatorPool = sync.Pool{}
)

// worker accumulates usage data for a set of cache keys.
type worker struct {
	// channel for reports of cache key activity
	eventChan chan []model.Event
	// channel for requests for the current data summary
	resRequest chan struct{}
	// channel for data summaries
	resReply chan result
	// channel for requests to reset all data t to an empty state
	resetRequest chan bool

	// create KeyAggregators based on the configured format
	aggregatorFactory aggregate.KeyAggregatorFactory
	// one KeyAggregator per key, where key is determined by aggregatorFactory
	aggregators       map[string]aggregate.KeyAggregator
}

// errQueueFull is returned by handleGetResponse if the worker cannot keep
// up with incoming calls.
var errQueueFull = errors.New("analysis worker queue full")

func newWorker(kaf aggregate.KeyAggregatorFactory) worker {
	w := worker{
		eventChan:    make(chan []model.Event, 1024),
		resRequest:   make(chan struct{}),
		resReply:     make(chan result),
		resetRequest: make(chan bool),

		aggregatorFactory: kaf,
		aggregators:       make(map[string]aggregate.KeyAggregator),
	}
	go w.loop()
	return w
}

// handleEvents asynchronously processes events.
// handleEvents is threadsafe.
// evts should not be modified by the caller.
func (w *worker) handleEvents(evts []model.Event) error {
	// Make sure we copy r.Key before we return, since it may be a pointer
	// into a buffer that will be overwritten.
	select {
	case w.eventChan <- evts:
		return nil
	default:
		return errQueueFull
	}
}

// result returns a data summary of all keys tracked by this worker.
// result is threadsafe.
func (w *worker) result() result {
	w.resRequest <- struct{}{}
	return <-w.resReply
}

// reset clears all key data tracked by this worker.
// Some data may be lost if there is no synchronization with calls
// to result and handleEvents.
func (w *worker) reset() {
	w.resetRequest <- true
}

// close exits this worker. Calls to handleEvents after calling close
// will panic.
func (w *worker) close() {
	close(w.eventChan)
}

func (w *worker) loop() {
	for {
		select {
		case events, ok := <-w.eventChan:
			if !ok {
				return
			}
			for _, evt := range events {
				w.handleEvent(evt)
			}

		case <-w.resRequest:
			w.resReply <- w.assembleResults()

		case <-w.resetRequest:
			w.resetAggregators()
		}
	}
}

func (w *worker) resetAggregators() {
	for key, ka := range w.aggregators {
		delete(w.aggregators, key)
		ka.Reset()
		aggregatorPool.Put(&ka)
	}
}

func (w *worker) handleEvent(evt model.Event) {
	mapKey := w.aggregatorFactory.FlatKey(evt)
	ka, ok := w.aggregators[mapKey]
	if !ok {
		// Need to create an aggregator for this key.
		// First try to reuse an aggregator from the pool.
		if fromPool := aggregatorPool.Get(); fromPool != nil {
			ka = *fromPool.(*aggregate.KeyAggregator)
		} else {
			agg := w.aggregatorFactory.New()
			ka = agg
		}

		ka.Key = w.aggregatorFactory.Key(evt)
		w.aggregators[mapKey] = ka
	}

	ka.Add(evt)
}

type result struct {
	// keyFields[x] is the list of values used as keys for a set of aggregates.
	keyFields  [][]string
	// aggResults[x] is the aggregate results for keyFields[x], in format-determined order.
	aggResults [][]int64
}

func (w *worker) assembleResults() (res result) {
	res.keyFields = make([][]string, len(w.aggregators))
	res.aggResults = make([][]int64, len(w.aggregators))
	var i int
	for _, ka := range w.aggregators {
		res.keyFields[i] = ka.Key
		res.aggResults[i] = ka.Result()
		i++
	}
	return
}
