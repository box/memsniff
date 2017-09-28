// Package analysis implements accumulation of individual operations into
// summary statistics.
package analysis

import (
	"github.com/box/memsniff/analysis/aggregate"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
	"hash/fnv"
	"sync/atomic"
)

// Pool tracks datastore activity by hashing inputs to fixed workers.
// The number of workers is determined when the Pool is created.
// The implementation prioritizes responsiveness over consistency,
// and data is dropped if the rate of input is too high to be handled
// by the Pool.
type Pool struct {
	// A Logger instance for debugging.  No logging is done if nil.
	Logger  log.Logger
	workers []worker
	filter  filter
	stats   Stats

	kaf aggregate.KeyAggregatorFactory
}

// Stats contains performance metrics for a Pool.
type Stats struct {
	// number of events sent to HandleEvents that were recorded
	EventsHandled int64
	// number of events sent to HandleEvents that were discarded
	EventsDropped int64
}

func (s *Stats) addHandled(n int) {
	atomic.AddInt64(&s.EventsHandled, int64(n))
}

func (s *Stats) addDropped(n int) {
	atomic.AddInt64(&s.EventsDropped, int64(n))

}

// New returns a new Pool.
//
// numWorkers determines the number of workers to hotlists to create.  More
// workers gives more potential parallelism and performance, but increased
// memory consumption.
//
// reportSize determines the number of entries returned from Report.
func New(numWorkers int, format string) (*Pool, error) {
	kaf, err := aggregate.NewKeyAggregatorFactory(format)
	if err != nil {
		return nil, err
	}
	p := &Pool{
		kaf:     kaf,
		workers: make([]worker, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		p.workers[i] = newWorker(kaf)
	}

	return p, nil
}

// HandleEvents adds records for a set of datastore operations to the Pool.
//
// The events will be dispatched to their assigned workers.  If a worker
// is overloaded, all inputs for that worker  will be discarded and statistics
// for this Pool updated to reflect the lost data.
//
// HandleEvents is threadsafe.
func (p *Pool) HandleEvents(evts []model.Event) {
	perWorkerEvents := p.partitionEvents(p.filter.filterEvents(evts))
	for i, events := range perWorkerEvents {
		if len(events) > 0 {
			err := p.workers[i].handleEvents(events)
			if err == errQueueFull {
				p.stats.addDropped(len(events))
				continue
			}
			p.stats.addHandled(len(events))
		}
	}
}

func (p *Pool) partitionEvents(evts []model.Event) [][]model.Event {
	perWorkerEvents := make([][]model.Event, len(p.workers))
	for _, e := range evts {
		slot := p.keySlot(e.Key)
		perWorkerEvents[slot] = append(perWorkerEvents[slot], e)
	}
	return perWorkerEvents
}

// SetFilterPattern sets an RE2 pattern for future data points.  Only operations
// on keys matching pattern will have statistics collected.  Setting a
// new filter invalidates existing results, so current statistics are cleared
// before returning.  If pattern is the empty string statistics are collected
// for all keys.
func (p *Pool) SetFilterPattern(pattern string) error {
	err := p.filter.setPattern(pattern)
	if err != nil {
		return err
	}
	p.Reset()
	return nil
}

// Reset clears all recorded activity from this Pool.  This operation is
// asynchronous, and may still be in progress when Reset returns.  New data
// added by calling HandleGetResponse after Reset returns may be lost, and
// results from Report immediately after a call to Reset may still contain some
// information recorded before the call to Reset.
func (p *Pool) Reset() {
	for _, w := range p.workers {
		w.reset()
	}
}

// Stats returns a record of total activity reported to this Pool, including
// input that was dropped due to not keeping up.
func (p *Pool) Stats() Stats {
	return p.stats
}

func (p *Pool) keySlot(key string) int {
	hash := fnv.New64a()
	// writing to a Hash can never fail
	_, _ = hash.Write([]byte(key))
	h := hash.Sum64() % uint64(len(p.workers))
	return int(h)
}
