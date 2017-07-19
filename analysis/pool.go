// Package analysis implements accumulation of individual operations into
// summary statistics.
package analysis

import (
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol"
	"hash/fnv"
)

// Pool tracks cache activity by hashing inputs to fixed workers.  The number
// of workers is determined when the Pool is created.  The implementation
// prioritizes responsiveness over consistency, and data is dropped if the
// rate of input is too high to be handled by the Pool.
type Pool struct {
	// A Logger instance for debugging.  No logging is done if nil.
	Logger     log.Logger
	reportSize int
	workers    []worker
	filter     filter
	stats      Stats
}

// Stats contains performance metrics for a Pool.
type Stats struct {
	// number of responses sent to HandleGetResponses that were recorded
	ResponsesHandled int
	// number of responses sent to HandleGetResponses that were discarded
	ResponsesDropped int
}

// New returns a new Pool.
//
// numWorkers determines the number of workers to hotlists to create.  More
// workers gives more potential parallelism and performance, but increased
// memory consumption.
//
// reportSize determines the number of entries returned from Report.
func New(numWorkers, reportSize int) *Pool {
	c := &Pool{
		reportSize: reportSize,
		workers:    make([]worker, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		c.workers[i] = newWorker()
	}

	return c
}

// HandleGetResponses adds records for a set of cache GET operation to the Pool.
//
// The GetResponses will be dispatched to its assigned worker.  If the worker
// is overloaded, all inputs for that worker  will be discarded and statistics
// for this Pool updated to reflect the lost data.
//
// HandleGetResponse is threadsafe.
func (p *Pool) HandleGetResponses(rs []*protocol.GetResponse) {
	perWorkerResponses := p.partitionResponses(p.filter.filterResponses(rs))
	for i, responses := range perWorkerResponses {
		if len(responses) > 0 {
			err := p.workers[i].handleGetResponses(responses)
			if err == errQueueFull {
				p.stats.ResponsesDropped += len(responses)
				continue
			}
			p.stats.ResponsesHandled += len(responses)
		}
	}
}

func (p *Pool) partitionResponses(rs []*protocol.GetResponse) [][]*protocol.GetResponse {
	perWorkerResponses := make([][]*protocol.GetResponse, len(p.workers))
	for _, r := range rs {
		slot := p.keySlot(r.Key)
		perWorkerResponses[slot] = append(perWorkerResponses[slot], r)
	}
	return perWorkerResponses
}

// SetFilterPattern sets an RE2 pattern for future data points.  Only operations
// on cache keys matching pattern will have statistics collected.  Setting a
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

func (p *Pool) getWorker(key []byte) worker {
	return p.workers[p.keySlot(key)]
}

func (p *Pool) keySlot(key []byte) int {
	hash := fnv.New64a()
	// writing to a Hash can never fail
	_, _ = hash.Write(key)
	h := hash.Sum64() % uint64(len(p.workers))
	return int(h)
}
