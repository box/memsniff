package assembly

import (
	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/decode"
	"github.com/box/memsniff/log"
)

// Pool manages a set of workers each responsible for a set of TCP conversations (stream pairs).
type Pool struct {
	Logger  log.Logger
	workers []worker
}

// New creates a new pool for reassembling TCP streams.
func New(logger log.Logger, analysis *analysis.Pool, memcachePorts []int, numWorkers int) *Pool {
	p := &Pool{
		logger,
		make([]worker, numWorkers),
	}
	for i := 0; i < numWorkers; i++ {
		p.workers[i] = newWorker(logger, analysis, memcachePorts)
	}
	return p
}

// HandlePackets partitions packets by connection and dispatches them to assembly workers.
func (p *Pool) HandlePackets(dps []*decode.DecodedPacket) (err error) {
	perWorker := p.partition(dps)
	doneCh := make(chan struct{}, len(p.workers))
	var batchesSent int
	for i, packets := range perWorker {
		if len(packets) > 0 {
			batchesSent++
			err = p.workers[i].handlePackets(packets, doneCh)
			if err != nil {
				p.Logger.Log(err)
			}
		}
	}
	for i := 0; i < batchesSent; i++ {
		<-doneCh
	}
	return nil
}

func (p *Pool) partition(dps []*decode.DecodedPacket) [][]*decode.DecodedPacket {
	perWorker := make([][]*decode.DecodedPacket, len(p.workers))
	for _, dp := range dps {
		s := p.slot(dp)
		perWorker[s] = append(perWorker[s], dp)
	}
	return perWorker
}

func (p *Pool) slot(dp *decode.DecodedPacket) int {
	return int(dp.FlowHash % uint64(len(p.workers)))
}
