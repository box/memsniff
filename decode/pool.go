// Package decode provides multithreaded TCP packet decoding.
package decode

import (
	"github.com/box/memsniff/capture"
	"github.com/box/memsniff/log"
	"github.com/google/gopacket/pcap"
	"io"
)

type workerQueue chan *worker

// Stats contains runtime performance statistics for a Pool.
type Stats struct {
	PacketsCaptured int
	PacketsDropped  int
}

// Pool is a set of workers for decoding network packets.  It is bound to a
// single PacketSource. A Pool operates on a pull model, only requesting packet
// data when there is a worker ready to receive it.
type Pool struct {
	logger  log.Logger
	src     capture.PacketSource
	handler Handler

	readyQ     workerQueue
	numWorkers int
	stats      Stats
}

// NewPool creates a new Pool of workers.  As packets are captured and decoded,
// handler is invoked.  handler is invoked from multiple worker gorountines
// concurrently and thus must be threadsafe.
func NewPool(logger log.Logger, src capture.PacketSource, handler Handler) *Pool {
	return &Pool{
		logger:  logger,
		src:     src,
		handler: handler,
		readyQ:  make(workerQueue, 8),
	}
}

// Run starts the Pool decoding packets from the configured PacketSource and
// sending the results to the PacketHandler.
func (p *Pool) Run() {
	for {
		select {
		case nextWorker := <-p.readyQ:
			err := p.sendToWorker(nextWorker)
			if err == io.EOF {
				p.logger.Log("Reached EOF, waiting for workers to finish")
				nextWorker.close()
				for i := 1; i < p.numWorkers; i++ {
					(<-p.readyQ).close()
				}
				p.logger.Log("Decoder exiting")
				return
			}

		default:
			p.startWorker()
		}
	}
}

// Stats returns runtime statistics for a Pool.
func (p *Pool) Stats() Stats {
	return p.stats
}

func (p *Pool) sendToWorker(w *worker) error {
	var err error
	for {
		// write packet data directly into the worker's working area
		// to avoid an extra copy
		err = p.src.CollectPackets(w.buf())
		if err != pcap.NextErrorTimeoutExpired {
			p.stats.PacketsCaptured += w.buf().PacketLen()
			break
		}
	}
	if err == io.EOF {
		return err
	}
	if err != nil {
		p.logger.Log("Error from CollectPackets", err)
		return err
	}
	w.work()
	return nil
}

// startWorker creates a background Worker that will send itself to q when it
// is ready for a new batch of packets.  This Worker can then be given work
// or closed, which will clean up the goroutine.
//
// handler will be invoked on the Worker's background goroutine.
func (p *Pool) startWorker() {
	d := newDecoder(p.logger, p.handler)
	w := worker{
		id:          p.numWorkers,
		workerQueue: p.readyQ,
		pb:          capture.NewPacketBuffer(batchSize, 8*1024*1024),
		workReady:   make(chan struct{}, 1),
		handler:     d.decodeBatch,
	}
	p.numWorkers++

	p.logger.Log("starting decode worker", w.id)
	go w.loop()
}
