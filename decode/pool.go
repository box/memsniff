// Package decode provides multithreaded TCP packet decoding.
package decode

import (
	"github.com/box/memsniff/capture"
	"github.com/box/memsniff/log"
	"github.com/google/gopacket/pcap"
	"io"
	"time"
)

type workerQueue chan *worker

// Stats contains runtime performance statistics for a Pool.
type Stats struct {
	PacketsDropped int
}

// Pool is a set of workers for decoding network packets.  It is bound to a
// single PacketSource. A Pool operates on a pull model, only requesting packet
// data when there is a worker ready to receive it.
type Pool struct {
	logger     log.Logger
	numWorkers int
	src        capture.PacketSource
	readyQ     workerQueue
	stats      Stats
}

// NewPool creates a new Pool of workers.  As packets are captured and decoded,
// handler is invoked.  handler is invoked from multiple worker gorountines
// concurrently and thus must be threadsafe.
func NewPool(logger log.Logger, snapLen int, numWorkers int, src capture.PacketSource, handler Handler) *Pool {
	p := &Pool{
		logger:     logger,
		numWorkers: numWorkers,
		src:        src,
		readyQ:     make(workerQueue, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		decoder := newDecoder(logger, handler)
		p.startWorker(p.readyQ, decoder.decodeBatch, 1000, 8*1024*1024, i)
	}

	return p
}

// Run starts the Pool decoding packets from the configured PacketSource and
// sending the results to the PacketHandler.
//
// Packets are dropped if they arrive more rapidly than the Pool can handle
// them.
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
			err := p.src.DiscardPacket()
			if err == pcap.NextErrorTimeoutExpired {
				// loop again
			} else if err == io.EOF {
				// wait for a worker to become ready so we can
				// shut them down and avoid a busy-wait loop.
				time.Sleep(10 * time.Millisecond)
			} else if err == nil {
				p.stats.PacketsDropped++
			} else {
				p.logger.Log("Error from DiscardPacket", err)
			}
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
	// tell worker how much of its working area contains valid new packets
	w.work()
	return nil
}
