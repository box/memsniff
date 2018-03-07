package decode

import (
	"github.com/box/memsniff/capture"
)

type packetHandler func(pb *capture.PacketBuffer)

type worker struct {
	id          int
	workerQueue workerQueue
	pb          *capture.PacketBuffer
	workReady   chan struct{}
	handler     packetHandler
}

// startWorker creates a background Worker that will send itself to q when it
// is ready for a new batch of packets.  This Worker can then be given work
// or closed, which will clean up the goroutine.
//
// handler will be invoked on the Worker's background goroutine.
func (p *Pool) startWorker(q workerQueue, handler packetHandler, batchSize int, maxBytes int, id int) {
	w := worker{
		id:          id,
		workerQueue: q,
		pb:          capture.NewPacketBuffer(batchSize, maxBytes),
		workReady:   make(chan struct{}, 1),
		handler:     handler,
	}
	go w.loop()
}

// buf returns the worker's capture buffer, where packet data should be
// written.  buf should only be called after the worker publishes itself
// to the worker queue.
//
// The returned buffer is invalid once work is called and must not be
// modified.
func (w *worker) buf() *capture.PacketBuffer {
	return w.pb
}

// work begins work on the first count elements of the worker's capture buffer.
func (w *worker) work() {
	if w.pb.PacketLen() > 0 {
		w.workReady <- struct{}{}
	} else {
		// no work to do, just rejoin the WorkerQueue
		w.workerQueue <- w
	}
}

// close shuts down a worker's goroutine after the current batch is processed.
func (w *worker) close() {
	close(w.workReady)
}

func (w *worker) loop() {
	for {
		w.workerQueue <- w
		_, ok := <-w.workReady
		if !ok {
			// workReady channel was closed
			return
		}
		w.handler(w.pb)
	}
}
