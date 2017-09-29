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

// buf returns the worker's capture buffer, where packet data should be
// written.  buf should only be called after the worker publishes itself
// to the worker queue.
//
// The returned buffer is invalid once work is called and must not be
// modified.
func (w *worker) buf() *capture.PacketBuffer {
	return w.pb
}

// work begins work on the worker's capture buffer.
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
