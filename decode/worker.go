// Copyright 2017 Box, Inc.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decode

import (
	"github.com/box/memsniff/capture"
)

type packetHandler func(pd []capture.PacketData)

type worker struct {
	id          int
	workerQueue workerQueue
	pd          []capture.PacketData
	workReady   chan int
	handler     packetHandler
}

// batch returns the worker's capture buffer, where packet data should be
// written.  batch should only be called after the worker publishes itself
// to the worker queue.
//
// The returned slice is invalid once work is called and must not be
// modified.
func (w *worker) batch() []capture.PacketData {
	return w.pd
}

// work begins work on the first count elements of the worker's capture buffer.
func (w *worker) work(count int) {
	if count > 0 {
		w.workReady <- count
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
		count := <-w.workReady
		if count == 0 {
			// workReady channel was closed
			return
		}
		w.handler(w.pd[:count])
	}
}
