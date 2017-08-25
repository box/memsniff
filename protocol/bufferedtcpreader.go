package protocol

import (
	"github.com/google/gopacket/tcpassembly"
	"io"
	"github.com/box/memsniff/capture"
)

const (
	maxPackets = 1000
	maxBytes = 4 * 1024
)

type ReassemblyQueue struct {
	reassembled []tcpassembly.Reassembly
	bytes       capture.BlockBuffer
	cursor      int
}

func NewReassemblyQueue() ReassemblyQueue {
	return ReassemblyQueue{
		bytes: capture.NewBlockBuffer(maxPackets, maxBytes),
	}
}

func (q *ReassemblyQueue) Add(reassembly []tcpassembly.Reassembly) int {
	lenBefore := len(q.reassembled)
	q.reassembled = append(q.reassembled, reassembly...)
	for i := lenBefore; i < len(q.reassembled); i++ {
		// defensive copy
		err := q.bytes.Append(q.reassembled[i].Bytes)
		if err != nil {
			q.reassembled = q.reassembled[:i]
			return i - lenBefore
		}
		q.reassembled[i].Bytes = q.bytes.Block(i)
	}
	return len(reassembly)
}

func (q *ReassemblyQueue) Next() (*tcpassembly.Reassembly, error) {
	if q.cursor >= len(q.reassembled) {
		return nil, io.EOF
	}
	q.cursor++
	return &q.reassembled[q.cursor-1], nil
}

func (q *ReassemblyQueue) Clear() {
	q.reassembled = q.reassembled[:0]
	q.cursor = 0
	q.bytes.Clear()
}

type TCPReaderStream struct {
	batches    [2]ReassemblyQueue
	writeBatch int
	readBatch  int
	ready      chan struct{}
	done       chan struct{}
	current    *tcpassembly.Reassembly
}

func NewTCPReaderStream() *TCPReaderStream {
	r := &TCPReaderStream{
		batches: [2]ReassemblyQueue{
			NewReassemblyQueue(),
			NewReassemblyQueue(),
		},
		writeBatch: 0,
		readBatch:  1,
		ready:      make(chan struct{}, 1),
		done:       make(chan struct{}, 1),
	}
	return r
}

func (r *TCPReaderStream) Reassembled(reassembly []tcpassembly.Reassembly) {
	numAdded := r.batches[r.writeBatch].Add(reassembly)
	for numAdded < len(reassembly) {
		r.flush()
		numAdded += r.batches[r.writeBatch].Add(reassembly[numAdded:])
	}
}

func (r *TCPReaderStream) ReassemblyComplete() {
	r.flush()
	go func() {
		<-r.done
		close(r.ready)
	}()
}

func (r *TCPReaderStream) flush() {
	// wait for read side to be done
	<-r.done
	r.writeBatch, r.readBatch = r.readBatch, r.writeBatch
	// allow read side to proceed
	r.ready <- struct{}{}
	r.batches[r.writeBatch].Clear()
}

func (r *TCPReaderStream) Read(p []byte) (numBytes int, err error) {
	for r.current == nil {
		r.current, err = r.batches[r.readBatch].Next()
		if err != nil {
			// prompt write side to swap buffers
			r.done <- struct{}{}
			_, ok := <-r.ready
			if !ok {
				// end of stream
				close(r.done)
				r.batches[r.readBatch].Clear()
				return 0, io.EOF
			}
		}
	}
	numBytes = copy(p, r.current.Bytes)
	r.current.Bytes = r.current.Bytes[numBytes:]
	if len(r.current.Bytes) == 0 {
		r.current = nil
	}
	return
}
