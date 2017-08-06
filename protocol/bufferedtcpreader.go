package protocol

import (
	"bytes"
	"fmt"
	"github.com/box/memsniff/capture"
	"github.com/google/gopacket/tcpassembly"
	"io"
	"sync"
)

const (
	maxPackets = 1000
	maxBytes   = 4 * 1024
)

var (
	reassemblyQueuePool = sync.Pool{
		New: func() interface{} { return NewReassemblyQueue() },
	}
	bufferPool = sync.Pool{
		New: func() interface{} { return new(bytes.Buffer) },
	}
)

type ReassemblyQueue struct {
	reassembled [maxPackets]tcpassembly.Reassembly
	bytes       capture.BlockBuffer
	cursor      int
}

func NewReassemblyQueue() *ReassemblyQueue {
	return &ReassemblyQueue{
		bytes: capture.NewBlockBuffer(maxPackets, maxBytes),
	}
}

func (q *ReassemblyQueue) Add(reassembly []tcpassembly.Reassembly) int {
	lenBefore := q.bytes.BlockLen()
	// defensive copy of all but Bytes (overwritten below)
	copy(q.reassembled[lenBefore:], reassembly)
	for i, r := range reassembly {
		// defensive copy of data
		err := q.bytes.Append(r.Bytes)
		if err != nil {
			return i
		}
		// repoint saved Reassembly to the copy
		q.reassembled[lenBefore+i].Bytes = q.bytes.Block(lenBefore + i)
	}
	return len(reassembly)
}

func (q *ReassemblyQueue) Next() (*tcpassembly.Reassembly, error) {
	if q.cursor >= q.bytes.BlockLen() {
		return nil, io.EOF
	}
	q.cursor++
	return &q.reassembled[q.cursor-1], nil
}

func (q *ReassemblyQueue) Clear() {
	q.cursor = 0
	q.bytes.Clear()
}

type ErrLostData struct {
	Lost int
}

func (e ErrLostData) Error() string {
	if e.Lost < 0 {
		return "lost unknown amount of data from stream start"
	} else {
		return fmt.Sprintln("lost", e.Lost, "bytes from stream")
	}
}

type TCPReaderStream struct {
	LossErrors bool
	writeBatch *ReassemblyQueue
	readBatch  *ReassemblyQueue
	ready      chan struct{}
	done       chan struct{}
	eof        bool
	closed     bool
	current    *tcpassembly.Reassembly
	lineBuf    *bytes.Buffer
}

func NewTCPReaderStream() *TCPReaderStream {
	r := &TCPReaderStream{
		writeBatch: reassemblyQueuePool.Get().(*ReassemblyQueue),
		readBatch:  reassemblyQueuePool.Get().(*ReassemblyQueue),
		ready:      make(chan struct{}, 1),
		done:       make(chan struct{}, 1),
		lineBuf:    bufferPool.Get().(*bytes.Buffer),
	}
	return r
}

func (r *TCPReaderStream) Reassembled(reassembly []tcpassembly.Reassembly) {
	if r.closed {
		return
	}
	numAdded := r.writeBatch.Add(reassembly)
	for numAdded < len(reassembly) {
		r.flush()
		if r.closed {
			return
		}
		numAdded += r.writeBatch.Add(reassembly[numAdded:])
	}
}

func (r *TCPReaderStream) ReassemblyComplete() {
	if r.closed {
		r.releaseResources()
		return
	}
	// send last batch to reader
	r.flush()
	go func() {
		// wait for reader to be done with last batch
		<-r.done
		// tell reader there will be no more batches
		close(r.ready)
		r.releaseResources()
	}()
}

func (r *TCPReaderStream) flush() {
	// wait for read side to be done
	<-r.done
	r.writeBatch, r.readBatch = r.readBatch, r.writeBatch
	// allow read side to proceed
	r.ready <- struct{}{}
	r.writeBatch.Clear()
}

func (r *TCPReaderStream) releaseResources() {
	r.readBatch.Clear()
	reassemblyQueuePool.Put(r.readBatch)
	r.readBatch = nil
	r.writeBatch.Clear()
	reassemblyQueuePool.Put(r.writeBatch)
	r.writeBatch = nil
}

func (r *TCPReaderStream) Read(p []byte) (n int, err error) {
	if r.current == nil {
		err = r.nextAssembly()
		if err != nil {
			return
		}
	}
	if r.current.Skip != 0 && r.LossErrors {
		err = ErrLostData{r.current.Skip}
		r.current.Skip = 0
		return 0, err
	}
	for n < len(p) {
		numBytes := copy(p[n:], r.current.Bytes)
		n += numBytes
		r.current.Bytes = r.current.Bytes[numBytes:]
		if len(r.current.Bytes) == 0 {
			err = r.nextAssembly()
			if err != nil {
				r.current = nil
				return n, nil
			}
		}
	}
	return
}

func (r *TCPReaderStream) ReadLine() ([]byte, error) {
	if r.current == nil {
		err := r.nextAssembly()
		if err != nil {
			return nil, err
		}
	}
	if r.current.Skip != 0 && r.LossErrors {
		err := ErrLostData{r.current.Skip}
		r.current.Skip = 0
		return nil, err
	}
	pos := -1
	for pos < 0 {
		if pos = bytes.IndexByte(r.current.Bytes, '\n'); pos < 0 {
			r.lineBuf.Write(r.current.Bytes)
			err := r.nextAssembly()
			if err != nil {
				return r.lineBuf.Bytes(), nil
			}
		}
	}
	if pos > 0 && r.current.Bytes[pos-1] == '\r' {
		r.lineBuf.Write(r.current.Bytes[:pos-1])
	} else {
		r.lineBuf.Write(r.current.Bytes[:pos])
	}
	r.current.Bytes = r.current.Bytes[pos+1:]
	return r.lineBuf.Bytes(), nil
}

func (r *TCPReaderStream) Discard(n int) (discarded int, err error) {
	if r.current == nil {
		err = r.nextAssembly()
		if err != nil {
			return
		}
	}
	for toSkip := n; toSkip > 0; {
		if r.current.Skip >= toSkip {
			r.current.Skip -= toSkip
			return n, nil
		}

		toSkip -= r.current.Skip
		if len(r.current.Bytes) > toSkip {
			r.current.Skip = 0
			r.current.Bytes = r.current.Bytes[toSkip:]
			return n, nil
		}

		// discard whole Reassembly since toSkip exceeds r.current.Skip + r.current.Bytes
		toSkip -= len(r.current.Bytes)
		err = r.nextAssembly()
		if err != nil {
			return n - toSkip, nil
		}
	}
	return n, nil
}

func (r *TCPReaderStream) Close() error {
	r.closed = true
	close(r.done)
	r.current = nil
	bufferPool.Put(r.lineBuf)
	r.lineBuf = nil
	return nil
}

func (r *TCPReaderStream) nextAssembly() (err error) {
	if r.closed {
		panic("read from closed TCPReaderStream")
	}
	if r.eof {
		return io.EOF
	}
	for {
		r.current, err = r.readBatch.Next()
		if err == nil {
			return
		}
		// prompt write side to swap buffers
		r.done <- struct{}{}
		_, ok := <-r.ready
		if !ok {
			// end of stream
			close(r.done)
			r.eof = true
			return io.EOF
		}
	}
}
