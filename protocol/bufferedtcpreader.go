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
		New: func() interface{} { return newReassemblyQueue() },
	}
	bufferPool = sync.Pool{
		New: func() interface{} { return new(bytes.Buffer) },
	}
)

type reassemblyQueue struct {
	reassembled [maxPackets]tcpassembly.Reassembly
	bytes       capture.BlockBuffer
	cursor      int
}

func newReassemblyQueue() *reassemblyQueue {
	return &reassemblyQueue{
		bytes: capture.NewBlockBuffer(maxPackets, maxBytes),
	}
}

func (q *reassemblyQueue) add(reassembly []tcpassembly.Reassembly) int {
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

func (q *reassemblyQueue) next() (*tcpassembly.Reassembly, error) {
	if q.cursor >= q.bytes.BlockLen() {
		return nil, io.EOF
	}
	q.cursor++
	return &q.reassembled[q.cursor-1], nil
}

func (q *reassemblyQueue) clear() {
	q.cursor = 0
	q.bytes.Clear()
}

// ErrLostData is returned when there is a gap in the
// TCP stream due to missing or late packets.  It is returned only if
// LossErrors is set to true, and only once for each gap.  Successive
// read attempts will succeed, proceeding at the next available data.
type ErrLostData struct {
	Lost int
}

func (e ErrLostData) Error() string {
	if e.Lost < 0 {
		return "lost unknown amount of data from stream start"
	}
	return fmt.Sprintln("lost", e.Lost, "bytes from stream")
}

// TCPReaderStream implements tcpassembly.Stream and model.Reader
type TCPReaderStream struct {
	writeBatch *reassemblyQueue
	readBatch  *reassemblyQueue
	ready      chan struct{}
	done       chan struct{}
	current    *tcpassembly.Reassembly
	buf        *bytes.Buffer
	eof        bool
	closed     bool

	LossErrors bool
}

// NewTCPReaderStream creates a new TCPReaderStream.
func NewTCPReaderStream() *TCPReaderStream {
	r := &TCPReaderStream{
		writeBatch: reassemblyQueuePool.Get().(*reassemblyQueue),
		readBatch:  reassemblyQueuePool.Get().(*reassemblyQueue),
		ready:      make(chan struct{}, 1),
		done:       make(chan struct{}, 1),
		buf:        bufferPool.Get().(*bytes.Buffer),
	}
	return r
}

// Reassembled buffers stream data for later consumption by this reader.
// If the buffer fills it is sent to the reader side, blocking if the reader
// is falling behind.
func (r *TCPReaderStream) Reassembled(reassembly []tcpassembly.Reassembly) {
	if r.closed {
		return
	}
	numAdded := r.writeBatch.add(reassembly)
	for numAdded < len(reassembly) {
		r.flush()
		if r.closed {
			return
		}
		numAdded += r.writeBatch.add(reassembly[numAdded:])
	}
}

// ReassemblyComplete sends any buffered stream data to the reader.
func (r *TCPReaderStream) ReassemblyComplete() {
	if r.closed {
		r.releaseQueues()
		return
	}
	// send last batch to reader
	r.flush()
	go func() {
		// wait for reader to be done with last batch
		<-r.done
		// tell reader there will be no more batches
		close(r.ready)
		r.releaseQueues()
	}()
}

func (r *TCPReaderStream) flush() {
	// wait for read side to be done
	<-r.done
	r.writeBatch, r.readBatch = r.readBatch, r.writeBatch
	// allow read side to proceed
	r.ready <- struct{}{}
	r.writeBatch.clear()
}

func (r *TCPReaderStream) releaseQueues() {
	r.readBatch.clear()
	reassemblyQueuePool.Put(r.readBatch)
	r.readBatch = nil
	r.writeBatch.clear()
	reassemblyQueuePool.Put(r.writeBatch)
	r.writeBatch = nil
}

// Read attempts to read sufficient data to fill p.
// It returns the number of bytes read into p.
// If EOF is encountered, n will be less than len(p).
// At EOF, the count will be zero and err will be io.EOF.
func (r *TCPReaderStream) Read(p []byte) (n int, err error) {
	err = r.ensureCurrent()
	if err != nil {
		return
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

// ReadN returns the next n bytes.
//
// If EOF is encountered before reading n bytes, the available bytes are returned
// along with ErrUnexpectedEOF.
//
// The returned buffer is only valid until the next call to ReadN, ReadLine or Close.
func (r *TCPReaderStream) ReadN(n int) ([]byte, error) {
	err := r.ensureCurrent()
	if err != nil {
		return nil, err
	}

	if r.current.Skip != 0 && r.LossErrors {
		err := ErrLostData{r.current.Skip}
		r.current.Skip = 0
		return nil, err
	}

	// see if we can satisfy without copying
	if len(r.current.Bytes) >= n {
		out := r.current.Bytes[:n]
		r.current.Bytes = r.current.Bytes[n:]
		return out, nil
	}

	// need to accumulate
	r.buf.Reset()
	for len(r.current.Bytes) < n {
		r.buf.Write(r.current.Bytes)
		n -= len(r.current.Bytes)
		err := r.nextAssembly()
		if err != nil {
			return r.buf.Bytes(), io.ErrUnexpectedEOF
		}
	}

	// note n may be zero here
	r.buf.Write(r.current.Bytes[:n])
	r.current.Bytes = r.current.Bytes[n:]
	return r.buf.Bytes(), nil
}

// Peek returns up to n bytes of the next data from r, without
// advancing the stream.
//
// Peek does not modify the state of r.  In particular
// Peek will not advance past lost data, and will repeatedly
// return ErrLostData until a Read, ReadN, or ReadLine operation.
func (r *TCPReaderStream) Peek(n int) ([]byte, error) {
	err := r.ensureCurrent()
	if err != nil {
		return nil, err
	}

	if r.current.Skip != 0 && r.LossErrors {
		err := ErrLostData{r.current.Skip}
		return nil, err
	}

	if n > len(r.current.Bytes) {
		n = len(r.current.Bytes)
	}
	return r.current.Bytes[:n], nil
}

// ReadLine returns a single line, not including the end-of-line bytes.
// The returned buffer is only valid until the next call to ReadN, ReadLine or Close.
// ReadLine either returns a non-nil line or it returns an error, never both.
//
// The text returned from ReadLine does not include the line end ("\r\n" or "\n").
// No indication or error is given if the input ends without a final line end.
func (r *TCPReaderStream) ReadLine() ([]byte, error) {
	err := r.ensureCurrent()
	if err != nil {
		return nil, err
	}
	if r.current.Skip != 0 && r.LossErrors {
		err := ErrLostData{r.current.Skip}
		r.current.Skip = 0
		return nil, err
	}
	r.buf.Reset()
	pos := -1
	for pos < 0 {
		if pos = bytes.IndexByte(r.current.Bytes, '\n'); pos < 0 {
			r.buf.Write(r.current.Bytes)
			err := r.nextAssembly()
			if err != nil {
				return r.buf.Bytes(), nil
			}
		}
	}
	if pos > 0 && r.current.Bytes[pos-1] == '\r' {
		r.buf.Write(r.current.Bytes[:pos-1])
	} else {
		r.buf.Write(r.current.Bytes[:pos])
	}
	r.current.Bytes = r.current.Bytes[pos+1:]
	return r.buf.Bytes(), nil
}

// Discard skips the next n bytes, returning the number of bytes discarded.
//
// If Discard skips fewer than n bytes, it also returns an error.
func (r *TCPReaderStream) Discard(n int) (discarded int, err error) {
	if n <= 0 {
		return
	}

	err = r.ensureCurrent()
	if err != nil {
		return
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
			return n - toSkip, err
		}
	}
	return n, nil
}

// Close releases held resources, and discards any remaining data.
// After Close is called any buffers returned from ReadN or ReadLine
// are invalid.
func (r *TCPReaderStream) Close() error {
	r.closed = true
	close(r.done)
	r.current = nil
	bufferPool.Put(r.buf)
	r.buf = nil
	return nil
}

func (r *TCPReaderStream) ensureCurrent() (err error) {
	if r.current != nil && (r.current.Skip > 0 || len(r.current.Bytes) > 0) {
		return nil
	}
	return r.nextAssembly()
}

func (r *TCPReaderStream) nextAssembly() (err error) {
	if r.closed {
		panic("read from closed TCPReaderStream")
	}
	if r.eof {
		return io.EOF
	}
	for {
		r.current, err = r.readBatch.next()
		if err == nil {
			return
		}
		// prompt write side to swap buffers
		r.done <- struct{}{}
		_, ok := <-r.ready
		if !ok {
			// end of stream
			r.eof = true
			return io.EOF
		}
	}
}
