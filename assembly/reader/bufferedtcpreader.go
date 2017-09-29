package reader

import (
	"bytes"
	"fmt"
	"github.com/box/memsniff/capture"
	"github.com/google/gopacket/tcpassembly"
	"io"
	"sync"
)

const (
	maxPackets = 100
	maxBytes   = 8 * 1024
)

var (
	reassemblyQueuePool = sync.Pool{New: func() interface{} { return newReassemblyQueue() }}
	bufferPool          = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
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
	// q.reassembled only holds references into q.bytes, and write cursor is managed via q.bytes.BlockLen(),
	// so no need to clear out q.reassembled.
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
	partner *TCPReaderStream

	// owned by writer side

	// will be nil if we have already sent EOF
	writeBatch *reassemblyQueue
	// will receive nil after the final reassemblyQueue to indicate EOF
	filled chan *reassemblyQueue

	// owned by reader side

	readBatch *reassemblyQueue
	// partially consumed Reassembly
	current *tcpassembly.Reassembly
	// buf accumulates blocks that extends over multiple Reassemblies in ReadN and ReadLine
	buf     *bytes.Buffer
	seenEOF bool
	closed  chan struct{}

	LossErrors bool
}

// NewPair creates an associated pair of TCPReaderStreams.
func NewPair() (client, server *TCPReaderStream) {
	client = New()
	server = New()
	client.partner = server
	server.partner = client
	return
}

// New creates a new TCPReaderStream.
func New() *TCPReaderStream {
	r := &TCPReaderStream{
		writeBatch: reassemblyQueuePool.Get().(*reassemblyQueue),
		filled:     make(chan *reassemblyQueue, 2),
		buf:        bufferPool.Get().(*bytes.Buffer),
		closed:     make(chan struct{}),
	}
	return r
}

// Reassembled buffers stream data for later consumption by this reader.
// If the buffer fills it is sent to the reader side, blocking if the reader
// is falling behind.
func (r *TCPReaderStream) Reassembled(reassembly []tcpassembly.Reassembly) {
	if r.isClosed() {
		if !r.sentEOF() {
			r.sendEOF()
		}
		return
	}
	numAdded := r.writeBatch.add(reassembly)
	for numAdded < len(reassembly) {
		r.flushBoth()
		if r.isClosed() {
			return
		}
		numAdded += r.writeBatch.add(reassembly[numAdded:])
	}
}

// ReassemblyComplete sends any buffered stream data to the reader.
func (r *TCPReaderStream) ReassemblyComplete() {
	if !r.isClosed() {
		// send last batch to reader
		r.flushBoth()
	}
	if !r.sentEOF() {
		r.sendEOF()
	}
}

func (r *TCPReaderStream) isClosed() bool {
	// r.closed is closed by the reader side, so we can read it as much as we like.
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}

func (r *TCPReaderStream) sentEOF() bool {
	return r.writeBatch == nil
}

func (r *TCPReaderStream) sendEOF() {
	r.writeBatch.clear()
	reassemblyQueuePool.Put(r.writeBatch)
	r.writeBatch = nil
	r.filled <- nil
}

func (r *TCPReaderStream) flushBoth() {
	if r.partner != nil {
		// try to make sure it is possible for flush to complete if
		// the consumer of this reader is waiting for data from our partner.
		r.partner.flush()
	}
	r.flush()
}

// flush sends any buffered data to the reader side.
func (r *TCPReaderStream) flush() {
	if r.sentEOF() || r.writeBatch.bytes.BlockLen() == 0 {
		return
	}
	r.filled <- r.writeBatch
	r.writeBatch = reassemblyQueuePool.Get().(*reassemblyQueue)
}

// Read attempts to read sufficient data to fill p.
// It returns the number of bytes read into p.
// If EOF is encountered, n will be less than len(p).
// At EOF, the count will be zero and err will be io.EOF.
func (r *TCPReaderStream) Read(p []byte) (n int, err error) {
	if err = r.reportSkip(); err != nil {
		return
	}

	for n < len(p) {
		numBytes := copy(p[n:], r.current.Bytes)
		n += numBytes
		r.current.Bytes = r.current.Bytes[numBytes:]
		if len(r.current.Bytes) == 0 {
			err = r.nextReassembly()
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
	if err := r.reportSkip(); err != nil {
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
		err := r.nextReassembly()
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
	// use ensureCurrent() instead of reportSkip() to avoid advancing past the gap.
	if err := r.ensureCurrent(); err != nil {
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
	if err := r.reportSkip(); err != nil {
		return nil, err
	}

	var pos int
	// see if we can satisfy without copying
	if pos = bytes.IndexByte(r.current.Bytes, '\n'); pos >= 0 {
		out := trimCR(r.current.Bytes[:pos])
		r.current.Bytes = r.current.Bytes[pos+1:]
		return out, nil
	}

	// need to accumulate
	r.buf.Reset()
	for pos < 0 {
		if pos = bytes.IndexByte(r.current.Bytes, '\n'); pos < 0 {
			r.buf.Write(r.current.Bytes)
			err := r.nextReassembly()
			if err != nil {
				return r.buf.Bytes(), nil
			}
		}
	}
	r.buf.Write(trimCR(r.current.Bytes[:pos]))
	r.current.Bytes = r.current.Bytes[pos+1:]
	return r.buf.Bytes(), nil
}

func trimCR(in []byte) []byte {
	if len(in) > 0 && in[len(in)-1] == '\r' {
		return in[:len(in)-1]
	}
	return in
}

// Discard skips the next n bytes, returning the number of bytes discarded.
//
// If Discard skips fewer than n bytes, it also returns an error.
func (r *TCPReaderStream) Discard(n int) (discarded int, err error) {
	if n <= 0 {
		return
	}

	// use ensureCurrent instead of reportSkip() since we can do something meaningful
	if err = r.ensureCurrent(); err != nil {
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
		err = r.nextReassembly()
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
	// notify the writer; now isClosed() will return true
	close(r.closed)

	if r.readBatch != nil {
		r.readBatch.clear()
		reassemblyQueuePool.Put(r.readBatch)
		r.readBatch = nil
	}

	r.current = nil
	r.buf.Reset()
	bufferPool.Put(r.buf)
	r.buf = nil

	if !r.seenEOF {
		// closing before reaching EOF from the writer.
		// consume and discard any data sent before the writer detected the close.
		go func() {
			for q := <-r.filled; q != nil; q = <-r.filled {
				q.clear()
				reassemblyQueuePool.Put(q)
			}
		}()
	}

	return nil
}

func (r *TCPReaderStream) ensureCurrent() (err error) {
	if r.current != nil && (r.current.Skip > 0 || len(r.current.Bytes) > 0) {
		return nil
	}
	return r.nextReassembly()
}

func (r *TCPReaderStream) reportSkip() (err error) {
	if err = r.ensureCurrent(); err != nil {
		return err
	}
	if r.current.Skip != 0 && r.LossErrors {
		err = ErrLostData{r.current.Skip}
		r.current.Skip = 0
	}
	return
}

func (r *TCPReaderStream) nextReassembly() (err error) {
	if r.seenEOF {
		return io.EOF
	}
	if r.readBatch == nil {
		if err = r.nextReadBatch(); err != nil {
			return
		}
	}
	for {
		r.current, err = r.readBatch.next()
		if err == nil {
			// got another Reassembly from the current batch, continue reading
			return
		}

		// reached end of current batch, return it to the pool
		r.readBatch.clear()
		reassemblyQueuePool.Put(r.readBatch)
		if err = r.nextReadBatch(); err != nil {
			return
		}
		// loop around to read first Reassembly from the new batch
	}
}

func (r *TCPReaderStream) nextReadBatch() (err error) {
	r.readBatch = <-r.filled
	if r.readBatch == nil {
		r.seenEOF = true
		return io.EOF
	}
	return nil
}
