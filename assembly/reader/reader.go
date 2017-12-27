package reader

import (
	"io"

	"github.com/google/gopacket/tcpassembly"
)

const (
	BufferSize = 8 * 1024
)

// Reader implements the model.ConsumerSource interface using a Buffer.
type Reader struct {
	buf    Buffer
	closed bool
	eof    bool
	err    error
}

func New() *Reader {
	return &Reader{
		buf: *NewBuffer(BufferSize),
	}
}

func (r *Reader) Reassembled(rs []tcpassembly.Reassembly) {
	if r.closed || r.err != nil {
		return
	}
	for _, reassembly := range rs {
		err := r.buf.Write(reassembly.Skip, reassembly.Bytes)
		if err != nil {
			r.err = err
			return
		}
	}
}

func (r *Reader) ReassemblyComplete() {
	r.eof = true
}

func (r *Reader) Reset() {
	r.buf.Reset()
	r.closed = false
	r.eof = false
	r.err = nil
}

func (r *Reader) Truncate() {
	r.buf.Reset()
}

func (r *Reader) Discard(n int) (discarded int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	r.buf.Discard(n)
	return n, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	out, err := r.ReadN(len(p))
	if err == ErrShortRead {
		err = nil
	}
	if err != nil {
		return
	}
	copy(p, out)
	return len(out), err
}

func (r *Reader) ReadN(n int) (out []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	out, err = r.buf.ReadN(n)
	if err == ErrShortRead && r.eof {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (r *Reader) ReadLine() (out []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	out, err = r.buf.ReadLine()
	if err == ErrShortRead && r.eof {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (r *Reader) Close() error {
	r.closed = true
	r.buf.Reset()
	return nil
}
