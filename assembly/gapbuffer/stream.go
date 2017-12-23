package gapbuffer

import (
	"io"

	"github.com/google/gopacket/tcpassembly"
)

const (
	BufferSize = 8 * 1024
)

// Stream implements the model.ConsumerSource interface using a Buffer.
type Stream struct {
	buf    Buffer
	closed bool
	eof    bool
	err    error
}

func NewStream() *Stream {
	return &Stream{
		buf: *New(BufferSize),
	}
}

func (s *Stream) Reassembled(rs []tcpassembly.Reassembly) {
	if s.closed || s.err != nil {
		return
	}
	for _, r := range rs {
		err := s.buf.Write(r.Skip, r.Bytes)
		if err != nil {
			s.err = err
			return
		}
	}
}

func (s *Stream) ReassemblyComplete() {
	s.eof = true
}

func (s *Stream) Reset() {
	s.buf.Reset()
}

func (s *Stream) Discard(n int) (discarded int, err error) {
	if s.err != nil {
		return 0, s.err
	}
	s.buf.Discard(n)
	return n, nil
}

func (s *Stream) Read(p []byte) (n int, err error) {
	out, err := s.ReadN(len(p))
	if err == ErrShortRead {
		err = nil
	}
	if err != nil {
		return
	}
	copy(p, out)
	return len(out), err
}

func (s *Stream) ReadN(n int) (out []byte, err error) {
	if s.err != nil {
		return nil, s.err
	}
	out, err = s.buf.ReadN(n)
	if err == ErrShortRead && s.eof {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (s *Stream) ReadLine() (out []byte, err error) {
	if s.err != nil {
		return nil, s.err
	}
	out, err = s.buf.ReadLine()
	if err == ErrShortRead && s.eof {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (s *Stream) Close() error {
	s.closed = true
	s.buf = Buffer{}
	return nil
}
