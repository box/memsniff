package model

import (
	"io"

	"github.com/google/gopacket/tcpassembly"
)

type DummySource struct{}

func (s *DummySource) Reassembled(rs []tcpassembly.Reassembly) {}

func (s *DummySource) ReassemblyComplete() {}

func (s *DummySource) Discard(n int) (discarded int, err error) {
	return 0, nil
}

func (s *DummySource) ReadN(n int) ([]byte, error) {
	return nil, io.EOF
}

func (s *DummySource) PeekN(n int) ([]byte, error) {
	return nil, io.EOF
}

func (s *DummySource) ReadLine() ([]byte, error) {
	return nil, io.EOF
}

func (s *DummySource) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (s *DummySource) Close() error {
	return nil
}

func (s *DummySource) Reset() {}

func (s *DummySource) Truncate() {}
