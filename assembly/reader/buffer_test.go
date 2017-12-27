package reader

import (
	"bytes"
	"io"
	"testing"
)

func TestWriteOverrun(t *testing.T) {
	b := New(8)
	err := b.Write(0, []byte("hello world"))
	if err != io.ErrShortWrite {
		t.Fail()
	}
}

func TestReadLine(t *testing.T) {
	b := New(128)
	b.Write(0, []byte("hello\nworld\n"))

	o, err := b.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(o, []byte("hello")) {
		t.Error(string(o), "hello")
	}
	if b.Len() != 6 {
		t.Error(b.Len(), 6)
	}

	o, err = b.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(o, []byte("world")) {
		t.Error(string(o), "world")
	}
	if b.Len() != 0 {
		t.Error(b.Len(), 0)
	}
}

func TestReadLineAcrossBlocks(t *testing.T) {
	b := New(128)
	b.Write(0, []byte("hel"))
	b.Write(0, []byte("lo\n"))

	o, err := b.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(o, []byte("hello")) {
		t.Error(string(o), "hello")
	}
	if b.Len() != 0 {
		t.Error(b.Len(), 0)
	}
}

func TestReadIncompleteLineIsNoop(t *testing.T) {
	b := New(128)
	b.Write(0, []byte("hello world"))
	o, err := b.ReadLine()
	if o != nil {
		t.Error(o, nil)
	}
	if err != ErrShortRead {
		t.Error(err)
	}
	if b.Len() != 11 {
		t.Error(b.Len(), 11)
	}
}

func TestDiscardNewWrites(t *testing.T) {
	b := New(128)
	b.Discard(3)
	b.Write(0, []byte("hello"))

	testReadN(t, b, "lo", 0)
}

func TestDiscardOverGap(t *testing.T) {
	b := New(128)
	b.Write(2, []byte("rld"))
	b.Discard(3)

	testReadN(t, b, "ld", 0)
}

func TestDiscardMultipleBlocks(t *testing.T) {
	b := New(128)
	b.Write(2, nil)
	b.Write(2, nil)
	b.Write(0, []byte("hello"))
	b.Discard(5)

	testReadN(t, b, "ell", 1)
}

func TestDiscardBeforeWriteMultiple(t *testing.T) {
	b := New(128)
	b.Discard(5)
	b.Write(2, nil)
	b.Write(2, nil)
	b.Write(0, []byte("hello"))

	testReadN(t, b, "ell", 1)
}

func TestReadNHitsGap(t *testing.T) {
	b := New(128)
	b.Write(0, []byte("hello"))
	b.Write(2, []byte("orld"))

	o, err := b.ReadN(11)
	if err != (ErrLostData{2}) {
		t.Error(err, ErrLostData{2})
	}
	if !bytes.Equal(o, []byte("hello")) {
		t.Error(string(o), "hello")
	}
	if b.Len() != 4 {
		t.Error(b.Len(), 4)
	}
}

func TestReadLineThroughGap(t *testing.T) {
	b := New(128)
	b.Write(0, []byte("hello"))
	b.Write(2, []byte("orld\r\n"))

	o, err := b.ReadLine()
	if err != (ErrLostData{2}) {
		t.Error(err, ErrLostData{2})
	}
	if b.Len() != 6 {
		t.Error(b.Len(), 6)
	}

	o, err = b.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(o, []byte("orld")) {
		t.Error(string(o), "orld")
	}
	if b.Len() != 0 {
		t.Error(b.Len(), 0)
	}
}

func TestReadAcrossRingBufferWrap(t *testing.T) {
	b := New(8)
	b.Write(0, []byte("hello"))
	b.Discard(4)
	b.Write(0, []byte("wor\nld"))
	out, err := b.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(out, []byte("owor")) {
		t.Error(string(out), "owor")
	}
	if b.Len() != 2 {
		t.Error(b.Len(), 2)
	}
}

func TestDiscardAcrossRingBufferWrap(t *testing.T) {
	b := New(8)
	b.Write(0, []byte("hello"))
	b.Discard(4)
	b.Write(0, []byte("world"))
	b.Discard(5)
	testReadN(t, b, "d", 0)
}

func TestDiscardEntireSplitBlock(t *testing.T) {
	b := New(8)
	b.Write(0, []byte("hello"))
	b.Discard(4)
	b.Write(0, []byte("worl"))
	b.Write(0, []byte("d"))
	b.Discard(5)
	testReadN(t, b, "d", 0)
}

func TestDiscardPartialGap(t *testing.T) {
	b := New(8)
	b.Write(4, []byte("hello"))
	b.Discard(2)
	b.Discard(3)
	testReadN(t, b, "el", 2)
}

func TestDiscardUpdatesBlockLength(t *testing.T) {
	b := New(8)
	b.Write(0, []byte("hello\r\n"))
	b.Discard(2)
	b.ReadLine()
}

func testReadN(t *testing.T, b *Buffer, expect string, remain int) {
	o, err := b.ReadN(len(expect))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(o, []byte(expect)) {
		t.Error(string(o), expect)
	}
	if b.Len() != remain {
		t.Error(b.Len(), remain)
	}
}
