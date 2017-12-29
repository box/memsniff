package reader

import (
	"bytes"
	"fmt"
	"io"
)

type Buffer struct {
	buf bytes.Buffer
	// length including gaps
	len     int
	cap     int
	blocks  []block
	discard int
}

func NewBuffer(cap int) *Buffer {
	return &Buffer{
		cap: cap,
	}
}

func (b *Buffer) Reset() {
	b.buf.Reset()
	b.len = 0
	b.blocks = b.blocks[:0]
	b.discard = 0
}

func (b *Buffer) Write(skip int, data []byte) error {
	if skip < 0 {
		// starting mid-conversation
		skip = 0
	}
	if b.discard >= skip+len(data) {
		// discard all of data
		b.discard = b.discard - skip - len(data)
		return nil
	}

	if skip >= b.discard {
		skip -= b.discard
	} else {
		data = data[b.discard-skip:]
		skip = 0
	}

	if b.buf.Len()+len(data) > b.cap {
		return io.ErrShortWrite
	}
	b.buf.Write(data)
	b.discard = 0
	if skip == 0 && len(b.blocks) > 0 {
		b.blocks[len(b.blocks)-1].dataLen += len(data)
	} else {
		b.blocks = append(b.blocks, block{skip, len(data)})
	}
	b.len += skip + len(data)
	return nil
}

func (b *Buffer) Len() int {
	return b.len
}

func (b *Buffer) ReadN(n int) (out []byte, err error) {
	if b.len < n {
		return nil, ErrShortRead
	}
	avail, gap := b.contiguousAvailable()
	if avail < n {
		out = b.buf.Bytes()[:avail]
		b.Discard(avail + gap)
		return out, ErrLostData{gap}
	}
	out = b.buf.Bytes()[:n]
	b.Discard(n)
	return
}

func (b *Buffer) PeekN(n int) (out []byte, err error) {
	if b.len < n {
		return nil, ErrShortRead
	}
	avail, gap := b.contiguousAvailable()
	if avail < n {
		out = b.buf.Bytes()[:avail]
		return out, ErrLostData{gap}
	}
	out = b.buf.Bytes()[:n]
	return
}

func (b *Buffer) IndexAny(chars string) (int, error) {
	pos, avail, gap := b.indexAny(chars)
	if pos < 0 {
		if avail < b.len {
			return pos, ErrLostData{gap}
		}
		return pos, ErrShortRead
	}
	return pos, nil
}

func (b *Buffer) indexAny(chars string) (pos, avail, gap int) {
	avail, gap = b.contiguousAvailable()
	pos = bytes.IndexAny(b.buf.Bytes()[:avail], chars)
	return
}

func (b *Buffer) ReadLine() ([]byte, error) {
	pos, avail, gap := b.indexAny("\n")
	if pos < 0 {
		if avail < b.len {
			b.Discard(avail + gap)
			return nil, ErrLostData{gap}
		}
		return nil, ErrShortRead
	}

	out := b.buf.Bytes()[:pos]
	if len(out) >= 1 && out[len(out)-1] == '\r' {
		// trim \r
		out = out[:len(out)-1]
	}
	// discard \n
	b.Discard(pos + 1)
	return out, nil
}

func (b *Buffer) Discard(n int) {
	toDiscard := n
	for i, block := range b.blocks {
		l := block.len()
		if l > toDiscard {
			b.blocks[i].discard(b, toDiscard)
			b.dropBlocks(i)
			return
		}
		toDiscard -= l
	}
	b.buf.Reset()
	b.len = 0
	b.blocks = b.blocks[:0]
	b.discard += toDiscard
}

func (b *Buffer) contiguousAvailable() (avail int, gap int) {
	for _, block := range b.blocks {
		if block.hasGap() {
			return avail, block.gap
		}
		avail += block.dataLen
	}
	return
}

func (b *Buffer) totalData() (avail int) {
	for _, block := range b.blocks {
		avail += block.dataLen
	}
	return
}

func (b *Buffer) validate() {
	if b.totalData() != b.buf.Len() {
		panic(fmt.Sprintln(b.totalData(), b.buf.Len()))
	}
}

func (b *Buffer) dropBlocks(n int) {
	var dataToDrop int
	for _, block := range b.blocks[:n] {
		b.len -= block.len()
		dataToDrop += block.dataLen
	}
	copy(b.blocks, b.blocks[n:])
	b.blocks = b.blocks[:len(b.blocks)-n]
	b.buf.Next(dataToDrop)
}

// block is a contiguous set of bytes from the input stream,
// possibly including a preceding gap.
type block struct {
	// number of lost bytes before the actual data
	gap int
	// number of bytes of data
	dataLen int
}

func (b block) hasGap() bool {
	return b.gap > 0
}

// len returns the number of bytes in the input stream covered
// by this block, including any leading gap.
func (b block) len() int {
	return b.gap + b.dataLen
}

// discard modifies b to discard the first n bytes.
func (b *block) discard(buf *Buffer, n int) {
	if n >= b.len() {
		if n > b.len() {
			panic("can't discard more bytes from block than it contains")
		}
		panic("should not call discard() to discard full block")
	}

	buf.len -= n
	if b.gap > n {
		b.gap -= n
		return
	}
	n -= b.gap
	b.gap = 0
	b.dataLen -= n
	buf.buf.Next(n)
}
