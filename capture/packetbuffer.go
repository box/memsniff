package capture

import (
	"errors"
	"github.com/google/gopacket"
)

var (
	// ErrBytesFull is returned if the buffer cannot hold enough
	// data bytes to store this block.
	ErrBytesFull = errors.New("buffer out of space for more bytes")
	// ErrBlocksFull is returned if the buffer cannot hold another block.
	ErrBlocksFull = errors.New("buffer out of space for more packets")
)

// BlockBuffer stores a sequence of byte slices in compact form.
type BlockBuffer struct {
	// stores where the nth block ends in data
	offsets []int
	// block n is located at data[offsets[n-1]:offsets[n]]
	data []byte
}

// NewBlockBuffer creates a new BlockBuffer that can hold a maximum of
// maxBlocks slices or a total of at most maxBytes.
func NewBlockBuffer(maxBlocks, maxBytes int) BlockBuffer {
	return BlockBuffer{
		offsets: make([]int, 0, maxBlocks),
		data:    make([]byte, 0, maxBytes),
	}
}

// BytesRemaining returns the number of additional bytes this buffer can hold.
func (b *BlockBuffer) BytesRemaining() int {
	return cap(b.data) - len(b.data)
}

// Block returns a slice referencing block n, indexed from 0.
// Any modifications to the returned slice modify the contents of the buffer.
func (b *BlockBuffer) Block(n int) []byte {
	start := 0
	if n > 0 {
		start = b.offsets[n-1]
	}
	end := b.offsets[n]
	return b.data[start:end:end]
}

// BlockLen returns the number of blocks currently stored in the buffer.
func (b *BlockBuffer) BlockLen() int {
	return len(b.offsets)
}

// Clear removes all blocks.
func (b *BlockBuffer) Clear() {
	b.offsets = b.offsets[:0]
	b.data = b.data[:0]
}

// Append adds data to the buffer as a new block.
// Makes a copy of data, which may be modified freely after Append returns.
// Returns ErrBytesFull or ErrBlocksFull if there is insufficient space.
func (b *BlockBuffer) Append(data []byte) error {
	if cap(b.offsets) == 0 {
		panic("attempted to Append to uninitialized BlockBuffer")
	}
	if len(b.offsets) >= cap(b.offsets) {
		return ErrBlocksFull
	}
	start := len(b.data)
	end := len(b.data) + len(data)
	if end > cap(b.data) {
		return ErrBytesFull
	}

	b.data = b.data[:end]
	copy(b.data[start:], data)
	b.offsets = b.offsets[:len(b.offsets)+1]
	b.offsets[len(b.offsets)-1] = end

	return nil
}

// PacketBuffer stores captured packets in a compact format.
type PacketBuffer struct {
	BlockBuffer
	cis []gopacket.CaptureInfo
}

// NewPacketBuffer creates a PacketBuffer with the specified limits.
func NewPacketBuffer(maxPackets, maxBytes int) *PacketBuffer {
	return &PacketBuffer{
		BlockBuffer: NewBlockBuffer(maxPackets, maxBytes),
		cis:         make([]gopacket.CaptureInfo, maxPackets),
	}
}

// Append adds a packet to the PacketBuffer.
// Makes a copy of pd.Data, which may be modified after Append returns.
// Returns ErrBytesFull or ErrBlocksFull if there is insufficient space.
func (pb *PacketBuffer) Append(pd PacketData) error {
	err := pb.BlockBuffer.Append(pd.Data)
	if err != nil {
		return err
	}
	pb.cis[len(pb.BlockBuffer.offsets)-1] = pd.Info
	return nil
}

// PacketCap returns the capacity for packets in the PacketBuffer.
func (pb *PacketBuffer) PacketCap() int {
	return cap(pb.BlockBuffer.offsets)
}

// PacketLen returns the number of packets in the PacketBuffer.
func (pb *PacketBuffer) PacketLen() int {
	return pb.BlockBuffer.BlockLen()
}

// Packet returns the packet at the specified index, which must be
// less than PacketLen.  The Data field of the returned PacketData
// points into this PacketBuffer and must not be modified.
func (pb *PacketBuffer) Packet(n int) PacketData {
	return PacketData{
		Info: pb.cis[n],
		Data: pb.BlockBuffer.Block(n),
	}
}

// Clear removes all packets.
func (pb *PacketBuffer) Clear() {
	pb.BlockBuffer.Clear()
}
