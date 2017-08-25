package capture

import (
	"errors"
	"github.com/google/gopacket"
)

var (
	// ErrBytesFull is returned if the PacketBuffer cannot hold enough
	// data bytes to store this packet
	ErrBytesFull = errors.New("PacketBuffer out of space for more bytes")
	// ErrPacketsFull is returned if the PacketBuffer cannot hold more packets
	ErrPacketsFull = errors.New("PacketBuffer out of space for more packets")
)

type BlockBuffer struct {
	// stores where the nth packet ends in data
	offsets []int
	// packet n is located at data[offsets[n-1]:offsets[n]]
	data []byte
}

func NewBlockBuffer(maxBlocks, maxBytes int) BlockBuffer {
	return BlockBuffer{
		offsets: make([]int, 0, maxBlocks),
		data:    make([]byte, 0, maxBytes),
	}
}

// PacketBuffer stores packets in a compact format
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

// BytesRemaining returns the number of additional bytes this PacketBuffer can hold.
func (b *BlockBuffer) BytesRemaining() int {
	return cap(b.data) - len(b.data)
}

func (b *BlockBuffer) Block(n int) []byte {
	start := 0
	if n > 0 {
		start = b.offsets[n-1]
	}
	end := b.offsets[n]
	return b.data[start:end]
}

func (b *BlockBuffer) BlockLen() int {
	return len(b.offsets)
}

func (b *BlockBuffer) Clear() {
	b.offsets = b.offsets[:0]
	b.data = b.data[:0]
}

// Append adds a packet to the PacketBuffer.
// Makes a copy of pd.Data, which may be modified after Append returns.
// Returns ErrBytesFull or ErrPacketsFull if there is insufficient space.
func (b *BlockBuffer) Append(data []byte) error {
	if cap(b.offsets) == 0 {
		panic("attempted to Append to uninitialized BlockBuffer")
	}
	if len(b.offsets) >= cap(b.offsets) {
		return ErrPacketsFull
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

func (b *PacketBuffer) Append(pd PacketData) error {
	err := b.BlockBuffer.Append(pd.Data)
	if err != nil {
		return err
	}
	b.cis[len(b.offsets)-1] = pd.Info
	return nil
}

// PacketCap returns the capacity for packets in the PacketBuffer.
func (pb *PacketBuffer) PacketCap() int {
	return len(pb.offsets)
}

// PacketLen returns the number of packets in the PacketBuffer.
func (pb *PacketBuffer) PacketLen() int {
	return pb.BlockLen()
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
