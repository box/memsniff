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

// PacketBuffer stores packets in a compact format
type PacketBuffer struct {
	numPackets int
	cis        []gopacket.CaptureInfo
	// stores where the nth packet ends in data
	offsets []int
	// packet n is located at data[offsets[n-1]:offsets[n]]
	data []byte
}

// NewPacketBuffer creates a PacketBuffer with the specified limits.
func NewPacketBuffer(maxPackets, maxBytes int) *PacketBuffer {
	return &PacketBuffer{
		cis:     make([]gopacket.CaptureInfo, maxPackets),
		offsets: make([]int, maxPackets),
		data:    make([]byte, maxBytes),
	}
}

func (pb *PacketBuffer) bytesStored() int {
	if pb.numPackets == 0 {
		return 0
	}
	return pb.offsets[pb.numPackets-1]
}

// BytesRemaining returns the number of additional bytes this PacketBuffer can hold.
func (pb *PacketBuffer) BytesRemaining() int {
	return len(pb.data) - pb.bytesStored()
}

// Append adds a packet to the PacketBuffer.
// Makes a copy of pd.Data, which may be modified after Append returns.
// Returns ErrBytesFull or ErrPacketsFull if there is insufficient space.
func (pb *PacketBuffer) Append(pd PacketData) error {
	if pb.numPackets >= len(pb.offsets) {
		return ErrPacketsFull
	}
	if pb.bytesStored()+pd.Info.CaptureLength > len(pb.data) {
		return ErrBytesFull
	}

	pb.cis[pb.numPackets] = pd.Info
	pb.offsets[pb.numPackets] = pb.bytesStored() + pd.Info.CaptureLength
	copy(pb.data[pb.bytesStored():], pd.Data)
	pb.numPackets++

	return nil
}

// PacketCap returns the capacity for packets in the PacketBuffer.
func (pb *PacketBuffer) PacketCap() int {
	return len(pb.offsets)
}

// PacketLen returns the number of packets in the PacketBuffer.
func (pb *PacketBuffer) PacketLen() int {
	return pb.numPackets
}

// Packet returns the packet at the specified index, which must be
// less than PacketLen.  The Data field of the returned PacketData
// points into this PacketBuffer and must not be modified.
func (pb *PacketBuffer) Packet(n int) PacketData {
	start := 0
	if n > 0 {
		start = pb.offsets[n-1]
	}
	end := pb.offsets[n]

	return PacketData{
		Info: pb.cis[n],
		Data: pb.data[start:end],
	}
}

// Clear removes all packets.
func (pb *PacketBuffer) Clear() {
	pb.numPackets = 0
	pb.offsets[0] = 0
}
