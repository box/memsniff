package capture

import (
	"fmt"
	"github.com/box/memsniff/log"
	"github.com/google/gopacket/pcap"
	"time"
)

// replayer throttles results from a PacketSource according to the Timestamp
// accompanying each packet.  It is most useful for recreating the input rate
// of a previously captured pcap file.
type replayer struct {
	// A Logger instance for debugging.  No logging is done if nil.
	Logger log.Logger
	// The wall time that this replayer was created.
	start time.Time
	// The timestamp of the first packet returned from src, usually
	// the first packet in a capture file.
	first time.Time
	// The buffer we ask src to fill as much as possible.
	buf *PacketBuffer
	// The next packet in buf to be returned to the user.
	cursor int
	// The number of packets returned to the user.  Used by Stats.
	received int
	dropped  int
	src      PacketSource
}

// replayerTimeout emulates the default behavior of pcap.ReadPacketData,
// waiting up to 10 ms to assemble a batch of packets.
const replayerTimeout = -pcap.BlockForever

func newReplayer(src PacketSource, batchSize int, maxBytes int) *replayer {
	return &replayer{
		buf: NewPacketBuffer(batchSize, maxBytes),
		src: src,
	}
}

func (r *replayer) CollectPackets(pb *PacketBuffer) error {
	pb.Clear()
	if r.start.IsZero() {
		r.start = time.Now()
	}

	elapsed := time.Since(r.start)
	r.dropExpired(elapsed)
	for r.cursor >= r.buf.PacketLen() {
		err := r.fill()
		if err != nil {
			return err
		}
		r.dropExpired(elapsed)
	}

	l := r.buf.PacketLen()
	writeUntil := r.first.Add(elapsed + replayerTimeout)
	for ; r.cursor < l && pb.BytesRemaining() >= snapLen; r.cursor++ {
		p := r.buf.Packet(r.cursor)
		r.received++
		if p.Info.Timestamp.After(writeUntil) {
			time.Sleep(replayerTimeout)
			if pb.PacketLen() == 0 {
				return pcap.NextErrorTimeoutExpired
			}
			return nil
		}
		if err := pb.Append(p); err != nil {
			return err
		}
	}

	return nil
}

func (r *replayer) dropExpired(elapsed time.Duration) {
	dropUntil := r.first.Add(elapsed).Add(replayerTimeout / -2)
	for ; r.cursor < r.buf.PacketLen(); r.cursor++ {
		p := r.buf.Packet(r.cursor)
		if p.Info.Timestamp.After(dropUntil) {
			break
		}
		r.dropped++
	}
}

func (r *replayer) Stats() (*pcap.Stats, error) {
	return &pcap.Stats{
		PacketsReceived: r.received,
		PacketsDropped:  r.dropped,
	}, nil
}

func (r *replayer) fill() error {
	err := r.src.CollectPackets(r.buf)
	if err != nil {
		return err
	}
	r.cursor = 0
	if r.first.IsZero() {
		r.first = r.buf.Packet(0).Info.Timestamp
	}
	return nil
}

func (r *replayer) String() string {
	var nextTimestamp time.Time
	if r.cursor < r.buf.PacketLen() {
		nextTimestamp = r.buf.Packet(r.cursor).Info.Timestamp
	}
	return fmt.Sprintf("{first=%v avail=%v next=%v}", r.first, r.buf.PacketLen()-r.cursor, nextTimestamp)
}

func (r *replayer) log(items ...interface{}) {
	if r.Logger != nil {
		r.Logger.Log(items...)
	}
}
