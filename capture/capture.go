// Package capture provides utilities for reading packets from network
// interfaces or files.
package capture

import (
	"errors"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"io"
	"time"
)

var (
	ErrNoSource        = errors.New("must specify a network interface or file")
	ErrAmbiguousSource = errors.New("cannot specify both network interface and file")
)

// PacketData represents a single packet's data plus metadata indicating when
// the packet was captured.
type PacketData struct {
	Info gopacket.CaptureInfo
	Data []byte
}

// StatProvider provides statistics on packet capture.
type StatProvider interface {
	// Stats returns statistics compatible with those from a pcap handle.
	Stats() (*pcap.Stats, error)
}

// PacketSource is an abstract source of network packets.
type PacketSource interface {
	// CollectPackets fills pd with packets.
	// Returns the number of packets written to the slice pd[:count].
	CollectPackets(pd []PacketData) (count int, err error)
	// DiscardPacket reads a single packet and discards its contents.
	DiscardPacket() error
	StatProvider
}

type source struct {
	*pcap.Handle
}

// New creates a PacketSource bound to the specified network interface or pcap
// file.  When performing live capture, only the first snapLen bytes of each
// packet are captured.
//
// bufferSize determines the amount of kernel memory (in MiB) to allocate for
// temporary storage. A larger bufferSize can reduce dropped packets as
// revealed by Stats, but use caution as kernel memory is a precious resource.
func New(netInterface string, infile string, snapLen int, bufferSize int, noDelay bool) (PacketSource, error) {
	var err error
	handle, err := makeHandle(netInterface, infile, snapLen, bufferSize)
	if err != nil {
		return nil, err
	}
	if !noDelay && infile != "" {
		return newReplayer(source{handle}, snapLen), nil
	}
	return source{handle}, nil
}

func makeHandle(netInterface string, infile string, snapLen int, bufferSize int) (*pcap.Handle, error) {
	var src *pcap.Handle
	var err error

	if netInterface != "" && infile != "" {
		return nil, ErrAmbiguousSource
	}
	if netInterface != "" {
		src, err = newLiveCapture(netInterface, snapLen, bufferSize)
		if err != nil {
			return nil, err
		}
	} else if infile != "" {
		// OpenOffline interprets "-" as stdin
		src, err = pcap.OpenOffline(infile)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrNoSource
	}

	return src, nil
}

func newLiveCapture(netInterface string, snapLen, bufferSize int) (*pcap.Handle, error) {
	inactive, err := pcap.NewInactiveHandle(netInterface)
	defer inactive.CleanUp()
	if err != nil {
		return nil, err
	}
	err = inactive.SetSnapLen(snapLen)
	if err != nil {
		return nil, err
	}
	err = inactive.SetPromisc(true)
	if err != nil {
		return nil, err
	}
	err = inactive.SetTimeout(10 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	err = inactive.SetBufferSize(bufferSize * 1024 * 1024)
	if err != nil {
		return nil, err
	}

	return inactive.Activate()
}

// NewPacketBuffer allocates space to store size packets of snaplen bytes each.
// The individual Data slices of each PacketData are subslices of a contiguous
// block for better cache performance.  Avoid reslicing them to larger than
// original size, or overlap may cause unexpected behavior.
func NewPacketBuffer(snaplen, size int) []PacketData {
	buf := make([]byte, size*snaplen)
	b := make([]PacketData, size)
	for i := 0; i < size; i++ {
		b[i].Data = buf[i*snaplen : i*snaplen+snaplen]
	}
	return b
}

func (s source) CollectPackets(pd []PacketData) (count int, err error) {
	l := len(pd)
	for i := 0; i < l; i++ {
		// use ZeroCopyReadPacketData to avoid allocation, even though
		// we copy the data later
		buf, ci, err := s.ZeroCopyReadPacketData()
		if (err == io.EOF || err == pcap.NextErrorTimeoutExpired) &&
			i > 0 {
			return i, nil
		}
		if err != nil {
			return 0, err
		}
		// buf is owned by ps and will be overwritten on next call to
		// ZeroCopyReadPacketData
		bytesCopied := copy(pd[i].Data, buf)
		pd[i].Info = ci
		if pd[i].Info.CaptureLength > bytesCopied {
			pd[i].Info.CaptureLength = bytesCopied
		}
	}
	return l, nil
}

func (s source) DiscardPacket() error {
	_, _, err := s.ZeroCopyReadPacketData()
	return err
}
