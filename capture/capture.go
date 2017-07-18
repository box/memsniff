// Package capture provides utilities for reading packets from network
// interfaces or files.
package capture

import (
	"errors"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"io"
	"strconv"
	"time"
)

const (
	snapLen = 65535
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
	// CollectPackets fills pb with packets.
	CollectPackets(pb *PacketBuffer) error
	// DiscardPacket reads a single packet and discards its contents.
	DiscardPacket() error
	StatProvider
}

type source struct {
	*pcap.Handle
}

// New creates a PacketSource bound to the specified network interface or pcap
// file.
//
// bufferSize determines the amount of kernel memory (in MiB) to allocate for
// temporary storage. A larger bufferSize can reduce dropped packets as
// revealed by Stats, but use caution as kernel memory is a precious resource.
func New(netInterface string, infile string, bufferSize int, noDelay bool, ports []int) (PacketSource, error) {
	var err error
	handle, err := makeHandle(netInterface, infile, bufferSize)
	if err != nil {
		return nil, err
	}
	bpf, err := portFilter(ports)
	if err != nil {
		return nil, err
	}
	if err = handle.SetBPFFilter(bpf); err != nil {
		return nil, err
	}
	if !noDelay && infile != "" {
		return newReplayer(source{handle}, 1000, 8*1024*1024), nil
	}
	return source{handle}, nil
}

func makeHandle(netInterface string, infile string, bufferSize int) (*pcap.Handle, error) {
	var src *pcap.Handle
	var err error

	if netInterface != "" && infile != "" {
		return nil, ErrAmbiguousSource
	}
	if netInterface != "" {
		src, err = newLiveCapture(netInterface, bufferSize)
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

func portFilter(ports []int) (string, error) {
	if len(ports) < 1 {
		return "", errors.New("need at least one port")
	}

	filterExpr := []byte("tcp src port " + strconv.Itoa(int(ports[0])))
	for _, port := range ports[1:] {
		filterExpr = append(filterExpr,
			[]byte(" or tcp src port "+strconv.Itoa(int(port)))...)
	}

	return string(filterExpr), nil
}

func newLiveCapture(netInterface string, bufferSize int) (*pcap.Handle, error) {
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

func (s source) CollectPackets(pb *PacketBuffer) error {
	pb.Clear()
	l := pb.PacketCap()
	for i := 0; i < l; i++ {
		// use ZeroCopyReadPacketData to avoid allocation, even though
		// we copy the data later
		buf, ci, err := s.ZeroCopyReadPacketData()
		if (err == io.EOF || err == pcap.NextErrorTimeoutExpired) &&
			i > 0 {
			return nil
		}
		if err != nil {
			return err
		}
		// Append makes a copy of the data, which is required because
		// buf is overwritten on the next call to ZeroCopyReadPacketData.
		err = pb.Append(PacketData{ci, buf})
		if err == ErrBytesFull {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s source) DiscardPacket() error {
	_, _, err := s.ZeroCopyReadPacketData()
	return err
}
