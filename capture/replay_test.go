package capture

import (
	"github.com/box/memsniff/log"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"testing"
	"time"
)

type testSource struct {
	pd []PacketData
}

func (s *testSource) CollectPackets(pb *PacketBuffer) error {
	pb.Clear()
	if len(s.pd) == 0 {
		return pcap.NextErrorTimeoutExpired
	}
	for _, pd := range s.pd {
		pb.Append(pd)
	}
	s.pd = nil
	return nil
}

func (s *testSource) DiscardPacket() error {
	if len(s.pd) > 0 {
		s.pd = s.pd[1:]
	}
	return nil
}

func (s *testSource) Stats() (*pcap.Stats, error) {
	return &pcap.Stats{}, nil
}

func (s *testSource) AddPacket(t time.Time, d []byte) {
	ci := gopacket.CaptureInfo{
		Timestamp:     t,
		CaptureLength: len(d),
		Length:        len(d),
	}
	s.pd = append(s.pd, PacketData{ci, d})
}

func TestPacing(t *testing.T) {
	start := time.Time{}.Add(time.Hour)
	delay := 4 * replayerTimeout
	ts := &testSource{}
	ts.AddPacket(start, []byte{0})
	ts.AddPacket(start.Add(delay), []byte{1})

	uut := newReplayer(ts, 1000, 8*1024*1024)
	uut.Logger = log.ConsoleLogger{}
	buf := NewPacketBuffer(1000, 1)

	var err error

	err = uut.CollectPackets(buf)
	n := buf.PacketLen()
	if n != 1 {
		t.Error(err)
	}

	// expect no more data until time has passed
	err = uut.CollectPackets(buf)
	n = buf.PacketLen()
	if n != 0 || err != pcap.NextErrorTimeoutExpired {
		t.Error("got", n, "packet too early:", err)
	}

	time.Sleep(replayerTimeout)
	err = uut.CollectPackets(buf)
	n = buf.PacketLen()
	if n != 1 {
		t.Error(err)
	}
}

func TestDrop(t *testing.T) {
	start := time.Time{}.Add(time.Hour)
	delay := 2 * replayerTimeout
	ts := &testSource{}
	ts.AddPacket(start, []byte{0})
	ts.AddPacket(start.Add(delay), []byte{1})

	uut := newReplayer(ts, 1000, 8*1024*1024)
	buf := NewPacketBuffer(1000, 1)

	var err error

	err = uut.CollectPackets(buf)
	n := buf.PacketLen()
	if n != 1 {
		t.Error(err)
	}

	time.Sleep(2 * delay)
	err = uut.CollectPackets(buf)
	if err != pcap.NextErrorTimeoutExpired {
		t.Error(err)
	}

	var s *pcap.Stats
	s, err = uut.Stats()
	if s.PacketsDropped != 1 {
		t.Error("expected a dropped packet")
	}
}
