package capture

import (
	"github.com/google/gopacket"
	"testing"
	"time"
)

var (
	ci = gopacket.CaptureInfo{
		Timestamp: time.Date(1, 2, 3, 4, 5, 6, 7, time.UTC),
	}
)

func TestAddEmptyPacket(t *testing.T) {
	uut := NewPacketBuffer(1, 1)
	pd := PacketData{ci, make([]byte, 0)}
	err := uut.Append(pd)
	if err != nil {
		t.Fail()
	}
}

func TestTooManyPackets(t *testing.T) {
	uut := NewPacketBuffer(1, 1)
	pd := PacketData{ci, make([]byte, 0)}
	err := uut.Append(pd)
	if err != nil {
		t.Fail()
	}
	err = uut.Append(pd)
	if err != ErrPacketsFull {
		t.Fail()
	}
}

func TestTooManyBytes(t *testing.T) {
	uut := NewPacketBuffer(10, 10)
	ci1 := ci
	ci1.Length = 5
	ci1.CaptureLength = 5
	pd := PacketData{ci1, make([]byte, 5)}

	err := uut.Append(pd)
	if err != nil {
		t.Error("Got error from first Append:", err)
	}
	err = uut.Append(pd)
	if err != nil {
		t.Error("Got error from second Append:", err)
	}

	if uut.PacketLen() != 2 {
		t.Error("PacketLen returned", uut.PacketLen(), "instead of 2")
	}

	err = uut.Append(pd)
	if err != ErrBytesFull {
		t.Error("Got error from third Append:", err)
	}
}

func TestCorrectSize(t *testing.T) {
	uut := NewPacketBuffer(10, 100)
	for i := 0; i < 10; i++ {
		data := make([]byte, i)
		for j := 0; j < len(data); j++ {
			data[j] = byte(i)
		}
		var pd PacketData
		pd.Data = data
		pd.Info.CaptureLength = len(data)
		if err := uut.Append(pd); err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < 10; i++ {
		pd := uut.Packet(i)
		if len(pd.Data) != i {
			t.Error("Packet", i, "had length", len(pd.Data), "expected", i)
		}
		if pd.Info.CaptureLength != i {
			t.Error("Packet", i, "had CaptureLength", pd.Info.CaptureLength, "expected", i)
		}
		for _, x := range pd.Data {
			if int(x) != i {
				t.Error("Packet", i, "had data byte", x, "expected", i)
			}
		}
	}
}

func TestBytesRemaining(t *testing.T) {
	l := 100
	uut := NewPacketBuffer(1, l)
	if uut.BytesRemaining() != l {
		t.Fail()
	}
	pd := PacketData{
		Info: gopacket.CaptureInfo{
			Length:        l,
			CaptureLength: l,
		},
		Data: make([]byte, l),
	}
	err := uut.Append(pd)
	if err != nil {
		t.Error(err)
	}
	if uut.BytesRemaining() != 0 {
		t.Fail()
	}
}
