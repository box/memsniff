package decode

import (
	"github.com/box/memsniff/capture"
	"github.com/google/gopacket/pcap"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

type testLogger struct {
	t *testing.T
}

func (tl testLogger) Log(items ...interface{}) {
	tl.t.Log(items...)
}

// TestSeparateGoroutine tests that the packet handler is invoked on a separate
// goroutine from the caller
func TestSeparateGoroutine(t *testing.T) {
	handler := func(dps []*DecodedPacket) {
		for i := 1; ; i++ {
			pc, _, _, ok := runtime.Caller(i)
			if !ok {
				break
			}
			name := runtime.FuncForPC(pc).Name()
			if strings.Contains(name, "TestSeparateGoroutine") {
				var stacktrace [4096]byte
				l := runtime.Stack(stacktrace[:], false)
				t.Error(string(stacktrace[:l]))
			}
		}
	}

	p := NewPool(testLogger{t}, &testSource{[]capture.PacketData{{}}}, handler)
	p.Run()
}

type testSource struct {
	packets []capture.PacketData
}

func (ts *testSource) CollectPackets(pb *capture.PacketBuffer) error {
	if len(ts.packets) == 0 {
		return io.EOF
	}
	pb.Append(ts.packets[0])
	ts.packets = ts.packets[1:]
	return nil
}

func (ts *testSource) Stats() (*pcap.Stats, error) {
	return &pcap.Stats{}, nil
}

// TestGoroutineCount checks that Pool shuts down the workers at the end of input.
func TestGoroutineCount(t *testing.T) {
	// allow state to settle, since sometimes Goroutines owned by the runtime exit during this test.
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()
	p := NewPool(testLogger{t}, &testSource{[]capture.PacketData{{}}}, func(dps []*DecodedPacket) {})

	p.Run()
	time.Sleep(100 * time.Millisecond)

	afterRun := runtime.NumGoroutine()
	if afterRun != before {
		t.Error("Pool left behind", afterRun-before, "goroutines")
	}
}
