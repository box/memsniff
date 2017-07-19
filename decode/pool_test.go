package decode

import (
	"github.com/box/memsniff/capture"
	"github.com/google/gopacket/pcap"
	"io"
	"runtime"
	"strings"
	"sync"
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
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(dps []*DecodedPacket) {
		defer wg.Done()
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

	p := NewPool(testLogger{t}, 8, nil, handler)
	w := <-p.readyQ
	_ = w.buf().Append(capture.PacketData{})
	w.work()
	wg.Wait()
}

// emptySource is a capture.PacketSource that immediately returns EOF.
type emptySource struct{}

func (es emptySource) CollectPackets(pb *capture.PacketBuffer) error {
	return io.EOF
}

func (es emptySource) DiscardPacket() error {
	return nil
}

func (es emptySource) Stats() (*pcap.Stats, error) {
	return &pcap.Stats{}, nil
}

// TestGoroutineCount checks that Pool starts the expected number of worker
// goroutines and shuts them down at the end of input.
func TestGoroutineCount(t *testing.T) {
	workers := 4
	before := runtime.NumGoroutine()
	p := NewPool(testLogger{t}, workers, &emptySource{}, nil)
	after := runtime.NumGoroutine()
	if after != before+workers {
		t.Error("NewPool started", after-before, "new goroutines instead of", workers)
	}

	p.Run()
	time.Sleep(100 * time.Millisecond)

	afterRun := runtime.NumGoroutine()
	if afterRun != before {
		t.Error("Pool left behind", afterRun-before, "goroutines")
	}
}
