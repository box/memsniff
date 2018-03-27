package assembly

import (
	"errors"
	"time"

	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/decode"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
	"github.com/google/gopacket/tcpassembly"
)

var (
	errQueueFull = errors.New("assembly worker queue full")
)

type workItem struct {
	dps    []*decode.DecodedPacket
	doneCh chan<- struct{}
}

type worker struct {
	logger    log.Logger
	assembler *tcpassembly.Assembler
	wiCh      chan workItem
}

func newWorker(logger log.Logger, analysis *analysis.Pool, protocol model.ProtocolType, ports []int) worker {
	sf := streamFactory{
		logger:   logger,
		analysis: analysis,
		protocol: protocol,
		ports:    ports,

		halfOpen: make(map[connectionKey]*model.Consumer),
	}
	w := worker{
		logger:    logger,
		assembler: tcpassembly.NewAssembler(tcpassembly.NewStreamPool(&sf)),
		wiCh:      make(chan workItem, 128),
	}
	// Don't let the Assembly buffer much data in an attempt to compensate for out-of-order
	// and missing packets.  Just report the data as lost downstream and continue.
	w.assembler.MaxBufferedPagesPerConnection = 1
	w.assembler.MaxBufferedPagesTotal = 1
	go w.loop()
	return w
}

func (w worker) handlePackets(dps []*decode.DecodedPacket, doneCh chan<- struct{}) error {
	select {
	case w.wiCh <- workItem{dps, doneCh}:
		return nil
	default:
		return errQueueFull
	}
}

func (w worker) loop() {
	ticker := time.NewTicker(time.Second)
	var mostRecent time.Time
	for {
		select {
		case <-ticker.C:
			f, c := w.assembler.FlushOlderThan(mostRecent.Add(-time.Minute))
			if f > 0 || c > 0 {
				w.log("Flushed", f, "Closed", c)
			}

		case wi, ok := <-w.wiCh:
			if !ok {
				return
			}
			for _, dp := range wi.dps {
				w.assembler.AssembleWithTimestamp(dp.NetFlow, &dp.TCP, dp.Info.Timestamp)
			}
			wi.doneCh <- struct{}{}
		}
	}
}

func (w worker) log(items ...interface{}) {
	if w.logger != nil {
		w.logger.Log(items...)
	}
}
