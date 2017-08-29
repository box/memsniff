package assembly

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/assembly/reader"
	"github.com/box/memsniff/decode"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/mctext"
	"github.com/box/memsniff/protocol/model"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/tcpassembly"
	"time"
)

var (
	errQueueFull = errors.New("assembly worker queue full")
)

type connectionKey struct {
	netFlow       gopacket.Flow
	transportFlow gopacket.Flow
}

func (c *connectionKey) Reverse() connectionKey {
	return connectionKey{
		netFlow:       c.netFlow.Reverse(),
		transportFlow: c.transportFlow.Reverse(),
	}
}

func (c *connectionKey) String() string {
	return fmt.Sprintf("%s:%s -> %s:%s",
		c.netFlow.Src(),
		c.transportFlow.Src(),
		c.netFlow.Dst(),
		c.transportFlow.Dst())
}

func (c *connectionKey) DstString() string {
	return fmt.Sprintf("%s:%s", c.netFlow.Dst(), c.transportFlow.Dst())
}

type streamFactory struct {
	logger        log.Logger
	memcachePorts []int
	halfOpen      map[connectionKey]*model.Consumer
	analysis      *analysis.Pool
}

func (sf *streamFactory) IsFromServer(transportFlow gopacket.Flow) bool {
	port := srcPort(transportFlow)
	return isInPortlist(sf.memcachePorts, port)
}

func srcPort(transportFlow gopacket.Flow) int {
	if transportFlow.EndpointType() != layers.EndpointTCPPort {
		panic("non TCP flow")
	}
	return int(binary.BigEndian.Uint16(transportFlow.Src().Raw()))
}

func isInPortlist(ports []int, port int) bool {
	for _, p := range ports {
		if port == p {
			return true
		}
	}
	return false
}

func (sf *streamFactory) New(netFlow, transportFlow gopacket.Flow) tcpassembly.Stream {
	if sf.halfOpen == nil {
		sf.halfOpen = make(map[connectionKey]*model.Consumer)
	}
	ck := connectionKey{
		netFlow:       netFlow,
		transportFlow: transportFlow,
	}
	fromServer := sf.IsFromServer(transportFlow)
	if !fromServer {
		ck = ck.Reverse()
	}

	var c *model.Consumer
	var ok bool
	if c, ok = sf.halfOpen[ck]; ok {
		delete(sf.halfOpen, ck)
	} else {
		c = sf.createConsumer(ck)
		sf.halfOpen[ck] = c
	}

	var stream tcpassembly.Stream
	if fromServer {
		stream = c.ServerReader
	} else {
		stream = c.ClientReader
	}

	//return wrap{stream, transportFlow}
	return stream
}

func (sf *streamFactory) createConsumer(ck connectionKey) *model.Consumer {
	h := func(evt model.Event) {
		sf.analysis.HandleEvents([]model.Event{evt})
	}

	client, server := reader.NewPair()
	client.LossErrors = true
	server.LossErrors = true

	c := &mctext.Consumer{
		//Logger:       log.NewContext(sf.logger, ck.DstString()),
		Handler:      h,
		ClientReader: client,
		ServerReader: server,
	}
	go c.Run()
	return (*model.Consumer)(c)
}

func (sf *streamFactory) log(items ...interface{}) {
	if sf.logger != nil {
		sf.logger.Log(items...)
	}
}

type workItem struct {
	dps    []*decode.DecodedPacket
	doneCh chan<- struct{}
}

type worker struct {
	logger    log.Logger
	assembler *tcpassembly.Assembler
	wiCh      chan workItem
}

func NewWorker(logger log.Logger, analysis *analysis.Pool, memcachePorts []int) worker {
	sf := streamFactory{
		logger:        logger,
		memcachePorts: memcachePorts,
		analysis:      analysis,
	}
	w := worker{
		logger:    logger,
		assembler: tcpassembly.NewAssembler(tcpassembly.NewStreamPool(&sf)),
		wiCh:      make(chan workItem, 128),
	}
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
	for {
		select {
		case <-ticker.C:
			f, c := w.assembler.FlushOlderThan(time.Now().Add(-time.Minute))
			if f > 0 || c > 0 {
				w.log("Flushed", f, "Closed", c)
			}

		case wi, ok := <-w.wiCh:
			if !ok {
				return
			}
			for _, dp := range wi.dps {
				w.assembler.Assemble(dp.NetFlow, &dp.TCP)
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
