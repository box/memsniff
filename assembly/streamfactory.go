package assembly

import (
	"encoding/binary"
	"fmt"
	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/assembly/reader"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/mctext"
	"github.com/box/memsniff/protocol/model"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/tcpassembly"
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
	analysis      *analysis.Pool
	memcachePorts []int

	halfOpen map[connectionKey]*model.Consumer
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

	return stream
}

func (sf *streamFactory) createConsumer(ck connectionKey) *model.Consumer {
	client, server := reader.NewPair()
	client.LossErrors = true
	server.LossErrors = true

	c := &mctext.Consumer{
		//Logger:       log.NewContext(sf.logger, ck.DstString()),
		Handler:      sf.analysis.HandleEvents,
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
