// Copyright 2017 Box, Inc.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decode

import (
	"github.com/box/memsniff/capture"
	"github.com/box/memsniff/log"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

// DecodedPacket holds the broken down structure of a decoded TCP packet.
type DecodedPacket struct {
	Info gopacket.CaptureInfo

	ethParser *gopacket.DecodingLayerParser
	loParser  *gopacket.DecodingLayerParser
	decoded   []gopacket.LayerType
	ether     layers.Ethernet
	lo        layers.Loopback
	dot1q     layers.Dot1Q
	ipv4      layers.IPv4
	ipv6      layers.IPv6
	TCP       layers.TCP
	Payload   gopacket.Payload
	FlowHash  uint64
	NetFlow   gopacket.Flow
}

func (dp *DecodedPacket) init() {
	dp.ethParser = gopacket.NewDecodingLayerParser(dp.ether.LayerType())
	dp.ethParser.AddDecodingLayer(&dp.ether)
	dp.ethParser.AddDecodingLayer(&dp.dot1q)
	dp.ethParser.AddDecodingLayer(&dp.ipv4)
	dp.ethParser.AddDecodingLayer(&dp.ipv6)
	dp.ethParser.AddDecodingLayer(&dp.TCP)
	dp.ethParser.AddDecodingLayer(&dp.Payload)

	dp.loParser = gopacket.NewDecodingLayerParser(dp.lo.LayerType())
	dp.loParser.AddDecodingLayer(&dp.lo)
	dp.loParser.AddDecodingLayer(&dp.dot1q)
	dp.loParser.AddDecodingLayer(&dp.ipv4)
	dp.loParser.AddDecodingLayer(&dp.ipv6)
	dp.loParser.AddDecodingLayer(&dp.TCP)
	dp.loParser.AddDecodingLayer(&dp.Payload)
}

func (dp *DecodedPacket) IsTCP() bool {
	for _, lt := range dp.decoded {
		if lt == layers.LayerTypeTCP {
			return true
		}
	}
	return false
}

// Handler is a user-provided function for processing a single packet.
type Handler func(db DecodedPacket)

type decoder struct {
	logger        log.Logger
	handler       Handler
	largestPacket int
	decoded       DecodedPacket
}

func newDecoder(logger log.Logger, handler Handler) *decoder {
	d := &decoder{
		logger:  logger,
		handler: handler,
	}
	d.decoded.init()
	return d
}

// decode parses a single packet from raw byte data and updates the decoded
// field of d.
//
// decode is not threadsafe.
func (d *decoder) decode(ci gopacket.CaptureInfo, data []byte) {
	dp := d.decoded
	dp.Info = ci
	dp.FlowHash = 0
	dp.Payload = dp.Payload[:0]
	parser := dp.ethParser
	err := parser.DecodeLayers(data, &dp.decoded)
	if !dp.IsTCP() {
		parser = dp.loParser
		err = parser.DecodeLayers(data, &dp.decoded)
	}
	if err != nil {
		d.logger.Log("Error from DecodeLayers:", err)
	}

	if parser.Truncated && ci.Length > d.largestPacket {
		d.logger.Log("Packet of length", ci.Length, "truncated. Consider increasing snaplen.")
		d.largestPacket = ci.Length
	}
	var netFlow gopacket.Flow
	for _, layer := range dp.decoded {
		switch layer {
		case layers.LayerTypeIPv4:
			netFlow = dp.ipv4.NetworkFlow()
		case layers.LayerTypeIPv6:
			netFlow = dp.ipv6.NetworkFlow()
		case layers.LayerTypeTCP:
			dp.FlowHash = hashCombine(netFlow.FastHash(), dp.TCP.TransportFlow().FastHash())
		default:
		}
	}
}

// decode parses a batch of packets from raw byte data and invokes d's handler
// for each packet.
//
// decodeBatch is not threadsafe.
func (d *decoder) decodeBatch(pds []capture.PacketData) {
	for _, pd := range pds {
		d.decode(pd.Info, pd.Data)
		d.handler(d.decoded)
	}
}

// based on boost::hash_combine
// http://www.boost.org/doc/libs/1_63_0/boost/functional/hash/hash.hpp
func hashCombine(h, k uint64) uint64 {
	m := uint64(0xc6a4a7935bd1e995)
	r := uint64(47)

	k *= m
	k ^= k >> r
	k *= m

	h ^= k
	h *= m

	return h + 0xe6546b64
}
