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

package capture

import (
	"fmt"
	"github.com/google/gopacket/pcap"
	"time"
)

// replayer throttles results from a PacketSource according to the Timestamp
// accompanying each packet.  It is most useful for recreating the input rate
// of a previously captured pcap file.
type replayer struct {
	// The wall time that this replayer was created.
	start time.Time
	// The timestamp of the first packet returned from src, usually
	// the first packet in a capture file.
	first time.Time
	// The buffer we ask src to fill as much as possible.
	buf []PacketData
	// A subslice of buf; the packets waiting to be returned to the user.
	avail []PacketData
	// The number of packets returned to the user.  Used by Stats.
	received int
	dropped  int
	snaplen  int
	src      PacketSource
}

// replayerTimeout emulates the default behavior of pcap.ReadPacketData,
// waiting up to 10 ms to assemble a batch of packets.
const replayerTimeout = -pcap.BlockForever

func newReplayer(src PacketSource, snaplen int) PacketSource {
	return &replayer{
		snaplen: snaplen,
		src:     src,
	}
}

func (r *replayer) CollectPackets(pd []PacketData) (count int, err error) {
	if r.start.IsZero() {
		r.start = time.Now()
	}

	if len(r.buf) < len(pd) {
		r.buf = NewPacketBuffer(r.snaplen, len(pd))
		copy(r.buf, r.avail)
		r.avail = r.buf[0:len(r.avail)]
	}

	elapsed := time.Since(r.start)
	r.dropExpired(elapsed)
	for len(r.avail) == 0 {
		err = r.fill()
		if err != nil {
			return 0, err
		}
		r.dropExpired(elapsed)
	}

	writeUntil := r.first.Add(elapsed + replayerTimeout)
	writeOffset := 0
	for i, p := range r.avail {
		if p.Info.Timestamp.After(writeUntil) {
			time.Sleep(replayerTimeout)
			if writeOffset == 0 {
				return 0, pcap.NextErrorTimeoutExpired
			}
			r.avail = r.avail[i:]
			r.received += i
			return writeOffset, nil
		}
		pd[writeOffset].Info = p.Info
		copy(pd[writeOffset].Data, p.Data)
		writeOffset++
	}

	r.received += len(r.avail)
	r.avail = nil
	return writeOffset, nil
}

func (r *replayer) dropExpired(elapsed time.Duration) {
	dropUntil := r.first.Add(elapsed).Add(replayerTimeout / -2)
	toDrop := len(r.avail)
	for i, p := range r.avail {
		if p.Info.Timestamp.After(dropUntil) {
			toDrop = i
			break
		}
	}
	if toDrop > 0 {
		r.dropped += toDrop
		r.avail = r.avail[toDrop:]
	}
}

func (r *replayer) DiscardPacket() error {
	if len(r.avail) == 0 {
		err := r.fill()
		if len(r.avail) == 0 || err != nil {
			return err
		}
	}

	p := r.avail[0]
	offset := p.Info.Timestamp.Sub(r.first)
	elapsed := time.Since(r.start)
	if offset > elapsed+replayerTimeout {
		time.Sleep(replayerTimeout)
		return pcap.NextErrorTimeoutExpired
	}

	r.avail = r.avail[1:]
	r.received++
	return nil
}

func (r *replayer) Stats() (*pcap.Stats, error) {
	return &pcap.Stats{
		PacketsReceived: r.received,
		PacketsDropped:  r.dropped,
	}, nil
}

func (r *replayer) fill() error {
	n, err := r.src.CollectPackets(r.buf)
	r.avail = r.buf[:n]
	if err != nil || n == 0 {
		return err
	}
	if r.first.IsZero() {
		r.first = r.buf[0].Info.Timestamp
	}
	return nil
}

func (r *replayer) String() string {
	var nextTimestamp time.Time
	if len(r.avail) > 0 {
		nextTimestamp = r.avail[0].Info.Timestamp
	}
	return fmt.Sprintf("{first=%v avail=%v next=%v}", r.first, len(r.avail), nextTimestamp)
}
