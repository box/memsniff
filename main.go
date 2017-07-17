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

// memsniff is an interactive console tool for realtime display of memcached
// activity, based on passive inspection of server network traffic.
package main

import (
	"fmt"
	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/capture"
	"github.com/box/memsniff/decode"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/presentation"
	"github.com/box/memsniff/protocol"
	flag "github.com/spf13/pflag"
	"os"
	"os/signal"
	"time"
)

var (
	netInterface = flag.StringP("interface", "i", "", "network interface to sniff")
	infile       = flag.StringP("read", "r", "", "file to read (- for stdin)")
	snapLen      = flag.IntP("snaplen", "s", 1600, "maximum bytes of packet data to capture")
	bufferSize   = flag.IntP("buffersize", "b", 8, "MiB of kernel buffer for packet data")

	decodeWorkers   = flag.Int("decodeworkers", 8, "number of decode workers")
	analysisWorkers = flag.Int("analysisworkers", 32, "number of analysis workers")
	profiles        = flag.StringSlice("profile", []string{}, "profile types to store (one or more of cpu, heap, block)")

	filter     = flag.StringP("filter", "f", "", "regex pattern of cache keys to track")
	footprint  = flag.Int("footprint", 100, "number of keys to track per analysis worker")
	reportSize = flag.IntP("top", "t", 100, "number of keys to report")
	interval   = flag.IntP("interval", "n", 1, "report top keys every this many seconds")
	cumulative = flag.Bool("cumulative", false, "accumulate keys over all time instead of an interval")

	noDelay = flag.Bool("nodelay", false, "replay from file at maximum speed instead of rate of original capture")
	noGui   = flag.Bool("nogui", false, "disable interactive interface")
)

var logger = &log.ProxyLogger{}

func main() {
	flag.Parse()

	// Actually execute startProfiling(), capture the returned function (which writes
	// profiling results), and defer it to be executed when main() exits.
	defer startProfiling()()

	buffered := &log.BufferLogger{}
	logger.SetLogger(buffered)
	logger.Log(fmt.Sprintf("memsniff version %v (revision %v) starting", Version, GitRevision))

	analysisPool := analysis.New(*analysisWorkers, *footprint, *reportSize)
	analysisPool.SetFilterPattern(*filter)
	packetSource, err := capture.New(*netInterface, *infile, *snapLen, *bufferSize, *noDelay)
	if err != nil {
		(&log.ConsoleLogger{}).Log(err)
		os.Exit(2)
	}

	decodePool := decode.NewPool(logger, *snapLen, *decodeWorkers, packetSource, packetHandler(analysisPool))
	go decodePool.Run()

	if *noGui {
		logger.SetLogger(log.ConsoleLogger{})
		buffered.WriteTo(logger)

		exitChan := make(chan os.Signal)
		signal.Notify(exitChan, os.Interrupt)
		<-exitChan
	} else {
		updateInterval := time.Duration(*interval) * time.Second
		statProvider := statGenerator(packetSource, decodePool, analysisPool)
		cui := presentation.New(analysisPool, updateInterval, *cumulative, statProvider)

		logger.SetLogger(cui)
		go buffered.WriteTo(cui)

		err := cui.Run()
		if err != nil {
			logger.SetLogger(log.ConsoleLogger{})
			buffered.WriteTo(logger)
			logger.Log(err)
		}
	}
}

var stats presentation.Stats

func statGenerator(captureProvider capture.StatProvider, decodePool *decode.Pool, analysisPool *analysis.Pool) presentation.StatProvider {
	return func() presentation.Stats {
		captureStats, err := captureProvider.Stats()
		if err == nil {
			stats.PacketsReceived = captureStats.PacketsReceived
			stats.PacketsDroppedKernel = captureStats.PacketsIfDropped + captureStats.PacketsDropped
		}

		decodeStats := decodePool.Stats()
		stats.PacketsDroppedParser = decodeStats.PacketsDropped

		analysisStats := analysisPool.Stats()
		stats.ResponsesParsed = analysisStats.ResponsesHandled
		stats.PacketsDroppedAnalysis = analysisStats.ResponsesDropped

		stats.PacketsDroppedTotal = stats.PacketsDroppedKernel + stats.PacketsDroppedParser + stats.PacketsDroppedAnalysis

		return stats
	}
}

func packetHandler(analysisPool *analysis.Pool) func(p decode.DecodedPacket) {
	return func(p decode.DecodedPacket) {
		if p.TCP.SrcPort == 11211 && len(p.Payload) > 0 {
			// ignore err, just try to read what we can
			responses, _ := protocol.Read(p.Payload)
			for _, r := range responses {
				analysisPool.HandleGetResponse(r)
			}
		}
	}
}
