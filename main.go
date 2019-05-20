// memsniff is an interactive console tool for realtime display of memcached
// activity, based on passive inspection of server network traffic.
package main

import (
	"fmt"
	"github.com/box/memsniff/protocol/model"
	"os"
	"os/signal"
	"time"

	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/assembly"
	"github.com/box/memsniff/capture"
	"github.com/box/memsniff/decode"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/presentation"
	flag "github.com/spf13/pflag"
)

var (
	netInterface = flag.StringP("interface", "i", "", "network interface to sniff")
	infile       = flag.StringP("read", "r", "", "file to read (- for stdin)")
	bufferSize   = flag.IntP("buffersize", "b", 8, "MiB of kernel buffer for packet data")
	protocol     = flag.StringP("protocol", "P", "infer", "datastore protocol (one of mctext, redis, or infer to guess based on content)")
	ports        = flag.IntSliceP("ports", "p", []int{6379, 11211}, "ports to listen on")

	assemblyWorkers = flag.Int("assemblyworkers", 8, "number of TCP assembly workers")
	decodeWorkers   = flag.Int("decodeworkers", 8, "number of decode workers")
	analysisWorkers = flag.Int("analysisworkers", 32, "number of analysis workers")
	profiles        = flag.StringSlice("profile", []string{}, "profile types to store (one or more of cpu, heap, block)")

	filter     = flag.String("filter", "", "regex pattern of cache keys to track")
	format     = flag.StringP("format", "f", "key,max(size),sum(size)", "fields (key, size) and aggregates (avg, max, min, sum, p50 (median), p995 (99.5th percentile), etc.) to display")
	interval   = flag.IntP("interval", "n", 1, "report top keys every this many seconds")
	cumulative = flag.Bool("cumulative", false, "accumulate keys over all time instead of an interval")

	reportFilePath = flag.String("reportfile", "/var/log/memsniff.report.log", "report file where the report will get printed")

	noDelay = flag.Bool("nodelay", false, "replay from file at maximum speed instead of rate of original capture")
	noGui   = flag.Bool("nogui", false, "disable interactive interface")

	displayVersion = flag.Bool("version", false, "display version information")
)

var logger = &log.ProxyLogger{}

func main() {
	flag.Parse()
	if *displayVersion {
		log.ConsoleLogger{}.Log(fmt.Sprintf("memsniff version %v (revision %v)", Version, GitRevision))
		return
	}

	// Actually execute startProfiling(), capture the returned function (which writes
	// profiling results), and defer it to be executed when main() exits.
	defer startProfiling()()

	buffered := &log.BufferLogger{}
	logger.SetLogger(buffered)

	analysisPool, err := analysis.New(*analysisWorkers, *format)
	if err != nil {
		log.ConsoleLogger{}.Log(err)
		os.Exit(1)
	}
	if err = analysisPool.SetFilterPattern(*filter); err != nil {
		log.ConsoleLogger{}.Log(err)
		os.Exit(1)
	}

	protocolType := model.GetProtocolType(*protocol)
	if protocolType == model.ProtocolUnknown {
		log.ConsoleLogger{}.Log("unknown protocol: ", *protocol)
		os.Exit(1)
	}

	packetSource, err := capture.New(*netInterface, *infile, *bufferSize, *noDelay, *ports)
	if err != nil {
		log.ConsoleLogger{}.Log(err)
		os.Exit(2)
	}

	decodePool := decode.NewPool(logger, *decodeWorkers, packetSource, packetHandler(protocolType, analysisPool))
	eofChan := make(chan struct{}, 1)
	go func() {
		decodePool.Run()
		eofChan <- struct{}{}
	}()

	if *noGui {
		logger.SetLogger(log.ConsoleLogger{})
		buffered.WriteTo(logger)

		exitChan := make(chan os.Signal, 1)
		signal.Notify(exitChan, os.Interrupt)
		select {
		case <-exitChan:
		case <-eofChan:
		}
	} else {
		updateInterval := time.Duration(*interval) * time.Second
		statProvider := statGenerator(packetSource, decodePool, analysisPool)
		cui := presentation.New(analysisPool, updateInterval, *cumulative, statProvider, *reportFilePath)

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
			stats.PacketsEnteredFilter = captureStats.PacketsReceived
			stats.PacketsDroppedKernel = captureStats.PacketsIfDropped + captureStats.PacketsDropped
		}

		decodeStats := decodePool.Stats()
		stats.PacketsCaptured = decodeStats.PacketsCaptured
		stats.PacketsDroppedParser = decodeStats.PacketsDropped

		analysisStats := analysisPool.Stats()
		stats.ResponsesParsed = int(analysisStats.EventsHandled)
		stats.PacketsDroppedAnalysis = int(analysisStats.EventsDropped)

		stats.PacketsPassedFilter = stats.PacketsDroppedKernel + stats.PacketsCaptured
		stats.PacketsDroppedTotal = stats.PacketsDroppedKernel + stats.PacketsDroppedParser + stats.PacketsDroppedAnalysis

		return stats
	}
}

func packetHandler(protocol model.ProtocolType, analysisPool *analysis.Pool) func(dps []*decode.DecodedPacket) {
	pool := assembly.New(logger, analysisPool, protocol, *ports, *assemblyWorkers)
	return func(dps []*decode.DecodedPacket) {
		err := pool.HandlePackets(dps)
		if err != nil {
			logger.Log(err)
		}
	}
}
