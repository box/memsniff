// Package presentation implements interactive and non-interactive reporting
// of cache activity.
package presentation

import (
	"fmt"
	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/log"
	"time"
)

// UIHandler is the external API for an interactive user interface.
type UIHandler interface {
	// Run starts this UIHandler running.
	// It does not return unless an error occurs.
	Run() error
	// Log displays a log message to the user.
	Log(items ...interface{})
}

type uiContext struct {
	logger              log.Logger
	analysis            *analysis.Pool
	interval            time.Duration
	statProvider        StatProvider
	messages            []string
	msgChan             chan string
	prevReport          analysis.Report
	cumulative          bool
	paused              bool
	useTermbox          bool
	topX                uint16
	minKeySizeThreshold uint64
	outputFile          string
}

type StatsSet struct {
	Culumative  *Stats
	Incremental *Stats
}

// Stats collects statistics on runtime performance to be displayed to the user.
type Stats struct {
	// count of packets that entered the kernel BPF
	PacketsEnteredFilter int `json:"PacketsEnteredFilter"`
	// count of packets that passed the BPF and queued or dropped by the kernel
	PacketsPassedFilter int `json:"PacketsPassedFilter"`
	// count of packets received from pcap
	PacketsCaptured int `json:"PacketsCaptured"`
	// count of packets dropped due to kernel buffer overflow
	PacketsDroppedKernel int `json:"PacketsDroppedKernel"`
	// count of packets dropped due to no decoder available
	PacketsDroppedParser int `json:"PacketsDroppedParser"`
	// count of packets dropped due to analysis queue being full
	PacketsDroppedAnalysis int `json:"PacketsDroppedAnalysis"`
	PacketsDroppedTotal    int `json:"PacketsDroppedTotal"`
	ResponsesParsed        int `json:"ResponsesParsed"`
}

// Subtracts other from these original set of stats, returning a new Stats
func (s Stats) Diff(other Stats) Stats {
	newStats := s
	newStats.PacketsEnteredFilter = s.PacketsEnteredFilter - other.PacketsEnteredFilter
	newStats.PacketsPassedFilter = s.PacketsPassedFilter - other.PacketsPassedFilter
	newStats.PacketsCaptured = s.PacketsCaptured - other.PacketsCaptured
	newStats.PacketsDroppedKernel = s.PacketsDroppedKernel - other.PacketsDroppedKernel
	newStats.PacketsDroppedParser = s.PacketsDroppedParser - other.PacketsDroppedParser
	newStats.PacketsDroppedAnalysis = s.PacketsDroppedAnalysis - other.PacketsDroppedAnalysis
	newStats.PacketsDroppedTotal = s.PacketsDroppedTotal - other.PacketsDroppedTotal
	newStats.ResponsesParsed = s.ResponsesParsed - other.ResponsesParsed
	return newStats
}

// StatProvider returns a snapshot of current runtime statistics.
type StatProvider func() StatsSet

// New returns a UIHandler that is ready to run
func New(logger log.Logger, analysisPool *analysis.Pool, interval time.Duration, cumulative bool, statProvider StatProvider,
	useTermbox bool, topX uint16, minKeySizeThreshold uint64, outputFile string) UIHandler {

	return &uiContext{
		logger:              logger,
		analysis:            analysisPool,
		interval:            interval,
		statProvider:        statProvider,
		msgChan:             make(chan string, 128),
		prevReport:          analysis.Report{},
		cumulative:          cumulative,
		paused:              false,
		useTermbox:          useTermbox,
		topX:                topX,
		minKeySizeThreshold: minKeySizeThreshold,
		outputFile:          outputFile,
	}
}

func (u uiContext) Run() error {
	if u.useTermbox {
		return u.runTermbox()
	} else {
		return u.runLoggingEventLoop()
	}
}

func (u uiContext) Log(items ...interface{}) {
	u.msgChan <- fmt.Sprint(items...)
}

func (u *uiContext) truncateResultsToMaxAndTopX(rep *analysis.Report) {
	repRows := rep.Rows
	i := 0
	for n, r := range repRows {
		if r.Values[1] >= int64(u.minKeySizeThreshold) && n < int(u.topX) {
			i++
		} else {
			break
		}
	}

	rep.Rows = repRows[:(i)]
}
