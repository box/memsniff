// Package presentation implements interactive and non-interactive reporting
// of cache activity.
package presentation

import (
	"fmt"
	"github.com/box/memsniff/analysis"
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
	analysis       *analysis.Pool
	interval       time.Duration
	statProvider   StatProvider
	messages       []string
	msgChan        chan string
	prevReport     analysis.Report
	cumulative     bool
	paused         bool
	reportFilePath string
	extras         map[string]string
}

// Stats collects statistics on runtime performance to be displayed to the user.
type Stats struct {
	// count of packets that entered the kernel BPF
	PacketsEnteredFilter int
	// count of packets that passed the BPF and queued or dropped by the kernel
	PacketsPassedFilter int
	// count of packets received from pcap
	PacketsCaptured int
	// count of packets dropped due to kernel buffer overflow
	PacketsDroppedKernel int
	// count of packets dropped due to no decoder available
	PacketsDroppedParser int
	// count of packets dropped due to analysis queue being full
	PacketsDroppedAnalysis int
	PacketsDroppedTotal    int
	ResponsesParsed        int
}

// StatProvider returns a snapshot of current runtime statistics.
type StatProvider func() Stats

// New returns a UIHandler that is ready to run
func New(analysisPool *analysis.Pool, interval time.Duration, cumulative bool, statProvider StatProvider, reportFilePath string, extras map[string]string) UIHandler {
	return &uiContext{
		analysis:       analysisPool,
		interval:       interval,
		statProvider:   statProvider,
		msgChan:        make(chan string, 128),
		prevReport:     analysis.Report{},
		cumulative:     cumulative,
		paused:         false,
		reportFilePath: reportFilePath,
		extras:         extras,
	}
}

func (u uiContext) Run() error {
	return u.runReporter()
}

func (u uiContext) Log(items ...interface{}) {
	u.msgChan <- fmt.Sprintln(items...)
}
