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
	analysis     *analysis.Pool
	interval     time.Duration
	statProvider StatProvider
	messages     []string
	msgChan      chan string
	prevReport   analysis.Report
	cumulative   bool
	paused       bool
}

// Stats collects statistics on runtime performance to be displayed to the user.
type Stats struct {
	PacketsReceived        int
	PacketsDroppedKernel   int
	PacketsDroppedParser   int
	PacketsDroppedAnalysis int
	PacketsDroppedTotal    int
	ResponsesParsed        int
}

// StatProvider returns a snapshot of current runtime statistics.
type StatProvider func() Stats

// New returns a UIHandler that is ready to run
func New(analysisPool *analysis.Pool, interval time.Duration, cumulative bool, statProvider StatProvider) UIHandler {
	return &uiContext{
		analysis:     analysisPool,
		interval:     interval,
		statProvider: statProvider,
		msgChan:      make(chan string, 128),
		prevReport:   analysis.Report{},
		cumulative:   cumulative,
		paused:       false,
	}
}

func (u uiContext) Run() error {
	return u.runTermbox()
}

func (u uiContext) Log(items ...interface{}) {
	u.msgChan <- fmt.Sprintln(items...)
}
