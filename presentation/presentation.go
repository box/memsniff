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
