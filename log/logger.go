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

// Package log provides flexible logging redirection.
package log

import (
	"log"
	"sync"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// Logger is a general-purpose interface for displaying messages.
type Logger interface {
	Log(items ...interface{})
}

// ConsoleLogger sends its input directly to the default Go logger tied to
// os.Stderr.
type ConsoleLogger struct{}

// Log sends a message to the default Go logger.
func (l ConsoleLogger) Log(items ...interface{}) {
	log.Println(items...)
}

// BufferLogger stores all log messages.  It is primarily useful during startup
// when the eventual Logger implementation to use is not yet known.
type BufferLogger struct {
	sync.Mutex
	buf [][]interface{}
}

// Log records a formatted log message for future retrieval.
func (b *BufferLogger) Log(items ...interface{}) {
	b.Lock()
	defer b.Unlock()
	b.buf = append(b.buf, items)
}

// WriteTo sends recorded log messages to another Logger in order.
func (b *BufferLogger) WriteTo(l Logger) {
	b.Lock()
	defer b.Unlock()
	for _, items := range b.buf {
		l.Log(items...)
	}
}

// ProxyLogger is a stable recipient of log messages that can be configured
// at runtime to forward the messages to different Logger implementations.
type ProxyLogger struct {
	sync.RWMutex
	l Logger
}

// Log forwards its message to the underlying Logger implementation.  Panics if
// called before the first call to SetLogger.
func (p *ProxyLogger) Log(items ...interface{}) {
	p.RLock()
	defer p.RUnlock()
	p.l.Log(items...)
}

// SetLogger assigns an underlying Logger implementation to this ProxyLogger.
func (p *ProxyLogger) SetLogger(l Logger) {
	p.Lock()
	defer p.Unlock()
	p.l = l
}

// ContextLogger is a logger that prepends a string to each log message.
type ContextLogger struct {
	context string
	l       Logger
}

// NewContext creates a new ContextLogger using l as the underlying destination.
func NewContext(l Logger, context string) Logger {
	return &ContextLogger{context, l}
}

// Log prepends the present context from NewContext and passes the resulting
// log message to the underlying Logger.
func (c *ContextLogger) Log(items ...interface{}) {
	args := append([]interface{}{c.context}, items...)
	c.l.Log(args...)
}
