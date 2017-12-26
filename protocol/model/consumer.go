package model

import (
	"io"

	"github.com/box/memsniff/log"
	"github.com/google/gopacket/tcpassembly"
)

// EventType described what sort of event has occurred.
type EventType int

const (
	// EventUnknown is an unhandled event.
	EventUnknown EventType = iota
	// EventGetHit is a successful data retrieval that returned data.
	EventGetHit
	// EventGetMiss is a data retrieval that did not result in data.
	EventGetMiss
)

// Event is a single event in a datastore conversation
type Event struct {
	// Type of the event.
	Type EventType
	// Datastore key affected by this event.
	Key string
	// Size of the datastore value affected by this event.
	Size int
}

// EventHandler consumes a batch of events.
type EventHandler func(evts []Event)

// Reader represents a subset of the bufio.Reader interface.
type Reader interface {
	// Discard skips the next n bytes, returning the number of bytes discarded.
	// If Discard skips fewer than n bytes, it also returns an error.
	Discard(n int) (discarded int, err error)

	// ReadN returns the next n bytes.
	//
	// If EOF is encountered before reading n bytes, the available bytes are returned
	// along with ErrUnexpectedEOF.
	//
	// The returned buffer is only valid until the next call to ReadN or ReadLine.
	ReadN(n int) ([]byte, error)

	// ReadLine returns a single line, not including the end-of-line bytes.
	// The returned buffer is only valid until the next call to ReadN or ReadLine.
	// ReadLine either returns a non-nil line or it returns an error, never both.
	//
	// The text returned from ReadLine does not include the line end ("\r\n" or "\n").
	// No indication or error is given if the input ends without a final line end.
	ReadLine() ([]byte, error)
}

// ConsumerSource buffers tcpassembly.Stream data and exposes it as a closeable Reader.
type ConsumerSource interface {
	Reader
	io.Closer
	tcpassembly.Stream
	Reset()
}

// State is a function to be called to process buffered data in a particular connection state.
type State func() error

// Consumer is a generic reader of a datastore conversation.
type Consumer struct {
	// A Logger instance for debugging.  No logging is done if nil.
	Logger log.Logger
	// Handler receives events derived from the conversation.
	Handler EventHandler
	// ClientReader exposes data sent by the client to the server.
	ClientReader ConsumerSource
	// ServerReader exposes data send by the server to the client.
	ServerReader ConsumerSource

	Run   func()
	State State

	eventBuf []Event
}

func (c *Consumer) AddEvent(evt Event) {
	if c.eventBuf == nil {
		c.eventBuf = make([]Event, 0, 128)
	}
	c.eventBuf = append(c.eventBuf, evt)
	if len(c.eventBuf) == cap(c.eventBuf) {
		c.FlushEvents()
	}
}

func (c *Consumer) FlushEvents() {
	c.Handler(c.eventBuf)
	c.eventBuf = c.eventBuf[:0]
}

func (c *Consumer) ClientStream() tcpassembly.Stream {
	return (*ClientStream)(c)
}

func (c *Consumer) ServerStream() tcpassembly.Stream {
	return (*ServerStream)(c)
}

// ClientStream is a view on a Consumer that consumes tcpassembly data from the client
type ClientStream Consumer

func (cs *ClientStream) Reassembled(rs []tcpassembly.Reassembly) {
	cs.ClientReader.Reassembled(rs)
	(*Consumer)(cs).Run()
}

func (cs *ClientStream) ReassemblyComplete() {
	cs.ClientReader.ReassemblyComplete()
	(*Consumer)(cs).FlushEvents()
}

// ServerStream is a view on a Consumer that consumes tcpassembly data from the server
type ServerStream Consumer

func (ss *ServerStream) Reassembled(rs []tcpassembly.Reassembly) {
	ss.ServerReader.Reassembled(rs)
	(*Consumer)(ss).Run()
}

func (ss *ServerStream) ReassemblyComplete() {
	ss.ServerReader.ReassemblyComplete()
	(*Consumer)(ss).FlushEvents()
}
