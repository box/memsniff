package model

import (
	"github.com/google/gopacket/tcpassembly"
	"io"
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

// Reader represents a subset of the bufio.Reader interface.
type Reader interface {
	io.Reader

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
}

// Consumer is a generic reader of a datastore conversation.
type Consumer struct {
	// Handler receives events derived from the conversation.
	Handler EventHandler
	// ClientReader exposes data sent by the client to the server.
	ClientReader ConsumerSource
	// ServerReader exposes data send by the server to the client.
	ServerReader ConsumerSource
}

// Event is a single event in a datastore conversation
type Event struct {
	// Type of the event.
	Type EventType
	// Datastore key affected by this event.
	Key string
	// Size of the datastore value affected by this event.
	Size int
}

// EventHandler consumes a single event.
type EventHandler func(evt Event)
