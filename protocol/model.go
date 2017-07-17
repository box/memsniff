// Package protocol provides heuristic-based parsing of the memcached protocol.
package protocol

import (
	"errors"
)

// GetResponse summarizes a single cache value response to a get request.
type GetResponse struct {
	// the requested cache key
	Key []byte
	// the size of the associated cache value
	Size int
}

// MCError represents an error encountered while parsing a memcached response
// stream.
type MCError struct {
	error
	// true if it is possible to continue parsing the response stream
	IsResumable bool
}

func newMCError(msg string, isResumable bool) error {
	return MCError{errors.New(msg), isResumable}
}

// Read parses a block of memcached response stream data.
func Read(d []byte) ([]*GetResponse, error) {
	return readText(d)
}
