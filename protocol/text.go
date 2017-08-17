package protocol

import (
	"bytes"
	"github.com/box/memsniff/protocol/model"
	"strconv"
)

var (
	errNoTextResponseFound = newMCError("Could not find VALUE response in header", true)
	errTextProtocolError   = newMCError("text response was malformed", false)
)

var (
	textResponseStart        = []byte("VALUE ")
	textResponseContinuation = []byte("\r\nVALUE ")
	crlf                     = []byte("\r\n")
)

// readText searches d for memcached text protocol VALUE responses.
// It returns the empty slice (i.e. nil) as well as a nil error if no VALUE
// response headers are found in the input.
func readText(d []byte) ([]model.Event, error) {
	var events []model.Event
	for {
		rem, evt, err := readSingleText(d)
		if err == errNoTextResponseFound {
			return events, nil
		}
		if err != nil {
			return events, err
		}
		events = append(events, evt)
		d = rem
	}
}

// readSingleText parses the first complete VALUE response in d and returns a
// GetResponse with the key and size of the response.  It also returns a slice
// containing the remainder of d after the response, if any.
//
// If the entirety of a VALUE response is not contained in d, i.e. the response
// is truncated, readSingleText returns an empty remainder along with a summary
// derived from the response header.
//
// readSingleText uses a heuristic to find a response header.  If d begins
// within the body of a VALUE response and that response includes content that
// resembles a VALUE header, readSingleText may return incorrect results.
func readSingleText(d []byte) ([]byte, model.Event, error) {
	if len(d) < 6 {
		return nil, model.Event{}, errNoTextResponseFound
	}
	var start int
	if bytes.Equal(d[:6], textResponseStart) {
		start = len(textResponseStart)
	} else {
		start = bytes.Index(d, textResponseContinuation)
		if start >= 0 {
			start += len(textResponseContinuation)
		}
	}
	if start < 0 {
		return nil, model.Event{}, errNoTextResponseFound
	}

	d = d[start:]

	endOfFirstLine := bytes.Index(d, crlf)
	if endOfFirstLine < 0 {
		return nil, model.Event{}, errNoTextResponseFound
	}

	// VALUE <key> <flags> <size> [<cas>]\r\n
	//       ^-- d[0]                    ^-- d[endOfFirstLine]
	fields := bytes.SplitN(d[:endOfFirstLine], []byte(" "), 4)
	if len(fields) < 3 {
		return nil, model.Event{}, errTextProtocolError
	}

	size, err := strconv.Atoi(string(fields[2]))
	if err != nil {
		return nil, model.Event{}, errTextProtocolError
	}

	evt := model.Event{
		Type: model.EventGetHit,
		Key:  string(fields[0]),
		Size: size,
	}

	startOfBody := endOfFirstLine + 2
	endOfBody := startOfBody + size + 2 // include terminating CRLF

	if endOfBody > len(d) {
		endOfBody = len(d)
	}

	return d[endOfBody:], evt, nil
}
