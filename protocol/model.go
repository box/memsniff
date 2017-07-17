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
