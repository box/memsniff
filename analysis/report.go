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

package analysis

import (
	"github.com/box/memsniff/hotlist"
	"sort"
	"time"
)

// KeyReport contains activity information for a single cache key.
type KeyReport struct {
	// cache key
	Name string
	// size of the cache value in bytes
	Size int
	// number of requests for this cache key
	RequestsEstimate int
	// amount of bandwidth consumed by traffic for this cache key in bytes
	TrafficEstimate int
}

// Report represents key activity submitted to a Pool since the last call to
// Reset.
type Report struct {
	// when this report was generated
	Timestamp time.Time
	// key reports in descending order by TrafficEstimate
	Keys []KeyReport
}

// Len implements sort.Interface for Report.
func (r Report) Len() int {
	return len(r.Keys)
}

// Less implements sort.Interface for Report, sorting KeyReports in descending
// order by TrafficEstimate.
func (r Report) Less(i, j int) bool {
	return r.Keys[j].TrafficEstimate < r.Keys[i].TrafficEstimate
}

// Swap implements sort.Interface for Report.
func (r Report) Swap(i, j int) {
	r.Keys[i], r.Keys[j] = r.Keys[j], r.Keys[i]
}

// Report returns a summary of activity recorded in this Pool since the last
// call to Reset.
//
// The returned report does not represent a consistent snapshot since
// information is collected from workers concurrent with new information
// coming in.
//
// If shouldReset is true, then a best effort will be made to clear data
// in the Pool while building the report.  Since clearing data is an
// asynchronous operation across the workers in the pool, some information
// may be carried over between successive reports, and some data may be
// lost entirely.
func (p *Pool) Report(shouldReset bool) Report {
	allEntries := make([]hotlist.Entry, 0, p.reportSize*len(p.workers))
	for _, w := range p.workers {
		workerEntries := w.top(p.reportSize)
		if shouldReset {
			w.reset()
		}
		allEntries = append(allEntries, workerEntries...)
	}

	reportSize := len(allEntries)
	if reportSize > p.reportSize {
		reportSize = p.reportSize
	}

	ret := Report{
		Timestamp: time.Now(),
		Keys:      make([]KeyReport, 0, reportSize),
	}

	for _, e := range allEntries {
		ret.Keys = append(ret.Keys, keyReport(e))
	}

	sort.Sort(ret)

	return ret
}

func keyReport(e hotlist.Entry) KeyReport {
	ki := e.Item().(keyInfo)
	return KeyReport{
		Name:             ki.name,
		Size:             ki.size,
		RequestsEstimate: e.Count(),
		TrafficEstimate:  e.Count() * ki.size,
	}
}
