package analysis

import (
	"sort"
	"time"
)

// ReportRow contains activity information for a single cache key.
type ReportRow struct {
	Key    []string `json:"key"`
	Values []int64  `json:"values"`
}

// Report represents key activity submitted to a Pool since the last call to
// Reset.
type Report struct {
	// when this report was generated
	Timestamp   time.Time   `json:"timestamp"`
	KeyColNames []string    `json:"key_col_names"`
	ValColNames []string    `json:"val_col_names"`
	Rows        []ReportRow `json:"rows"`
}

func (r *Report) SortBy(columns ...int) {
	sort.Sort(&reportSort{r, columns})
}

type reportSort struct {
	report      *Report
	sortColumns []int
}

func (rs *reportSort) Len() int {
	return len(rs.report.Rows)
}

func (rs *reportSort) Less(a, b int) bool {
	for _, col := range rs.sortColumns {
		descending := col < 0
		if descending {
			col = -col
		}

		var onValue bool
		if col >= len(rs.report.KeyColNames) {
			onValue = true
			col -= len(rs.report.KeyColNames)
		}

		if onValue {
			// sorting by int64 values
			valA := rs.report.Rows[a].Values[col]
			valB := rs.report.Rows[b].Values[col]
			if valA == valB {
				continue
			}
			if descending {
				return valA > valB
			}
			return valA < valB
		}

		// sorting by string
		valA := rs.report.Rows[a].Key[col]
		valB := rs.report.Rows[b].Key[col]
		if valA == valB {
			continue
		}
		if descending {
			return valA > valB
		}
		return valA < valB
	}
	return false
}

func (rs *reportSort) Swap(a, b int) {
	rs.report.Rows[a], rs.report.Rows[b] = rs.report.Rows[b], rs.report.Rows[a]
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
	var rows []ReportRow
	for _, w := range p.workers {
		workerEntries := w.result()
		if shouldReset {
			w.reset()
		}
		for i := range workerEntries.keyFields {
			row := ReportRow{
				Key:    workerEntries.keyFields[i],
				Values: workerEntries.aggResults[i],
			}
			rows = append(rows, row)
		}
	}
	return Report{
		Timestamp:   time.Now(),
		KeyColNames: p.kaf.KeyFields,
		ValColNames: p.kaf.AggFields,
		Rows:        rows,
	}
}
