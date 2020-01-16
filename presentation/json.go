package presentation

import (
	"encoding/json"
	"github.com/box/memsniff/analysis"
	"os"
	"time"
)

func (u *uiContext) runLoggingEventLoop() error {
	updateTick := time.NewTicker(u.interval)
	defer updateTick.Stop()
	if err := u.update(); err != nil {
		return err
	}

	for {
		select {
		case <-updateTick.C:
			if err := u.updateReport(); err != nil {
				return err
			}

		case msg := <-u.msgChan:
			u.logger.Log(msg)
		}
	}
}

func (u *uiContext) updateReport() error {
	rep := u.analysis.Report(!u.cumulative)
	rep.SortBy(-2)

	numKeysSeen := len(rep.Rows)
	totalBandwidthUsed := totalBytesUseForKeys(rep.Rows)

	s := u.statProvider()

	u.truncateResultsToMaxAndTopX(&rep)
	u.prevReport = rep
	reportedKeysBandwidth := totalBytesUseForKeys(rep.Rows)

	var f *os.File = nil
	if u.outputFile != "" {
		var err error
		f, err = os.OpenFile(u.outputFile, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	reportJson := formatReportAsJson(u.prevReport, s, numKeysSeen, totalBandwidthUsed, reportedKeysBandwidth)
	reportJsonBytes, err := json.Marshal(reportJson)
	reportJsonString := string(reportJsonBytes)

	if err != nil {
		u.Log(err)
	} else {
		if f != nil {
			// Write to the output file if specified
			f.WriteString(reportJsonString + "\n")
		} else {
			u.Log(reportJsonString)
		}
	}

	return nil
}

type JsonReport struct {
	Timestamp                   string
	TotalKeys                   int
	TotalBandwidth              int64
	ReportedKeys                int
	ReportedBandwidth           int64
	ReportedBandwidthPercentage float64
	Rows                        []map[string]interface{}
	Stats                       StatsSet
}

func formatReportAsJson(report analysis.Report, stats StatsSet, totalKeys int, totalBandwidth int64, reportedBandwidth int64) JsonReport {
	var reportedBandwidthPercentage float64 = 0
	if reportedBandwidth > 0 && totalBandwidth > 0 {
		reportedBandwidthPercentage = 100 * float64(reportedBandwidth) / float64(totalBandwidth)
	}

	return JsonReport{
		Timestamp:                   report.Timestamp.Format("2006-01-02T15:04:05-0700"),
		TotalKeys:                   totalKeys,
		TotalBandwidth:              totalBandwidth,
		ReportedKeys:                len(report.Rows),
		ReportedBandwidth:           reportedBandwidth,
		ReportedBandwidthPercentage: reportedBandwidthPercentage,

		Rows:  reportToList(report),
		Stats: stats,
	}
}

func reportToList(report analysis.Report) []map[string]interface{} {
	reportList := make([]map[string]interface{}, len(report.Rows))
	for idx, row := range report.Rows {
		reportList[idx] = reportRowToMap(row, report.KeyColNames, report.ValColNames)
	}
	return reportList
}

func reportRowToMap(reportRow analysis.ReportRow, keyColNames []string, valueColNames []string) map[string]interface{} {
	rowMap := map[string]interface{}{}

	for idx, keyCol := range keyColNames {
		rowMap[keyCol] = reportRow.Key[idx]
	}

	for idx, valueCol := range valueColNames {
		rowMap[valueCol] = reportRow.Values[idx]
	}

	return rowMap
}

// Given a slice of rows, return the sum of the sum(size) columns
func totalBytesUseForKeys(rows []analysis.ReportRow) int64 {
	total := int64(0)
	for _, r := range rows {
		// This code assumes that we are using the default format, same as the sorting
		// code.  This should also be updated whenever we update the sorting code to not
		// depend on the default format.
		const maxSizeIndex = 1
		total = total + r.Values[maxSizeIndex]
	}
	return total
}
