package presentation

import (
	"encoding/json"
	"fmt"
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
	numReportedKeys := len(rep.Rows)
	reportedKeysBandwidth := totalBytesUseForKeys(rep.Rows)

	f, err := u.openFile(u.outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	reportJson, _ := formatReportAsJson(u.prevReport, s, numKeysSeen, totalBandwidthUsed,numReportedKeys,reportedKeysBandwidth)

	u.Log(fmt.Sprintf("All_Keys(Bandwidth=%d,Num=%d) Reported_Keys(Bandwidth=%d (%f%%),Num=%d) Incremental(Packets: %d Responses: %d %s) Cumulative(Packets: %d Responses: %d %s)",
		totalBandwidthUsed, numKeysSeen,
		reportedKeysBandwidth, 100*float64(reportedKeysBandwidth)/float64(totalBandwidthUsed), numReportedKeys,
		s.Incremental.PacketsPassedFilter, s.Incremental.ResponsesParsed, u.dropLabel(*s.Incremental),
		s.Culumative.PacketsPassedFilter, s.Culumative.ResponsesParsed, u.dropLabel(*s.Culumative)))

	if f != nil {
		// Write to the output file if specified
		f.WriteString(string(reportJson) + "\n")
	} else {
		u.Log(string(reportJson))
	}

	return nil
}

func (u *uiContext) openFile(filename string) (*os.File, error) {
	if filename != "" {
		f, err := os.OpenFile(u.outputFile, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
		return f, err
	} else {
		return nil, nil
	}
}

func formatReportAsJson(report analysis.Report, stats StatsSet, totalKeys int, totalBandwidth int64, reportedKeys int, reportedBandwidth int64) ([]byte, error) {
	reportMap := map[string]interface{}{}
	reportMap["ts"] = report.Timestamp.UTC().Unix()
	reportMap["ts_s"] = report.Timestamp.String()
	reportMap["totalKeys"] = totalKeys
	reportMap["totalBandwidth"] = totalBandwidth
	reportMap["reportedKeys"] = reportedKeys
	reportMap["reportedBandwidth"] = reportedBandwidth
	reportMap["reportedBandwidthPercentage"] = 100*float64(reportedBandwidth)/float64(totalBandwidth)
	reportMap["rows"] = reportToList(report)
	reportMap["stats"] = statsSetToMap(stats)
	return json.Marshal(reportMap)
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

func statsSetToMap(stats StatsSet) map[string]interface{} {
	statsSetMap := map[string]interface{}{}
	statsSetMap["incremental"] = statsToMap(*stats.Incremental)
	return statsSetMap
}

func statsToMap(stats Stats) map[string]interface{} {
	statsMap := map[string]interface{}{}
	statsMap["PacketsEnteredFilter"] = stats.PacketsEnteredFilter
	statsMap["PacketsPassedFilter"] = stats.PacketsPassedFilter
	statsMap["PacketsCaptured"] = stats.PacketsCaptured
	statsMap["PacketsDroppedKernel"] = stats.PacketsDroppedKernel
	statsMap["PacketsDroppedParser"] = stats.PacketsDroppedParser
	statsMap["PacketsDroppedAnalysis"] = stats.PacketsDroppedAnalysis
	statsMap["PacketsDroppedTotal"] = stats.PacketsDroppedTotal
	statsMap["ResponsesParsed"] = stats.ResponsesParsed
	return statsMap
}

// Given a slice of rows, return the sum of the sum(size) columns
func totalBytesUseForKeys(rows []analysis.ReportRow) int64 {
	total := int64(0)
	for _,r := range rows {
		total = total + r.Values[1]
	}
	return total
}