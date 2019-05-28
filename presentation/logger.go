package presentation

import (
	"encoding/json"
	"fmt"
	"github.com/box/memsniff/analysis"
	"github.com/box/memsniff/internal/pkg/model"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (u *uiContext) runReporter() error {
	reportFile, err := os.OpenFile(u.reportFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(u.interval)
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)
	select {
	case <-ticker.C:
		log.Println("doReport the stuff")
		u.doReport(u.analysis.Report(false), reportFile)
		return nil
	case <-sigchan:
		log.Print("gracefully exiting")
		return nil
	}
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func (u *uiContext) doReport(report analysis.Report, reportFile *os.File) {
	for idx, valColName := range report.ValColNames {
		sortOrder := []int{-(len(report.KeyColNames) + idx)}
		for i := 0; i < len(report.ValColNames); i += 1 {
			if i != idx {
				sortOrder = append(sortOrder, -(len(report.KeyColNames) + i))
			}
		}
		for i := 0; i < len(report.KeyColNames); i += 1 {
			sortOrder = append(sortOrder, i)
		}

		// descending sort of each value column
		report.SortBy(sortOrder...)

		// making a slice explicitly to get empty array in case of json marshal
		rows := make([]analysis.ReportRow, len(report.Rows[:min(len(report.Rows), 20)]))
		copy(rows, report.Rows[:min(len(report.Rows), 20)])
		colReport, err := json.Marshal(model.TopResultsModel{
			Message:             fmt.Sprintf("Top 20 %s", valColName),
			AggregateColumnName: valColName,
			EventType:           "z-memsniff-statistics",
			Extras:              u.extras,
			AnalysisReport: analysis.Report{
				Timestamp:   report.Timestamp,
				KeyColNames: report.KeyColNames,
				ValColNames: report.ValColNames,
				Rows:        rows,
			},
		})
		if err != nil {
			log.Println("unable to json.Marshal the payload", err)
			continue
		}
		err = syscall.Flock(int(reportFile.Fd()), syscall.LOCK_EX)
		if err != nil {
			// there isn't much we can do on this case
			log.Println("unable to obtain an exclusive lock", err)
		}

		_, err = reportFile.Write([]byte(fmt.Sprintf("%s\n", colReport)))
		if err != nil {
			log.Println("unable to write to the reportFile", err)
		}

		err = syscall.Flock(int(reportFile.Fd()), syscall.LOCK_UN)
		if err != nil {
			// there isn't much we can do on this case
			log.Println("unable to release an exclusive lock", err)
		}
	}
}
