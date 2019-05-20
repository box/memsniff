package presentation

import (
	"encoding/json"
	"fmt"
	"github.com/box/memsniff/analysis"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (u *uiContext) runReporter() error {
	reportFile, err := os.OpenFile(u.reportFilePath, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0777)
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

		colReport, err := json.Marshal(struct {
			Message        string
			AnalysisReport analysis.Report
		}{
			Message: fmt.Sprintf("Top 20 %s", valColName),
			AnalysisReport: analysis.Report {
				Timestamp:   report.Timestamp,
				KeyColNames: report.KeyColNames,
				ValColNames: report.ValColNames,
				Rows:        report.Rows[:20],
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
			log.Println("unable to write to the reportFile")
		}

		err = syscall.Flock(int(reportFile.Fd()), syscall.LOCK_UN)
		if err != nil {
			// there isn't much we can do on this case
			log.Println("unable to release an exclusive lock", err)
		}
	}
}
