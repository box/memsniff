package aggregator

import (
	"github.com/box/memsniff/internal/pkg/events"
	"github.com/box/memsniff/internal/pkg/model"
)

var messageMapping = map[string]string{
	"avg(size)": "BIG Keys",
	"cnt(size)": "Keys with Highest Throughput (RPM)",
	"sum(size)": "keys consuming insane bandwidth",
}

func Aggregate(kevents []events.Event) []model.TopResultsModel {
	results := make(map[string]model.TopResultsModel)
	for _, kevent := range kevents {
		col := kevent.EventMessage.AggregateColumnName
		if _, ok := results[col]; !ok {
			msg := kevent.EventMessage
			msg.Message = messageMapping[col]
			results[col] = msg
			continue
		}
		results[col].AnalysisReport.Rows = append(
			results[col].AnalysisReport.Rows,
			kevent.EventMessage.AnalysisReport.Rows...,
		)
	}

}
