package model

import "github.com/box/memsniff/analysis"

type TopResultsModel struct {
	Message             string            `json:"message"`
	AggregateColumnName string            `json:"aggregate_column_name"`
	EventType           string            `json:"event_type"`
	Extras              map[string]string `json:"extras"`
	AnalysisReport      analysis.Report   `json:"analysis_report"`
}
