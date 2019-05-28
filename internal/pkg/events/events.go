package events

import (
	"github.com/box/memsniff/internal/pkg/model"
	"time"
)

type BeatType struct {
	Name     string `json:"name"`
	Hostname string `json:"hostname"`
	Version  string `json:"version"`
}

type Event struct {
	EventMessage model.TopResultsModel `json:"event_message"`
	Beat         BeatType              `json:"beat"`
	Timestamp    time.Time             `json:"@timestamp"`
}
