package model

// EventType described what sort of event has occurred.
type EventType int

const (
	// EventUnknown is an unhandled event.
	EventUnknown EventType = iota
	// EventGetHit is a successful data retrieval that returned data.
	EventGetHit
	// EventGetMiss is a data retrieval that did not result in data.
	EventGetMiss
)

// Event is a single event in a datastore conversation
type Event struct {
	// Type of the event.
	Type EventType
	// Datastore key affected by this event.
	Key string
	// Size of the datastore value affected by this event.
	Size int
}

// EventHandler consumes a batch of events.
type EventHandler func(evts []Event)

// EventFieldMask efficiently identifies a field or set of fields in an Event.
// Each value is a power-of-2, and can be OR-ed together to express a set.
type EventFieldMask int

const (
	// FieldNone is a mask representing the empty set of Event fields.
	FieldNone EventFieldMask = 0
	FieldKey  EventFieldMask = 1 << iota
	FieldSize

	// FieldEndOfFields is a dummy value to use as the endpoint of an iteration.
	FieldEndOfFields
)

const (
	// IntFields is a mask identifying the set of fields that can be viewed as integers,
	// and are viable targets for aggregation.
	IntFields = FieldSize
)
