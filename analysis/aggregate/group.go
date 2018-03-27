package aggregate

import (
	"github.com/box/memsniff/protocol/model"
	"strings"
)

// KeyAggregator tracks data across all requested event fields for a single key.
type KeyAggregator struct {
	// Key is the list of key fields over which we are aggregating.
	Key []string

	// aggFieldIDs is the list of event fields whose values we take for aggregation,
	// in the same order as aggs and as the descriptor string provided to the
	// KeyAggregatorFactory.
	aggFieldIDs []model.EventFieldMask
	// aggs is the actual aggregators, in the same order as the descriptor string.
	aggs []Aggregator
}

// Add updates all aggregators tracked for this key according to the provided event.
func (ka KeyAggregator) Add(e model.Event) {
	for i := range ka.aggs {
		ka.aggs[i].Add(fieldAsInt64(e, ka.aggFieldIDs[i]))
	}
}

// Result returns the aggregation results for this key, in order of their appearance
// in the descriptor used to create the KeyAggregatorFactory.
func (ka KeyAggregator) Result() []int64 {
	res := make([]int64, len(ka.aggs))
	for i := range ka.aggs {
		res[i] = ka.aggs[i].Result()
	}
	return res
}

// Reset clears all aggregators to their initial state.
func (ka *KeyAggregator) Reset() {
	ka.Key = nil
	for _, agg := range ka.aggs {
		agg.Reset()
	}
}

// NewKeyAggregatorFactory creates a KeyAggregatorFactory.  The descriptor should be a
// comma-separated list of field names (key, size, etc.) and aggregate descriptions
// (sum(size), p99(latency), etc.).
func NewKeyAggregatorFactory(desc string) (KeyAggregatorFactory, error) {
	fieldDescs := strings.Split(desc, ",")

	var kaf KeyAggregatorFactory
	for _, field := range fieldDescs {
		field = strings.TrimSpace(field)

		fieldID, aggDesc, err := parseField(field)
		if err != nil {
			return KeyAggregatorFactory{}, err
		}
		if aggDesc == "" {
			// simple field
			kaf.KeyFields = append(kaf.KeyFields, field)
			kaf.keyFieldMask |= fieldID
		} else {
			// can aggregate integer fields only
			if fieldID&model.IntFields == 0 {
				return KeyAggregatorFactory{}, BadDescriptorError(field)
			}
			aggFactory, err := NewFactoryFromDescriptor(aggDesc)
			if err != nil {
				return KeyAggregatorFactory{}, err
			}

			kaf.AggFields = append(kaf.AggFields, field)
			kaf.aggFieldIDs = append(kaf.aggFieldIDs, fieldID)
			kaf.aggFactories = append(kaf.aggFactories, aggFactory)
		}
	}

	return kaf, nil
}

// parseField determines whether a descriptor field names an event field or an aggregate.
// If the field is an event field name, the fieldId is returned, and aggDesc is the empty string.
// If the field describes an aggregate as in "p95(size)", fieldId over which to aggregate,
// and aggDesc is the type of aggregation to perform (e.g. "p95").
func parseField(field string) (fieldID model.EventFieldMask, aggDesc string, err error) {
	// check for literal field name
	fieldID, err = fieldIDFromDescriptor(field)
	if err == nil {
		return
	}

	// try to parse as aggregate descriptor
	matches := aggregatorRegex.FindStringSubmatch(field)
	if matches == nil {
		return 0, "", BadDescriptorError(field)
	}
	if !IsValidAgg(matches[1]) {
		return 0, "", BadDescriptorError(field)
	}

	aggDesc = matches[1]
	fieldID, err = fieldIDFromDescriptor(matches[2])
	if err != nil {
		return 0, "", err
	}

	return
}

// KeyAggregatorFactory creates KeyAggregators that all share the same method of aggregation
// across event fields.
type KeyAggregatorFactory struct {
	// keyFields is the names of the fields to use as keys: ["key", "size"]
	KeyFields []string
	// keyFieldMask is the logical-OR of the field IDs to use as keys.
	keyFieldMask model.EventFieldMask
	// AggFields is the names of the fields to aggregate over, in order of display.
	AggFields []string
	// aggFieldIDs is the fieldIds of the fields to aggregate over, in order of display.
	aggFieldIDs []model.EventFieldMask
	// aggFactories are AggregatorFactories to create the correct type of aggregator for the matching aggField.
	aggFactories []AggregatorFactory
}

// New creates a new KeyAggregator configured to perform aggregation based on the descriptor
// used to create this KeyAggregatorFactory.
func (f KeyAggregatorFactory) New() (ka KeyAggregator) {
	ka.aggFieldIDs = f.aggFieldIDs
	ka.aggs = make([]Aggregator, len(f.aggFactories))
	for i := range f.aggFactories {
		ka.aggs[i] = f.aggFactories[i]()
	}
	return
}

// FlatKey returns a string key based on the flattened key fields of an event,
// suitable for use in a map.
func (f KeyAggregatorFactory) FlatKey(e model.Event) string {
	return fieldsAsString(e, f.keyFieldMask)
}

// Key returns a list of strings used together as the composite key for an event.
func (f KeyAggregatorFactory) Key(e model.Event) []string {
	return fieldsAsStrings(e, f.keyFieldMask)
}
