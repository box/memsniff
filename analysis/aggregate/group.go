package aggregate

import (
	"github.com/box/memsniff/protocol/model"
	"strings"
)

type KeyBuilder func(e model.Event) string

type KeyAggregator struct {
	KeyFields   []string
	aggFieldIDs []model.EventFieldMask
	aggs        []Aggregator
}

func (ka KeyAggregator) Add(e model.Event) {
	for i := range ka.aggs {
		ka.aggs[i].Add(fieldAsInt64(e, ka.aggFieldIDs[i]))
	}
}

func (ka KeyAggregator) Result() []int64 {
	res := make([]int64, len(ka.aggs))
	for i := range ka.aggs {
		res[i] = ka.aggs[i].Result()
	}
	return res
}

// parseDescription creates a KeyAggregatorFactory.  The descriptor should be a
// comma-separated list of field names (key, size, etc.) and aggregate descriptions
// (sum(size), p99(latency), etc.).
func parseDescriptor(desc string) (KeyAggregatorFactory, error) {
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
			kaf.keyFields = append(kaf.keyFields, field)
			kaf.keyFieldMask |= fieldID
		} else {
			// can aggregate integer fields only
			if fieldID & model.IntFields == 0 {
				return KeyAggregatorFactory{}, BadDescriptorError(field)
			}
			aggFactory, err := NewFactoryFromDescriptor(aggDesc)
			if err != nil {
				return KeyAggregatorFactory{}, err
			}

			kaf.aggFields = append(kaf.aggFields, field)
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
	fieldID, err = fieldIdFromDescriptor(field)
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
	fieldID, err = fieldIdFromDescriptor(matches[2])
	if err != nil {
		return 0, "", err
	}

	return
}

type KeyAggregatorFactory struct {
	// keyFields is the names of the fields to use as keys: ["key", "size"]
	keyFields []string
	// keyFieldMask is the logical-OR of the field IDs to use as keys.
	keyFieldMask model.EventFieldMask
	// aggFields is the names of the fields to aggregate over, in order of display.
	aggFields []string
	// aggFieldIDs is the fieldIds of the fields to aggregate over, in order of display.
	aggFieldIDs []model.EventFieldMask
	// aggFactories are AggregatorFactories to create the correct type of aggregator for the matching aggField.
	aggFactories []AggregatorFactory
}

func (f KeyAggregatorFactory) New() (ka KeyAggregator) {
	ka.KeyFields = f.keyFields
	ka.aggFieldIDs = f.aggFieldIDs
	ka.aggs = make([]Aggregator, len(f.aggFactories))
	for i := range f.aggFactories {
		ka.aggs[i] = f.aggFactories[i]()
	}
	return
}

// FlatKey returns a unique key based on the key fields of an event,
// suitable for use in a map.
func (f KeyAggregatorFactory) FlatKey(e model.Event) string {
	return fieldsAsString(e, f.keyFieldMask)
}
