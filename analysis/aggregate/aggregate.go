package aggregate

import (
	"fmt"
	"github.com/codahale/hdrhistogram"
	"math"
	"strconv"
)

// Aggregator summarizes a set of integer data points to a single number.
type Aggregator interface {
	// Add records a single data point.
	Add(n int64)
	// Result returns the final output of aggregation.
	Result() int64
}

// maxMicros is the longest time interval we are interested in tracking by default, just over a minute assuming
// time is measured in microseconds.
const maxMicros = 64 * 1024 * 1024

// BadDescriptorError is returned when an aggregator descriptor is malformed
type BadDescriptorError string

func (b BadDescriptorError) Error() string {
	return fmt.Sprintln("Bad aggregate type:", string(b))
}

// Max retains the maximum value in the aggregated data.
type Max struct {
	max       int64
	seenFirst bool
}

func (m *Max) Add(n int64) {
	if !m.seenFirst {
		m.max = n
		m.seenFirst = true
		return
	}
	if m.max < n {
		m.max = n
	}
}

func (m *Max) Result() int64 {
	return m.max
}

// Min retains the minimum value in the aggregated data.
type Min struct {
	min       int64
	seenFirst bool
}

func (m *Min) Add(n int64) {
	if !m.seenFirst {
		m.min = n
		m.seenFirst = true
		return
	}
	if m.min > n {
		m.min = n
	}
}

func (m *Min) Result() int64 {
	return m.min
}

// Sum returns the sum of the aggregated data.
type Sum struct {
	sum int64
}

func (s *Sum) Add(n int64) {
	s.sum += n
}

func (s *Sum) Result() int64 {
	return s.sum
}

// Mean returns the arithmetic mean of the aggregated data.
type Mean struct {
	sum   int64
	count int64
}

func (m *Mean) Add(n int64) {
	m.count++
	m.sum += n
}

func (m *Mean) Result() int64 {
	if m.count == 0 {
		return 0
	}
	return m.sum / m.count
}

// Percentile returns the nth percentile sample from the aggregated data.
type Percentile struct {
	q float64
	h *hdrhistogram.Histogram
}

func NewPercentile(quantile float64, maxValue int64) *Percentile {
	return &Percentile{
		q: quantile,
		h: hdrhistogram.New(1, maxValue, 3),
	}
}

func (p *Percentile) Add(n int64) {
	err := p.h.RecordValue(n)
	if err != nil {
		// value too large, record as large as we can
		p.h.RecordValue(p.h.HighestTrackableValue())
	}
}

func (p *Percentile) Result() int64 {
	v := p.h.ValueAtQuantile(p.q)
	if v >= p.h.HighestTrackableValue() {
		// make it very obvious that the actual value is unknown but large
		return math.MaxInt64
	}
	return v
}

func IsValidAgg(desc string) bool {
	switch desc {
	case "max", "min", "mean", "avg", "sum":
		return true

	default:
		if len(desc) >= 3 && desc[0] == 'p' {
			_, err := strconv.Atoi(desc[1:])
			if err == nil {
				return true
			}
		}
		return false
	}
}

func NewFromDescriptor(desc string) (Aggregator, error) {
	f, err := NewFactoryFromDescriptor(desc)
	if err != nil {
		return nil, err
	}
	return f(), nil
}

type AggregatorFactory func() Aggregator

func NewFactoryFromDescriptor(desc string) (AggregatorFactory, error) {
	switch desc {
	case "max":
		return func() Aggregator { return &Max{} }, nil

	case "min":
		return func() Aggregator { return &Min{} }, nil

	case "mean", "avg":
		return func() Aggregator { return &Mean{} }, nil

	case "sum":
		return func() Aggregator { return &Sum{} }, nil

	default:
		if len(desc) >= 3 && desc[0] == 'p' {
			return percentileFactoryFromDescriptor(desc)
		}
		return nil, BadDescriptorError(desc)
	}
}

func percentileFactoryFromDescriptor(desc string) (AggregatorFactory, error) {
	n, err := strconv.Atoi(desc[1:])
	if err != nil {
		return nil, BadDescriptorError(desc)
	}

	// p99   => 99    shift = 0
	// p999  => 99.9  shift = 1
	// p9999 => 99.99 shift = 2
	shift := len(desc) - 3
	q := float64(n)
	for i := 0; i < shift; i++ {
		q /= 10
	}

	return func() Aggregator { return NewPercentile(q, maxMicros) }, nil
}
