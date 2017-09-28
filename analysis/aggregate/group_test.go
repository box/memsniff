package aggregate

import (
	"github.com/box/memsniff/protocol/model"
	"testing"
)

func TestKeyString(t *testing.T) {
	kaf, err := NewKeyAggregatorFactory("key, size, sum(size)")
	if err != nil {
		t.Error(err)
	}
	key := kaf.FlatKey(model.Event{Type: model.EventGetHit, Key: "key1", Size: 20})

	expected := "key1\x0020\x00"
	if key != expected {
		t.Errorf("%q %q", expected, key)
	}
}

func TestKeyAggregator(t *testing.T) {
	kaf, err := NewKeyAggregatorFactory("key,max(size),sum(size),avg(size)")
	if err != nil {
		t.Error(err)
	}

	events := eventsWithSizes(10, 10, 10, 10, 60)
	ka := kaf.New()
	ka.Key = kaf.Key(events[0])
	for _, e := range events {
		ka.Add(e)
	}

	if len(ka.Key) != 1 || ka.Key[0] != "key1" {
		t.Error(ka.Key)
	}

	res := ka.Result()
	if len(res) != 3 {
		t.Error(res)
	}
	// max(size)
	if res[0] != 60 {
		t.Error(res)
	}
	// sum(size)
	if res[1] != 100 {
		t.Error(res)
	}
	// avg(size)
	if res[2] != 20 {
		t.Error(res)
	}
}

func TestPercentile(t *testing.T) {
	kaf, err := NewKeyAggregatorFactory("p50(size), p90(size)")
	if err != nil {
		t.Error(err)
	}

	ka := kaf.New()
	for _, e := range eventsWithSizes(0, 10, 20, 30, 40) {
		ka.Add(e)
	}

	res := ka.Result()
	if len(res) != 2 {
		t.Error(res)
	}
	if res[0] != 20 {
		t.Error("median:", res[0])
	}
	if res[1] != 40 {
		t.Error("p90:", res[1])
	}
}

func eventsWithSizes(sizes ...int) []model.Event {
	res := make([]model.Event, len(sizes))
	for i, s := range sizes {
		res[i] = model.Event{Type: model.EventGetHit, Key: "key1", Size: s}
	}
	return res
}
