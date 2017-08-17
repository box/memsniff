package analysis

import (
	"github.com/box/memsniff/protocol/model"
	"testing"
)

func TestEmptyMatchesAll(t *testing.T) {
	f := &filter{}
	_ = f.setPattern("")
	if !match(f, "hello") {
		t.Fail()
	}
	if !match(f, "") {
		t.Fail()
	}
}

func TestPatternMatchesSubstring(t *testing.T) {
	f := &filter{}
	_ = f.setPattern("world")
	if !match(f, "hello world") {
		t.Fail()
	}
}

func TestPatternFiltersNonMatching(t *testing.T) {
	f := &filter{}
	_ = f.setPattern("world")
	if match(f, "hello nurse") {
		t.Fail()
	}
}

func TestEmptyOverwritesPrior(t *testing.T) {
	f := &filter{}
	_ = f.setPattern("hello")
	_ = f.setPattern("")
	if !match(f, "foobar") {
		t.Fail()
	}
}

func TestInvalidMatchesAll(t *testing.T) {
	f := &filter{}
	err := f.setPattern("[abc")
	if err == nil {
		t.Error("did not return error for invalid regex")
	}
	if !match(f, "foobar") {
		t.Error("invalid pattern should match all")
	}
}

func match(f *filter, key string) bool {
	return len(f.filterEvents([]model.Event{
		{
			Key: key,
		},
	})) == 1
}
