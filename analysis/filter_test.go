package analysis

import (
	"github.com/box/memsniff/protocol"
	"testing"
)

func TestEmptyMatchesAll(t *testing.T) {
	f := filter{}
	f.setPattern("")
	if !match(f, []byte("hello")) {
		t.Fail()
	}
	if !match(f, []byte("")) {
		t.Fail()
	}
}

func TestPatternMatchesSubstring(t *testing.T) {
	var f filter
	f.setPattern("world")
	if !match(f, []byte("hello world")) {
		t.Fail()
	}
}

func TestPatternFiltersNonMatching(t *testing.T) {
	var f filter
	f.setPattern("world")
	if match(f, []byte("hello nurse")) {
		t.Fail()
	}
}

func TestEmptyOverwritesPrior(t *testing.T) {
	var f filter
	f.setPattern("hello")
	f.setPattern("")
	if !match(f, []byte("foobar")) {
		t.Fail()
	}
}

func TestInvalidMatchesAll(t *testing.T) {
	var f filter
	err := f.setPattern("[abc")
	if err == nil {
		t.Error("did not return error for invalid regex")
	}
	if !match(f, []byte("foobar")) {
		t.Error("invalid pattern should match all")
	}
}

func match(f filter, bs []byte) bool {
	return len(f.filterResponses([]*protocol.GetResponse{
		&protocol.GetResponse{
			Key: bs,
		},
	})) == 1
}
