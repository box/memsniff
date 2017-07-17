package analysis

import (
	"testing"
)

func TestEmptyMatchesAll(t *testing.T) {
	f := filter{}
	f.setPattern("")
	if !f.match([]byte("hello")) {
		t.Fail()
	}
	if !f.match([]byte("")) {
		t.Fail()
	}
}

func TestPatternMatchesSubstring(t *testing.T) {
	var f filter
	f.setPattern("world")
	if !f.match([]byte("hello world")) {
		t.Fail()
	}
}

func TestPatternFiltersNonMatching(t *testing.T) {
	var f filter
	f.setPattern("world")
	if f.match([]byte("hello nurse")) {
		t.Fail()
	}
}

func TestEmptyOverwritesPrior(t *testing.T) {
	var f filter
	f.setPattern("hello")
	f.setPattern("")
	if !f.match([]byte("foobar")) {
		t.Fail()
	}
}

func TestInvalidMatchesAll(t *testing.T) {
	var f filter
	err := f.setPattern("[abc")
	if err == nil {
		t.Error("did not return error for invalid regex")
	}
	if !f.match([]byte("foobar")) {
		t.Error("invalid pattern should match all")
	}
}
