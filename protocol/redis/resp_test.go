package redis

import (
	"bytes"
	"reflect"
	"regexp"
	"testing"

	"github.com/box/memsniff/assembly/reader"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/gopacket/tcpassembly"
)

var (
	replaceNL, _ = regexp.Compile("\n[ \t]*")
)

func TestSimpleString(t *testing.T) {
	r := reader.New()
	write(r, "+OK\n")
	p := NewParser(r)
	err := p.Run()
	if err != nil {
		t.Error(err)
	}
	res := string(p.Result().(string))
	if res != "OK" {
		t.Error(res, "OK")
	}
}

func TestInteger(t *testing.T) {
	r := reader.New()
	write(r, ":123\n")
	p := NewParser(r)
	err := p.Run()
	if err != nil {
		t.Error(err)
	}
	res := p.Result().(int)
	if res != 123 {
		t.Error(res, 123)
	}
}

func TestPartialRead(t *testing.T) {
	r := reader.New()
	p := NewParser(r)
	err := p.Run()
	if err != reader.ErrShortRead {
		t.Error(err)
	}
	write(r, ":1")
	err = p.Run()
	if err != reader.ErrShortRead {
		t.Error(err)
	}
	write(r, "23\n")
	err = p.Run()
	if err != nil {
		t.Error(err)
	}
	res := p.Result().(int)
	if res != 123 {
		t.Error(res, 123)
	}
}

func TestNestedArrays(t *testing.T) {
	r := reader.New()
	write(r, `*2
		*1
		+Value 1
		*2
		+hello
		+world
		`)
	p := NewParser(r)
	err := p.Run()
	if err != nil {
		t.Error(err)
	}

	expected := []interface{}{
		[]interface{}{
			"Value 1",
		},
		[]interface{}{
			"hello",
			"world",
		},
	}

	actual := p.Result()
	if !reflect.DeepEqual(actual, expected) {
		t.Error(spew.Sdump(actual), spew.Sdump(expected))
	}
}

func TestCaptureBulk(t *testing.T) {
	r := reader.New()
	p := NewParser(r)
	p.Options.BulkCaptureLimit = 1024
	write(r, "$5\nhello\n")
	err := p.Run()
	if err != nil {
		t.Error(err)
	}
	if b, ok := p.Result().([]byte); !ok || string(b) != "hello" {
		t.Error(string(b), ok, "hello")
	}
}

func TestDiscardLargeBulk(t *testing.T) {
	kb := string(make([]byte, 1024))
	r := reader.New()
	p := NewParser(r)
	write(r, "$65536\n")
	for i := 0; i < 64; i++ {
		write(r, kb)
		err := p.Run()
		if err != nil {
			t.Error(i, err)
		}
	}
	write(r, "\n")
	err := p.Run()
	if err != nil {
		t.Error(err)
	}
	if i, ok := p.Result().(int); !ok || i != 65536 {
		t.Error(i, ok, 65536)
	}
}

func TestCaptureLargeBulk(t *testing.T) {
	kb := string(make([]byte, 1024))
	r := reader.New()
	p := NewParser(r)
	p.Options.BulkCaptureLimit = 64 * 1024
	write(r, "$65536\n")
	for i := 0; i < 64; i++ {
		write(r, kb)
		err := p.Run()
		if err != nil && err != reader.ErrShortRead {
			t.Fatal(i, err)
		}
	}
	write(r, "\n")
	err := p.Run()
	if err != nil {
		t.Error(err)
	}
	if b, ok := p.Result().([]byte); !ok {
		t.Error(p.Result())
	} else {
		if !bytes.Equal(b, make([]byte, 64*1024)) {
			t.Error("bad contents")
		}
	}
}

func TestStackLimit(t *testing.T) {
	r := reader.New()
	p := NewParser(r)
	for i := 0; i < 8; i++ {
		write(r, "*1\n")
	}
	err := p.Run()
	if err != RecursionLimitErr {
		t.Error(err)
	}
}

func write(r *reader.Reader, s string) {
	s = replaceNL.ReplaceAllString(s, "\r\n")
	r.Reassembled([]tcpassembly.Reassembly{{Bytes: []byte(s)}})
}
