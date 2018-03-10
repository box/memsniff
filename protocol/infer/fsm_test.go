package infer

import (
	"testing"

	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
	"github.com/google/gopacket/tcpassembly"
)

func TestInferRedis(t *testing.T) {
	input := []string{
		"*2",
		"$3",
		"GET",
		"$3",
		"foo",
	}
	output := []string{
		"$3",
		"bar",
	}
	expected := []model.Event{
		{
			Type: model.EventGetHit,
			Key:  "foo",
			Size: 3,
		},
	}
	test(t, input, output, expected)
}

func TestInferMemcached(t *testing.T) {
	input := []string{
		"get hello",
	}
	output := []string{
		"VALUE hello 0 5",
		"world",
		"END",
	}
	expected := []model.Event{
		{
			Type: model.EventGetHit,
			Key:  "hello",
			Size: 5,
		},
	}
	test(t, input, output, expected)
}

func reassemblyString(s string) []tcpassembly.Reassembly {
	return []tcpassembly.Reassembly{{Bytes: []byte(s)}}
}

func test(t *testing.T, input []string, output []string, expected []model.Event) {
	handler := func(evts []model.Event) {
		for _, e := range evts {
			if e != expected[0] {
				t.Error("Expected", expected[0], "got", e)
			}
			expected = expected[1:]
		}
	}
	c := model.New(handler, NewFsm(&log.ConsoleLogger{}))
	for _, s := range input {
		c.ClientStream().Reassembled(reassemblyString(s))
		c.ClientStream().Reassembled(reassemblyString("\r\n"))
	}
	for _, s := range output {
		c.ServerStream().Reassembled(reassemblyString(s))
		c.ServerStream().Reassembled(reassemblyString("\r\n"))
	}
	c.ClientStream().ReassemblyComplete()
	c.ServerStream().ReassemblyComplete()

	if len(expected) > 0 {
		t.Error("Expected", expected, "events but never received")
	}
}
