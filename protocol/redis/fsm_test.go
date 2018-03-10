package redis

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
	"github.com/google/gopacket/tcpassembly"
)

func TestBasicGet(t *testing.T) {
	input := []string{
		"*2",
		"$3",
		"get",
		"$4",
		"key1",
	}
	output := []string{
		"$5",
		"hello",
	}
	expected := []model.Event{
		{
			Type: model.EventGetHit,
			Key:  "key1",
			Size: 5,
		},
	}
	test(t, input, output, expected)
}

func TestIgnoreUnknown(t *testing.T) {
	input := []string{
		"*2",
		"$4",
		"PING",
		"$3",
		"123",

		"*2",
		"$3",
		"GET",
		"$5",
		"hello",
	}
	output := []string{
		"$3",
		"123",

		"$5",
		"world",
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

func resp(fields ...string) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(fields))
	for _, f := range fields {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(f), f)
	}
	return buf.String()
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

	c := model.New(handler, NewFsm(log.ConsoleLogger{}))
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
