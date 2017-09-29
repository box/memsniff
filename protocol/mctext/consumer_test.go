package mctext

import (
	"github.com/box/memsniff/assembly/reader"
	"github.com/box/memsniff/protocol/model"
	"github.com/google/gopacket/tcpassembly"
	"testing"
)

func TestTextMulti(t *testing.T) {
	lines := []string{
		"VALUE key1 0 5",
		"hello",
		"VALUE key2 10 5",
		"world",
	}
	testReadText(t, lines, []model.Event{
		{model.EventGetHit, "key1", 5},
		{model.EventGetHit, "key2", 5},
	})
}

func TestTextEmptyValue(t *testing.T) {
	lines := []string{
		"VALUE key3|foo 32 0",
		"",
	}
	testReadText(t, lines, []model.Event{
		{model.EventGetHit, "key3|foo", 0},
	})
}

func TestTextIncompleteHeader(t *testing.T) {
	lines := []string{
		"VALUE key1 42 5",
		"world",
		"VALUE ",
	}
	testReadText(t, lines, []model.Event{
		{model.EventGetHit, "key1", 5},
	})
}

func TestTextIncompleteBody(t *testing.T) {
	lines := []string{
		"VALUE key1 42 5",
		"wor",
	}
	testReadText(t, lines, []model.Event{
		{model.EventGetHit, "key1", 5},
	})
}

func testReadText(t *testing.T, lines []string, expected []model.Event) {
	handler := func(evts []model.Event) {
		for _, e := range evts {
			if e != expected[0] {
				t.Error("Expected", expected[0], "got", e)
			}
			expected = expected[1:]
		}
	}
	client, server := reader.NewPair()
	r := Consumer{
		Handler:      handler,
		ClientReader: client,
		ServerReader: server,
	}

	done := make(chan struct{})
	go func() {
		r.Run()
		close(done)
	}()

	r.ClientReader.Reassembled(reassemblyString("get key1 key2 key3\r\n"))
	for _, l := range lines {
		r.ServerReader.Reassembled(reassemblyString(l + "\r\n"))
	}
	r.ClientReader.ReassemblyComplete()
	r.ServerReader.ReassemblyComplete()

	<-done
	if len(expected) > 0 {
		t.Error("Expected", expected, "events but never received")
	}
}

func reassemblyString(s string) []tcpassembly.Reassembly {
	return []tcpassembly.Reassembly{{Bytes: []byte(s)}}
}
