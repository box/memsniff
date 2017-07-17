package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestTextMulti(t *testing.T) {
	lines := []string{
		"VALUE key1 0 5",
		"hello",
		"VALUE key2 10 5",
		"world",
		"", // terminating CRLF
	}
	testReadSingleText(t, lines, 2, "key1", 5)
	testReadText(t, lines, []GetResponse{
		GetResponse{[]byte("key1"), 5},
		GetResponse{[]byte("key2"), 5},
	})
}

func TestTextContinuation(t *testing.T) {
	lines := []string{
		"some prior value",
		"VALUE key1 0 5",
		"hello",
		"VALUE key2 10 5",
		"world",
		"VALUE key3|foo 32 0",
		"",
		"", // terminating CRLF
	}
	testReadSingleText(t, lines, 3, "key1", 5)
	testReadText(t, lines, []GetResponse{
		GetResponse{[]byte("key1"), 5},
		GetResponse{[]byte("key2"), 5},
		GetResponse{[]byte("key3|foo"), 0},
	})
}

func TestTextEmptyValue(t *testing.T) {
	lines := []string{
		"VALUE key3|foo 32 0",
		"",
		"", // terminating CRLF
	}
	testReadSingleText(t, lines, 2, "key3|foo", 0)
	testReadText(t, lines, []GetResponse{
		GetResponse{[]byte("key3|foo"), 0},
	})
}

func TestTextIncompleteHeader(t *testing.T) {
	lines := []string{
		"VALUE key1 42 5",
		"world",
		"VALUE ",
	}
	testReadSingleText(t, lines, 2, "key1", 5)
	testReadText(t, lines, []GetResponse{
		GetResponse{[]byte("key1"), 5},
	})
}

func TestTextIncompleteBody(t *testing.T) {
	lines := []string{
		"VALUE key1 42 5",
		"wor",
	}
	testReadSingleText(t, lines, 2, "key1", 5)
	testReadText(t, lines, []GetResponse{
		GetResponse{[]byte("key1"), 5},
	})
}

func testReadSingleText(t *testing.T, lines []string, remainderStart int, expectedKey string, expectedSize int) {
	data := strings.Join(lines, "\r\n")
	rem, resp, err := readSingleText([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if string(rem) != strings.Join(lines[remainderStart:], "\r\n") {
		t.Fatal("incorrect remainder: \"", string(rem), "\"")
	}
	if resp == nil || !equalResponse(*resp, GetResponse{[]byte(expectedKey), expectedSize}) {
		t.Fatal("incorrect GetResponse:", resp)
	}
}

func testReadText(t *testing.T, lines []string, expected []GetResponse) {
	data := strings.Join(lines, "\r\n")
	actual, err := readText([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(actual) != len(expected) {
		t.Fatal("expected", len(expected), "responses, got", len(actual))
	}
	for i := 0; i < len(expected); i++ {
		if !equalResponse(*actual[i], expected[i]) {
			t.Fatal("mismatched response", i, ":", actual[i], expected[i])
		}
	}
}

func equalResponse(a, b GetResponse) bool {
	return a.Size == b.Size && bytes.Equal(a.Key, b.Key)
}
