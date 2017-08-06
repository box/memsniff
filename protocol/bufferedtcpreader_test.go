package protocol

import (
	"bytes"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"hash/fnv"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSmallPackets(t *testing.T) {
	r := NewTCPReaderStream()
	var out []byte
	var err error
	done := make(chan struct{}, 1)
	go func() {
		out, err = ioutil.ReadAll(r)
		done <- struct{}{}
	}()
	p1 := reassembly(10)
	p2 := reassembly(10)
	r.Reassembled(p1)
	r.Reassembled(p2)
	r.ReassemblyComplete()
	<-done

	if err != nil {
		t.Error("got error from ReadAll", err)
	}
	if !bytes.Equal(out[:10], p1[0].Bytes) || !bytes.Equal(out[10:20], p2[0].Bytes) {
		t.Error("expected", "got", string(out))
	}
}

func TestReadEmpty(t *testing.T) {
	r := NewTCPReaderStream()
	done := make(chan error, 1)
	go func() {
		_, err := ioutil.ReadAll(r)
		done <- err
	}()
	r.ReassemblyComplete()
	err := <-done
	if err != nil {
		t.Error(err)
	}
}

func TestReadMissing(t *testing.T) {
	r := NewTCPReaderStream()
	r.LossErrors = true
	reassembly := []tcpassembly.Reassembly{
		reassemblyWithSkip(-1, "hello"),
	}
	go func() {
		r.Reassembled(reassembly)
		r.ReassemblyComplete()
	}()
	buf := make([]byte, 32)

	n, err := r.Read(buf)
	if n != 0 {
		t.Error("expected 0 bytes read, got", n)
	}
	if err != (ErrLostData{-1}) || !strings.Contains(err.Error(), "unknown") {
		t.Error("expected 10 bytes lost, got", err)
	}

	n, err = r.Read(buf)
	if n != 5 {
		t.Error("expected 0 bytes read, got", n)
	}
	if err != nil {
		t.Error("expected no error, got", err)
	}
	if !bytes.Equal(buf[:n], []byte("hello")) {
		t.Error("expected", "hello", "got", string(buf[:n]))
	}

	n, err = r.Read(buf)
	if n != 0 {
		t.Error("expected 0 bytes read, got", n)
	}
	if err != io.EOF {
		t.Error("expected EOF, got", err)
	}
}

func TestLotsOfData(t *testing.T) {
	r := NewTCPReaderStream()
	var out []byte
	done := make(chan error, 1)
	go func() {
		var err error
		out, err = ioutil.ReadAll(r)
		done <- err
	}()

	hash1 := fnv.New64a()
	for i := 0; i < 100*1000; i++ {
		ra := reassembly(1000)
		r.Reassembled(ra)
		hash1.Write(ra[0].Bytes)
	}
	r.ReassemblyComplete()
	<-done

	hash2 := fnv.New64a()
	hash2.Write(out)
	if hash1.Sum64() != hash2.Sum64() {
		t.Error("hashes did not match")
	}
}

func TestReadLine(t *testing.T) {
	testReadLine(t, []tcpassembly.Reassembly{
		reassemblyString("hel")[0],
		reassemblyString("lo\n")[0],
		reassemblyString("world")[0],
	}, "hello", nil)
	testReadLine(t, reassemblyString("helloworld"), "helloworld", nil)
	testReadLine(t, reassemblyString("hello\r\n"), "hello", nil)
	testReadLine(t, nil, "", io.EOF)
	testReadLine(t, reassemblyString("\n"), "", nil)
}

func TestReadLineMissing(t *testing.T) {
	uut := NewTCPReaderStream()
	uut.LossErrors = true
	go func() {
		uut.Reassembled([]tcpassembly.Reassembly{reassemblyWithSkip(5, "world")})
		uut.ReassemblyComplete()
	}()
	_, err := uut.ReadLine()
	if err != (ErrLostData{5}) || !strings.Contains(err.Error(), "lost 5 bytes") {
		t.Error("Expected ErrLostData but got", err)
	}
	l, err := uut.ReadLine()
	if !bytes.Equal(l, []byte("world")) {
		t.Error("expected world but got", string(l))
	}
	if err != nil {
		t.Error("error from ReadLine", err)
	}
}

func testReadLine(t *testing.T, input []tcpassembly.Reassembly, expected string, expectedErr error) {
	r := NewTCPReaderStream()
	done := make(chan error, 1)
	var out []byte
	go func() {
		var err error
		out, err = r.ReadLine()
		done <- err
	}()

	for _, reassembly := range input {
		r.Reassembled([]tcpassembly.Reassembly{reassembly})
	}
	r.ReassemblyComplete()
	err := <-done
	if err != expectedErr {
		t.Error(err)
	}
	if !bytes.Equal(out, []byte(expected)) {
		t.Error("expected", expected, "but got", string(out), "(", out, ")")
	}
	t.Log("input", input, "gave correct output", expected)
}

func TestDiscard(t *testing.T) {
	// block boundary
	testDiscard(t, []tcpassembly.Reassembly{
		reassemblyString("1")[0],
		reassemblyString("23")[0],
		reassemblyString("hello\n")[0],
	}, 3, "hello\n")

	// split last block
	testDiscard(t, []tcpassembly.Reassembly{
		reassemblyString("1")[0],
		reassemblyString("2")[0],
		reassemblyString("3hello\n")[0],
	}, 3, "hello\n")

	// insufficient data
	testDiscard(t, reassemblyString("12"), 2, "")

	// empty
	testDiscard(t, nil, 0, "")
}

func TestDiscardMissingRecovery(t *testing.T) {
	uut := NewTCPReaderStream()
	uut.LossErrors = true
	go func() {
		uut.Reassembled([]tcpassembly.Reassembly{
			reassemblyWithSkip(3, "ab"),
			reassemblyWithSkip(5, "world"),
		})
		uut.ReassemblyComplete()
	}()
	uut.Discard(2)
	uut.Discard(10)
	l, err := uut.ReadLine()
	if err != nil {
		t.Error("error from ReadLine", err)
	}
	if !bytes.Equal(l, []byte("rld")) {
		t.Error("expected rld but got", string(l))
	}
}

func testDiscard(t *testing.T, input []tcpassembly.Reassembly, expectedN int, expectedOut string) {
	r := NewTCPReaderStream()
	done := make(chan error, 1)
	var out []byte
	go func() {
		n, err := r.Discard(3)
		if n != expectedN {
			t.Error("got", n, "expected 3")
		}
		out, err = ioutil.ReadAll(r)
		done <- err
	}()
	for _, reassembly := range input {
		r.Reassembled([]tcpassembly.Reassembly{reassembly})
	}
	r.ReassemblyComplete()
	err := <-done
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal([]byte(expectedOut), out) {
		t.Error(string(out))
	}
}

func TestCloseAllowsWritesToProceed(t *testing.T) {
	uut := NewTCPReaderStream()
	uut.Close()
	// all following data should now be discarded
	for i := 0; i < 100000; i++ {
		uut.Reassembled(reassemblyString("foobarbaz"))
	}
	uut.ReassemblyComplete()
}

func TestReadAfterClosePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fail()
		}
	}()
	uut := NewTCPReaderStream()
	uut.Close()
	// expect panic
	_, _ = uut.ReadLine()
	t.Fail()
}

func TestCloseDuringFlush(t *testing.T) {
	uut := NewTCPReaderStream()
	done := make(chan bool)
	// fill buffer
	for i := 0; i < maxPackets; i++ {
		uut.Reassembled(reassemblyString("\n"))
	}
	go func() {
		// this should block until Close() is called
		uut.Reassembled(reassemblyString("\n"))
		uut.Reassembled(reassemblyString("\n"))
		uut.ReassemblyComplete()
		done <- true
	}()

	// give goroutine a chance to get blocked
	time.Sleep(time.Second)

	uut.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("ReassemblyComplete did not return")
	}
}

type ReaderStream interface {
	tcpassembly.Stream
	io.Reader
}

func BenchmarkManySmallPackets(b *testing.B) {
	uut := NewTCPReaderStream()
	for i := 0; i < b.N; i++ {
		bench(uut, func() ReaderStream { return NewTCPReaderStream() })
	}
}

func BenchmarkStandardReader(b *testing.B) {
	rs := tcpreader.NewReaderStream()
	for i := 0; i < b.N; i++ {
		bench(&rs, func() ReaderStream {
			rs := tcpreader.NewReaderStream()
			return &rs
		})
	}
}

func bench(uut tcpassembly.Stream, factory func() ReaderStream) {
	numStreams := 10000
	numPackets := 5
	var wg sync.WaitGroup
	wg.Add(numStreams)
	for g := 0; g < numStreams; g++ {
		uut := factory()
		go func() {
			for i := 0; i < numPackets; i++ {
				uut.Reassembled(reassembly(100))
			}
			uut.ReassemblyComplete()
		}()
		go func() {
			ioutil.ReadAll(uut)
			wg.Done()
		}()
	}
	wg.Wait()
}

func reassemblyString(s string) []tcpassembly.Reassembly {
	return []tcpassembly.Reassembly{
		{
			Bytes: []byte(s),
			Seen:  time.Now(),
		},
	}
}

func reassembly(size int) []tcpassembly.Reassembly {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}
	return []tcpassembly.Reassembly{
		{
			Bytes: data,
			Seen:  time.Now(),
		},
	}
}

func reassemblyWithSkip(skipped int, s string) tcpassembly.Reassembly {
	return tcpassembly.Reassembly{
		Skip:  skipped,
		Bytes: []byte(s),
		Seen:  time.Now(),
	}
}
