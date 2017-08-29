package reader

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
	r := New()
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
	r := New()
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
	r := New()
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
	r := New()
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

func TestReadN(t *testing.T) {
	r := New()
	go func() {
		r.Reassembled(reassemblyString("hello"))
		r.Reassembled(reassemblyString("world"))
		r.ReassemblyComplete()
	}()

	b, err := r.ReadN(3)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(b, []byte("hel")) {
		t.Error("expected hel got", string(b))
	}

	// now something that requires combining across
	b, err = r.ReadN(4)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(b, []byte("lowo")) {
		t.Error("expected lowo got", string(b))
	}

	// run out of data?
	b, err = r.ReadN(4)
	if err != io.ErrUnexpectedEOF {
		t.Error(err)
	}
	if !bytes.Equal(b, []byte("rld")) {
		t.Error("expected rld got", string(b))
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

func TestReadLineMultiple(t *testing.T) {
	r := New()
	go func() {
		r.Reassembled(reassemblyString("hello\nworld\n"))
		r.ReassemblyComplete()
	}()

	l, err := r.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(l, []byte("hello")) {
		t.Error("expected hello actual", string(l))
	}

	l, err = r.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(l, []byte("world")) {
		t.Error("expected world actual", string(l))
	}

}

func TestReadLineMissing(t *testing.T) {
	r := New()
	r.LossErrors = true
	go func() {
		r.Reassembled([]tcpassembly.Reassembly{reassemblyWithSkip(5, "world")})
		r.ReassemblyComplete()
	}()
	_, err := r.ReadLine()
	if err != (ErrLostData{5}) || !strings.Contains(err.Error(), "lost 5 bytes") {
		t.Error("Expected ErrLostData but got", err)
	}
	l, err := r.ReadLine()
	if !bytes.Equal(l, []byte("world")) {
		t.Error("expected world but got", string(l))
	}
	if err != nil {
		t.Error("error from ReadLine", err)
	}
}

func testReadLine(t *testing.T, input []tcpassembly.Reassembly, expected string, expectedErr error) {
	r := New()
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
}

func TestDiscard(t *testing.T) {
	// block boundary
	testDiscard(t, []tcpassembly.Reassembly{
		reassemblyString("1")[0],
		reassemblyString("23")[0],
		reassemblyString("hello\n")[0],
	}, 3, "hello\n", false)

	// split last block
	testDiscard(t, []tcpassembly.Reassembly{
		reassemblyString("1")[0],
		reassemblyString("2")[0],
		reassemblyString("3hello\n")[0],
	}, 3, "hello\n", false)

	// insufficient data
	testDiscard(t, reassemblyString("12"), 2, "", true)

	// empty
	testDiscard(t, nil, 0, "", true)
}

func TestDiscardMissingRecovery(t *testing.T) {
	r := New()
	r.LossErrors = true
	go func() {
		r.Reassembled([]tcpassembly.Reassembly{
			reassemblyWithSkip(3, "ab"),
			reassemblyWithSkip(5, "world"),
		})
		r.ReassemblyComplete()
	}()
	r.Discard(2)
	r.Discard(10)
	l, err := r.ReadLine()
	if err != nil {
		t.Error("error from ReadLine", err)
	}
	if !bytes.Equal(l, []byte("rld")) {
		t.Error("expected rld but got", string(l))
	}
}

func testDiscard(t *testing.T, input []tcpassembly.Reassembly, expectedN int, expectedOut string, expectEOF bool) {
	r := New()
	done := make(chan error, 1)
	var out []byte
	go func() {
		n, err := r.Discard(3)
		if err != io.EOF && expectEOF {
			t.Error("input=", input, "got error:", err)
		}
		if err != nil && !expectEOF {
			t.Error("input=", input, "got error:", err)
		}
		if n != expectedN {
			t.Error("got", n, "expected", expectedN)
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
	r := New()
	r.Close()
	// all following data should now be discarded
	for i := 0; i < 100000; i++ {
		r.Reassembled(reassemblyString("foobarbaz"))
	}
	r.ReassemblyComplete()
}

func TestReadAfterClosePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fail()
		}
	}()
	r := New()
	r.Close()
	// expect panic
	_, _ = r.ReadLine()
	t.Fail()
}

func TestCloseDuringFlush(t *testing.T) {
	r := New()
	done := make(chan bool)
	// fill buffer
	for i := 0; i < maxPackets; i++ {
		r.Reassembled(reassemblyString("\n"))
	}
	go func() {
		// this should block until Close() is called
		r.Reassembled(reassemblyString("\n"))
		r.Reassembled(reassemblyString("\n"))
		r.ReassemblyComplete()
		done <- true
	}()

	// give goroutine a chance to get blocked
	select {
	case <-done:
		t.Error("goroutine did not block as expected")
	case <-time.After(10 * time.Millisecond):
	}

	r.Close()

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Error("ReassemblyComplete did not return")
	}
}

func TestPartnerFlush(t *testing.T) {
	s1, s2 := NewPair()
	pushDone := make(chan struct{}, 1)
	go func() {
		s2.Reassembled(reassemblyString("2\r\n"))
		for i := 0; i < maxPackets*2; i++ {
			s1.Reassembled(reassemblyString("1\r\n"))
		}
		pushDone <- struct{}{}
	}()

	go func() {
		// cannot proceed until s2 is flushed, which happens when s1 fills its buffer
		s2.ReadLine()
		// this read allows s1 to be flushed, which allows the writes to complete
		s1.ReadLine()
	}()

	select {
	case <-pushDone:
	case <-time.After(time.Second):
		t.Error("timed out")
	}
}

func TestInitialFlush(t *testing.T) {
	s1, s2 := NewPair()
	sync := make(chan struct{}, 1)
	go func() {
		s2.Reassembled(reassemblyString("2\r\n"))
		// flush through s1, but reader hasn't started yet
		for i := 0; i < maxPackets*2; i++ {
			s1.Reassembled(reassemblyString("1\r\n"))
		}
		// allow reader to start
		sync <- struct{}{}
		for i := 0; i < maxPackets*2; i++ {
			s1.Reassembled(reassemblyString("1\r\n"))
		}
		s1.ReassemblyComplete()
		// make sure writer did not get blocked
		sync <- struct{}{}
	}()
	<-sync
	s2.ReadLine()
	for i := 0; i < maxPackets*4; i++ {
		s1.ReadLine()
	}
	select {
	case <-sync:
	case <-time.After(time.Second):
		t.Error("timed out")
	}
}

type streamReader interface {
	tcpassembly.Stream
	io.Reader
}

func BenchmarkManySmallPackets(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bench(func() streamReader { return New() })
	}
}

func BenchmarkStandardReader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bench(func() streamReader {
			rs := tcpreader.NewReaderStream()
			return &rs
		})
	}
}

func bench(factory func() streamReader) {
	numStreams := 10000
	numPackets := 5
	var wg sync.WaitGroup
	wg.Add(numStreams)
	for g := 0; g < numStreams; g++ {
		r := factory()
		go func() {
			for i := 0; i < numPackets; i++ {
				r.Reassembled(reassembly(100))
			}
			r.ReassemblyComplete()
		}()
		go func() {
			ioutil.ReadAll(r)
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
