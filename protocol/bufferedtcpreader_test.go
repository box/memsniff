package protocol

import (
	"github.com/google/gopacket/tcpassembly"
	"io/ioutil"
	"testing"
	"time"
	"math/rand"
	"bytes"
	"hash/fnv"
	"io"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"sync"
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

func TestLotsOfData(t *testing.T) {
	r := NewTCPReaderStream()
	var out []byte
	var err error
	done := make(chan struct{}, 1)
	go func() {
		out, err = ioutil.ReadAll(r)
		done <- struct{}{}
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
			for i := 0;i < numPackets; i++ {
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
