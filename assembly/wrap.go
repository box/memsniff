package assembly

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"log"
)

type wrap struct {
	tcpassembly.Stream
	flow gopacket.Flow
}

func (w wrap) Reassembled(rs []tcpassembly.Reassembly) {
	toLog := make([]string, len(rs))
	for i, r := range rs {
		if r.Skip > 0 {
			toLog[i] = fmt.Sprintf("(skip %d) %d", r.Skip, len(r.Bytes))
		} else {
			toLog[i] = fmt.Sprintf(" %d", len(r.Bytes))
		}
	}
	log.Println(w.flow, toLog)
	w.Stream.Reassembled(rs)
}

func (w wrap) ReassemblyComplete() {
	log.Println("ReassemblyComplete")
	w.Stream.ReassemblyComplete()
}
