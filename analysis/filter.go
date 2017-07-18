package analysis

import (
	"github.com/box/memsniff/protocol"
	"regexp"
	"sync"
)

// filter is a threadsafe container for a regex.
type filter struct {
	sync.RWMutex
	r *regexp.Regexp
}

func (f *filter) filterResponses(rs []*protocol.GetResponse) []*protocol.GetResponse {
	re := f.regex()
	if re == nil {
		return rs
	}

	matches := make([]*protocol.GetResponse, 0, len(rs))
	for _, r := range rs {
		if re.Match(r.Key) {
			matches = append(matches, r)
		}
	}
	return matches
}

func (f *filter) regex() *regexp.Regexp {
	f.RLock()
	defer f.RUnlock()
	return f.r
}

func (f *filter) setPattern(pattern string) (err error) {
	var r *regexp.Regexp
	if pattern != "" {
		r, err = regexp.Compile(pattern)
		if err != nil {
			return
		}
	}

	f.Lock()
	defer f.Unlock()
	f.r = r

	return
}
