package analysis

import (
	"github.com/box/memsniff/protocol/model"
	"regexp"
	"sync"
)

// filter is a threadsafe container for a regex.
type filter struct {
	sync.RWMutex
	r *regexp.Regexp
}

func (f *filter) filterEvents(rs []model.Event) []model.Event {
	re := f.regex()
	if re == nil {
		return rs
	}

	matches := make([]model.Event, 0, len(rs))
	for _, r := range rs {
		if re.MatchString(r.Key) {
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
