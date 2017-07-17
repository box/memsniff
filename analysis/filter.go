package analysis

import (
	"regexp"
	"sync"
)

// filter is a threadsafe container for a regex.
type filter struct {
	sync.RWMutex
	r *regexp.Regexp
}

func (f *filter) match(bs []byte) bool {
	r := f.regex()
	return r == nil || r.Match(bs)
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
