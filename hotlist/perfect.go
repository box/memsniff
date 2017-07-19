package hotlist

type perfectHotlist map[Item]int

// NewPerfect returns an implementation of HotList that tracks all items
// added, without sampling loss.
//
// Note that the returned HotList has unbounded memory consumption.
func NewPerfect() HotList {
	return perfectHotlist(make(map[Item]int))
}

func (hl perfectHotlist) AddWeighted(x Item) {
	hl.AddNWeighted(x, 1)
}

func (hl perfectHotlist) AddNWeighted(x Item, n int) {
	hl[x] += n
}

func (hl perfectHotlist) Reset() {
	for k := range hl {
		delete(hl, k)
	}
}

func (hl perfectHotlist) Top(k int) []Entry {
	return orderedTop(k, hl)
}
