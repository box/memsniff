// Package hotlist provides utilities for summarizing frequently encountered items.
package hotlist

import (
	"sort"
)

// Item instances are tracked by a Hotlist, possible based on the associated weight.
type Item interface {
	Weight() int
}

// HotList tracks the frequency of items added to it, discarding infrequent
// items and potentially retaining items based on relative weights.
type HotList interface {
	AddWeighted(x Item)
	AddNWeighted(x Item, n int)
	Reset()
	Top(k int) []Entry
}

// Entry represents the number of times an Item has occurred.
type Entry interface {
	Item() Item
	Count() int
}

type itemCount struct {
	item        Item
	count       int
	totalWeight int
}

func (ic itemCount) Item() Item {
	return ic.item
}

func (ic itemCount) Count() int {
	return ic.count
}

type descByTotalWeight []itemCount

func (cs descByTotalWeight) Len() int           { return len(cs) }
func (cs descByTotalWeight) Less(i, j int) bool { return cs[j].totalWeight < cs[i].totalWeight }
func (cs descByTotalWeight) Swap(i, j int)      { cs[i], cs[j] = cs[j], cs[i] }

func orderedTop(k int, unordered map[Item]int) []Entry {
	if len(unordered) < k {
		k = len(unordered)
	}
	ordered := make(descByTotalWeight, 0, len(unordered))
	for item, count := range unordered {
		ordered = append(ordered, itemCount{item, count, item.Weight() * count})
	}
	sort.Sort(ordered)

	entries := make([]Entry, 0, len(ordered))
	for _, ic := range ordered {
		// For sorting purposes, precompute total weight
		entries = append(entries, ic)
	}

	return entries[0:k]
}
