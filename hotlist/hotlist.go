// Copyright 2017 Box, Inc.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package hotlist provides utilities for summarizing frequently encountered items.
package hotlist

import (
	"sort"
)

const (
	initialThreshold = 1
)

type Item interface {
	Weight() int
}

type HotList interface {
	AddWeighted(x Item)
	AddNWeighted(x Item, n int)
	Reset()
	Top(k int) []Entry
}

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
