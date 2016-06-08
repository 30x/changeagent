package raft

import (
	"sort"
)

func reverseSortUint64(a []uint64) {
	sorter := &uintSorter{a: a, reverse: true}
	sort.Sort(sorter)
}

type uintSorter struct {
	a       []uint64
	reverse bool
}

func (s *uintSorter) Len() int {
	return len(s.a)
}

func (s *uintSorter) Less(i, j int) bool {
	if s.reverse {
		return s.a[j] < s.a[i]
	}
	return s.a[i] < s.a[j]
}

func (s *uintSorter) Swap(i, j int) {
	tmp := s.a[i]
	s.a[i] = s.a[j]
	s.a[j] = tmp
}
