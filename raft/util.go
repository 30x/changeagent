package raft

import (
  "sort"
)

/*
 * Sort a slice of uint64s in place in numeric order.
 */
func sortUint64(a []uint64) {
  sorter := &uintSorter{a: a}
  sort.Sort(sorter)
}

type uintSorter struct {
  a []uint64
}

func (s *uintSorter) Len() int {
  return len(s.a)
}

func (s *uintSorter) Less(i, j int) bool {
  return s.a[i] < s.a[j]
}

func (s *uintSorter) Swap(i, j int) {
  tmp := s.a[i]
  s.a[i] = s.a[j]
  s.a[j] = tmp
}

