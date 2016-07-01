package common

import "strconv"

// NodeID represents the unique ID of a single Raft node
type NodeID uint64

func (n NodeID) String() string {
	return strconv.FormatUint(uint64(n), 36)
}

/*
ParseNodeID takes the string generated by "String" and turns it back
in to a NodeID.
*/
func ParseNodeID(s string) NodeID {
	v, err := strconv.ParseUint(s, 36, 64)
	if err != nil {
		return 0
	}
	return NodeID(v)
}