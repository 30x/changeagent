// Code generated by "stringer -type LoopCommand ."; DO NOT EDIT

package raft

import "fmt"

const _LoopCommand_name = "UpdateConfigurationJoinAsFollowerJoinAsCandidate"

var _LoopCommand_index = [...]uint8{0, 19, 33, 48}

func (i LoopCommand) String() string {
	if i < 0 || i >= LoopCommand(len(_LoopCommand_index)-1) {
		return fmt.Sprintf("LoopCommand(%d)", i)
	}
	return _LoopCommand_name[_LoopCommand_index[i]:_LoopCommand_index[i+1]]
}