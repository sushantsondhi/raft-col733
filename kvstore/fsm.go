package kvstore

import (
	"github.com/sushantsondhi/raft-col733/raft"
)

// KeyValFSM is the implementation of the raft.FSM interface
// for the key-value store. We store the key value pairs
// in-memory because they can be reliably reconstructed
// on server restarts by simply replaying the log
type KeyValFSM struct {
	store map[string]string
}

var _ raft.FSM = &KeyValFSM{}

func (fsm *KeyValFSM) Apply(entry raft.LogEntry) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}
