package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/sushantsondhi/raft-col733/common"
)

type RequestType int

const (
	Get RequestType = iota
	Set
)

type Request struct {
	Type     RequestType
	Key, Val string
}

// KeyValFSM is the implementation of the raft.FSM interface
// for the key-value store. We store the key value pairs
// in-memory because they can be reliably reconstructed
// on server restarts by simply replaying the log
type KeyValFSM struct {
	store map[string]string
}

var _ common.FSM = &KeyValFSM{}

func NewKeyValFSM() *KeyValFSM {
	return &KeyValFSM{
		store: make(map[string]string),
	}
}

func (fsm *KeyValFSM) Apply(entry common.LogEntry) ([]byte, error) {
	var request Request
	if err := json.Unmarshal(entry.Data, &request); err != nil {
		return nil, err
	}
	switch request.Type {
	case Get:
		if val, ok := fsm.store[request.Key]; ok {
			return []byte(val), nil
		} else {
			return nil, fmt.Errorf("key does not exist")
		}
	case Set:
		fsm.store[request.Key] = request.Val
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid request type")
	}
}
