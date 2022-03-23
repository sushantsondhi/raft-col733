package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
)

type RequestType int

const (
	Get RequestType = iota
	Set
)

type Request struct {
	Type          RequestType
	Key, Val      string
	TransactionId uuid.UUID
}

// KeyValFSM is the implementation of the raft.FSM interface
// for the key-value Store. We Store the key value pairs
// in-memory because they can be reliably reconstructed
// on server restarts by simply replaying the log
type KeyValFSM struct {
	Store               map[string]string
	appliedTransactions map[uuid.UUID][]byte // map of (transId, returned value)
}

var _ common.FSM = &KeyValFSM{}

func NewKeyValFSM() *KeyValFSM {
	return &KeyValFSM{
		Store:               make(map[string]string),
		appliedTransactions: make(map[uuid.UUID][]byte),
	}
}

func (fsm *KeyValFSM) Apply(entry common.LogEntry) ([]byte, error) {
	var request Request
	if err := json.Unmarshal(entry.Data, &request); err != nil {
		return nil, err
	}
	switch request.Type {
	case Get:
		if val, ok := fsm.appliedTransactions[request.TransactionId]; ok {
			return val, nil
		}
		if val, ok := fsm.Store[request.Key]; ok {
			fsm.appliedTransactions[request.TransactionId] = []byte(val)
			return []byte(val), nil
		} else {
			fsm.appliedTransactions[request.TransactionId] = nil
			return nil, fmt.Errorf("key does not exist")
		}

	case Set:
		if _, ok := fsm.appliedTransactions[request.TransactionId]; ok {
			return nil, nil
		}
		fsm.appliedTransactions[request.TransactionId] = nil
		fsm.Store[request.Key] = request.Val
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid request type")
	}
}
