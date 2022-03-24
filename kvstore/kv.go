package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
)

// KVStore implements a simple key-value store over the Raft implementation.
// This acts as a simple abstraction over Raft's RPC interface intended to be used
// as a library by the clients.
// This is a thread-safe library.
type KVStore struct {
	RaftServers        []common.RPCServer
	LastKnownResponder *atomic.Int32
}

func NewKeyValStore(addrs []common.Server, manager common.RPCManager) (*KVStore, error) {
	store := KVStore{
		LastKnownResponder: atomic.NewInt32(0),
	}
	for _, addr := range addrs {
		server, err := manager.ConnectToPeer(addr.NetAddress, addr.ID)
		if err != nil {
			return nil, fmt.Errorf("error connecting to raft server at %v\n", addr.NetAddress)
		}
		store.RaftServers = append(store.RaftServers, server)
	}
	return &store, nil
}

// SetWithUUID method creates a PUT request with given id, if the store has
// already seen a request (even if GET) with the same id it will not apply
// this operation again.
func (kv *KVStore) SetWithUUID(key, val string, id uuid.UUID) (err error) {
	request := Request{
		Type:          Set,
		Key:           key,
		Val:           val,
		TransactionId: id,
	}
	var bytes []byte
	bytes, err = json.Marshal(request)
	if err != nil {
		return
	}
	lastKnownResponder := int(kv.LastKnownResponder.Load())
	for i := 0; i < len(kv.RaftServers); i++ {
		server := kv.RaftServers[(i+lastKnownResponder)%len(kv.RaftServers)]
		var result common.ClientRequestRPCResult
		reqErr := server.ClientRequest(&common.ClientRequestRPC{
			Data: bytes,
		}, &result)
		if reqErr != nil {
			err = multierr.Append(err, reqErr)
			continue
		}
		kv.LastKnownResponder.Store(int32((i + lastKnownResponder) % len(kv.RaftServers)))
		if !result.Success {
			err = multierr.Append(err, errors.New(result.Error))
		}
		break
	}
	return
}

// Set method can be used to add or update key-value pair in the store.
// It returns a UUID which may be used to retry the operation with
// idempotence guarantees using the SetWithUUID method.
func (kv *KVStore) Set(key, val string) (uuid.UUID, error) {
	id := uuid.New()
	return id, kv.SetWithUUID(key, val, id)
}

func (kv *KVStore) GetWithUUID(key string, id uuid.UUID) (val string, err error) {
	request := Request{
		Type:          Get,
		Key:           key,
		TransactionId: id,
	}
	var bytes []byte
	bytes, err = json.Marshal(request)
	if err != nil {
		return
	}
	lastKnownResponder := int(kv.LastKnownResponder.Load())
	for i := 0; i < len(kv.RaftServers); i++ {
		server := kv.RaftServers[(i+lastKnownResponder)%len(kv.RaftServers)]
		var result common.ClientRequestRPCResult
		reqErr := server.ClientRequest(&common.ClientRequestRPC{
			Data: bytes,
		}, &result)
		if reqErr != nil {
			err = multierr.Append(err, reqErr)
			continue
		}
		kv.LastKnownResponder.Store(int32((i + lastKnownResponder) % len(kv.RaftServers)))
		if !result.Success {
			err = multierr.Append(err, errors.New(result.Error))
		}
		val = string(result.Data)
		break
	}
	return
}

// Get method can be used to get the value corresponding to the given key in the store.
// It also returns a UUID that may be used to retry this operation with
// idempotence guarantees. In particular for get operation this means the call with
// return an older value that was at the time of the first call.
func (kv *KVStore) Get(key string) (uuid.UUID, string, error) {
	id := uuid.New()
	val, err := kv.GetWithUUID(key, id)
	return id, val, err
}
