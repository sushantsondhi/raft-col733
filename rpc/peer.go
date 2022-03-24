package rpc

import (
	"errors"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"io"
	"net/rpc"
	"time"
)

// Peer is the implementation of raft.RPCServer interface using the
// golang's net/rpc package
type Peer struct {
	id           uuid.UUID
	address      common.ServerAddress
	client       *rpc.Client
	disconnected bool
}

// NewPeer creates a Peer instance with lazy initialization.
// Actual RPC connection is not established until an actual RPC
// call takes place.
func NewPeer(address common.ServerAddress, id uuid.UUID) *Peer {
	return &Peer{
		id:      id,
		address: address,
	}
}

// call takes care of automatically re-trying on transient failures
func (peer *Peer) call(method string, args interface{}, result interface{}) (err error) {
	if peer.disconnected && method != "RPCServer.ClientRequest" {
		return errors.New("disconnected rpc connection")
	}
	for i := 0; i < 3; i++ {
		if peer.client == nil {
			if peer.client, err = rpc.Dial("tcp", string(peer.address)); err != nil {
				// retry with one-second delay
				peer.client = nil
				time.Sleep(time.Second)
				continue
			}
		}
		if err = peer.client.Call(method, args, result); err == io.EOF {
			// likely that connection timed out, retry immediately
			peer.client.Close()
			peer.client = nil
			continue
		}
		break
	}
	return
}

func (peer *Peer) GetID() uuid.UUID {
	return peer.id
}

func (peer *Peer) ClientRequest(args *common.ClientRequestRPC, result *common.ClientRequestRPCResult) error {
	return peer.call("RPCServer.ClientRequest", args, result)
}

func (peer *Peer) RequestVote(args *common.RequestVoteRPC, result *common.RequestVoteRPCResult) error {
	return peer.call("RPCServer.RequestVote", args, result)
}

func (peer *Peer) AppendEntries(args *common.AppendEntriesRPC, result *common.AppendEntriesRPCResult) error {
	return peer.call("RPCServer.AppendEntries", args, result)
}
