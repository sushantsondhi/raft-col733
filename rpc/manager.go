package rpc

import (
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"net"
	"net/rpc"
)

// Manager is the implementation of raft.RPCManager interface using
// the golang's net/rpc package
type Manager struct{}

var _ common.RPCManager = &Manager{}

func (manager *Manager) Start(address common.ServerAddress, server common.RPCServer) error {
	rpcServ := rpc.NewServer()
	if err := rpcServ.RegisterName("RPCServer", server); err != nil {
		return err
	}

	for {
		listener, err := net.Listen("tcp", string(address))
		if err != nil {
			return err
		}
		rpcServ.Accept(listener)
		// Code can only reach this line if there was a serious network
		// error preventing listener to break, so we loop and try to
		// re-establish listener.
	}
}

func (manager *Manager) ConnectToPeer(address common.ServerAddress, id uuid.UUID) (common.RPCServer, error) {
	return NewPeer(address, id), nil
}
