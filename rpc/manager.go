package rpc

import (
	"github.com/sushantsondhi/raft-col733/raft"
	"net"
	"net/rpc"
)

// Manager is the implementation of raft.RPCManager interface using
// the golang's net/rpc package
type Manager struct{}

func (manager *Manager) Start(address raft.ServerAddress, server raft.RPCServer) error {
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

func (manager *Manager) ConnectToPeer(address raft.ServerAddress) (raft.RPCServer, error) {
	return NewPeer(address), nil
}
