package rpc

import (
	"errors"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"log"
	"net"
	"net/rpc"
)

// Manager is the implementation of raft.RPCManager interface using
// the golang's net/rpc package
type Manager struct {
	listener net.Listener
	peers    []*Peer
	stopChan chan bool
}

var _ common.RPCManager = &Manager{}

func NewManager() *Manager {
	return &Manager{
		stopChan: make(chan bool, 1),
	}
}

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
		manager.listener = listener
		rpcServ.Accept(listener)
		log.Printf("%v: got disconnected\n", server.GetID())
		if _, ok := <-manager.stopChan; ok {
			// we were explicitly disconnected, so don't resume
			return errors.New("disconnected")
		}
		// Code can only reach this line if there was a serious network
		// error preventing listener to break, so we loop and try to
		// re-establish listener.
	}
}

func (manager *Manager) ConnectToPeer(address common.ServerAddress, id uuid.UUID) (common.RPCServer, error) {
	peer := NewPeer(address, id)
	manager.peers = append(manager.peers, peer)
	return peer, nil
}

func (manager *Manager) Stop() error {
	manager.stopChan <- true
	return manager.listener.Close()
}

func (manager *Manager) Disconnect() {
	for _, peer := range manager.peers {
		peer.disconnected = true
	}
}

func (manager *Manager) Reconnect() {
	for _, peer := range manager.peers {
		peer.disconnected = false
	}
}
