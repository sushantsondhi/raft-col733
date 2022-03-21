package rpc_test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/rpc"
	"sync"
	"testing"
	"time"
)

// TestRaft is a mock implementation of raft server for testing purposes
type TestRaft struct{}

func (TestRaft) GetID() uuid.UUID {
	return uuid.Nil
}

func (TestRaft) ClientRequest(args *common.ClientRequestRPC, result *common.ClientRequestRPCResult) error {
	fmt.Printf("Received request: %+v\n", *args)
	result.Success = true
	return nil
}

func (TestRaft) RequestVote(args *common.RequestVoteRPC, result *common.RequestVoteRPCResult) error {
	// RequestVote always fails
	return fmt.Errorf("encountered some error")
}

func (TestRaft) AppendEntries(args *common.AppendEntriesRPC, result *common.AppendEntriesRPCResult) error {
	//TODO implement me
	panic("implement me")
}

func Test_CreateRaftServers(t *testing.T) {
	// This method tests that we are able to spawn raft servers
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			server := TestRaft{}
			manager := rpc.NewManager()
			err := manager.Start(common.ServerAddress(fmt.Sprintf(":%d", 1234+i)), server)
			assert.NoError(t, err)
		}()
	}
	// wait for a few seconds to ensure that servers get proper time to start
	time.Sleep(time.Second * 2)
}

func Test_CanConnect(t *testing.T) {
	// This method tests that we are able to connect to existing raft servers
	// spawn a test raft server at 1234
	manager := rpc.NewManager()
	go func() {
		server := TestRaft{}
		err := manager.Start(common.ServerAddress(fmt.Sprintf(":%d", 1234)), server)
		assert.NoError(t, err)
	}()

	// let's now attempt to call some methods on it
	// 100 peers will concurrently try to contact server
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			// let's now attempt to connect to it
			// note: this is a lazy connect
			peer, err := manager.ConnectToPeer(":1234", uuid.Nil)
			assert.NoError(t, err)

			// even if server is not actually up yet, this method
			// will not fail (it will retry internally)
			var resp1 common.ClientRequestRPCResult
			err = peer.ClientRequest(&common.ClientRequestRPC{
				Data: []byte("asdf"),
			}, &resp1)
			assert.NoError(t, err)
			assert.True(t, resp1.Success)

			var resp2 common.RequestVoteRPCResult
			err = peer.RequestVote(&common.RequestVoteRPC{}, &resp2)
			assert.EqualError(t, err, "encountered some error")
			wg.Done()
		}()
	}
	wg.Wait()
}
