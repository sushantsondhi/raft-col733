package rpc_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/raft"
	"github.com/sushantsondhi/raft-col733/rpc"
	"sync"
	"testing"
	"time"
)

// TestRaft is a mock implementation of raft server for testing purposes
type TestRaft struct{}

func (TestRaft) ClientRequest(args *raft.ClientRequestRPC, result *raft.ClientRequestRPCResult) error {
	fmt.Printf("Received request: %+v\n", *args)
	result.Success = true
	return nil
}

func (TestRaft) RequestVote(args *raft.RequestVoteRPC, result *raft.RequestVoteRPCResult) error {
	// RequestVote always fails
	return fmt.Errorf("encountered some error")
}

func (TestRaft) AppendEntries(args *raft.AppendEntriesRPC, result *raft.AppendEntriesRPCResult) error {
	//TODO implement me
	panic("implement me")
}

func Test_CreateRaftServers(t *testing.T) {
	// This method tests that we are able to spawn raft servers
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			server := TestRaft{}
			manager := rpc.Manager{}
			err := manager.Start(raft.ServerAddress(fmt.Sprintf(":%d", 1234+i)), server)
			assert.NoError(t, err)
		}()
	}
	// wait for a few seconds to ensure that servers get proper time to start
	time.Sleep(time.Second * 2)
}

func Test_CanConnect(t *testing.T) {
	// This method tests that we are able to connect to existing raft servers
	// spawn a test raft server at 1234
	manager := rpc.Manager{}
	go func() {
		server := TestRaft{}
		err := manager.Start(raft.ServerAddress(fmt.Sprintf(":%d", 1234)), server)
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
			peer, err := manager.ConnectToPeer(":1234")
			assert.NoError(t, err)

			// even if server is not actually up yet, this method
			// will not fail (it will retry internally)
			var resp1 raft.ClientRequestRPCResult
			err = peer.ClientRequest(&raft.ClientRequestRPC{
				MinAppliedIndex: 10,
				Data:            []byte("asdf"),
			}, &resp1)
			assert.NoError(t, err)
			assert.True(t, resp1.Success)

			var resp2 raft.RequestVoteRPCResult
			err = peer.RequestVote(&raft.RequestVoteRPC{}, &resp2)
			assert.EqualError(t, err, "encountered some error")
			wg.Done()
		}()
	}
	wg.Wait()
}
