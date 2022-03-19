package raft

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/rpc"
	"testing"
	"time"
)

func Test_SimpleElection(t *testing.T) {
	serverDetails := []common.Server{
		{
			ID:         uuid.New(),
			NetAddress: ":12345",
		},
		{
			ID:         uuid.New(),
			NetAddress: ":12346",
		},
		{
			ID:         uuid.New(),
			NetAddress: ":12347",
		},
	}
	var servers []*RaftServer
	cluster := common.ClusterConfig{
		Cluster:          serverDetails,
		HeartBeatTimeout: 50 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
	}

	for i := range serverDetails {
		logStore, err := persistent.CreateDbLogStore("logstore-" + serverDetails[i].ID.String() + ".db")
		assert.NoError(t, err)
		persistentStore, err := persistent.NewPStore("persistentstore-" + serverDetails[i].ID.String() + ".db")
		server := NewRaftServer(serverDetails[i], cluster, nil, logStore, persistentStore, &rpc.Manager{})
		servers = append(servers, server)
	}

	for i := 0; i < 50; i++ {
		leaders := make(map[int64][]uuid.UUID)
		for _, server := range servers {
			server.Mutex.Lock()
			if server.State == Leader {
				leaders[server.Term] = append(leaders[server.Term], server.GetID())
			}
			server.Mutex.Unlock()
		}
		for term, ldrs := range leaders {
			fmt.Printf("Term = %d, ldrs = %v\n", term, ldrs)
			assert.LessOrEqualf(t, len(ldrs), 1, "multiple leaders for term %d", term)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
