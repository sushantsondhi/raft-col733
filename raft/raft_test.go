package raft

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeRaftCluster(t *testing.T, configs ...common.ClusterConfig) (servers []*RaftServer) {
	for i := range configs {
		logstore, err := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(t, err)
		pstore, err := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(t, err)
		raftServer := NewRaftServer(configs[i].Cluster[i], configs[i], nil, logstore, pstore, &rpc.Manager{})
		assert.NotNil(t, raftServer)
		servers = append(servers, raftServer)
	}
	return
}

func cleanupDbFiles() {
	matches, err := filepath.Glob("*.db")
	if err != nil {
		panic(err)
	}
	for _, match := range matches {
		os.Remove(match)
	}
}

func generateClusterConfig(n int) common.ClusterConfig {
	var servers []common.Server
	for i := 0; i < n; i++ {
		servers = append(servers, common.Server{
			ID:         uuid.New(),
			NetAddress: common.ServerAddress(fmt.Sprintf(":%d", 12345+i)),
		})
	}
	return common.ClusterConfig{
		Cluster:          servers,
		HeartBeatTimeout: 50 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
	}
}

func Test_SimpleElection(t *testing.T) {
	clusterConfig := generateClusterConfig(3)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	t.Cleanup(cleanupDbFiles)

	for i := 0; i < 20; i++ {
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
