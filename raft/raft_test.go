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
		raftServer := NewRaftServer(configs[i].Cluster[i], configs[i], nil, logstore, pstore, rpc.NewManager())
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

func verifyElectionSafetyAndLiveness(t *testing.T, servers []*RaftServer) {
	liveness := false
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
			liveness = true
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Truef(t, liveness, "election liveness not satisfied (no leader elected ever)")
}

func Test_SimpleElection(t *testing.T) {
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers)
}

func Test_ElectionWithoutHeartbeat(t *testing.T) {
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	clusterConfig.HeartBeatTimeout = 10 * time.Hour
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers)
}

func Test_ReElection(t *testing.T) {
	t.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig1
	// purposefully delay the election timeouts of 2 & 3 to ensure that 1 gets elected as leader first
	clusterConfig2.ElectionTimeout = time.Second
	clusterConfig3.ElectionTimeout = time.Second

	servers := makeRaftCluster(t, clusterConfig1, clusterConfig2, clusterConfig3)
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, servers[0].State, Leader)
	// now 1 must have been elected as leader, so we disconnect it from cluster
	servers[0].Disconnect()
	// someone else should be elected as a leader
	verifyElectionSafetyAndLiveness(t, servers)
	assert.True(t, servers[1].State == Leader || servers[2].State == Leader)
	// note that server 1 will still remain a leader but of an older term
	assert.Equal(t, servers[0].State, Leader)
	assert.Less(t, servers[0].Term, servers[1].Term)

	// now reconnect server 1 to cluster
	// it will convert to follower with same term
	servers[0].Reconnect()
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, servers[0].State, Follower)
	assert.Equal(t, servers[0].Term, servers[1].Term)
}

func Test_ReJoin(t *testing.T) {
	t.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig1
	// purposefully delay the election timeouts of 2 & 3 to ensure that 1 gets elected as leader first
	clusterConfig2.ElectionTimeout = time.Second
	clusterConfig3.ElectionTimeout = time.Second

	servers := makeRaftCluster(t, clusterConfig1, clusterConfig2, clusterConfig3)
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, servers[0].State, Leader)

	// now disconnect 2 (a follower) from the cluster
	servers[2].Disconnect()
	// it should not affect election safety and liveness
	verifyElectionSafetyAndLiveness(t, servers)
	// wait for a few more seconds
	time.Sleep(3 * time.Second)
	// term of 2 must be ahead of the other two
	assert.Equal(t, servers[2].State, Candidate)
	assert.Greater(t, servers[2].Term, servers[0].Term)
	assert.Greater(t, servers[2].Term, servers[1].Term)

	// now we reconnect 2
	servers[2].Reconnect()
	verifyElectionSafetyAndLiveness(t, servers)
}
