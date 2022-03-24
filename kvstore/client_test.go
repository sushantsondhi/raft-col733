package kvstore

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/raft"
	"github.com/sushantsondhi/raft-col733/rpc"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func makeRaftCluster(b *testing.B, configs ...common.ClusterConfig) (servers []*raft.RaftServer) {
	for i := range configs {
		logstore, err := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(b, err)
		pstore, err := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(b, err)
		raftServer := raft.NewRaftServer(configs[i].Cluster[i], configs[i], NewKeyValFSM(), logstore, pstore, rpc.NewManager())
		assert.NotNil(b, raftServer)
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
			NetAddress: common.ServerAddress(fmt.Sprintf("127.0.0.1:%d", 12345+i)),
		})
	}
	return common.ClusterConfig{
		Cluster:          servers,
		HeartBeatTimeout: 50 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
	}
}

func verifyElectionSafetyAndLiveness(b *testing.B, servers []*raft.RaftServer) {
	liveness := false
	for i := 0; i < 20; i++ {
		leaders := make(map[int64][]uuid.UUID)
		for _, server := range servers {
			server.Mutex.Lock()
			if server.State == raft.Leader {
				leaders[server.Term] = append(leaders[server.Term], server.GetID())
			}
			server.Mutex.Unlock()
		}
		for term, ldrs := range leaders {
			fmt.Printf("Term = %d, ldrs = %v\n", term, ldrs)
			assert.LessOrEqualf(b, len(ldrs), 1, "multiple leaders for term %d", term)
			liveness = true
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Truef(b, liveness, "election liveness not satisfied (no leader elected ever)")
}

func spinUpClusterAndGetStoreInterface(b *testing.B) *KVStore {
	b.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig1

	raftServers := makeRaftCluster(b, clusterConfig1, clusterConfig2, clusterConfig3)
	verifyElectionSafetyAndLiveness(b, raftServers)
	clientManager := rpc.NewManager()

	store, err := NewKeyValStore(clusterConfig1.Cluster, clientManager)
	assert.NoError(b, err)
	return store
}

func BenchmarkClient(b *testing.B) {

	store := spinUpClusterAndGetStoreInterface(b)
	numRequests := 100

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		reqNumber := i // Warning: Loop variables captured by 'func' literals in 'go'
		// statements might have unexpected values
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key%d", reqNumber)
			val := fmt.Sprintf("val%d", reqNumber)
			store.Set(key, val)
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("[Benchmark] %d requests took %s\n", numRequests, elapsed)
}
