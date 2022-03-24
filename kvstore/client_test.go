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

func spinUpClusterAndGetStoreInterface(b *testing.B) *KVStore {
	b.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig1

	_ = makeRaftCluster(b, clusterConfig1, clusterConfig2, clusterConfig3)

	clientManager := rpc.NewManager()

	store, err := NewKeyValStore(clusterConfig1.Cluster, clientManager)
	assert.NoError(b, err)
	return store
}

func BenchmarkClient(b *testing.B) {

	store := spinUpClusterAndGetStoreInterface(b)

	numRequests := 2

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
