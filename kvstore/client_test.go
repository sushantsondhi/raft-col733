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

func spinUpClusterAndGetStoreInterface(b *testing.B, numServers int) (*KVStore, []*raft.RaftServer) {
	b.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(numServers)
	var clusterConfigs []common.ClusterConfig
	for i := 0; i < numServers; i++ {
		clusterConfigs = append(clusterConfigs, clusterConfig)
	}

	raftServers := makeRaftCluster(b, clusterConfigs...)
	verifyElectionSafetyAndLiveness(b, raftServers)
	clientManager := rpc.NewManager()

	store, err := NewKeyValStore(clusterConfig.Cluster, clientManager)
	assert.NoError(b, err)
	return store, raftServers
}

func BenchmarkClient_ReadWriteThroughput(b *testing.B) {

	numServers := 3
	// Write ThroughPut
	store, _ := spinUpClusterAndGetStoreInterface(b, numServers)
	numRequests := 100

	start := time.Now()

	//var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		//wg.Add(1)
		reqNumber := i // Warning: Loop variables captured by 'func' literals in 'go'
		// statements might have unexpected values
		//go func() {
		//	defer wg.Done()
		key := fmt.Sprintf("key%d", reqNumber)
		val := fmt.Sprintf("val%d", reqNumber)
		store.Set(key, val)
		//}()
	}

	//wg.Wait()
	elapsed := time.Since(start)
	writeTime := elapsed
	fmt.Printf("[Benchmark] %d write requests took %s on %d servers.\n", numRequests, writeTime, numServers)

	// Read ThroughPut

	//start = time.Now()
	//wg = sync.WaitGroup{}
	//for i := 0; i < numRequests; i++ {
	//	wg.Add(1)
	//	reqNumber := i
	//	go func() {
	//		defer wg.Done()
	//		key := fmt.Sprintf("key%d", reqNumber)
	//		store.Get(key)
	//	}()
	//}
	//
	//wg.Wait()
	//elapsed = time.Since(start)
	//readTime := elapsed
	//fmt.Printf("[Benchmark] %d read requests took %s on %d servers.\n", numRequests, readTime, numServers)
	//
	//assert.Less(b, math.Abs((readTime - writeTime).Seconds()), 1.0)

}

func BenchmarkServer_CatchUpTime(b *testing.B) {
	numServers := 3
	numLogsToCatchUp := 100

	laggingServerIndex := 2

	store, servers := spinUpClusterAndGetStoreInterface(b, numServers)

	servers[laggingServerIndex].Disconnect()

	var wg sync.WaitGroup
	for i := 0; i < numLogsToCatchUp; i++ {
		wg.Add(1)
		reqNumber := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key%d", reqNumber)
			val := fmt.Sprintf("val%d", reqNumber)
			store.Set(key, val)
		}()
	}

	wg.Wait()

	servers[laggingServerIndex].Reconnect()

	start := time.Now()
	// Assuming correctness
	for {
		logLength, err := servers[laggingServerIndex].LogStore.Length()
		assert.NoError(b, err)
		if int(logLength) == numLogsToCatchUp+1 {
			break
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("[Benchmark] lagging server took took %s to catch up %d entries on a %d server raft.\n", elapsed, numLogsToCatchUp, numServers)
}
