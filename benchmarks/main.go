package benchmarks

import (
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/kvstore"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/raft"
	"github.com/sushantsondhi/raft-col733/rpc"
	"go.uber.org/multierr"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func cleanupDbFiles() {
	matches, err := filepath.Glob("*.db")
	if err != nil {
		panic(err)
	}
	for _, match := range matches {
		os.Remove(match)
	}
}

func makeRaftCluster(configs ...common.ClusterConfig) (servers []*raft.RaftServer) {
	for i := range configs {
		logstore, logErr := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", configs[i].Cluster[i].ID))
		pstore, pErr := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", configs[i].Cluster[i].ID))
		err := multierr.Combine(logErr, pErr)
		if err != nil {
			fmt.Println(err)
			os.Exit(2)
		}
		raftServer := raft.NewRaftServer(configs[i].Cluster[i], configs[i], kvstore.NewKeyValFSM(), logstore, pstore, rpc.NewManager())
		servers = append(servers, raftServer)
	}
	return
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

func spinUpClusterAndGetStoreInterface(numServers int) (*kvstore.KVStore, []*raft.RaftServer) {
	clusterConfig := generateClusterConfig(numServers)
	var clusterConfigs []common.ClusterConfig
	for i := 0; i < numServers; i++ {
		clusterConfigs = append(clusterConfigs, clusterConfig)
	}

	raftServers := makeRaftCluster(clusterConfigs...)
	time.Sleep(3*time.Second)
	clientManager := rpc.NewManager()

	store, err := kvstore.NewKeyValStore(clusterConfig.Cluster, clientManager)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	return store, raftServers
}


func benchmarkClientReadWriteThroughput(args []string) {
	flagset := flag.NewFlagSet("bench1", flag.ExitOnError)
	var numServers int
	flagset.IntVar(&numServers, "numServers", 3, "Number of servers to spin")
	if err := flagset.Parse(args); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	store, _  := spinUpClusterAndGetStoreInterface(numServers)
	defer cleanupDbFiles()
	// Write ThroughPut
	fmt.Printf("Running Performance Check: Client Read Write Throughput")
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
	writeTime := elapsed
	fmt.Printf("[Benchmark] %d write requests took %s on %d servers.\n", numRequests, writeTime, numServers)

	// Read ThroughPut

	start = time.Now()
	wg = sync.WaitGroup{}
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		reqNumber := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key%d", reqNumber)
			store.Get(key)
		}()
	}

	wg.Wait()
	elapsed = time.Since(start)
	readTime := elapsed
	fmt.Printf("[Benchmark] %d read requests took %s on %d servers.\n", numRequests, readTime, numServers)

}

func benchmarkServerCatchUpTime(args []string) {
	flagset := flag.NewFlagSet("bench2", flag.ExitOnError)
	var numServers int
	flagset.IntVar(&numServers, "numServers", 3, "Number of servers to spin")
	if err := flagset.Parse(args); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	store, servers  := spinUpClusterAndGetStoreInterface(numServers)
	defer cleanupDbFiles()
	fmt.Printf("Running Performance Check: Server catch up time")
	numLogsToCatchUp := 100
	laggingServerIndex := 2

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
		logLength, _ := servers[laggingServerIndex].LogStore.Length()
		if int(logLength) == numLogsToCatchUp+1 {
			break
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("[Benchmark] lagging server took took %s to catch up %d entries on a %d server raft.\n", elapsed, numLogsToCatchUp, numServers)
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		fmt.Printf("usage: %s config | server | client ...\n", os.Args[0])
		os.Exit(2)
	}
	switch args[0] {
	case "bench1":
		benchmarkClientReadWriteThroughput(args[1:])
	case "bench2":
		benchmarkServerCatchUpTime(args[1:])
	default:
		fmt.Printf("unknown sub-command: %s\n", args[0])
		os.Exit(2)
	}
}

