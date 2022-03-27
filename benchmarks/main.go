package benchmarks

import (
	"flag"
	"fmt"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/kvstore"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/raft"
	"github.com/sushantsondhi/raft-col733/rpc"
	"go.uber.org/multierr"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type config struct {
	Cluster          []common.Server
	HeartbeatTimeout int // In milliseconds
	ElectionTimeout  int // In milliseconds
}

func runServer(cfg config, index int) *raft.RaftServer {
	if index < 0 || index >= len(cfg.Cluster) {
		fmt.Printf("invalid index: %d (config file specified %d servers only)\n", index, len(cfg.Cluster))
	}
	var clusterConfig common.ClusterConfig
	clusterConfig.Cluster = cfg.Cluster
	clusterConfig.ElectionTimeout = time.Millisecond * time.Duration(cfg.ElectionTimeout)
	clusterConfig.HeartBeatTimeout = time.Millisecond * time.Duration(cfg.HeartbeatTimeout)

	logStore, logErr := persistent.CreateDbLogStore(fmt.Sprintf("%v_logstore.db", cfg.Cluster[index].ID))
	pStore, pErr := persistent.NewPStore(fmt.Sprintf("%v_pstore.db", cfg.Cluster[index].ID))
	err := multierr.Combine(logErr, pErr)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	fsm := kvstore.NewKeyValFSM()
	manager := rpc.NewManager()
	server := raft.NewRaftServer(
		cfg.Cluster[index],
		clusterConfig,
		fsm,
		logStore,
		pStore,
		manager,
	)
	if server == nil {
		os.Exit(2)
	}
	return server
}

func BenchmarkClientReadWriteThroughput(args []string) {
	flagset := flag.NewFlagSet("bench1", flag.ExitOnError)
	configFile := flagset.String("config", "config.yaml", "YAML file containing cluster details")
	var numRequests int
	flagset.IntVar(&numRequests, "numServers", 100, "Number of servers to spin")
	if err := flagset.Parse(args); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	bytes, err := ioutil.ReadFile(*configFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	var cfg config
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	manager := rpc.NewManager()
	store, err := kvstore.NewKeyValStore(cfg.Cluster, manager)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	// Write ThroughPut
	fmt.Printf("Running Performance Check: Client Read Write Throughput")
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
	fmt.Printf("[Benchmark] %d write requests took %s on %d servers.\n", numRequests, writeTime, len(cfg.Cluster))

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
	fmt.Printf("[Benchmark] %d read requests took %s on %d servers.\n", numRequests, readTime, len(cfg.Cluster))

}

func BenchmarkServerCatchUpTime(args []string) {
	flagset := flag.NewFlagSet("bench2", flag.ExitOnError)
	configFile := flagset.String("config", "config.yaml", "YAML file containing cluster details")
	var numRequests int
	flagset.IntVar(&numRequests, "numServers", 100, "Number of servers to spin")
	if err := flagset.Parse(args); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	bytes, err := ioutil.ReadFile(*configFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	var cfg config
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	manager := rpc.NewManager()
	store, err := kvstore.NewKeyValStore(cfg.Cluster, manager)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	fmt.Printf("Running Performance Check: Server catch up time")
	numLogsToCatchUp := numRequests
	laggingServerIndex := 2

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
	server2 := runServer(cfg, laggingServerIndex)
	start := time.Now()
	// Assuming correctness
	for {
		logLength, _ := server2.LogStore.Length()
		if int(logLength) == numLogsToCatchUp+1 {
			break
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("[Benchmark] lagging server took took %s to catch up %d entries on a %d server raft.\n", elapsed, numLogsToCatchUp, len(cfg.Cluster))
}



