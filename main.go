package main

import (
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/benchmarks"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/kvstore"
	"github.com/sushantsondhi/raft-col733/kvstore/client"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/raft"
	"github.com/sushantsondhi/raft-col733/rpc"
	"go.uber.org/multierr"
	"gopkg.in/yaml.v2"
	"io/fs"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"time"
)

type config struct {
	Cluster          []common.Server
	HeartbeatTimeout int // In milliseconds
	ElectionTimeout  int // In milliseconds
}

func runServer(args []string) {
	flagset := flag.NewFlagSet("server", flag.ExitOnError)
	configFile := flagset.String("config", "", "YAML file containing cluster & configuration details")
	index := flagset.Int("me", -1, "Index of this server in the config file")
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
	if *index < 0 || *index >= len(cfg.Cluster) {
		fmt.Printf("invalid index: %d (config file specified %d servers only)\n", *index, len(cfg.Cluster))
	}
	var clusterConfig common.ClusterConfig
	clusterConfig.Cluster = cfg.Cluster
	clusterConfig.ElectionTimeout = time.Millisecond * time.Duration(cfg.ElectionTimeout)
	clusterConfig.HeartBeatTimeout = time.Millisecond * time.Duration(cfg.HeartbeatTimeout)

	logStore, logErr := persistent.CreateDbLogStore(fmt.Sprintf("%v_logstore.db", cfg.Cluster[*index].ID))
	pStore, pErr := persistent.NewPStore(fmt.Sprintf("%v_pstore.db", cfg.Cluster[*index].ID))
	err = multierr.Combine(logErr, pErr)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	fsm := kvstore.NewKeyValFSM()
	manager := rpc.NewManager()
	server := raft.NewRaftServer(
		cfg.Cluster[*index],
		clusterConfig,
		fsm,
		logStore,
		pStore,
		manager,
	)
	if server == nil {
		os.Exit(2)
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	<-c
	fmt.Println("Stopping server ...")
	if err := server.Stop(); err != nil {
		fmt.Println(err)
	}
}

func generateConfig(args []string) {
	flagset := flag.NewFlagSet("config", flag.ExitOnError)
	var filepath, servers string
	var electionTimeout, heartbeatTimeout int
	flagset.StringVar(&filepath, "file", "config.yaml", "full path of config file to write to")
	flagset.StringVar(&servers, "servers", "localhost:12345,localhost:12346,localhost:12347", "comma-seperated list of server addresses of raft servers")
	flagset.IntVar(&electionTimeout, "electionTimeout", 200, "value of election timeout (in milliseconds)")
	flagset.IntVar(&heartbeatTimeout, "heartbeatTimeout", 50, "value of heartbeat timeout (in milliseconds)")
	if err := flagset.Parse(args); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	var cfg config
	for _, addr := range strings.Split(servers, ",") {
		cfg.Cluster = append(cfg.Cluster, common.Server{
			ID:         uuid.New(),
			NetAddress: common.ServerAddress(addr),
		})
	}
	cfg.HeartbeatTimeout = heartbeatTimeout
	cfg.ElectionTimeout = electionTimeout

	if bytes, err := yaml.Marshal(cfg); err != nil {
		fmt.Println(err)
		os.Exit(2)
	} else {
		err := ioutil.WriteFile(filepath, bytes, fs.ModePerm)
		if err != nil {
			fmt.Println(err)
			os.Exit(2)
		}
	}
}

func runClient(args []string) {
	flagset := flag.NewFlagSet("client", flag.ExitOnError)
	configFile := flagset.String("config", "", "YAML file containing cluster details")
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
	err = client.RunCliClient(cfg.Cluster, manager)
	fmt.Println(err)
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		fmt.Printf("usage: %s config | server | client ...\n", os.Args[0])
		os.Exit(2)
	}
	switch args[0] {
	case "config":
		generateConfig(args[1:])
	case "server":
		runServer(args[1:])
	case "client":
		runClient(args[1:])
	case "bench1":
		benchmarks.BenchmarkClientReadWriteThroughput(args[1:])
	case "bench2":
		benchmarks.BenchmarkServerCatchUpTime(args[1:])
	case "bench3":
		benchmarks.BenchmarkParallelClientThroughput(args[1:])
	default:
		fmt.Printf("unknown sub-command: %s\n", args[0])
		os.Exit(2)
	}
}
