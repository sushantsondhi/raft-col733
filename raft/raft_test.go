package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/kvstore"
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/rpc"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func makeRaftCluster(t *testing.T, configs ...common.ClusterConfig) (servers []*RaftServer) {
	for i := range configs {
		logstore, err := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(t, err)
		pstore, err := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(t, err)
		raftServer := NewRaftServer(configs[i].Cluster[i], configs[i], kvstore.NewKeyValFSM(), logstore, pstore, rpc.NewManager())
		assert.NotNil(t, raftServer)
		servers = append(servers, raftServer)
	}
	return
}

func makeRaftServer(t *testing.T, config common.ClusterConfig, i int) (server *RaftServer) {
	logstore, err := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", config.Cluster[i].ID))
	assert.NoError(t, err)
	pstore, err := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", config.Cluster[i].ID))
	assert.NoError(t, err)
	raftServer := NewRaftServer(config.Cluster[i], config, kvstore.NewKeyValFSM(), logstore, pstore, rpc.NewManager())
	assert.NotNil(t, raftServer)
	return raftServer
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

func jsonHelpers(t *testing.T) (func(key, val string, transactionId uuid.UUID) []byte, func(key string) []byte) {
	setMarshaller := func(key, val string, transactionId uuid.UUID) []byte {
		bytes, err := json.Marshal(kvstore.Request{
			Type:          kvstore.Set,
			Key:           key,
			Val:           val,
			TransactionId: transactionId,
		})
		assert.NoError(t, err)
		return bytes
	}

	getMarshaller := func(key string) []byte {
		bytes, err := json.Marshal(kvstore.Request{
			Type:          kvstore.Get,
			Key:           key,
			TransactionId: uuid.New(),
		})
		assert.NoError(t, err)
		return bytes
	}
	return setMarshaller, getMarshaller
}

// Sends concurrent requests
func sendClientSetRequests(t *testing.T, server *RaftServer, numRequests int64, waitToFinish bool, valSuffix string) {
	setMarshaller, _ := jsonHelpers(t)
	var wg sync.WaitGroup
	for i := int64(0); i < numRequests; i++ {
		wg.Add(1)
		reqNumber := i // Warning: Loop variables captured by 'func' literals in 'go'
		// statements might have unexpected values
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key%d", reqNumber)
			val := fmt.Sprintf("%s%d", valSuffix, reqNumber)

			req := common.ClientRequestRPC{
				Data: setMarshaller(key, val, uuid.New()),
			}
			res := common.ClientRequestRPCResult{}
			err := server.ClientRequest(&req, &res)
			assert.NoError(t, err, "Client request got error")
			assert.Equal(t, res.Error, "", "Error in setting value")
			assert.Truef(t, res.Success, "set request failed")
		}()
	}
	if waitToFinish {
		wg.Wait()
	}
}

// Waits for all raft servers to match up
// Should be used after all client requests have returned
func waitForLogsToMatch(t *testing.T, servers []*RaftServer, waitTimeSeconds int) {

	var success bool

	for itr := 0; itr < waitTimeSeconds; itr++ {

		for _, server := range servers {
			server.Mutex.Lock()
		}

		var leader *RaftServer = nil

		for _, server := range servers {
			if server.State == Leader {
				leader = server
			}
		}

		if leader == nil {
			for _, server := range servers {
				server.Mutex.Unlock()
			}
			time.Sleep(time.Second)
			continue
		}

		leaderLastEntry, err := leader.LogStore.GetLast()
		assert.NoError(t, err)

		matched := true
		for _, server := range servers {
			lastEntry, err := server.LogStore.GetLast()
			assert.NoError(t, err)
			check := leaderLastEntry.Term == lastEntry.Term
			check = check && (leaderLastEntry.Index == lastEntry.Index)
			check = check && (bytes.Compare(leaderLastEntry.Data, lastEntry.Data) == 0)
			if !check {
				matched = false
			}
		}

		for _, server := range servers {
			server.Mutex.Unlock()
		}

		if matched {
			success = true
			break
		}
		time.Sleep(time.Second)
	}

	assert.Truef(t, success, "servers took too long to match up.")

}

func checkEqualFSM(t *testing.T, servers []*RaftServer, numKeys int64) {
	_, getMarshaller := jsonHelpers(t)
	for _, server := range servers[1:] {
		assert.Equal(t, servers[0].CommitIndex, server.CommitIndex, "Commit Index doesn't match")
		assert.Equal(t, servers[0].AppliedIndex, server.AppliedIndex, "Applied Index doesn't match")
		for index := int64(0); index < numKeys; index++ {
			newLogEntry := common.LogEntry{
				Data: getMarshaller(fmt.Sprintf("key%d", index)),
			}
			entry1, err := servers[0].FSM.Apply(newLogEntry)
			assert.NoError(t, err)
			entry2, err := server.FSM.Apply(newLogEntry)
			assert.NoError(t, err)
			assert.Equal(t, entry1, entry2, "value at key: key%d does not match", index)
		}
	}

}

func checkEqualLogs(t *testing.T, servers []*RaftServer) {
	logLength, err := servers[0].LogStore.Length()
	assert.NoError(t, err)
	for _, server := range servers[1:] {
		l, err := server.LogStore.Length()
		assert.NoError(t, err)
		assert.Equal(t, logLength, l)
	}

	for _, server := range servers[1:] {
		for index := 0; index < int(logLength); index++ {
			entry1, err := servers[0].LogStore.Get(int64(index))
			assert.NoError(t, err)
			entry2, err := server.LogStore.Get(int64(index))
			assert.NoError(t, err)
			assert.Equal(t, entry1.Term, entry2.Term, "index %d does not match", index)
			assert.Equal(t, entry1.Index, entry2.Index, "index %d does not match", index)
			assert.Equal(t, entry1.Data, entry2.Data, "index %d does not match", index)
		}
	}

}

func Test_SimpleElection(t *testing.T) {
	// Test to verify that a leader is elected when the raft is first started and for a given
	// term there is only 1 leader
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers)
}

func Test_ElectionWithoutHeartbeat(t *testing.T) {
	// Test to verify election safety when multiple leaders keep getting elected
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	clusterConfig.HeartBeatTimeout = 10 * time.Hour
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers)
}

func Test_ReElection(t *testing.T) {
	// This test verifies that if a leader gets disconnected then a new leader will be appointed.
	// The state of the disconnected leader will be Leader until it reconnects back into the cluster. The disconnected
	// leader will then become a follower and will have its term updated as the new leader.
	// Let's say server 1 becomes leader. We then disconnect server 1 and allows either server 2 or 3 to become leader.
	// The state of server 1 stays as Leader until it reconnects back into cluster, and then it becomes follower
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

	time.Sleep(3 * time.Second)

	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, servers[0].State, Follower)
	assert.Equal(t, servers[0].Term, servers[1].Term)
}

func Test_ReJoin(t *testing.T) {
	// This test verifies that a disconnected server, eventually catches up to other server after reconnecting
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
	time.Sleep(5 * time.Second)
	assert.Equal(t, servers[0].Term, servers[2].Term, "Disconnected server has not caught up")
}

func TestGetAndSetClient(t *testing.T) {
	// This test verifies that the client RPC is working correctly and the client requests are handled properly
	// This also ensures that correct value is applied in the FSM and returned to client
	setMarshaller, getMarshaller := jsonHelpers(t)
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers)

	var success bool
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })

		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)

		req := common.ClientRequestRPC{
			Data: setMarshaller(key, val, uuid.New()),
		}
		res := common.ClientRequestRPCResult{}
		success = false
		for _, server := range servers {
			err := server.ClientRequest(&req, &res)
			assert.NoError(t, err)
			if res.Success {
				success = true
				break
			}
		}

		assert.Truef(t, success, "set failed")
		assert.Equal(t, res.Error, "")
		req = common.ClientRequestRPC{
			Data: getMarshaller(key),
		}
		res = common.ClientRequestRPCResult{}
		success = false
		for _, server := range servers {
			err := server.ClientRequest(&req, &res)
			assert.NoError(t, err)
			if res.Success {
				success = true
				break
			}
		}
		assert.Truef(t, success, "set failed")
		assert.Equal(t, res.Data, []byte(val))
		assert.Equal(t, res.Error, "")
	}
}

func Test_SimpleLogStoreAndFSMCheck(t *testing.T) {
	// This test verifies that all the logs and FSM of all raft servers are in sync if there is no failure
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers)
	sendClientSetRequests(t, servers[0], 100, true, "val")
	waitForLogsToMatch(t, servers, 3)
	checkEqualLogs(t, servers)
	time.Sleep(2 * time.Second)
	checkEqualFSM(t, servers, 100)
}

func Test_OneVotePerServerPerTerm(t *testing.T) {
	// This function tests that a any raft server will vote only once in a given term
	t.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig1
	// purposefully delay the election timeouts of 3 to ensure that 3 never becomes a leader
	clusterConfig2.ElectionTimeout = 10 * time.Hour

	servers := makeRaftCluster(t, clusterConfig1, clusterConfig2)
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, servers[0].State, Leader)
	servers[0].Disconnect()
	assert.Equal(t, servers[1].Term, int64(1))
	servers = append(servers, makeRaftServer(t, clusterConfig3, 2))
	assert.Equal(t, servers[2].Term, int64(0))
	verifyElectionSafetyAndLiveness(t, servers[1:])
	assert.Equal(t, servers[2].State, Leader)
	assert.Equal(t, servers[2].Term, int64(2))
}

func Test_LogReplayability(t *testing.T) {
	// This test verifies that a restarted raft server is able to re-construct its state
	// by simply replaying the logs on its FSM.
	// It will also weakly test our persistence guarantees.
	// We start with a simple cluster of 3 servers - A, B & C. Initially the FSM starts empty F_0.
	// The client sends multiple read/write requests to the cluster so that the A's FSM state is now F_1.
	// Now we kill the server A by permanently stopping it.
	// We then respawn the server A.
	// We will now verify that -
	//  1. Eventually A's applied index is also restored and A's FSM state is again F_1.
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(3)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig)
	verifyElectionSafetyAndLiveness(t, servers[1:])
	sendClientSetRequests(t, servers[0], 10, true, "val")
	waitForLogsToMatch(t, servers, 10)
	checkEqualLogs(t, servers)
	time.Sleep(time.Second)
	checkEqualFSM(t, servers, 10)
	assert.Equal(t, servers[0].AppliedIndex, int64(10))

	assert.NoError(t, servers[0].Stop())
	verifyElectionSafetyAndLiveness(t, servers[1:])
	newServers := makeRaftCluster(t, clusterConfig)
	servers[0] = newServers[0]
	verifyElectionSafetyAndLiveness(t, servers)
	waitForLogsToMatch(t, servers, 10)
	checkEqualLogs(t, servers)
	time.Sleep(time.Second)
	checkEqualFSM(t, servers, 10)
	assert.Equal(t, servers[0].AppliedIndex, int64(10))
}

func Test_LaggingFollower(t *testing.T) {
	// This test verifies that a lagging (disconnected) follower will eventually be brought up to speed
	// in our implementation (correct raft behaviour).
	// We start with a cluster of 3 servers A, B & C.
	// Wait for first election to complete, WLOG assume A is elected leader.
	// Now, we will disconnect C (network partition).
	// Send multiple write/read requests to A or B.
	// Now, reconnect C. No more client requests will be sent.
	// We will verify that eventually C also has all the logs (even without any further client requests).

	t.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig1

	// purposefully delay the election timeouts of 2 & 3 to ensure that 1 gets elected as leader first
	clusterConfig2.ElectionTimeout = time.Second
	clusterConfig3.ElectionTimeout = time.Second

	servers := makeRaftCluster(t, clusterConfig1, clusterConfig2, clusterConfig3)
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, Leader, servers[0].State, "server[0] not elected as leader")
	// server 0 elected as leader,
	// Send some client requests
	sendClientSetRequests(t, servers[0], 10, true, "val")
	//Disconnecting server 2
	servers[2].Disconnect()
	//Sending more client requests
	sendClientSetRequests(t, servers[0], 100, true, "val")
	//Reconnect Server 2
	servers[2].Reconnect()

	verifyElectionSafetyAndLiveness(t, servers)
	assert.True(t, servers[0].State == Leader || servers[1].State == Leader)
	waitForLogsToMatch(t, servers, 20)
	checkEqualLogs(t, servers)
	checkEqualFSM(t, servers, 100)

	l, err := servers[0].LogStore.Length()
	assert.NoError(t, err)
	fmt.Printf("******* %d\n", l)
}

func Test_LeaderCompleteness(t *testing.T) {
	// This test verifies that our implementation obeys the leader completeness property.
	// To verify this we spin up a cluster of 5 raft servers but with pre-filled log stores
	// in a manner so that -
	// Server 1 has the following logs (term numbers in the index order):
	// 		1 2 3 4 5
	// Server 2 has the following logs (term numbers in the index order):
	// 		1 2
	// Server 3 has the following logs (term numbers in the index order):
	// 		1
	// Server 4 & 5 will remain stopped throughout the test
	// (Note that terms of all the servers will have to be initialized with 5)
	// We will then verify that -
	// 1. Server 1 is _eventually_ elected as the _first_ leader (possibly after multiple failed election rounds)
	// 2. Server 1 _eventually_ forces its logs upon others overwriting them if needed. At the end
	//    all 3 servers should have all the logs in the exact same order as server 1.
	t.Cleanup(cleanupDbFiles)
	clusterConfig := generateClusterConfig(5)

	type initialLogTerms struct {
		ExpectedFirstLeaderIndex int
		LogTerms                 [][]int
	}

	testLog1 := initialLogTerms{
		ExpectedFirstLeaderIndex: 0,
		LogTerms: [][]int{
			{1, 2, 3, 4, 5},
			{1, 2},
			{1},
			{1, 2, 3, 4}, // perpetually disconnected
			{1, 2, 3},    // perpetually disconnected
		},
	}

	configs := []common.ClusterConfig{clusterConfig, clusterConfig, clusterConfig, clusterConfig, clusterConfig}

	var servers []*RaftServer

	for i := 0; i < len(configs); i++ {
		if i >= 3 {
			// last 2 servers will always be off
			continue
		}
		logstore, err := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", configs[i].Cluster[i].ID))
		assert.NoError(t, err)

		err = logstore.Store(common.LogEntry{
			Index: 0,
		})
		assert.NoError(t, err)

		for index, term := range testLog1.LogTerms[i] {
			err := logstore.Store(common.LogEntry{
				Index: int64(index + 1), // Careful about index
				Term:  int64(term),
			})
			assert.NoError(t, err)
		}

		pstore, err := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", configs[i].Cluster[i].ID))
		setTerm(pstore, 5)
		assert.NoError(t, err)
		raftServer := NewRaftServer(configs[i].Cluster[i], configs[i], kvstore.NewKeyValFSM(), logstore, pstore, rpc.NewManager())
		assert.NotNil(t, raftServer)
		servers = append(servers, raftServer)
	}

	time.Sleep(3 * time.Second)
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, servers[testLog1.ExpectedFirstLeaderIndex].State, Leader)

	waitForLogsToMatch(t, servers, 100)
	checkEqualLogs(t, servers)
	checkEqualFSM(t, servers, 0)
	l, err := servers[0].LogStore.Length()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), l)
}

func Test_CommitDurability(t *testing.T) {
	// This test verifies that if an entry is committed, it is durable even if committing server crashes.
	// To verify this we spin up a cluster of 3 raft servers A, B & C.
	// Initially C is disconnected (network partitioned).
	// We assume WLOG that A is elected as leader in the first election (swap A & B otherwise).
	// A *write* client request is sent to C. It should fail.
	// Same request is then sent to B. It should succeed.
	// Now the log must be replicated and committed at both A & B.
	// We bring down A (by permanently stopping it), and then reconnect C to cluster so that now
	// we have only B & C in the cluster.
	// A *read* client request is sent to C which attempts to read the value written by previous request (on A),
	// verify that it succeeds and the read value is anticipated.
	t.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig2
	clusterConfig2.ElectionTimeout = 500 * time.Millisecond
	clusterConfig3.ElectionTimeout = 500 * time.Millisecond

	servers := makeRaftCluster(t, clusterConfig1, clusterConfig2, clusterConfig3)
	servers[2].Disconnect()
	verifyElectionSafetyAndLiveness(t, servers[:])
	assert.Equal(t, Leader, servers[0].State)

	var resp common.ClientRequestRPCResult
	if err := servers[2].ClientRequest(&common.ClientRequestRPC{}, &resp); err == nil {
		assert.False(t, resp.Success)
	}

	sendClientSetRequests(t, servers[1], 10, true, "val")
	time.Sleep(time.Second)
	checkEqualFSM(t, servers[:2], 10)

	assert.NoError(t, servers[0].Stop())
	servers[2].Reconnect()
	verifyElectionSafetyAndLiveness(t, servers[1:])
	waitForLogsToMatch(t, servers[1:], 10)
	checkEqualLogs(t, servers[1:])
	checkEqualFSM(t, servers[1:], 10)
}

func Test_OldTermsNotCommitted(t *testing.T) {
	// This test verifies that our implementation (correctly) does not commit old terms directly.
	// We will spawn 3 servers with initial term of 2 and following pre-filled log stores respectively -
	// Server 1:	1
	// Server 2:	1
	// Server 3:	1 2
	// All the logs should be write requests. Assume that server 3 has lower election timeout so that it is elected.
	// Now we will verify that
	// 1. Server 3 is elected as the first leader (for a term that is greater than 2)
	// 2. Even after many seconds the commit index of all the 3 servers stays at zero.
	// 3. After initiating a read request, the read succeeds and the commit index is also properly updated.
	t.Cleanup(cleanupDbFiles)
	clusterConfig1 := generateClusterConfig(3)
	clusterConfig2 := clusterConfig1
	clusterConfig3 := clusterConfig2
	clusterConfig1.ElectionTimeout = time.Second
	clusterConfig2.ElectionTimeout = time.Second
	clusterConfigs := []common.ClusterConfig{
		clusterConfig1, clusterConfig2, clusterConfig3,
	}
	var servers []*RaftServer
	setter, getter := jsonHelpers(t)
	type initialLogTerms struct {
		ExpectedFirstLeaderIndex int
		LogTerms                 [][]int
	}
	testLog1 := initialLogTerms{
		ExpectedFirstLeaderIndex: 2,
		LogTerms: [][]int{
			{1},
			{1},
			{1, 2},
		},
	}
	for i := 0; i < len(clusterConfigs); i++ {
		logstore, err := persistent.CreateDbLogStore(fmt.Sprintf("logstore-%v.db", clusterConfigs[i].Cluster[i].ID))
		assert.NoError(t, err)

		err = logstore.Store(common.LogEntry{
			Index: 0,
		})
		assert.NoError(t, err)

		for index, term := range testLog1.LogTerms[i] {
			key := fmt.Sprintf("key%d", index)
			err := logstore.Store(common.LogEntry{
				Index: int64(index + 1), // Careful about index
				Term:  int64(term),
				Data:  setter(key, fmt.Sprintf("val%d", index), uuid.NewSHA1(uuid.Nil, []byte(key))),
			})
			assert.NoError(t, err)
		}

		pstore, err := persistent.NewPStore(fmt.Sprintf("pstore-%v.db", clusterConfigs[i].Cluster[i].ID))
		setTerm(pstore, 2)
		assert.NoError(t, err)
		raftServer := NewRaftServer(clusterConfigs[i].Cluster[i], clusterConfigs[i], kvstore.NewKeyValFSM(), logstore, pstore, rpc.NewManager())
		assert.NotNil(t, raftServer)
		servers = append(servers, raftServer)
	}
	verifyElectionSafetyAndLiveness(t, servers)
	assert.Equal(t, Leader, servers[2].State)

	time.Sleep(5 * time.Second)
	checkEqualLogs(t, servers)
	assert.Equal(t, int64(0), servers[0].CommitIndex)
	assert.Equal(t, int64(0), servers[1].CommitIndex)
	assert.Equal(t, int64(0), servers[2].CommitIndex)

	var resp common.ClientRequestRPCResult
	assert.NoError(t, servers[0].ClientRequest(&common.ClientRequestRPC{
		Data: getter("key1"),
	}, &resp))
	assert.True(t, resp.Success)
	assert.EqualValues(t, []byte("val1"), resp.Data)

	time.Sleep(time.Second)
	checkEqualFSM(t, servers, 2)
}

func Test_ElectionSafety(t *testing.T) {

	t.Cleanup(cleanupDbFiles)

	clusterConfig := generateClusterConfig(5)
	servers := makeRaftCluster(t, clusterConfig, clusterConfig, clusterConfig, clusterConfig, clusterConfig)

	var disconnectedQueue []int

	for itr := 0; itr < 10; itr++ {
		if itr%2 == 0 {
			for serverIndex := range disconnectedQueue {
				servers[serverIndex].Reconnect()
			}
			disconnectedQueue = disconnectedQueue[len(disconnectedQueue):]
		} else {
			var idx1, idx2 int
			idx1 = rand.Intn(5)
			idx2 = rand.Intn(5)
			for idx2 == idx1 {
				idx2 = rand.Intn(5)
			}
			servers[idx1].Disconnect()
			servers[idx2].Disconnect()
			disconnectedQueue = append(disconnectedQueue, idx1)
			disconnectedQueue = append(disconnectedQueue, idx2)
		}
		verifyElectionSafetyAndLiveness(t, servers)
	}
}
