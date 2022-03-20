package raft

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"go.uber.org/multierr"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type ApplyMsg struct {
	Err   error
	Bytes []byte
}

type RaftServer struct {
	// Access to state must be synchronized between multiple goroutines
	state

	// Data Stores
	FSM             common.FSM
	LogStore        common.LogStore
	PersistentStore common.PersistentStore

	// Peers
	MyID  uuid.UUID
	Peers []common.RPCServer

	// Synchronization primitives
	Mutex                sync.Mutex
	ElectionTimeoutChan  chan bool
	HeartbeatTimeoutChan chan bool
	ApplyChan            map[int64]chan ApplyMsg
}

var _ common.RPCServer = &RaftServer{}

func NewRaftServer(
	me common.Server,
	cluster common.ClusterConfig,
	fsm common.FSM,
	logStore common.LogStore,
	persistentStore common.PersistentStore,
	manager common.RPCManager,
) *RaftServer {
	newRaftServer := &RaftServer{
		state: state{
			Term:          getTerm(persistentStore),
			VotedFor:      getVotedFor(persistentStore),
			CommitIndex:   getCommitIndex(persistentStore),
			State:         Candidate,
			AppliedIndex:  0,
			NextIndexMap:  make(map[uuid.UUID]int64),
			MatchIndexMap: make(map[uuid.UUID]int64),
		},
		FSM:             fsm,
		LogStore:        logStore,
		PersistentStore: persistentStore,
		MyID:            me.ID,
	}
	// Add a zero log entry
	err := logStore.Store(common.LogEntry{
		Index: 0,
		Term:  0,
		Data:  nil,
	})
	if err != nil {
		log.Printf("error initializing log store: %+v\n", err)
		return nil
	}
	for _, server := range cluster.Cluster {
		if server.ID == me.ID {
			continue
		}
		peer, err := manager.ConnectToPeer(server.NetAddress, server.ID)
		if err != nil {
			log.Printf("can't connect to peer %s\n", server.NetAddress)
			return nil
		}
		newRaftServer.Peers = append(newRaftServer.Peers, peer)
	}
	for _, peer := range newRaftServer.Peers {
		newRaftServer.NextIndexMap[peer.GetID()] = 0
		newRaftServer.MatchIndexMap[peer.GetID()] = 0
	}

	newRaftServer.ElectionTimeoutChan = make(chan bool, 10)
	newRaftServer.HeartbeatTimeoutChan = make(chan bool, 10)
	newRaftServer.ApplyChan = make(map[int64]chan ApplyMsg)

	newRaftServer.ElectionTimeoutChan <- true
	newRaftServer.HeartbeatTimeoutChan <- false
	go newRaftServer.electionTimeoutController(cluster.ElectionTimeout)
	go newRaftServer.heartBeatTimeoutController(cluster.HeartBeatTimeout)
	go func() {
		err := manager.Start(me.NetAddress, newRaftServer)
		if err != nil {
			log.Printf("%v: failed to start RPC server\n", me.ID)
		}
	}()

	log.Printf("Initialization complete for server %v\n", me.ID)
	return newRaftServer
}

func (server *RaftServer) GetID() uuid.UUID {
	return server.MyID
}

func (server *RaftServer) ClientRequest(args *common.ClientRequestRPC, result *common.ClientRequestRPCResult) error {
	server.Mutex.Lock()
	if server.State == Leader {
		NewLogEntry := common.LogEntry{
			Term: server.Term,
			Data: args.Data,
		}
		if length, err := server.LogStore.Length(); err == nil {
			NewLogEntry.Index = length
		} else {
			server.Mutex.Unlock()
			result.Success = false
			return fmt.Errorf("Unable to get logStore length: %+v\n", err)
		}

		if err := server.LogStore.Store(NewLogEntry); err != nil {
			server.Mutex.Unlock()
			result.Success = false
			return fmt.Errorf("Unable to store entry in leader logstore: %+v\n", err)
		}
		server.ApplyChan[NewLogEntry.Index] = make(chan ApplyMsg)
		server.Mutex.Unlock()
		server.broadcastAppendEntries()
		ret := <-server.ApplyChan[NewLogEntry.Index]
		result.Error = ret.Err.Error()
		result.Data = ret.Bytes
		if ret.Err != nil {
			result.Success = true
		} else {
			result.Success = false
		}
		return nil
	} else {
		server.Mutex.Unlock()
		for _, peer := range server.Peers {
			if server.CurrentLeader != nil && peer.GetID() == *server.CurrentLeader {
				return peer.ClientRequest(args, result)
			}
		}
		return fmt.Errorf("Not connected to Leader\n")
	}
}

func (server *RaftServer) RequestVote(args *common.RequestVoteRPC, result *common.RequestVoteRPCResult) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	if args.Term > server.Term {
		// Update term and convert to follower
		server.Term = args.Term
		setTerm(server.PersistentStore, server.Term)
		server.VotedFor = nil
		setVotedFor(server.PersistentStore, server.VotedFor)
		server.convertToFollower()
	}
	result.Term = server.Term
	// Return false if term < currentTerm (Section 5.1)
	if args.Term < server.Term {
		result.VoteGranted = false
		return nil
	}
	// Don't vote if already voted (Section 5.2)
	if server.VotedFor != nil && *server.VotedFor != args.CandidateID {
		result.VoteGranted = false
		return nil
	}
	// Only vote if candidate is sufficiently up-to-date (Section 5.4)
	lastLogEntry, err := server.getLastLogEntry()
	if err != nil {
		log.Printf("error getting log entry : %+v\n", err)
		return err
	}
	if args.LastLogTerm > lastLogEntry.Term {
		result.VoteGranted = true
		return nil
	}
	if args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogEntry.Index {
		result.VoteGranted = true
		return nil
	}
	result.VoteGranted = false
	return nil
}

func (server *RaftServer) AppendEntries(args *common.AppendEntriesRPC, result *common.AppendEntriesRPCResult) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	switch {
	case args.Term < server.Term:
		// leader is stale, reject request
		result.Success = false
		result.Term = server.Term
	case args.Term > server.Term:
		// update term and convert to follower
		server.Term = args.Term
		setTerm(server.PersistentStore, server.Term)
		server.VotedFor = nil
		setVotedFor(server.PersistentStore, server.VotedFor)
		fallthrough
	case args.Term == server.Term:
		if server.State != Follower {
			if server.State == Leader {
				panic("2 leaders in the same term")
			}
			server.convertToFollower()
		}
		if server.CurrentLeader == nil || *server.CurrentLeader != args.Leader {
			server.CurrentLeader = &args.Leader
		}

		if length, err := server.LogStore.Length(); err == nil {
			if args.PrevLogIndex < length {
				prevLogEntry, err := server.LogStore.Get(args.PrevLogIndex)
				if err != nil {
					return fmt.Errorf("Unable to get Previous Log entry from peer: %+v\n", err)
				}
				if prevLogEntry.Term != args.PrevLogTerm {
					return fmt.Errorf("Peer Entries are not upto date\n")
				}
			} else {
				return fmt.Errorf("Peer Entries are not upto date\n")
			}
		} else {
			return fmt.Errorf("Unable to get log length: %+v\n", err)
		}
		if err := server.LogStore.Store(args.Entries[0]); err != nil {
			return fmt.Errorf("Unable to append Entry: %+v\n", err)
		}
		result.Success = true
		result.Term = server.Term
		// reset election timeout
		server.ElectionTimeoutChan <- true
	}
	return nil
}

func (server *RaftServer) getLastLogEntry() (entry *common.LogEntry, err error) {
	var lengthErr, logErr error
	var logLength int64
	logLength, lengthErr = server.LogStore.Length()
	entry, logErr = server.LogStore.Get(logLength - 1)
	err = multierr.Combine(lengthErr, logErr)
	return
}

// convertToFollower method will initiate transition of Raft's server
// state to a follower, it assumes that the caller has already
// acquired mutex.
func (server *RaftServer) convertToFollower() {
	log.Printf("%v: converting to follower\n", server.MyID)
	server.State = Follower
	server.CurrentLeader = nil
	// (Re)start election timeouts
	server.ElectionTimeoutChan <- true
	server.HeartbeatTimeoutChan <- false
}

// convertToCandidate method will initiate transition of Raft's server
// state to a candidate, it assumes that the caller has already
// acquired mutex.
func (server *RaftServer) convertToCandidate() {
	log.Printf("%v: converting to candidate\n", server.MyID)
	if server.State == Leader {
		panic("Unexpected transition from Leader -> Candidate")
	}
	server.State = Candidate
	server.CurrentLeader = nil
	// TODO: this should be in a transaction
	server.Term++
	setTerm(server.PersistentStore, server.Term)
	server.VotedFor = &server.MyID
	setVotedFor(server.PersistentStore, server.VotedFor)

	// Send RequestVoteRPC to all servers
	totalServers := len(server.Peers) + 1
	reqToMajority := totalServers/2 + 1

	lastLogEntry, err := server.getLastLogEntry()
	if err != nil {
		log.Printf("error getting last log entry : %+v\n", err)
		return
	}

	requestVoteRPC := common.RequestVoteRPC{
		Term:         server.Term,
		CandidateID:  server.MyID,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}

	voteCh := make(chan bool, totalServers)
	for _, peer := range server.Peers {
		peer := peer
		go func() {
			var response common.RequestVoteRPCResult
			if err := peer.RequestVote(&requestVoteRPC, &response); err != nil {
				log.Printf("error requesting vote from peer: %+v\n", err)
				voteCh <- false
			} else {
				server.Mutex.Lock()
				defer server.Mutex.Unlock()
				if server.Term < response.Term {
					server.Term = response.Term
					setTerm(server.PersistentStore, server.Term)
					server.VotedFor = nil
					setVotedFor(server.PersistentStore, server.VotedFor)
					server.convertToFollower()
				}
				voteCh <- response.VoteGranted
			}
		}()
	}
	go func() {
		// We always vote ourselves
		votesReceived := 1
		positiveVotesReceived := 1
		for positiveVotesReceived < reqToMajority && votesReceived < totalServers {
			vote := <-voteCh
			votesReceived++
			if vote {
				positiveVotesReceived++
			}
		}
		if positiveVotesReceived >= reqToMajority {
			log.Printf("%v: majority votes (%d) received in election for term %d\n", server.MyID, positiveVotesReceived, requestVoteRPC.Term)
			server.Mutex.Lock()
			defer server.Mutex.Unlock()
			server.convertToLeader(requestVoteRPC.Term)
		}
	}()
}

// convertToLeader sets up transition to Leader state.
// It is assumed that caller has acquired lock before calling
// this method. convertToLeader will fail if the current
// server term is not equal to passed term (stale elections).
func (server *RaftServer) convertToLeader(term int64) {
	if term != server.Term {
		// stale election occurred
		log.Printf("%v: discarding stale election results (%d < %d)\n", server.MyID, term, server.Term)
		if term > server.Term {
			panic("fatal: term > server.Term")
		}
		return
	}
	if server.State != Candidate {
		panic("fatal: invalid transition from Follower/Leader -> Leader")
	}
	log.Printf("%v: converting to leader\n", server.MyID)
	server.State = Leader
	server.CurrentLeader = &server.MyID
	server.ElectionTimeoutChan <- false
	server.HeartbeatTimeoutChan <- true

	// Re-initialize matchIndex and nextIndex arrays
	lastLogEntry, err := server.getLastLogEntry()
	if err != nil {
		log.Printf("error getting last log entry: %+v\n", err)
	}
	for serverID := range server.NextIndexMap {
		// We assume, most optimistically, that the other servers already have our log entries (except the last one)
		// Note: the paper specifies to set it to lastLogEntry.Index + 1, but we set to lastLogEntry.Index.
		// Why? Because if set to lastLogEntry.Index + 1, the followers will never be updated in the event
		// that no more new requests come from client.
		server.NextIndexMap[serverID] = lastLogEntry.Index
		if lastLogEntry.Index == 0 {
			server.NextIndexMap[serverID] = 1
		}
		// At the same time we assume, most pessimistically, that we don't know if the other servers have even a single log entry
		server.MatchIndexMap[serverID] = 0
	}
	server.broadcastAppendEntries()
}

// broadcastAppendEntries sends append entry RPCs to all servers and waits for their response.
// It also updates nextIndex and matchIndex values.
func (server *RaftServer) broadcastAppendEntries() {
	log.Printf("%v: broadcasting append entries ...\n", server.MyID)
	for _, peer := range server.Peers {
		peer := peer
		go func() {
			server.Mutex.Lock()
			request := common.AppendEntriesRPC{
				Term:              server.Term,
				Leader:            server.MyID,
				LeaderCommitIndex: server.CommitIndex,
				PrevLogIndex:      -1,
				PrevLogTerm:       -1,
			}
			indexToSend := server.NextIndexMap[peer.GetID()]
			if length, err := server.LogStore.Length(); err == nil {
				if indexToSend < length {
					logEntry, err := server.LogStore.Get(indexToSend)
					if err != nil {
						server.Mutex.Unlock()
						log.Printf("failed to get from log store: %+v\n", err)
						return
					}
					request.Entries = append(request.Entries, *logEntry)
				}
			}
			prevLogEntry, err := server.LogStore.Get(indexToSend - 1)
			if err != nil {
				log.Printf("failed to get from log store: %+v\n", err)
				server.Mutex.Unlock()
				return
			}
			request.PrevLogIndex = prevLogEntry.Index
			request.PrevLogTerm = prevLogEntry.Term
			server.Mutex.Unlock()

			var response common.AppendEntriesRPCResult
			if err := peer.AppendEntries(&request, &response); err != nil {
				log.Printf("error on AppendEntriesRPC: %+v\n", err)
				return
			}
			server.Mutex.Lock()
			defer server.Mutex.Unlock()
			if response.Term != server.Term {
				// Either the peer was on a higher term, or our term number changed concurrently
				// In either case the request is invalid and must be discarded
				if response.Term > server.Term {
					server.Term = response.Term
					setTerm(server.PersistentStore, server.Term)
					server.VotedFor = nil
					setVotedFor(server.PersistentStore, server.VotedFor)
					server.convertToFollower()
				}
				return
			}
			if response.Success {
				if len(request.Entries) > 0 {
					// Update our match index and next index values for this peer
					if request.Entries[0].Index >= server.NextIndexMap[peer.GetID()] {
						server.NextIndexMap[peer.GetID()] = request.Entries[0].Index + 1
					}
					if request.Entries[0].Index >= server.MatchIndexMap[peer.GetID()] {
						server.MatchIndexMap[peer.GetID()] = request.Entries[0].Index
					}
					// Now check if this caused some entry to get committed
					server.commitEntries()
				}
			} else {
				log.Printf("append entries RPC success=false for peer %v\n", peer.GetID())
				// Failure means the server has holes in its log
				// So decrement nextIndex
				server.NextIndexMap[peer.GetID()]--
			}
		}()
	}
}

// commitEntries checks for new entries that are committed updating the commitedIndex.
// It assumes that the caller has already acquired mutex before calling this method.
func (server *RaftServer) commitEntries() {
	var matchIndexes []int64
	for _, index := range server.MatchIndexMap {
		matchIndexes = append(matchIndexes, index)
	}
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] < matchIndexes[j]
	})
	// Invariant i-th value in array matchIndexes is replicated in (n-i) servers (including ourselves)
	// Note that here n == number of servers in cluster is not equal to len(matchIndexes) == n-1
	// This implies that the value at floor(n/2) is the minimum value that is guaranteed to be
	// replicated at ceil(n/2) servers (including ourselves)
	n := len(matchIndexes) + 1
	if matchIndexes[n/2] > server.CommitIndex {
		server.CommitIndex = matchIndexes[n/2]
		setCommitIndex(server.PersistentStore, server.CommitIndex)
	}

	for server.AppliedIndex < server.CommitIndex {
		logEntry, err := server.LogStore.Get(server.AppliedIndex + 1)
		if err != nil {
			log.Printf("error getting log entry from log store: %+v\n", err)
			break
		}
		bytes, err := server.FSM.Apply(*logEntry)
		if err != nil {
			log.Printf("error applying log entry to FSM: :%+v\n", err)
		}
		if ch, ok := server.ApplyChan[server.AppliedIndex]; ok {
			ch <- ApplyMsg{
				Err:   err,
				Bytes: bytes,
			}
		}
		server.AppliedIndex++
	}
}

// electionTimeoutController should run in a separate goroutine and is
// responsible for managing election timeouts. This goroutine is indirectly
// controlled by the electionTimeoutChan channel. Passing false to the channel
// disables the controller until true is passed to the channel. Passing true
// to the channel simply resets the timer. Whenever a timeout occurs it
// initiates conversion to Candidate.
func (server *RaftServer) electionTimeoutController(timeout time.Duration) {
	timeoutRandomizer := func(timeout time.Duration) time.Duration {
		return timeout + time.Duration(rand.Float64()*float64(timeout))
	}
	log.Printf("%v: election timeout controller started\n", server.MyID)
	ticker := time.NewTicker(timeoutRandomizer(timeout))
	for {
		select {
		case <-ticker.C:
			log.Printf("%v: received election timeout tick\n", server.MyID)
			ticker.Stop()
			server.Mutex.Lock()
			server.convertToCandidate()
			server.Mutex.Unlock()
			ticker.Reset(timeoutRandomizer(timeout))
		case reset := <-server.ElectionTimeoutChan:
			if reset == true {
				log.Printf("%v: resetting election timer\n", server.MyID)
				ticker.Reset(timeoutRandomizer(timeout))
			} else {
				log.Printf("%v: stopping election timer\n", server.MyID)
				ticker.Stop()
			}
		}
	}
}

// heartbeatTimeoutController should run in a separate goroutine and is
// responsible for managing heartbeat timeouts. This goroutine can be
// indireclty controlled by the heartBeatTimeoutChan similarly to how
// electionTimeoutController is controlled. Whenever a timeout occurs
// this broadcasts an empty append entries to all servers.
func (server *RaftServer) heartBeatTimeoutController(timeout time.Duration) {
	ticker := time.NewTicker(timeout)
	for {
		select {
		case <-ticker.C:
			log.Printf("%v: received heartbeat timeout tick\n", server.MyID)
			ticker.Stop()
			server.broadcastAppendEntries()
			ticker.Reset(timeout)
		case reset := <-server.HeartbeatTimeoutChan:
			if reset == true {
				log.Printf("%v: resetting heartbeat timer\n", server.MyID)
				ticker.Reset(timeout)
			} else {
				log.Printf("%v: stopping heartbeat timer\n", server.MyID)
				ticker.Stop()
			}
		}
	}
}
