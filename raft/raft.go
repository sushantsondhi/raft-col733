package raft

import (
	"github.com/google/uuid"
	"go.uber.org/multierr"
	"log"
	"sort"
	"sync"
	"time"
)

type RaftServer struct {
	// Access to state must be synchronized between multiple goroutines
	state

	// Data Stores
	FSM             FSM
	LogStore        LogStore
	PersistentStore PersistentStore

	// Peers
	MyID  uuid.UUID
	Peers []RPCServer

	// Synchronization primitives
	Mutex                sync.Mutex
	ElectionTimeoutChan  chan bool
	HeartbeatTimeoutChan chan bool
}

func NewRaftServer(
	me Server,
	cluster ClusterConfig,
	fsm FSM,
	logStore LogStore,
	persistentStore PersistentStore,
	manager RPCManager,
) *RaftServer {
	// TODO: lots of TODO
	return &RaftServer{}
}

func (server *RaftServer) ClientRequest(args *ClientRequestRPC, result *ClientRequestRPCResult) error {
	//TODO implement me
	panic("implement me")
}

func (server *RaftServer) RequestVote(args *RequestVoteRPC, result *RequestVoteRPCResult) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	if result.Term > server.Term {
		// Update term and convert to follower
		// TODO: make this persistent after Sushant's PR
		server.Term = result.Term
		server.convertToFollower()
	}
	result.Term = args.Term
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

func (server *RaftServer) AppendEntries(args *AppendEntriesRPC, result *AppendEntriesRPCResult) error {
	//TODO implement me
	panic("implement me")
}

func (server *RaftServer) getLastLogEntry() (entry *LogEntry, err error) {
	var lengthErr, logErr error
	var logLength uint64
	logLength, lengthErr = server.LogStore.Length()
	entry, logErr = server.LogStore.Get(logLength - 1)
	err = multierr.Combine(lengthErr, logErr)
	return
}

// convertToFollower method will initiate transition of Raft's server
// state to a follower, it assumes that the caller has already
// acquired mutex.
func (server *RaftServer) convertToFollower() {
	server.State = Follower
	server.CurrentLeader = nil
	// (Re)start election timeouts
	server.ElectionTimeoutChan <- true
}

// convertToCandidate method will initiate transition of Raft's server
// state to a candidate, it assumes that the caller has already
// acquired mutex.
func (server *RaftServer) convertToCandidate() {
	if server.State == Leader {
		panic("Unexpected transition from Leader -> Candidate")
	}
	server.State = Candidate
	server.CurrentLeader = nil
	// TODO: make this persistent after sushant's PR
	server.Term++
	server.VotedFor = &server.MyID

	// Send RequestVoteRPC to all servers
	totalServers := 0 // TODO fill this after sushant's PR
	reqToMajority := totalServers/2 + 1

	lastLogEntry, err := server.getLastLogEntry()
	if err != nil {
		log.Printf("error getting last log entry : %+v\n", err)
		return
	}

	requestVoteRPC := RequestVoteRPC{
		Term:         server.Term,
		CandidateID:  server.MyID,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}

	voteCh := make(chan bool)
	for _, peer := range server.Peers {
		peer := peer
		go func() {
			var response RequestVoteRPCResult
			if err := peer.RequestVote(&requestVoteRPC, &response); err != nil {
				log.Printf("error requesting vote from peer: %+v\n", err)
				voteCh <- false
			} else {
				server.Mutex.Lock()
				defer server.Mutex.Unlock()
				if server.Term < response.Term {
					server.Term = response.Term
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
		if term > server.Term {
			panic("fatal: term > server.Term")
		}
		return
	}
	if server.State != Candidate {
		panic("fatal: invalid transition from Follower/Leader -> Leader")
	}
	server.State = Leader
	server.CurrentLeader = &server.MyID

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
		if lastLogEntry != nil {
			server.NextIndexMap[serverID] = lastLogEntry.Index
		} else {
			server.NextIndexMap[serverID] = 0
		}
		// At the same time we assume, most pessimistically, that we don't know if the other servers have even a single log entry
		server.MatchIndexMap[serverID] = -1
	}
	// TODO: asynchronously broadcast append entries
}

// broadcastAppendEntries sends append entry RPCs to all servers and waits for their response.
// It also updates nextIndex and matchIndex values.
func (server *RaftServer) broadcastAppendEntries() {
	for _, peer := range server.Peers {
		peer := peer
		go func() {
			server.Mutex.Lock()
			indexToSend := server.NextIndexMap[peer.GetID()]
			if length, err := server.LogStore.Length(); err == nil {
				if length == uint64(indexToSend) {
					server.Mutex.Unlock()
					return
				}
			}
			request := AppendEntriesRPC{
				Term:              server.Term,
				Leader:            server.MyID,
				LeaderCommitIndex: server.CommitIndex,
				PrevLogIndex:      -1,
				PrevLogTerm:       -1,
			}
			server.Mutex.Unlock()
			logEntry, err := server.LogStore.Get(uint64(indexToSend))
			if err != nil {
				log.Printf("failed to get from log store: %+v\n", err)
				return
			}
			if indexToSend > 0 {
				prevLogEntry, err := server.LogStore.Get(uint64(indexToSend - 1))
				if err != nil {
					log.Printf("failed to get from log store: %+v\n", err)
					return
				}
				request.PrevLogIndex = prevLogEntry.Index
				request.PrevLogTerm = prevLogEntry.Term
			}
			request.Entries = append(request.Entries, *logEntry)

			var response AppendEntriesRPCResult
			if err := peer.AppendEntries(&request, &response); err != nil {
				log.Printf("error on AppendEntriesRPC: %+v\n", err)
				return
			}
			server.Mutex.Lock()
			defer server.Mutex.Unlock()
			if response.Term != server.Term {
				// Either the peer was on a higher term, or our term number changed concurrently
				// In either case the request is invalid and must be discarded
				return
			}
			if response.Success {
				// Update our match index and next index values for this peer
				if request.Entries[0].Index >= server.NextIndexMap[peer.GetID()] {
					server.NextIndexMap[peer.GetID()] = request.Entries[0].Index + 1
				}
				if request.Entries[0].Index >= server.MatchIndexMap[peer.GetID()] {
					server.MatchIndexMap[peer.GetID()] = request.Entries[0].Index
				}
				// Now check if this caused some entry to get committed
				server.commitEntries()
			} else {
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
		// TODO: persist this after sushant's PR
		server.CommitIndex = matchIndexes[n/2]
	}
}

// electionTimeoutController should run in a separate goroutine and is
// responsible for managing election timeouts. This goroutine is indirectly
// controlled by the electionTimeoutChan channel. Passing false to the channel
// disables the controller until true is passed to the channel. Passing true
// to the channel simply resets the timer. Whenever a timeout occurs it
// initiates conversion to Candidate.
func (server *RaftServer) electionTimeoutController(timeout time.Duration) {
	ticker := time.NewTicker(timeout)
	ticker.Stop()
	select {
	case <-ticker.C:
		ticker.Stop()
		server.Mutex.Lock()
		server.convertToCandidate()
		server.Mutex.Unlock()
		ticker.Reset(timeout)
	case reset := <-server.ElectionTimeoutChan:
		if reset == true {
			ticker.Reset(timeout)
		} else {
			ticker.Stop()
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
	ticker.Stop()
	select {
	case <-ticker.C:
		ticker.Stop()
		// TODO: Asynchronously broadcast empty append entries
		ticker.Reset(timeout)
	case reset := <-server.ElectionTimeoutChan:
		if reset == true {
			ticker.Reset(timeout)
		} else {
			ticker.Stop()
		}
	}
}
