package raft

import "github.com/google/uuid"

type RaftServer struct {
	state
	LogStore        LogStore
	PersistentStore PersistentStore
}

func NewRaftServer(
	me Server,
	cluster ClusterConfig,
	logStore LogStore,
	persistentStore PersistentStore,
	manager RPCManager,
) *RaftServer {
	newRaftServer := &RaftServer{
		state: state{
			Term:          getTerm(persistentStore),
			VotedFor:      getVotedFor(persistentStore),
			CommitIndex:   getCommitIndex(persistentStore),
			State:         Candidate,
			AppliedIndex:  -1,
			NextIndexMap:  make(map[uuid.UUID]int64),
			MatchIndexMap: make(map[uuid.UUID]int64),
		},
		LogStore:        logStore,
		PersistentStore: persistentStore,
	}
	return newRaftServer
}

func (server *RaftServer) ClientRequest(args *ClientRequestRPC, result *ClientRequestRPCResult) error {
	//TODO implement me
	panic("implement me")
}

func (server *RaftServer) RequestVote(args *RequestVoteRPC, result *RequestVoteRPCResult) error {
	//TODO implement me
	panic("implement me")
}

func (server *RaftServer) AppendEntries(args *AppendEntriesRPC, result *AppendEntriesRPCResult) error {
	//TODO implement me
	panic("implement me")
}
