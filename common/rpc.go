package common

import (
	"github.com/google/uuid"
)

type ClientRequestRPC struct {
	Data []byte
}

type ClientRequestRPCResult struct {
	Success bool
	// Error will be non-empty iff Success is False
	Error string
	// Data can be non-nil for example for Get calls
	Data []byte
}

// See Raft paper for details on below RPCs

type RequestVoteRPC struct {
	Term         int64
	CandidateID  uuid.UUID
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteRPCResult struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesRPC struct {
	Term              int64
	Leader            uuid.UUID
	PrevLogIndex      int64
	PrevLogTerm       int64
	Entries           []LogEntry
	LeaderCommitIndex int64
}

type AppendEntriesRPCResult struct {
	Term    int64
	Success bool
}
