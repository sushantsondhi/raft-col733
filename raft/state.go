package raft

import "github.com/google/uuid"

type RaftState int

const (
	Candidate RaftState = iota
	Follower
	Leader
)

type state struct {
	// These 4 variables are persisted
	Term          int64
	VotedFor      *uuid.UUID
	CommitIndex   int64
	CurrentLeader *uuid.UUID

	// These 4 variables are volatile
	State        RaftState
	AppliedIndex int64

	NextIndexMap  map[uuid.UUID]int64
	MatchIndexMap map[uuid.UUID]int64
}
