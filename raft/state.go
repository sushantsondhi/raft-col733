package raft

import "github.com/google/uuid"

type RaftState int

const (
	Candidate RaftState = iota
	Follower
	Leader
)

type state struct {
	// These 3 variables are persisted
	Term        uint64
	VotedFor    *uuid.UUID
	CommitIndex uint64

	// These 4 variables are volatile
	State        RaftState
	AppliedIndex uint64

	NextIndexMap  map[uuid.UUID]uint64
	MatchIndexMap map[uuid.UUID]uint64
}
