package raft

import (
	"github.com/google/uuid"
	"time"
)

// ServerAddress represents a network address of a raft server (hostname:port)
type ServerAddress string

type Server struct {
	ID         uuid.UUID
	NetAddress ServerAddress
}

// ClusterConfig specifies configuration information related to a
// raft cluster. This includes tunable properties of the Raft
// protocol itself such as different timeouts.
type ClusterConfig struct {
	Cluster          []Server
	HeartBeatTimeout time.Duration
	ElectionTimeout  time.Duration
}

const (
	VotedFor    string = "votedFor"
	Term        string = "term"
	CommitIndex string = "commitIndex"
)
