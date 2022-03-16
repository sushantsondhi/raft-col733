package raft

// LogEntry represents one particular log entry in the raft
type LogEntry struct {
	Index, Term uint64
	Data        []byte
}

// LogStore is the interface that when implemented can be used as
// a store for storing logs of one raft server. LogStore is responsible
// for guaranteeing persistence of logs across server restarts.
type LogStore interface {
	// Store should overwrite the log entry if it already exists (at that index).
	Store(entry LogEntry) error
	Get(index uint64) (*LogEntry, error)
	Length() (uint64, error)
}

// PersistentStore implementations can be used as general-purpose stores
// for storing non-volatile data (such as Raft server's non-volatile state variables).
type PersistentStore interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

// FSM represents a general finite-state machine which has only a single operation -- Apply.
type FSM interface {
	Apply(entry LogEntry) (interface{}, error)
}

// Peer represents connection between 2 raft peers.
// This object manages RPC lifecycle between the 2 peers.
type Peer interface {
	RequestVote(args *RequestVoteRPC) (*RequestVoteRPCResult, error)
	AppendEntries(args *AppendEntriesRPC) (*AppendEntriesRPCResult, error)
}

// PeerConnManager can be used to instantiate Peer objects
type PeerConnManager interface {
	ConnectToPeer(address ServerAddress) (Peer, error)
}
