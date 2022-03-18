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
	Apply(entry LogEntry) ([]byte, error)
}

// RPCServer is the interface exposed by a Raft server
// to outside (including other Raft servers, and clients)
type RPCServer interface {
	ClientRequest(args *ClientRequestRPC, result *ClientRequestRPCResult) error
	RequestVote(args *RequestVoteRPC, result *RequestVoteRPCResult) error
	AppendEntries(args *AppendEntriesRPC, result *AppendEntriesRPCResult) error
}

// RPCManager abstracts away RPC handling from RPC servers
type RPCManager interface {
	// Start is a blocking call.
	// It starts the RPC server at the given address and blocks forever.
	// Start only returns error if it fails to start the server.
	Start(address ServerAddress, server RPCServer) error
	ConnectToPeer(address ServerAddress) (RPCServer, error)
}
