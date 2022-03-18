package raft

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
	// TODO: lots of TODO
	return &RaftServer{}
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
