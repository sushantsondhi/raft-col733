test:
	go test ./raft -run SimpleElection -count 1
	go test ./raft -run ElectionWithoutHeartbeat -count 1
	go test ./raft -run ReElection -count 1
	go test ./raft -run ReJoin -count 1
	go test ./raft -run GetAndSetClient -count 1
	go test ./raft -run SimpleLogStoreAndFSMCheck -count 1
	go test ./raft -run LogReplayability -count 1
	go test ./raft -run LaggingFollower -count 1
	go test ./raft -run LeaderCompleteness -count 1
	go test ./raft -run CommitDurability -count 1
	go test ./raft -run OldTermsNotCommitted -count 1
	go test ./raft -run ElectionSafety -count 1
