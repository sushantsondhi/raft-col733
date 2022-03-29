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

FOLDER=/home/student/project/raft-col733/
IP=sarthak

build:
	go build main.go

transfer_config:
	scp config.yaml student@$(shell cat .$(IP)):$(FOLDER)

transfer_project:
	scp -r /home/student/project/raft-col733 student@$(shell cat .$(IP)):/home/student/project/

transfer_binary:
	scp main student@$(shell cat .$(IP)):$(FOLDER)

config3_baadal:
	./main config -servers 10.17.50.164:12345,10.17.5.50:12345,10.17.50.70:12345

config5_baadal:
	./main config -servers 10.17.50.164:12345,10.17.50.164:12346,10.17.5.50:12345,10.17.5.50:12346,10.17.50.70:12345

config7_baadal:
	./main config -servers 10.17.50.164:12345,10.17.50.164:12346,10.17.50.164:12347,10.17.5.50:12345,10.17.5.50:12346,10.17.50.70:12345,10.17.50.70:12346

config9_baadal:
	./main config -servers 10.17.50.164:12345,10.17.50.164:12346,10.17.50.164:12347,10.17.5.50:12345,10.17.5.50:12346,10.17.5.50:12347,10.17.50.70:12345,10.17.50.70:12346,10.17.50.70:12347

config11_baadal:
	./main config -servers 10.17.50.164:12345,10.17.50.164:12346,10.17.50.164:12347,10.17.50.164:12348,10.17.5.50:12345,10.17.5.50:12346,10.17.5.50:12347,10.17.5.50:12348,10.17.50.70:12345,10.17.50.70:12346,10.17.50.70:12347

clean:
	rm -f *.db
	rm -f */*.db