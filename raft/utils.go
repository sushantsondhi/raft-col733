package raft

import (
	"github.com/google/uuid"
	"github.com/sushantsondhi/raft-col733/common"
	"strconv"
)

func getTerm(persistentStore common.PersistentStore) int64 {
	if r, err := persistentStore.GetDefault([]byte(common.Term), []byte("0")); err == nil {
		term, err := strconv.ParseInt(string(r), 10, 64)
		if err != nil {
			panic(err)
		}
		return term
	} else {
		panic(err)
	}
}

func setTerm(persistentStore common.PersistentStore, term int64) {
	err := persistentStore.Set([]byte(common.Term), []byte(strconv.FormatInt(term, 10)))
	if err != nil {
		panic(err)
	}
}

func getVotedFor(persistentStore common.PersistentStore) *uuid.UUID {
	if r, err := persistentStore.GetDefault([]byte(common.VotedFor), nil); err == nil {
		if r != nil {
			votedFor := uuid.MustParse(string(r))
			return &votedFor
		}
	} else {
		panic(err)
	}
	return nil
}

func setVotedFor(persistentStore common.PersistentStore, votedFor *uuid.UUID) {
	var err error
	if votedFor == nil {
		err = persistentStore.Set([]byte(common.VotedFor), nil)
	} else {
		err = persistentStore.Set([]byte(common.VotedFor), []byte((*votedFor).String()))
	}
	if err != nil {
		panic(err)
	}
}

func getCommitIndex(persistentStore common.PersistentStore) int64 {
	if r, err := persistentStore.GetDefault([]byte(common.CommitIndex), []byte("0")); err == nil {
		commitIndex, err := strconv.ParseInt(string(r), 10, 64)
		if err != nil {
			panic(err)
		}
		return commitIndex
	} else {
		panic(err)
	}
}

func setCommitIndex(persistentStore common.PersistentStore, commitIndex int64) {
	err := persistentStore.Set([]byte(common.CommitIndex), []byte(strconv.FormatInt(commitIndex, 10)))
	if err != nil {
		panic(err)
	}
}
