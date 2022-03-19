package raft

import (
	"github.com/google/uuid"
	"log"
	"strconv"
)

func getTerm(persistentStore PersistentStore) int64 {
	if r, err := persistentStore.GetDefault([]byte(Term), []byte("0")); err != nil {
		term, error := strconv.ParseInt(string(r), 10, 64)
		if error != nil {
			log.Printf("Error pasing term: %+v\n", err)
			return -1
		}
		return term
	} else {
		log.Printf("Error getting term from persistentStore: %+v\n", err)
		return -1
	}
}

func setTerm(persistentStore PersistentStore, term int64) error {
	return persistentStore.Set([]byte(Term), []byte(string(term)))
}

func getVotedFor(persistentStore PersistentStore) *uuid.UUID {
	if r, err := persistentStore.GetDefault([]byte(VotedFor), nil); err != nil {
		if r != nil {
			votedFor, error := uuid.ParseBytes(r)
			if error != nil {
				log.Printf("Error pasing votedFor: %+v\n", err)
			} else {
				return &votedFor
			}
		}
	} else {
		log.Printf("Error getting term from persistentStore: %+v\n", err)
	}
	return nil
}

func setVotedFor(persistentStore PersistentStore, votedFor *uuid.UUID) error {
	return persistentStore.Set([]byte(VotedFor), []byte((*votedFor).String()))
}

func getCommitIndex(persistentStore PersistentStore) int64 {
	if r, err := persistentStore.GetDefault([]byte(Term), []byte("-1")); err != nil {
		commitIndex, error := strconv.ParseInt(string(r), 10, 64)
		if error != nil {
			log.Printf("Error pasing commitIndex: %+v\n", err)
			return -1
		}
		return commitIndex
	} else {
		log.Printf("Error getting commitIndex from persistentStore: %+v\n", err)
		return -1
	}
}

func setCommitIndex(persistentStore PersistentStore, commitIndex int64) error {
	return persistentStore.Set([]byte(Term), []byte(string(commitIndex)))
}
