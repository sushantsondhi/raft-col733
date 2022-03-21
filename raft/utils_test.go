package raft

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/persistent"
	"os"
	"testing"
)

func TestGetAndSetTerm(t *testing.T) {
	var d string = "state.db"
	if err := os.Remove(d); err != nil && !os.IsNotExist(err) {
		t.FailNow()
	}

	store, err := persistent.NewPStore(d)
	if err != nil {
		t.Error("db creation failed", err)
	}
	var term int64
	term = getTerm(store)
	assert.Equal(t, term, int64(0), "Default term not 0")
	setTerm(store, 9)
	term = getTerm(store)
	assert.Equal(t, term, int64(9), "Term not equal to setted term")
	os.Remove(d)
}

func TestGetAndSetVotedFor(t *testing.T) {
	var d string = "state.db"
	if err := os.Remove(d); err != nil && !os.IsNotExist(err) {
		t.FailNow()
	}

	store, err := persistent.NewPStore(d)
	if err != nil {
		t.Error("db creation failed", err)
	}

	var votedFor *uuid.UUID
	votedFor = getVotedFor(store)
	assert.Nil(t, votedFor, "Default voted for not nil")
	var newUUID = uuid.New()
	setVotedFor(store, &newUUID)
	votedFor = getVotedFor(store)
	assert.Equal(t, *votedFor, newUUID, "Voted for is not same as setted VotedFor")
	os.Remove(d)
}

func TestGetAndSetCommitIndex(t *testing.T) {
	var d string = "state.db"
	if err := os.Remove(d); err != nil && !os.IsNotExist(err) {
		t.FailNow()
	}

	store, err := persistent.NewPStore(d)
	if err != nil {
		t.Error("db creation failed", err)
	}
	var commitIndex int64
	commitIndex = getCommitIndex(store)
	assert.Equal(t, commitIndex, int64(0), "Default commitIndex not 0")
	setCommitIndex(store, 9)
	commitIndex = getTerm(store)
	assert.Equal(t, commitIndex, int64(9), "CommitIndex not equal to setted commitIndex")
	os.Remove(d)
}
