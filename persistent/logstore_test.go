package persistent_test

import (
	"github.com/sushantsondhi/raft-col733/persistent"
	"github.com/sushantsondhi/raft-col733/raft"
	"testing"
)

func TestLogStore_Create(t *testing.T) {

	_, err := persistent.CreateDbLogStore("log.db")

	if err != nil {
		t.Error("db creation failed", err)
	}

}

func TestLogStore_Store(t *testing.T) {

	d, err := persistent.CreateDbLogStore("log.db")

	if err != nil {
		t.Error("db creation failed", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 0,
		Term:  0,
	})

	if err != nil {
		t.Error("failed to append in empty log", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 1,
		Term:  0,
	})

	if err != nil {
		t.Error("failed to append in non empty log", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 0,
		Term:  0,
	})

	if err != nil {
		t.Error("failed to add log at an existing index", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 69,
		Term:  0,
	})

	if err == nil {
		t.Error("allowed discontinuous append")
	}

}

func TestLogStore_Get(t *testing.T) {

	d, err := persistent.CreateDbLogStore("log.db")

	if err != nil {
		t.Error("db creation failed", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 0,
		Term:  0,
		Data:  []byte("entry0"),
	})

	if err != nil {
		t.Error("failed to append in empty log", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 1,
		Term:  0,
		Data:  []byte("entry1"),
	})

	if err != nil {
		t.Error("failed to append in non empty log", err)
	}

	var entry *raft.LogEntry
	entry, err = d.Get(0)

	if err != nil {
		t.Error("failed to get value at index 0", err)
	}

	if string(entry.Data) != "entry0" || entry.Index != 0 {
		t.Error("got corrupted/incorrect data", err)
	}

	err = d.Store(raft.LogEntry{
		Index: 0,
		Term:  0,
		Data:  []byte("updated_entry0"),
	})

	if err != nil {
		t.Error("failed to add entry at existing index", err)
	}

	entry, err = d.Get(0)

	if err != nil {
		t.Error("failed to get value at index 0", err)
	}

	if string(entry.Data) != "updated_entry0" || entry.Index != 0 {
		t.Error("didn't get updated data", err)
	}

	entry, err = d.Get(69)

	if err == nil {
		t.Error("got entry for non-existing index")
	}

}
