package persistent

import (
	"database/sql"
	"github.com/sushantsondhi/raft-col733/raft"
)

// DbLogStore is a log store implementation backed by a SQL DB
type DbLogStore struct {
	db *sql.DB
}

var _ raft.LogStore = DbLogStore{}

func NewDbLogStore(db *sql.DB) DbLogStore {
	return DbLogStore{
		db: db,
	}
}

func (d DbLogStore) Store(entry raft.LogEntry) error {
	//TODO implement me
	panic("implement me")
}

func (d DbLogStore) Get(index int64) (*raft.LogEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (d DbLogStore) Length() (int64, error) {
	//TODO implement me
	panic("implement me")
}
