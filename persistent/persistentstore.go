package persistent

import (
	"database/sql"
	"github.com/sushantsondhi/raft-col733/raft"
)

type PStore struct {
	db *sql.DB
}

var _ raft.PersistentStore = PStore{}

func NewPStore(db *sql.DB) PStore {
	return PStore{
		db: db,
	}
}

func (store PStore) Set(key, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (store PStore) Get(key []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}
