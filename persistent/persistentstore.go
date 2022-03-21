package persistent

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/sushantsondhi/raft-col733/common"
)

var stateBucketName = []byte("state")

type PStore struct {
	db *bolt.DB
}

var _ common.PersistentStore = PStore{}

func NewPStore(dataBaseFilePath string) (PStore, error) {
	db, err := bolt.Open(dataBaseFilePath, 0600, nil)

	if err != nil {
		return PStore{}, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(stateBucketName)
		return err
	})

	if err != nil {
		return PStore{}, err
	}

	return PStore{
		db: db,
	}, err
}

func (store PStore) Set(key, value []byte) error {

	return store.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(stateBucketName)
		return bucket.Put(key, value)

	})
}

func (store PStore) Get(key []byte) ([]byte, error) {

	var val []byte
	err := store.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(stateBucketName)
		val = bucket.Get(key)
		if val == nil {
			return errors.New("[Get]: index doesn't exist")
		}
		return nil
	})
	return val, err
}

func (store PStore) GetDefault(key []byte, defaultVal []byte) ([]byte, error) {
	var val []byte
	err := store.db.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(stateBucketName)
		val = bucket.Get(key)
		if val == nil {
			val = defaultVal
			return bucket.Put(key, defaultVal)
		}
		return nil
	})
	return val, err
}

func (d PStore) Close() error {
	return d.db.Close()
}
