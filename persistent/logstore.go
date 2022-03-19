package persistent

// Bolt is a pure Go key/value store  that don't require a full database server such as Postgres or MySQL
import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/sushantsondhi/raft-col733/raft"
)

var logsBucketName = []byte("logs")

// DbLogStore is a log store implementation backed by a Bolt DB
type DbLogStore struct {
	db *bolt.DB
}

var _ raft.LogStore = DbLogStore{}

func CreateDbLogStore(dataBaseFilePath string) (DbLogStore, error) {
	// Open the .db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(dataBaseFilePath, 0600, nil)

	if err != nil {
		return DbLogStore{}, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(logsBucketName)
		return err
	})

	if err != nil {
		return DbLogStore{}, err
	}

	return DbLogStore{
		db: db,
	}, err

}

func (d DbLogStore) Store(entry raft.LogEntry) error {

	return d.db.Update(func(tx *bolt.Tx) error {

		logLength := int64(tx.Bucket(logsBucketName).Stats().KeyN)
		if entry.Index > logLength {
			return errors.New("[Store]: can't append to index; greater than log length")
		}

		key := int64ToBytes(entry.Index)
		val, err := EncodeToBytes(entry)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(logsBucketName)
		return bucket.Put(key, val)

	})

}

func (d DbLogStore) Get(index int64) (*raft.LogEntry, error) {

	var entry raft.LogEntry

	err := d.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(logsBucketName)
		val := bucket.Get(int64ToBytes(index))

		if val == nil {
			return errors.New("[Get]: index doesn't exist")
		}
		var err error
		entry, err = DecodeToLogEntry(val)
		return err
	})

	return &entry, err

}

func (d DbLogStore) Length() (int64, error) {

	var logLength int64
	err := d.db.View(func(tx *bolt.Tx) error {
		logLength = int64(tx.Bucket(logsBucketName).Stats().KeyN)
		return nil
	})

	return logLength, err

}
