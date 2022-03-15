package persistent

// Bolt is a pure Go key/value store  that don't require a full database server such as Postgres or MySQL
import (
	"github.com/boltdb/bolt"
	"github.com/sushantsondhi/raft-col733/raft"
	"log"
)

var dataBaseFilePath = "db-store.log"
var logsBucketName = []byte("logs")

// DbLogStore is a log store implementation backed by a Bolt DB
type DbLogStore struct {
	db *bolt.DB
}

var _ raft.LogStore = DbLogStore{}

func CreateDbLogStore() (DbLogStore, error) {
	// Open the .db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(dataBaseFilePath, 0600, nil)
	return DbLogStore{
		db: db,
	}, err
}

func (d DbLogStore) Init() error {

	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists(logsBucketName)

	if err != nil {
		return err
	}

	return tx.Commit()

}

func (d DbLogStore) Store(entry raft.LogEntry) error {

	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	key := uint64ToBytes(entry.Index)
	val := EncodeToBytes(entry)

	bucket := tx.Bucket(logsBucketName)
	if err := bucket.Put(key, val); err != nil {
		return err
	}
	return tx.Commit()
}

func (d DbLogStore) Get(index uint64) (*raft.LogEntry, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(logsBucketName)
	val := bucket.Get(uint64ToBytes(index))

	if val == nil {
		log.Fatalf("Log for given index %d not found", index)
	}
	entry := DecodeToLogEntry(val)
	return &entry, nil
}

func (d DbLogStore) Length() (uint64, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	logLength := tx.Bucket(logsBucketName).Stats().KeyN

	return uint64(logLength), nil

}
