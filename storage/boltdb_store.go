package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

var systemBucketName = []byte("system")
var dataBucketName = []byte("data")
var binlogFileKey = []byte("binlogfile")
var binlogPositionKey = []byte("binlogpos")

// BoltDBStore Represents an instance of the BoltDB storage.
type BoltDBStore struct {
	started bool
	db      *bolt.DB
	tx      *bolt.Tx
	lock    sync.Mutex
}

// BinlogInformation Represents the information about the MySQL binlog.
type BinlogInformation struct {
	File     string
	Position uint32
}

// Open Opens the disk storage.
func (store *BoltDBStore) Open(filename string) error {
	// It will be created if it doesn't exist.
	var err error
	store.db, err = bolt.Open(filename, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
		// InitialMmapSize: 1000000000,
	})
	if err != nil {
		return err
	}
	store.db.NoSync = true

	// Ensure system bucket is created.
	store.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(systemBucketName)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists(dataBucketName)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	store.tx, err = store.db.Begin(true)
	if err != nil {
		return err
	}
	store.started = true
	return nil
}

// IsStarted Returns whether the BoltDB store is started.
func (store *BoltDBStore) IsStarted() bool {
	return store.started
}

// Close Closes the BoltDB storage.
func (store *BoltDBStore) Close() {
	store.db.Close()
}

// GetBinlogPosition Returns the persisted binlog position.
func (store *BoltDBStore) GetBinlogPosition() (*BinlogInformation, error) {
	file, err := store.GetFromBucket(systemBucketName, binlogFileKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open readonly transaction")
	}

	position, err := store.GetFromBucket(systemBucketName, binlogPositionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open readonly transaction")
	}

	if file == nil || position == nil {
		return nil, errors.New("binlog file or position not found")
	}

	return &BinlogInformation{File: string(file), Position: binary.LittleEndian.Uint32(position)}, nil
}

// SetBinlogPosition Sets and persists the current binlog position.
func (store *BoltDBStore) SetBinlogPosition(binlogInfo *BinlogInformation) error {
	fileBuffer := []byte(binlogInfo.File)
	positionBuffer := make([]byte, 4)

	binary.LittleEndian.PutUint32(positionBuffer, binlogInfo.Position)
	err := store.Set([]byte(binlogFileKey), fileBuffer)
	if err != nil {
		return err
	}
	err = store.Set([]byte(binlogPositionKey), positionBuffer)
	if err != nil {
		return err
	}
	return nil
}

// Get Gets the value associated with the specified key.
func (store *BoltDBStore) Get(key []byte) ([]byte, error) {
	return store.GetFromBucket(dataBucketName, key)
}

// GetFromBucket Gets the value associated with the specified key from the specified bucket.
func (store *BoltDBStore) GetFromBucket(bucket []byte, key []byte) ([]byte, error) {
	var value []byte

	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucket)
		value = bucket.Get(key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Set Sets the specified value associated with the specified key.
func (store *BoltDBStore) Set(key []byte, value []byte) error {
	return store.SetFromBucket(dataBucketName, key, value)
}

// SetFromBucket Sets the specified value associated with the specified key in the specified bucket.
func (store *BoltDBStore) SetFromBucket(bucket []byte, key []byte, value []byte) error {
	store.lock.Lock()

	bkt := store.tx.Bucket(bucket)
	err := bkt.Put(key, value)
	if err != nil {
		return err
	}

	store.lock.Unlock()
	return nil
}

// Commit Commits the current transaction.
func (store *BoltDBStore) Commit() error {
	var err error
	store.lock.Lock()
	if err = store.tx.Commit(); err != nil {
		store.lock.Unlock()
		return err
	}
	err = store.db.Sync()
	if err != nil {
		store.lock.Unlock()
		return err
	}
	store.tx, err = store.db.Begin(true)
	if err != nil {
		store.lock.Unlock()
		return err
	}
	store.lock.Unlock()
	return nil
}
