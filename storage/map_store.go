package storage

import (
	"encoding/binary"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

// MapStore Represents an instance of the in-memory storage.
type MapStore struct {
	started bool
	items   map[string]string
	lock    sync.Mutex
}

var binlogFileKeyString = "binlogfile"
var binlogPositionKeyString = "binlogpos"

// Open Opens the storage.
func (store *MapStore) Open(filename string) error {
	store.items = make(map[string]string)
	store.started = true
	return nil
}

// IsStarted Returns whether the store is started.
func (store *MapStore) IsStarted() bool {
	return store.started
}

// Close Closes the storage.
func (store *MapStore) Close() {
}

// GetBinlogPosition Returns the persisted binlog position.
func (store *MapStore) GetBinlogPosition() (*BinlogInformation, error) {
	binlogfile := store.items[binlogFileKeyString]
	binlogposString := store.items[binlogPositionKeyString]
	binlogpos, err := strconv.ParseUint(binlogposString, 10, 32)

	if err != nil {
		return nil, err
	}

	return &BinlogInformation{File: binlogfile, Position: uint32(binlogpos)}, nil
}

// SetBinlogPosition Sets and persists the current binlog position.
func (store *MapStore) SetBinlogPosition(binlogInfo *BinlogInformation) error {
	fileBuffer := []byte(binlogInfo.File)
	positionBuffer := make([]byte, 4)

	binary.LittleEndian.PutUint32(positionBuffer, binlogInfo.Position)
	err := store.Set(fileBuffer, positionBuffer)
	if err != nil {
		return err
	}
	return nil
}

// Get Gets the value associated with the specified key.
func (store *MapStore) Get(key []byte) ([]byte, error) {
	value, found := store.items[string(key)]
	if !found {
		return nil, errors.New("Key not found")
	}
	return []byte(value), nil
}

// Set Sets the specified value associated with the specified key.
func (store *MapStore) Set(key []byte, value []byte) error {
	store.lock.Lock()
	store.items[string(key)] = string(value)
	store.lock.Unlock()
	return nil
}

// Commit Commits the current transaction.
func (store *MapStore) Commit() error {
	return nil
}
