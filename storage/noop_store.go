package storage

import (
	"sync"

	"github.com/pkg/errors"
)

// NoopStore Represents a store instance that does nothing.
type NoopStore struct {
	started bool
	lock    sync.Mutex
}

// Open Opens the storage.
func (store *NoopStore) Open(filename string) error {
	store.started = true
	return nil
}

// IsStarted Returns whether the store is started.
func (store *NoopStore) IsStarted() bool {
	return store.started
}

// Close Closes the storage.
func (store *NoopStore) Close() {
}

// GetBinlogPosition Returns the persisted binlog position.
func (store *NoopStore) GetBinlogPosition() (*BinlogInformation, error) {
	return &BinlogInformation{File: "", Position: 0}, nil
}

// SetBinlogPosition Sets and persists the current binlog position.
func (store *NoopStore) SetBinlogPosition(binlogInfo *BinlogInformation) error {
	return nil
}

// Get Gets the value associated with the specified key.
func (store *NoopStore) Get(key []byte) ([]byte, error) {
	return nil, errors.New("Unable to find key.")
}

// Set Sets the specified value associated with the specified key.
func (store *NoopStore) Set(key []byte, value []byte) error {
	return nil
}

// Commit Commits the current transaction.
func (store *NoopStore) Commit() error {
	return nil
}
