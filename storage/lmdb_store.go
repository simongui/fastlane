package storage

import (
	"encoding/binary"
	"sync"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/pkg/errors"
)

// LMDBStore Represents an instance of the LMDB storage engine.
type LMDBStore struct {
	started          bool
	resetTransaction bool
	lock             sync.Mutex
	env              *lmdb.Env
	dbi              *lmdb.DBI
	tx               *lmdb.Txn
}

// IsStarted Returns whether the LMDB store is started.
func (store *LMDBStore) IsStarted() bool {
	return store.started
}

// Open Opens the disk storage.
func (store *LMDBStore) Open(filename string) error {
	var err error

	store.env, err = lmdb.NewEnv()
	if err != nil {
		return errors.Wrap(err, "unable to initialize LMDB environment")
	}
	//defer env.Close()

	// configure and open the environment.  most configuration must be done
	// before opening the environment.
	err = store.env.SetMaxDBs(1)
	if err != nil {
		return errors.Wrap(err, "unable to set max DB's")
	}
	err = store.env.SetMapSize(1 << 30)
	if err != nil {
		return errors.Wrap(err, "unable to set map size")
	}
	err = store.env.Open(filename, lmdb.NoSync, 0644)
	if err != nil {
		return errors.Wrap(err, "unable to open LMDB environment")
	}

	// open a database that can be used as long as the enviroment is mapped.
	err = store.env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.CreateDBI("data")
		store.dbi = &dbi
		return err
	})
	if err != nil {
		return errors.Wrap(err, "unable to open transaction")
	}

	store.tx, err = store.env.BeginTxn(nil, 0)
	if err != nil {
		return errors.Wrap(err, "unable to open transaction")
	}
	store.started = true
	return nil
}

// Get Gets the value associated with the specified key.
func (store *LMDBStore) Get(key []byte) ([]byte, error) {
	// var err error
	var value []byte

	// the database is now ready for use.  read the value for a key and print
	// it to standard output.
	err := store.env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(*store.dbi, key)
		value = v
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to find key")
	}
	return value, nil
}

// Set Sets the specified value associated with the specified key.
func (store *LMDBStore) Set(key []byte, value []byte) error {
	var err error
	store.lock.Lock()

	err = store.tx.Put(*store.dbi, key, value, 0)
	if err != nil {
		store.lock.Unlock()
		return err
	}
	store.lock.Unlock()
	return nil
}

// GetBinlogPosition Returns the persisted binlog position.
func (store *LMDBStore) GetBinlogPosition() (*BinlogInformation, error) {
	binlogInfo := &BinlogInformation{}
	binlogFile, err := store.Get(binlogFileKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get key")
	}
	if binlogFile == nil || len(binlogFile) <= 0 {
		return nil, errors.Wrap(err, "binlog file not found")
	}
	binlogInfo.File = string(binlogFile)

	binlogPositionBuffer, err := store.Get(binlogPositionKey)
	if err != nil {
		return nil, err
	}
	if len(binlogPositionBuffer) <= 0 {
		return nil, errors.Wrap(err, "binlog position not found")
	}
	binlogInfo.Position = binary.LittleEndian.Uint32(binlogPositionBuffer)

	// defer value.Free()
	return binlogInfo, nil
}

// SetBinlogPosition Sets and persists the current binlog position.
func (store *LMDBStore) SetBinlogPosition(binlogInfo *BinlogInformation) error {
	var err error

	fileBuffer := []byte(binlogInfo.File)
	positionBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(positionBuffer, binlogInfo.Position)

	err = store.Set(binlogFileKey, fileBuffer)
	if err != nil {
		return err
	}
	err = store.Set(binlogPositionKey, positionBuffer)
	if err != nil {
		return err
	}
	return nil
}

// Commit Commits the current write batch.
func (store *LMDBStore) Commit() error {
	store.lock.Lock()

	err := store.tx.Commit()
	if err != nil {
		return err
	}
	err = store.env.Sync(true)
	if err != nil {
		return err
	}
	// err = store.tx.Renew()
	store.tx, err = store.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	store.lock.Unlock()
	return nil
}
