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

	// err := store.env.Update(func(txn *lmdb.Txn) (err error) {
	// open a database, creating it if necessary.  the database is stored
	// outside the transaction via closure and can be use after the
	// transaction is committed.
	// dbi, err = txn.OpenDBI("data", lmdb.Create)
	// if err != nil {
	// 	return err
	// }
	// store.tx, err = store.env.BeginTxn(nil, 0)
	store.tx.Put(*store.dbi, key, value, 0)

	// commit the transaction, writing an entry for the newly created
	// database if it was just created and allowing the dbi to be used in
	// future transactions.
	// return nil
	// })
	if err != nil {
		panic(err)
	}

	store.lock.Unlock()

	return nil
}

// Commit Commits the current write batch.
func (store *LMDBStore) Commit() {
	store.lock.Lock()
	store.tx.Commit()
	store.tx.Reset()
	store.env.Sync(true)
	store.lock.Unlock()
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
	fileBuffer := []byte(binlogInfo.File)
	positionBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(positionBuffer, binlogInfo.Position)

	store.Set(binlogFileKey, fileBuffer)
	store.Set(binlogPositionKey, positionBuffer)
	return nil
}
