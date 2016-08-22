package storage

import (
	"encoding/binary"
	"fmt"
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
	db               *bolt.DB
	tx               *bolt.Tx
	writes           chan keyvaluepair
	resetTransaction bool
}

// BinlogInformation Represents the information about the MySQL binlog.
type BinlogInformation struct {
	File     string
	Position uint32
}

// KeyValuePair Represents a key/value pair to be written to storage.
type keyvaluepair struct {
	bucket []byte
	key    []byte
	value  []byte
}

// Open Opens the disk storage.
func (store *BoltDBStore) Open(filename string) error {
	store.writes = make(chan keyvaluepair, 10000000)

	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	var err error
	store.db, err = bolt.Open(filename, 0600, &bolt.Options{
		Timeout: 1 * time.Second,
		// InitialMmapSize: 1000000000,
	})
	if err != nil {
		return err
	}

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

	go store.handleWrites()
	return nil
}

// Close Closes the BoltDB storage.
func (store *BoltDBStore) Close() {
	store.db.Close()
}

// GetBinlogPosition Returns the persisted binlog position.
func (store *BoltDBStore) GetBinlogPosition() (*BinlogInformation, error) {
	binlogInfo := &BinlogInformation{}

	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(systemBucketName)
		file := bucket.Get(binlogFileKey)
		position := bucket.Get(binlogPositionKey)

		if file == nil || position == nil {
			return errors.New("binlog file and position not found")
		}
		binlogInfo.File = string(file)
		binlogInfo.Position = binary.LittleEndian.Uint32(position)
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to open readonly transaction")
	}
	return binlogInfo, nil
}

// SetBinlogPosition Sets and persists the current binlog position.
func (store *BoltDBStore) SetBinlogPosition(binlogInfo *BinlogInformation) error {
	fileBuffer := []byte(binlogInfo.File)
	positionBuffer := make([]byte, 4)
	// var err error

	binary.LittleEndian.PutUint32(positionBuffer, binlogInfo.Position)
	write := keyvaluepair{
		bucket: systemBucketName,
		key:    fileBuffer,
		value:  positionBuffer,
	}
	fmt.Println("SET START")
	store.writes <- write
	fmt.Println("SET END")
	// store.db.Update(func(tx *bolt.Tx) error {
	// 	bucket := tx.Bucket(systemBucketName)
	// 	err = bucket.Put([]byte(binlogFileKey), fileBuffer)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = bucket.Put([]byte(binlogPositionKey), positionBuffer)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	return nil
	// })
	return nil
}

// Commit2 Commits the current transaction.
func (store *BoltDBStore) Commit2() error {
	var err error

	if store.tx != nil {
		// Commit the transaction and check for error.
		if err = store.tx.Commit(); err != nil {
			return err
		}

		// Start a writable transaction.
		store.tx, err = store.db.Begin(true)
		if err != nil {
			return err
		}

		err = store.db.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// Get Gets the value associated with the specified key.
func (store *BoltDBStore) Get(key []byte) ([]byte, error) {
	var value []byte

	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dataBucketName)
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
	write := keyvaluepair{
		bucket: dataBucketName,
		key:    key,
		value:  value,
	}
	store.writes <- write
	return nil

	// store.db.Update(func(tx *bolt.Tx) error {
	// err := store.db.Batch(func(tx *bolt.Tx) error {
	// var err error
	//
	// if store.tx == nil {
	// 	// Start a writable transaction.
	// 	store.tx, err = store.db.Begin(true)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	//
	// bucket := store.tx.Bucket(dataBucketName)
	// err = bucket.Put(key, value)
	// if err != nil {
	// 	return err
	// }
	// return nil
	// })

	// if err != nil {
	// 	return err
	// }
	// return nil
}

func (store *BoltDBStore) handleWrites() {
	var err error

	write := <-store.writes

	fmt.Println("HIT handleWrites !!!!!!!!!!!!!!!!!!!")
	if store.tx == nil {
		// Start a writable transaction.
		store.tx, err = store.db.Begin(true)
		if err != nil {
			// return err
			fmt.Println("HIT 1 !!!!!!!!!!!!!!!!!!!")
		}
	} else if store.tx != nil && store.resetTransaction == true {
		fmt.Println("HIT COMMIT !!!!!!!!!!!!!!!!!!!")
		if err = store.tx.Commit(); err != nil {
			// return err
			fmt.Println("HIT 2 !!!!!!!!!!!!!!!!!!!")
		}

		// Start a writable transaction.
		store.tx, err = store.db.Begin(true)
		if err != nil {
			// return err
			fmt.Println("HIT 3 !!!!!!!!!!!!!!!!!!!")
		}

		fmt.Println("HIT 4 !!!!!!!!!!!!!!!!!!!")
		store.resetTransaction = false

		err = store.db.Sync()
		if err != nil {
			// return err
			fmt.Println("HIT 5 !!!!!!!!!!!!!!!!!!!")
		}
	}

	fmt.Println("HIT 6 !!!!!!!!!!!!!!!!!!!")
	bucket := store.tx.Bucket(write.bucket)
	err = bucket.Put(write.key, write.value)
	if err != nil {
		// return err
		fmt.Println("HIT 7 !!!!!!!!!!!!!!!!!!!")
	}
	fmt.Println("HIT 8 !!!!!!!!!!!!!!!!!!!")
}

// Commit Commits the current transaction.
func (store *BoltDBStore) Commit() {
	store.resetTransaction = true
}
