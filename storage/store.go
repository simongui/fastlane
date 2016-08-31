package storage

// Store Represents the storage API's.
type Store interface {
	IsStarted() bool
	Open(filename string) error
	GetBinlogPosition() (*BinlogInformation, error)
	SetBinlogPosition(binlogInfo *BinlogInformation) error
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Commit()
}
