package storage

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestSetBinlogPosition(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "temp.db")

	store := &BoltDBStore{}
	store.Open(file)

	store.SetBinlogPosition(&BinlogInformation{File: "binlog001.log", Position: 1234567890})
	binlogInfo, err := store.GetBinlogPosition()
	if err != nil || binlogInfo.File != "binlog001.log" || binlogInfo.Position != 1234567890 {
		t.Error("failed")
	}
}
