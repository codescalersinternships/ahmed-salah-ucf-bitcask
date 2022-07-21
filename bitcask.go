// package bitcask provides an API for storing and retrieving
// key/value data into a log-structured hash table.
package bitcask

import (
	"fmt"
	"os"
	"time"
)


type Keydir map[string] Record

var pendingWrites map[string][]byte

type Record struct {
	fileId string
	valueSize int
	valuePosition int64
	timeStamp time.Time
}

type Config struct {
	writePermission bool
	syncOnPut bool
}


type BitCask struct {
	activeFile *os.File
	cursor int
	dirName string
	keydir Keydir
	config Config
}

// Open opens files at directory at path directoryName, and parses
// these files into in-memory structure keydir. if the directory
// doesn't exist at this path, it creates a new directory. 
func Open(directoryPath string, config ...Config) (*BitCask, error) {
	if _, err := os.Stat(directoryPath); os.IsNotExist(err) {
		err = os.MkdirAll(directoryPath, 0700)
		if err != nil {
			return nil, err
		}
	}

	// build bitcask
	bc, err := new(directoryPath, config)

	return bc, err
}

// Get retrieves a value by key from a bitcask data store.
// returns err == ErrNullKey if key has nil value
// err == *pathError if can't open file
// err == io.EOF if can't read the complete value from file
func (bc *BitCask) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNullKeyOrValue
	}
	var data []byte
	var ok bool
	var n int
	var record Record

	if _, ok = pendingWrites[string(key)]; ok {
		return pendingWrites[string(key)], nil
	} else {
		record = bc.keydir[string(key)]
		data = make([]byte, record.valueSize)
		file, err := os.Open(record.fileId)
		if err != nil {
			return nil, fmt.Errorf("can't open file: " + record.fileId)
		}
		n, err = file.ReadAt(data, int64(record.valuePosition))
		if err != nil {
			return nil, fmt.Errorf("read only " + fmt.Sprintf("%d", n) + " bytes out of " +
							fmt.Sprintf("%d", record.valueSize))
		}

		return data, nil
	}
}

// Put store a key and value in a bitcask datastore
// 		sync the write if sync_on_put option is enabled.
func (bc *BitCask) Put(key, value []byte) error {
	if key == nil || value == nil {
		return ErrNullKeyOrValue
	}

	if !bc.config.writePermission {
		return ErrHasNoWritePerms
	}

	var err error
	if !bc.config.syncOnPut {
		if _, ok := pendingWrites[string(key)]; ok {
			pendingWrites[string(key)] = value

		} else {
			err = bc.loadToPendingWrites(key, value)
		}

	} else {
		err = bc.appendItem(key, value)
	}
	
	return err
}

func (bc *BitCask) Delete(key []byte) error {
	return nil
}

func (bc *BitCask) ListKeys() []string {
	return nil
}

func (bc *BitCask) Fold(fn func([]byte, []byte), acc []byte) []byte {
	return nil
}

func (bc *BitCask) Merge() error {
	return nil
}

func (bc *BitCask) Sync() error {
	return nil
}

func (bc *BitCask) Close() error {
	return nil
}