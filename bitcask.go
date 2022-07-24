// package bitcask provides an API for storing and retrieving
// key/value data into a log-structured hash table.
package bitcask

import (
	"bytes"
	"fmt"
	"os"
	"sort"
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
		os.MkdirAll(directoryPath, os.ModeDir | UserReadWriteExec)
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
	var n int
	var record Record

	if err := bc.isExist(key); err != nil {
		return nil, err
	}

	if value, ok := pendingWrites[string(key)]; ok {
		return value, nil

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


	bc.updateKeydir(key, value, time.Now())

	var err error
	if !bc.config.syncOnPut {
		if _, ok := pendingWrites[string(key)]; ok {
			pendingWrites[string(key)] = value

		} else {
			err = bc.loadToPendingWrites(key, value)
		}

	} else {
		bc.appendItem(key, value)
	}
	
	return err
}

func (bc *BitCask) Delete(key []byte) error {
	return nil
}

// ListKeys lists all the keys in a Bitcask store.
func (bc *BitCask) ListKeys() [][]byte {
	var result [][]byte

	for key := range bc.keydir {
		result = append(result, []byte(key))
	}

	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i], result[j]) == -1
	})

	return result
}

func (bc *BitCask) Fold(fn func([]byte, []byte), acc []byte) []byte {
	return nil
}

func (bc *BitCask) Merge() error {
	return nil
}

// Sync forces any pending writes to sync to disk.
// It returns err ==  ErrHasNoWritePerms if the calling process
// has no write permissions.
// After the append completes, an in-memory structure called
// ”keydir” is updated.
// When the active file meets a size threshold MaxFileSize,
// it will be closed and a new active file will be created.
func (bc *BitCask) Sync() error {
	if !bc.config.writePermission {
		return ErrHasNoWritePerms
	}

	for key := range pendingWrites {
		bc.appendItem([]byte(key), pendingWrites[key])
		delete(pendingWrites, key)
	}

	return nil
}

func (bc *BitCask) Close() error {
	return nil
}