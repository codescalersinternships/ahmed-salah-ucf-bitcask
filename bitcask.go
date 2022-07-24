// package bitcask provides an API for storing and retrieving
// key/value data into a log-structured hash table.
package bitcask

import (
	"fmt"
	"os"
	"path"
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
	cursor int64
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

	if value, ok := pendingWrites[string(key)]; ok {
		return value, nil

	} else if err := bc.isExist(key); err != nil {
		return nil, err
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
		bc.appendItem(key, value)
	}
	
	return err
}

// Delete appends a special TombStone value, which will be removed
// on the next merge. The key is deleted from keydir.
// returns err == ErrNullKeyOrValue if passed nil key
// err == ErrHasNoWritePerms if calling process has no write perms.
func (bc *BitCask) Delete(key []byte) error {
	if key == nil {
		return ErrNullKeyOrValue
	}

	if !bc.config.writePermission {
		return ErrHasNoWritePerms
	}

	delete(pendingWrites, string(key))

	delete(bc.keydir, string(key))

	bc.appendItem(key, []byte(TombStone))

	return nil
}

// ListKeys lists all the keys in a Bitcask store.
func (bc *BitCask) ListKeys() [][]byte {
	var result [][]byte

	bc.Sync()

	for key := range bc.keydir {
		result = append(result, []byte(key))
	}

	return result
}

func (bc *BitCask) Fold(fn func([]byte, []byte, any) any, acc any) any {
	for key := range bc.keydir {
		value, _ := bc.Get([]byte(key))
		acc = fn([]byte(key), value, acc)
	}

	return acc
}

func (bc *BitCask) Merge() error {
	if !bc.config.writePermission {
		return ErrHasNoWritePerms
	}
	bc.Sync()
	var currentCursor int64 = 0

	oldFiles := make(map[string]void)
	mergeFile := newFile(bc.dirName)

	for key, record := range bc.keydir {
		if record.fileId != bc.activeFile.Name() {
			oldFiles[record.fileId] = member
			tStamp := time.Now()
			value, _ := bc.Get([]byte(key))
			fileItem := bc.makeItem([]byte(key), value, tStamp)

			bc.updateKeydirRecord([]byte(key), value, mergeFile.Name(), currentCursor, tStamp)
			bc.keydir[key] = Record{
				fileId: mergeFile.Name(),
				valueSize: len(value),
				valuePosition: int64(currentCursor + 16 + int64(len(key))),
				timeStamp: tStamp,
			}

			if int64(len(fileItem)) + currentCursor > MaxFileSize {
				mergeFile.Close()
				mergeFile = newFile(bc.dirName)
				currentCursor = 0
			}
			n, _ := mergeFile.Write(fileItem)
			currentCursor += int64(n)
		}
	}
	for oldFile := range oldFiles {
		os.Remove(path.Join(bc.dirName, oldFile))
	}

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
	if bc.config.writePermission {
		bc.Sync()
		bc.Merge()
		bc.activeFile.Close()
	} else {
		os.Remove(path.Join(bc.dirName, keydirFileName))
	}
	return nil
}