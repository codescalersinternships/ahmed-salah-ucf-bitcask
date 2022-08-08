// package bitcask provides an API for storing and retrieving
// key/value data into a log-structured hash table.
package bitcask

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"time"
)

// processAccess is a type for determining the access permission of existing process.
type processAccess int


type Keydir map[string] Record

type Record struct {
	fileId string
	valueSize int
	valuePosition int64
	timeStamp time.Time
}

// Config contains the data for configuration options that the
// user passes to Open function.
type Config struct {
	writePermission bool
	syncOnPut bool
}

// Bitcask contains the data needed to manipulate the bitcask datastore.
// user creates an object of it to use the bitcask.
type BitCask struct {
	activeFile *os.File
	lock string
	cursor int64
	dirName string
	keydir Keydir
	config Config
	pendingWrites map[string][]byte
}


// Open opens files at directory at path directoryName, and parses
// these files into in-memory structure keydir. if the directory
// doesn't exist at this path, it creates a new directory. 
func Open(directoryPath string, config ...Config) (*BitCask, error) {
	if err := os.MkdirAll(directoryPath, os.ModeDir | UserReadWriteExec); err != nil {
		return nil, err
	}

	var opts Config
	var lock string
	if config == nil {
		opts = DefaultConfig
	} else {
		opts = config[0]
	}

	lockType := checkLock(directoryPath)
	if  lockType == writer {
		return nil, ErrBitCaskIsLocked
	} else if lockType == reader {
		if opts.writePermission {
			return nil, ErrBitCaskIsLocked
		}
	} else {
		if !opts.writePermission {
			lock = readLock + strconv.Itoa(int(time.Now().UnixMicro()))
		} else {
			lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
		}

		lockFile, _ := os.OpenFile(path.Join(directoryPath, lock), os.O_CREATE, 0600)
		lockFile.Close()
	}
	// build bitcask
	bc := new(directoryPath, opts, lock)

	return bc, nil
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

	if value, ok := bc.pendingWrites[string(key)]; ok {
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
		
		n, err = file.ReadAt(data, record.valuePosition)
		if err != nil {
			return nil, fmt.Errorf("read only " + fmt.Sprintf("%d", n) + " bytes out of " +
							fmt.Sprintf("%d", record.valueSize))
		}
		file.Close()
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
		if _, ok := bc.pendingWrites[string(key)]; ok {
			bc.pendingWrites[string(key)] = value

		} else {
			bc.loadToPendingWrites(key, value)
		}

	} else {
		item := bc.makeItem(key, value, bc.keydir[string(key)].timeStamp)
		bc.updateKeydirRecord(key, value, bc.activeFile.Name(), int64(bc.cursor), time.Now())
		bc.appendItemToFile(item, &bc.cursor, &bc.activeFile)
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

	delete(bc.pendingWrites, string(key))

	delete(bc.keydir, string(key))

	item := bc.makeItem(key, []byte(TombStone), bc.keydir[string(key)].timeStamp)
	bc.appendItemToFile(item, &bc.cursor, &bc.activeFile)

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

// Fold folds over all key/value pairs in a bitcask datastore.
// fn is expected to be closure in the form: F(K, V, Acc) -> Acc
func (bc *BitCask) Fold(fn func([]byte, []byte, any) any, acc any) any {
	for key := range bc.keydir {
		value, _ := bc.Get([]byte(key))
		acc = fn([]byte(key), value, acc)
	}

	return acc
}

// Merge merges several data files within a Bitcask datastore into
// a more compact form and deletes old files.
// All pending writes are synced to disk before merge happens.
// returns err == ErrHasNoWritePerms if the calling process has no
// write permissions.
func (bc *BitCask) Merge() error {
	if !bc.config.writePermission {
		return ErrHasNoWritePerms
	}
	
	var currentCursorPos int64 = 0
	newFilesSet := make(map[string]void)
	mergeFile := newFile(bc.dirName)
	var newKeydir Keydir = make(Keydir)

	bc.Sync()

	for key, record := range bc.keydir {
		if record.fileId != bc.activeFile.Name() {
			tStamp := time.Now()
			value, _ := bc.Get([]byte(key))
			
			fileItem := bc.makeItem([]byte(key), value, tStamp)
			itemBegin := bc.appendItemToFile(fileItem, &currentCursorPos, &mergeFile)
			newFilesSet[mergeFile.Name()] = member
			newKeydir[key] = Record {
				fileId: mergeFile.Name(),
				valueSize: len(value),
				valuePosition: int64(itemBegin + int64(16) + int64(len(key))),
				timeStamp: tStamp,
			}
		} else {
			newKeydir[key] = bc.keydir[key]
			newFilesSet[record.fileId] = member
		}
	}
	bc.keydir = newKeydir

	bc.deleteOldFiles(newFilesSet)

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

	for key := range bc.pendingWrites {
		item := bc.makeItem([]byte(key), bc.pendingWrites[key], bc.keydir[string(key)].timeStamp)
		bc.updateKeydirRecord([]byte(key), bc.pendingWrites[key], bc.activeFile.Name(), int64(bc.cursor), time.Now())
		bc.appendItemToFile(item, &bc.cursor, &bc.activeFile)
		delete(bc.pendingWrites, key)
	}

	return nil
}

// Close flushes all pending writes into disk, merges old files,
// removes read/write locks, builds keydir file and closes the bitcask datastore.
func (bc *BitCask) Close() {
	if bc.config.writePermission {
		bc.Sync()
		bc.activeFile.Close()
		bc.Merge()
		bc.buildKeydirFile()
	}

	os.Remove(path.Join(bc.dirName, bc.lock))
}