// package bitcask provides an API for storing and retrieving
// key/value data into a log-structured hash table.
package bitcask

import (
	"os"
	"time"
)


type Key string
type Keydir map[Key] Record
type pendingWrites map[Key][]byte

type Record struct {
	fileId string
	valueSize uint
	valuePosition uint
	timeStamp time.Time
}

type Config struct {
	writePermission bool
	syncOnPut bool
}


type BitCask struct {
	activeFile *os.File
	cursor uint
	dirName string
	keydir Keydir
	config Config
}

// Open opens files at directory at path directoryName, and parses
// these files into in-memory structure keydir. if the directory
// doesn't exist at this path, it creates a new directory. 
func Open(directoryPath string, config ...Config) (*BitCask, error){
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

func (bc *BitCask) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (bc *BitCask) Put(key, value []byte) error {
	return nil
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