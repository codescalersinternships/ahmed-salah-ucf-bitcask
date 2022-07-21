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
	fileId *os.File
	valueSize uint
	valuePosition uint
	timeStamp time.Time
}


type BitCask struct {
	activeFile *os.File
	cursor uint
	dirName string
	keydir Keydir
	config Config
}

type Config struct {
	writePermission bool
	syncOnPut bool
}


func (bc *BitCask) Open(directoryName string, config ...Config) {
	
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