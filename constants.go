package bitcask

import (
	"os"
	"path"
)

const (
	TombStone                = "bitcask_tombstone"
	keydirFileRecordSeprator = " "
	keydirFileName           = "keydir.cask"
	BitCaskFileExtension     = ".cask"
	
	MaxFileSize int64    = 1024

	UserReadWrite      = os.FileMode(0666)
	UserReadWriteExec = os.FileMode(0777)
	NoPermissions     = os.FileMode(0000)
)

var (
	DefaultConfig = Config {writePermission: false, syncOnPut: false}
	RWConfig      = Config {writePermission: true, syncOnPut: false}
	syncConfig    = Config {writePermission: false, syncOnPut: true}
	RWsyncConfig  = Config {writePermission: true, syncOnPut: true}
)

var (
	testBitcaskPath   = path.Join("bitcask")
	tetsListKeyBitcaskPath   = path.Join("bitcask_list")
	testBitcaskMergePath   = path.Join("bitcask_merge")
	testKeyDirPath    = path.Join("bitcask", "keydir.cask")
	testFilePath      = path.Join("bitcask", "testfile.cask")
)

type void struct{}
var member void

const (
	reader      processAccess = 0
    writer      processAccess = 1
    noProcess   processAccess = 2

	readLock = ".readlock"
    writeLock = ".writelock"
)