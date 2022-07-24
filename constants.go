package bitcask

import (
	"os"
	"path"
)

const (
	TombStone                = "bitcask_tombstone"
	keydirFileRecordSeprator = " "
	hintFileName           = "keydir.cask"
	BitCaskFileExtension     = ".cask"
	
	MaxFileSize int64    = 1024
	MaxPendingSize int64 = 1024

	// UserReadOnly      = os.FileMode(0600)
	UserReadWriteExec = os.FileMode(0700)
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
	testNoOpenDirPath = path.Join("no_open_directory")
	testFilePath      = path.Join("bitcask", "testfile.cask")
)

type void struct{}
var member void