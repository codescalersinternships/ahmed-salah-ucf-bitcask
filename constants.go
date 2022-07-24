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
	
	MaxFileSize    = 2147483648
	MaxPendingSize = 1024

	UserReadOnly      = os.FileMode(0400)
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
	tetsListKeyBitcaskPath   = path.Join("bitcaskList")
	testKeyDirPath    = path.Join("bitcask", "keydir.cask")
	testNoOpenDirPath = path.Join("no_open_directory")
	testFilePath      = path.Join("bitcask", "testfile.cask")
)