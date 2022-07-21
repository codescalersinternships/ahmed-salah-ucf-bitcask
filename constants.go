package bitcask

import "path"

const (
	TombStone     = "bitcask_tombstone"
	MaxFileSize = 2147483648
	MaxPendingSize = 1024
)

var (
	DefaultConfig = Config {writePermission: false, syncOnPut: false}
	RWConfig = Config {writePermission: true, syncOnPut: false}
	syncConfig = Config {writePermission: false, syncOnPut: true}
	RWsyncConfig = Config {writePermission: true, syncOnPut: true}
)

var (
	testBitcaskPath = path.Join("bitcask")
	testKeyDirPath = path.Join("bitcask", "keydir.cask")
	testNoOpenDirPath = path.Join("no_open_directory")
	testFilePath = path.Join("bitcask", "testfile.cask")
)