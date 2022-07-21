package bitcask

const (
	TombStone     = "bitcask_tombstone"
	MaxFileSize = 2147483648
)

var DefaultConfig = Config {writePermission: false, syncOnPut: false}