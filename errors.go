package bitcask

var (
	ErrNullKeyOrValue = BitCaskError("nil keys can't be allowed")
	ErrHasNoWritePerms = BitCaskError("you don't have write permissions")
	ErrKeyNotExist = BitCaskError("key doesn't exist")
	ErrBitCaskIsLocked = BitCaskError("there is another process that locked this bitcask")
)

type BitCaskError string

func (e BitCaskError) Error() string {
	return string(e)
}