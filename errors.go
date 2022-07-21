package bitcask

var (
	ErrNullKey = BitCaskError("nil keys can't be allowed")
)

type BitCaskError string

func (e BitCaskError) Error() string {
	return string(e)
}