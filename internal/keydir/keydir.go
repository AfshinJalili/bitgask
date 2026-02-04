package keydir

type Iterator func(key []byte, value interface{}) bool

type Keydir interface {
	Get(key []byte) (interface{}, bool)
	Set(key []byte, value interface{})
	Delete(key []byte)
	Len() int
	Range(fn Iterator)
	Prefix(prefix []byte, fn Iterator)
	Snapshot() Snapshot
}

type Snapshot interface {
	Get(key []byte) (interface{}, bool)
	Len() int
	Range(fn Iterator)
	Prefix(prefix []byte, fn Iterator)
}
