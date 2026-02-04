# API Reference

This document describes the public Go API for `github.com/AfshinJalili/bitgask`.

## Types

### DB
The main database handle. Safe for concurrent use. Writes are serialized internally.

### Txn
Transaction handle created by `db.Transaction()`. Transactions are single-goroutine, provide snapshot isolation and read-your-writes, and commit as a batch. Conflicts use last-commit-wins semantics.

### RecordMeta
```go
type RecordMeta struct {
	ExpiresAt time.Time
	Deleted   bool
	Timestamp time.Time
	FileID    uint32
	Offset    int64
	Size      uint32
}
```
- `ExpiresAt` is zero for non-expiring keys.
- `Deleted` is true for tombstones.
- `FileID`, `Offset`, and `Size` reference the record in the data file.

### Stats
```go
type Stats struct {
	TotalBytes int64
	DeadBytes  int64
	Keys       int
	DataFiles  int
	LastMerge  time.Time
}
```

### Iterator
Snapshot iterator returned by `NewIterator`. It reads values lazily when you call `Next`.

### MergeOptions
```go
type MergeOptions struct {
	Force bool
}
```

### CompressionType
```go
type CompressionType uint8

const (
	CompressionSnappy CompressionType = 0
	CompressionNone   CompressionType = 1
)
```

### Logger
```go
type Logger interface {
	Printf(format string, args ...interface{})
}
```

## Constructors and Lifecycle

### Open
```go
func Open(path string, opts ...Option) (*DB, error)
```
Creates or opens a database at `path`. Acquires a lock and loads the keydir.

### Repair
```go
func Repair(path string, opts ...Option) (*DB, error)
```
Rebuilds from data files and truncates corrupt tails when possible. Rewrites hint files if enabled.

### Validate
```go
func Validate(path string, opts ...Option) (Report, error)
```
Scans data and hint files for corruption without mutating the database.

### Close
```go
func (db *DB) Close() error
```
Closes the database and releases the lock. Subsequent calls return `ErrClosed`.

### Reopen
```go
func (db *DB) Reopen() error
```
Rebuilds the keydir and reopens files. Blocks other operations while running.

### Transaction
```go
func (db *DB) Transaction() *Txn
```
Returns a new transaction snapshot. Transactions are not safe for concurrent use.

## CRUD Operations

### Put
```go
func (db *DB) Put(key, value []byte) error
```
Writes a value. Returns `ErrInvalidKey` for empty keys, `ErrOversized` for large keys/values.

### PutWithTTL
```go
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error
```
Writes a value that expires after `ttl`.

### Get
```go
func (db *DB) Get(key []byte) ([]byte, error)
```
Returns `ErrKeyNotFound` if missing, or `ErrExpired` if the key is expired.

### Meta
```go
func (db *DB) Meta(key []byte) (RecordMeta, error)
```
Returns metadata for a key. Returns `ErrExpired` if expired.

### Delete
```go
func (db *DB) Delete(key []byte) error
```
Writes a tombstone. Returns `ErrKeyNotFound` if the key does not exist.

### Has
```go
func (db *DB) Has(key []byte) (bool, error)
```
Returns `true` if the key exists and is not expired.

## Transaction Operations

### Commit / Discard
```go
func (t *Txn) Commit() error
func (t *Txn) Discard()
```
`Commit` writes the batch and makes changes visible atomically to in-process readers. `Discard` releases the transaction without committing.

### Txn Put / PutWithTTL
```go
func (t *Txn) Put(key, value []byte) error
func (t *Txn) PutWithTTL(key, value []byte, ttl time.Duration) error
```
Queue writes in the transaction. Reads within the transaction see these writes immediately.

### Txn Get / Has / Delete
```go
func (t *Txn) Get(key []byte) ([]byte, error)
func (t *Txn) Has(key []byte) (bool, error)
func (t *Txn) Delete(key []byte) error
```
Reads are served from the transaction cache first, then the snapshot.

### Txn Iteration
```go
func (t *Txn) IterKeys(ctx context.Context, fn func(key []byte) bool) error
func (t *Txn) Iter(ctx context.Context, prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error
func (t *Txn) Scan(prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error
func (t *Txn) Range(start, end []byte, fn func(key, value []byte, meta RecordMeta) bool) error
```
Iterates over the transaction view (snapshot + pending writes). `Range` uses byte-wise ordering.

## Iteration

### Keys
```go
func (db *DB) Keys() <-chan []byte
```
Streams keys on a channel. The channel is closed when iteration completes.

### KeysContext
```go
func (db *DB) KeysContext(ctx context.Context) <-chan []byte
```
Like `Keys` but stops early on context cancellation.

### IterKeys
```go
func (db *DB) IterKeys(ctx context.Context, fn func(key []byte) bool) error
```
Streaming key iteration. The callback runs under a read lock. Avoid calling back into the DB.

### Iter
```go
func (db *DB) Iter(ctx context.Context, prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error
```
Streaming key-value iteration with an optional prefix. The callback runs under a read lock.

### Scan
```go
func (db *DB) Scan(prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error
```
Snapshots the keydir and iterates live keys with the given prefix.

### Range
```go
func (db *DB) Range(start, end []byte, fn func(key, value []byte, meta RecordMeta) bool) error
```
Iterates keys with `start <= key < end` using byte-wise ordering.

### Sift
```go
func (db *DB) Sift(pred func(key []byte, meta RecordMeta) bool, fn func(key, value []byte, meta RecordMeta) bool) error
```
Filters keys with a predicate, then iterates values.

### Fold
```go
func (db *DB) Fold(init any, fn func(any, []byte, []byte, RecordMeta) any) (any, error)
```
Reduces over live records and returns the final accumulator.

### NewIterator
```go
func (db *DB) NewIterator(prefix []byte) (*Iterator, error)
```
Creates a snapshot iterator. Call `Close` when done.

## Maintenance and Operations

### Sync
```go
func (db *DB) Sync() error
```
Forces an fsync of the active data file.

### RunGC
```go
func (db *DB) RunGC() (int, error)
```
Removes expired keys from the keydir. Returns the number of keys removed.

### Merge
```go
func (db *DB) Merge(opts ...MergeOptions) error
```
Compacts the database. Use `MergeOptions{Force: true}` to bypass thresholds.

### CompactIfNeeded
```go
func (db *DB) CompactIfNeeded() (bool, error)
```
Checks thresholds and runs a merge when needed. Returns whether a merge ran.

### Reclaimable
```go
func (db *DB) Reclaimable() (deadBytes int64, totalBytes int64, ratio float64)
```
Reports the amount of dead data that can be reclaimed by merge.

### Backup
```go
func (db *DB) Backup(dst string) error
```
Copies the database directory to `dst`.

### DeleteAll
```go
func (db *DB) DeleteAll() error
```
Deletes all data and resets the database in place.

### Stats
```go
func (db *DB) Stats() Stats
```
Returns current size and activity statistics.

## Errors
- `ErrInvalidKey` for empty keys
- `ErrKeyNotFound` for missing keys
- `ErrExpired` for expired keys
- `ErrOversized` for keys or values larger than configured limits
- `ErrClosed` for closed databases
- `ErrLocked` if the lock file is held by another process
- `ErrCorrupt` for data corruption

## Concurrency Notes
- Reads are concurrent; writes are serialized.
- `Reopen` and `Close` block other operations while running.
- Streaming iterators (`IterKeys`, `Iter`) hold a read lock and should not call back into the DB.

## Example: CRUD + TTL
```go
db, _ := bitgask.Open("./data")
defer db.Close()

_ = db.Put([]byte("k"), []byte("v"))
val, _ := db.Get([]byte("k"))
_ = val

_ = db.PutWithTTL([]byte("temp"), []byte("value"), 5*time.Second)
```

## Example: Streaming Iteration
```go
_ = db.IterKeys(context.Background(), func(key []byte) bool {
	fmt.Println(string(key))
	return true
})
```
