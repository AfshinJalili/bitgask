# bitgask

Production-grade Bitcask-style log-structured key-value store in Go.

## Features
- Append-only data files
- In-memory keydir using an adaptive radix tree
- CRC integrity checks
- Optional Snappy compression
- Per-entry TTL
- Background compaction plus manual merge
- Single-process lock
- Strong durability by default (fsync per write)
- CLI and a minimal RESP-compatible server

## Install
```bash
go get github.com/AfshinJalili/bitgask
```

## Quickstart (Go API)
```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/AfshinJalili/bitgask"
)

func main() {
	db, err := bitgask.Open("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(val))

	_ = db.PutWithTTL([]byte("temp"), []byte("value"), 10*time.Second)
}
```

## Examples

### Basic CRUD
```go
db, _ := bitgask.Open("./data")
defer db.Close()

_ = db.Put([]byte("k"), []byte("v"))
val, _ := db.Get([]byte("k"))
_ = db.Delete([]byte("k"))
```

### TTL
```go
_ = db.PutWithTTL([]byte("session"), []byte("abc"), 30*time.Second)

val, err := db.Get([]byte("session"))
_ = val
_ = err // ErrExpired when TTL elapses
```

### Safe vs High Throughput
```go
// Safe defaults: fsync on each write (default behavior).
db, _ := bitgask.Open("./data")

// Higher throughput: fsync in the background.
db, _ = bitgask.Open("./data",
	bitgask.WithSyncOnPut(false),
	bitgask.WithSyncOnDelete(false),
	bitgask.WithSyncInterval(1*time.Second),
)
```

### Merge and Reclaimable Space
```go
if _, _, ratio := db.Reclaimable(); ratio > 0.6 {
	_ = db.Merge()
}
```

### Streaming Iteration
```go
_ = db.IterKeys(context.Background(), func(key []byte) bool {
	fmt.Println(string(key))
	return true
})

_ = db.Iter(context.Background(), []byte("user:"), func(key, value []byte, meta bitgask.RecordMeta) bool {
	fmt.Println(string(key), string(value))
	return true
})
```

## Durability
By default each `Put` and `Delete` is fsynced for strong durability. To trade durability for throughput, disable per-write sync and enable periodic syncing with `WithSyncInterval`.

## Compaction
Compaction (merge) rewrites live keys into new data files and removes tombstones and expired entries. It runs in the background and can be triggered manually with `db.Merge()`.

## CLI Quickstart
```bash
# install
GO111MODULE=on go install github.com/AfshinJalili/bitgask/cmd/bitgask@latest

# put/get
bitgask put -dir ./data hello world
bitgask get -dir ./data hello

# stats
bitgask stats -dir ./data
```

## Server Quickstart
```bash
# install
GO111MODULE=on go install github.com/AfshinJalili/bitgask/cmd/bitgaskd@latest

# run RESP-compatible server
bitgaskd -dir ./data -addr 127.0.0.1:6380

# use redis-cli
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
```

## Iteration Modes
`Keys`, `Scan`, `Range`, `Sift`, and `NewIterator` snapshot the keydir and can allocate proportionally to key count.
`IterKeys` and `Iter` stream results with lower memory overhead but hold a read lock while calling the callback.

## Limitations and Non-goals
- Single-process access enforced by a lock file.
- Entire keydir is in memory, so RAM scales with key count.
- No transactions or multi-key atomicity.
- `bitgaskd` is not a full Redis implementation.

## Documentation
- API reference: docs/API.md
- Options and defaults: docs/Options.md
- Operations guide: docs/Operations.md
- Architecture: docs/Architecture.md
- CLI: docs/CLI.md
- Server: docs/Server.md

## License
MIT
