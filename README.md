# bitgask

`bitgask` is a production-oriented Bitcask-style log-structured key-value store for Go.

It provides an append-only storage engine, an in-memory keydir, TTL support, background compaction, strong durability by default, a CLI, and a minimal RESP-compatible server.

## Highlights
- Append-only data files with CRC validation
- In-memory keydir backed by radix/ART-style structures
- Optional Snappy compression
- Per-entry TTL and explicit expiry updates
- Background merge plus manual compaction
- Single-process lock enforcement
- Strong durability by default with configurable sync policy
- Go API, `bitgask` CLI, and `bitgaskd` server

## Requirements
- Go `1.22+`

## Install

### Library
Add the module to your project:

```bash
go get github.com/AfshinJalili/bitgask@latest
```

### CLI
```bash
go install github.com/AfshinJalili/bitgask/cmd/bitgask@latest
```

### Server
```bash
go install github.com/AfshinJalili/bitgask/cmd/bitgaskd@latest
```

## Quick Start

### Go API
```go
package main

import (
	"context"
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

	if err := db.PutWithTTL([]byte("session"), []byte("abc123"), 30*time.Second); err != nil {
		log.Fatal(err)
	}

	val, err := db.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(val))

	err = db.IterKeys(context.Background(), func(key []byte) bool {
		fmt.Println(string(key))
		return true
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

### CLI
```bash
bitgask put -dir ./data hello world
bitgask get -dir ./data hello
bitgask stats -dir ./data
```

### Server
```bash
bitgaskd -dir ./data -addr 127.0.0.1:6380

redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
redis-cli -p 6380 SET session token EX 30
```

## Core Usage

### Basic CRUD
```go
db, err := bitgask.Open("./data")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

if err := db.Put([]byte("k"), []byte("v")); err != nil {
	log.Fatal(err)
}

val, err := db.Get([]byte("k"))
if err != nil {
	log.Fatal(err)
}
fmt.Println(string(val))

if err := db.Delete([]byte("k")); err != nil {
	log.Fatal(err)
}
```

### TTL
```go
if err := db.PutWithTTL([]byte("session"), []byte("abc"), 30*time.Second); err != nil {
	log.Fatal(err)
}

ok, err := db.Expire([]byte("session"), 5*time.Minute)
if err != nil {
	log.Fatal(err)
}
fmt.Println(ok)
```

### Transactions
```go
txn := db.Transaction()
defer txn.Discard()

if err := txn.Put([]byte("user:1"), []byte("alice")); err != nil {
	log.Fatal(err)
}

val, err := txn.Get([]byte("user:1"))
if err != nil {
	log.Fatal(err)
}
fmt.Println(string(val))

if err := txn.Commit(); err != nil {
	log.Fatal(err)
}
```

Transactions are snapshot-based, single-goroutine, and use last-commit-wins conflict behavior. They are batched for commit, but they are not a distributed or cross-process transaction system.

### Iteration
```go
err := db.Iter(context.Background(), []byte("user:"), func(key, value []byte, meta bitgask.RecordMeta) bool {
	fmt.Printf("%s=%s expires=%v\n", key, value, meta.ExpiresAt)
	return true
})
if err != nil {
	log.Fatal(err)
}
```

Use `IterKeys` and `Iter` for lower-overhead streaming callbacks. Use `Keys`, `Scan`, `Range`, `Sift`, `Fold`, and `NewIterator` when snapshot-style traversal is a better fit.

## Durability and Performance

By default, each `Put`, `Delete`, and synchronous transaction commit is fsynced for strong durability.

For higher throughput, you can disable per-write fsync and enable periodic sync:

```go
db, err := bitgask.Open(
	"./data",
	bitgask.WithSyncOnPut(false),
	bitgask.WithSyncOnDelete(false),
	bitgask.WithSyncInterval(time.Second),
)
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

This trades crash durability for write throughput. Use it only when that tradeoff is acceptable.

## Compaction

Compaction rewrites live keys into fresh data files and removes overwritten, expired, and tombstoned entries.

```go
dead, total, ratio := db.Reclaimable()
if total > 0 && ratio > 0.60 {
	if err := db.Merge(); err != nil {
		log.Fatal(err)
	}
}
fmt.Println(dead, total, ratio)
```

You can also call `db.CompactIfNeeded()` or configure background merge behavior with options such as `WithMergeInterval`, `WithMergeMinTotal`, and `WithMergeTriggerRatio`.

## CLI Overview

The `bitgask` CLI supports:
- `get`, `put`, `del`, `exists`
- `keys`, `scan`
- `stats`, `reclaimable`, `gc`, `merge`
- `backup`, `delete-all`, `reopen`
- `validate`, `repair`, `version`

Examples:

```bash
bitgask put -dir ./data -entry-ttl 10s session token
bitgask scan -dir ./data user:
bitgask backup -dir ./data ./backup
bitgask validate -dir ./data
```

## Server Overview

`bitgaskd` is a minimal RESP-compatible server for testing and simple deployments. It is not a full Redis implementation.

Supported commands:
- `PING`
- `GET`, `SET`
- `DEL`, `EXISTS`
- `KEYS`, `SCAN`
- `TTL`, `PTTL`
- `EXPIRE`, `PEXPIRE`
- `DBSIZE`, `INFO`

Notes:
- `SET` supports `EX` and `PX`.
- `SCAN` uses an opaque cursor and bounded per-request traversal.
- `KEYS` still returns a full matching snapshot and is the expensive command.
- There is no authentication, replication, persistence protocol compatibility, or broader Redis feature parity.

## Operational Notes

- Single-process access is enforced with a lock file.
- The keydir is fully resident in memory, so RAM usage scales with key count.
- `Validate` performs offline integrity checks without mutating data.
- `Repair` rebuilds from data files and can recover from certain tail-corruption cases.
- `Backup` now expects a fresh or empty destination directory.

## Documentation

- [docs/API.md](docs/API.md)
- [docs/Options.md](docs/Options.md)
- [docs/Operations.md](docs/Operations.md)
- [docs/Architecture.md](docs/Architecture.md)
- [docs/CLI.md](docs/CLI.md)
- [docs/Server.md](docs/Server.md)

## License

MIT
