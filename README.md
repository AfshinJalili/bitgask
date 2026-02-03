# bitgask

Production-grade Bitcask-style log-structured KV store in Go.

## Features
- Append-only data files
- In-memory index + hint files
- CRC integrity
- Optional Snappy compression
- Per-entry TTL
- Background compaction + manual merge
- Single-process lock
- Strong durability by default (fsync per write)

## Install
```bash
go get github.com/AfshinJalili/bitgask
```

## Usage
```go
package main

import (
  "fmt"
  "log"
  "time"

  "github.com/AfshinJalili/bitgask"
)

func main() {
  db, err := bitgask.Open("./data", bitgask.Options{})
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  if err := db.Put([]byte("hello"), []byte("world"), 10*time.Second); err != nil {
    log.Fatal(err)
  }

  val, err := db.Get([]byte("hello"))
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(string(val))
}
```

## Durability
By default, each `Put`/`Delete` is fsynced for strong durability. Set `Options.SyncOnPut=false`
and `Options.SyncInterval` to trade durability for throughput.

## Compaction
Compaction merges live keys into new data files and removes dead/expired/tombstoned records.
It runs automatically on an interval, and can be triggered via `db.Merge()`.

## Record Format
```
| CRC32 (4) | ts_unix_nano (8) | expires_unix_nano (8) |
| key_len (4) | val_len (4) | flags (1) | codec (1) | reserved (2) |
| key (k) | value (v) |
```

## Errors
- `ErrKeyNotFound`
- `ErrClosed`
- `ErrCorrupt`
- `ErrLocked`
- `ErrOversized`
- `ErrExpired`

## License
MIT
