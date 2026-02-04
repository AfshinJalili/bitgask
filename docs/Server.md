# Server Guide

`bitgaskd` is a minimal RESP-compatible server built on top of Bitgask. It is useful for testing and simple deployments but is not a full Redis implementation.

## Install
```bash
go install github.com/AfshinJalili/bitgask/cmd/bitgaskd@latest
```

## Run
```bash
bitgaskd -dir ./data -addr 127.0.0.1:6380
```

## Client Example
```bash
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
```

## Server Flags
- `-dir` database directory
- `-addr` listen address (default `127.0.0.1:6380`)
- `-max-key` maximum key size in bytes
- `-max-value` maximum value size in bytes
- `-ttl` default TTL
- `-sync` fsync on writes
- `-sync-interval` background sync interval
- `-merge-interval` background merge interval
- `-compression` `snappy` or `none`
- `-hint-files` enable hint files
- `-hint-sync` fsync hint files

## Supported Commands
- `PING`
- `GET`
- `SET` with `EX` and `PX`
- `DEL`
- `EXISTS`
- `KEYS`
- `SCAN`
- `TTL`, `PTTL`
- `EXPIRE`, `PEXPIRE`
- `DBSIZE`
- `INFO`

## Limitations
- Not a full Redis implementation.
- No authentication or replication.
- `KEYS` and `SCAN` operate on an in-memory snapshot.
- Single-process database lock still applies.
