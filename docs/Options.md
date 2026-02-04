# Options and Defaults

All configuration is provided through `Option` functions.

## Option Reference

| Option | Default | Description |
| --- | --- | --- |
| `WithSyncOnPut(bool)` | `true` | Fsync the data file after each `Put`. |
| `WithSyncOnDelete(bool)` | `true` | Fsync the data file after each `Delete`. |
| `WithSyncInterval(time.Duration)` | `0` | Periodic fsync when per-write sync is disabled. `0` disables periodic syncing. |
| `WithDirMode(fs.FileMode)` | `0755` | Directory permissions for data directories. |
| `WithFileMode(fs.FileMode)` | `0644` | File permissions for data and hint files. |
| `WithFileIDWidth(int)` | `9` | Width of numeric data file IDs. |
| `WithMaxDataFileSize(int64)` | `256 MiB` | Maximum size of a single data file before rotation. |
| `WithIndexLoadConcurrency(int)` | `GOMAXPROCS` | Number of files loaded concurrently when building the keydir. |
| `WithMaxKeySize(int)` | `64` | Maximum key size in bytes. |
| `WithMaxValueSize(int)` | `64 KiB` | Maximum value size in bytes. |
| `WithDefaultTTL(time.Duration)` | `0` | Default TTL for new entries. `0` means no expiry unless a per-entry TTL is provided. |
| `WithMergeTriggerRatio(float64)` | `0.6` | Merge trigger ratio (dead bytes / total bytes). |
| `WithMergeMinTotal(int64)` | `64 MiB` | Minimum data size before automatic merge can run. |
| `WithMergeInterval(time.Duration)` | `10m` | Background merge interval. |
| `WithMergeOnOpen(bool)` | `false` | Run merge on `Open` when thresholds are exceeded. |
| `WithMergeConcurrency(int)` | `1` | Merge concurrency setting. |
| `WithMergeMaxFileSize(int64)` | `MaxDataFileSize` | Maximum size of merged data files. |
| `WithHintFiles(bool)` | `true` | Enable hint files for faster open. |
| `WithHintFileSync(bool)` | `true` | Fsync hint files on write when per-write sync is enabled. |
| `WithCompression(CompressionType)` | `CompressionSnappy` | Compression for values. |
| `WithCompressionThreshold(int)` | `256` | Minimum value size before compression is applied. |
| `WithLockTimeout(time.Duration)` | `0` | Lock acquisition timeout. `0` means fail fast if locked. |
| `WithLogger(Logger)` | `noop` | Logger for warning and recovery messages. |
| `WithValidateOnOpen(bool)` | `false` | Scan data files on open and skip hints. |

## Durability Profiles

### Safe Defaults
```go
db, _ := bitgask.Open("./data")
```

### Higher Throughput
```go
db, _ := bitgask.Open("./data",
	bitgask.WithSyncOnPut(false),
	bitgask.WithSyncOnDelete(false),
	bitgask.WithSyncInterval(1*time.Second),
)
```

## Compression
`CompressionSnappy` compresses values larger than `CompressionThreshold`. `CompressionNone` stores values as-is.

## Hint Files
Hints are best-effort and validated on load. If hint files are missing or corrupt, Bitgask falls back to scanning data files.

## Locking
A lock file is acquired in the database root to prevent multiple processes from writing at the same time. Use `WithLockTimeout` to wait for the lock.

## Validate-on-Open
`WithValidateOnOpen(true)` skips hints and scans the data files at startup for integrity verification.
