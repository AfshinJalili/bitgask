# Operations Guide

This guide covers storage layout, durability, compaction, and operational concerns.

## Data Layout
Database root:
- `data/` contains data and hint files
- `LOCK` is the process lock file
- `data/bitgask.meta` contains metadata

Data directory examples:
- `data/000000001.data`
- `data/000000001.hint`

During merge you may temporarily see:
- `merge.tmp/` (new files being built)
- `data.old/` (old data after rename)

## Record Format
Each record is encoded as:
```
| CRC32 (4) | ts_unix_nano (8) | expires_unix_nano (8) |
| key_len (4) | val_len (4) | flags (1) | codec (1) | reserved (2) |
| key (k) | value (v) |
```
CRC32 covers the header plus key and value (excluding the CRC field).

## TTL and Expiration
- `PutWithTTL` stores an expiration timestamp.
- Expired keys are treated as missing on read.
- Expired keys are removed from the in-memory index on access or via `RunGC`.
- Merge drops expired entries.

## Durability
- By default, every `Put` and `Delete` is fsynced.
- For higher throughput, disable per-write sync and set `WithSyncInterval`.
- `Sync()` can be used to manually flush the active file.

## Compaction and Merge
- Merge rewrites live records into new data files and removes tombstones and expired entries.
- Background merge runs on `MergeInterval` and triggers when `dead/total >= MergeTriggerRatio` and `total >= MergeMinTotal`.
- `Merge(MergeOptions{Force: true})` forces a merge.
- `Reclaimable()` reports dead bytes and the reclaimable ratio.
- Merge uses `merge.tmp` and atomic directory renames to swap in new data.

## Backup and Restore
- `Backup(dst)` copies the database directory to `dst`.
- To restore, open the backup directory with `Open`.

## Validate and Repair
- `Validate(path, ...)` scans data and hint files, returning a report without mutating the DB.
- `Repair(path, ...)` rebuilds the keydir from data files and truncates corrupt tails when possible.

## Operational Caveats
- Single-process access only. A lock file prevents multiple writers.
- The keydir lives in memory, so RAM usage grows with key count.
- Large values increase IO and merge cost.
- Network filesystems are not tested; local disks are recommended.
