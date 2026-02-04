# CLI Guide

The `bitgask` CLI provides basic management and query commands for a Bitgask database.

## Install
```bash
go install github.com/AfshinJalili/bitgask/cmd/bitgask@latest
```

## Common Flags
These flags apply to most commands:
- `-dir` database directory (default `./data`)
- `-max-key` maximum key size in bytes
- `-max-value` maximum value size in bytes
- `-ttl` default TTL for new entries
- `-sync` fsync on writes
- `-sync-interval` background sync interval
- `-compression` `snappy` or `none`
- `-merge-interval` background merge interval
- `-hint-files` enable hint files
- `-hint-sync` fsync hint files
- `-max-data` max data file size
- `-file-id-width` data file ID width
- `-validate-on-open` scan data files at open

## Commands

### get
```bash
bitgask get -dir ./data mykey
```

### put
```bash
bitgask put -dir ./data mykey myvalue
bitgask put -dir ./data -entry-ttl 10s mykey myvalue
```

### del
```bash
bitgask del -dir ./data mykey
```

### exists
```bash
bitgask exists -dir ./data mykey
```

### keys
```bash
bitgask keys -dir ./data
```

### scan
```bash
bitgask scan -dir ./data
bitgask scan -dir ./data prefix:
```

### stats
```bash
bitgask stats -dir ./data
```

### reclaimable
```bash
bitgask reclaimable -dir ./data
```

### merge
```bash
bitgask merge -dir ./data
bitgask merge -dir ./data -force
```

### gc
```bash
bitgask gc -dir ./data
```

### backup
```bash
bitgask backup -dir ./data ./backup
```

### delete-all
```bash
bitgask delete-all -dir ./data
```

### reopen
```bash
bitgask reopen -dir ./data
```

### validate
```bash
bitgask validate -dir ./data
```

### repair
```bash
bitgask repair -dir ./data
```
