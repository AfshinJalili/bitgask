package main

import (
	"flag"
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/AfshinJalili/bitgask"
	"github.com/tidwall/redcon"
)

// Set via goreleaser ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type serverConfig struct {
	dir           string
	addr          string
	maxKey        int
	maxValue      int
	defaultTTL    time.Duration
	syncWrites    bool
	syncInterval  time.Duration
	mergeInterval time.Duration
	compression   string
	hintFiles     bool
	hintSync      bool
}

func (c *serverConfig) options() []bitgask.Option {
	opts := []bitgask.Option{
		bitgask.WithMaxKeySize(c.maxKey),
		bitgask.WithMaxValueSize(c.maxValue),
		bitgask.WithDefaultTTL(c.defaultTTL),
		bitgask.WithSyncOnPut(c.syncWrites),
		bitgask.WithSyncOnDelete(c.syncWrites),
		bitgask.WithSyncInterval(c.syncInterval),
		bitgask.WithMergeInterval(c.mergeInterval),
		bitgask.WithHintFiles(c.hintFiles),
		bitgask.WithHintFileSync(c.hintSync),
	}
	if c.compression == "none" {
		opts = append(opts, bitgask.WithCompression(bitgask.CompressionNone))
	} else {
		opts = append(opts, bitgask.WithCompression(bitgask.CompressionSnappy))
	}
	return opts
}

func main() {
	cfg := &serverConfig{}
	flag.StringVar(&cfg.dir, "dir", "./data", "database directory")
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:6380", "listen address")
	flag.IntVar(&cfg.maxKey, "max-key", 64, "max key size in bytes")
	flag.IntVar(&cfg.maxValue, "max-value", 64<<10, "max value size in bytes")
	flag.DurationVar(&cfg.defaultTTL, "ttl", 0, "default TTL")
	flag.BoolVar(&cfg.syncWrites, "sync", true, "fsync on writes")
	flag.DurationVar(&cfg.syncInterval, "sync-interval", 0, "sync interval")
	flag.DurationVar(&cfg.mergeInterval, "merge-interval", 10*time.Minute, "merge interval")
	flag.StringVar(&cfg.compression, "compression", "snappy", "compression (snappy|none)")
	flag.BoolVar(&cfg.hintFiles, "hint-files", true, "write hint files")
	flag.BoolVar(&cfg.hintSync, "hint-sync", true, "fsync hint files")
	flag.Parse()

	db, err := bitgask.Open(cfg.dir, cfg.options()...)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	handler := func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) == 0 {
			conn.WriteError("ERR empty command")
			return
		}
		switch strings.ToUpper(string(cmd.Args[0])) {
		case "PING":
			if len(cmd.Args) > 1 {
				conn.WriteBulk(cmd.Args[1])
				return
			}
			conn.WriteString("PONG")
		case "GET":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for GET")
				return
			}
			val, err := db.Get(cmd.Args[1])
			if err != nil {
				if err == bitgask.ErrKeyNotFound || err == bitgask.ErrExpired {
					conn.WriteNull()
					return
				}
				conn.WriteError(err.Error())
				return
			}
			conn.WriteBulk(val)
		case "SET":
			if len(cmd.Args) < 3 {
				conn.WriteError("ERR wrong number of arguments for SET")
				return
			}
			key := cmd.Args[1]
			val := cmd.Args[2]
			var ttl time.Duration
			if len(cmd.Args) > 3 {
				if (len(cmd.Args)-3)%2 != 0 {
					conn.WriteError("ERR syntax error")
					return
				}
				for i := 3; i < len(cmd.Args); i += 2 {
					opt := strings.ToUpper(string(cmd.Args[i]))
					arg := string(cmd.Args[i+1])
					switch opt {
					case "EX":
						sec, err := strconv.ParseInt(arg, 10, 64)
						if err != nil {
							conn.WriteError("ERR invalid EX")
							return
						}
						ttl = time.Duration(sec) * time.Second
					case "PX":
						ms, err := strconv.ParseInt(arg, 10, 64)
						if err != nil {
							conn.WriteError("ERR invalid PX")
							return
						}
						ttl = time.Duration(ms) * time.Millisecond
					default:
						conn.WriteError("ERR unsupported option")
						return
					}
				}
			}
			if ttl > 0 {
				err = db.PutWithTTL(key, val, ttl)
			} else {
				err = db.Put(key, val)
			}
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			conn.WriteString("OK")
		case "DEL":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for DEL")
				return
			}
			deleted := 0
			for i := 1; i < len(cmd.Args); i++ {
				err := db.Delete(cmd.Args[i])
				if err == nil {
					deleted++
					continue
				}
				if err != bitgask.ErrKeyNotFound && err != bitgask.ErrExpired {
					conn.WriteError(err.Error())
					return
				}
			}
			conn.WriteInt(deleted)
		case "EXISTS":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for EXISTS")
				return
			}
			count := 0
			for i := 1; i < len(cmd.Args); i++ {
				ok, err := db.Has(cmd.Args[i])
				if err != nil {
					conn.WriteError(err.Error())
					return
				}
				if ok {
					count++
				}
			}
			conn.WriteInt(count)
		case "KEYS":
			pattern := "*"
			if len(cmd.Args) > 1 {
				pattern = string(cmd.Args[1])
			}
			keys := collectKeys(db, pattern)
			conn.WriteArray(len(keys))
			for _, k := range keys {
				conn.WriteBulk(k)
			}
		case "SCAN":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for SCAN")
				return
			}
			cursor, err := strconv.Atoi(string(cmd.Args[1]))
			if err != nil {
				conn.WriteError("ERR invalid cursor")
				return
			}
			pattern := "*"
			count := 10
			if len(cmd.Args) > 2 {
				if (len(cmd.Args)-2)%2 != 0 {
					conn.WriteError("ERR syntax error")
					return
				}
				for i := 2; i < len(cmd.Args); i += 2 {
					switch strings.ToUpper(string(cmd.Args[i])) {
					case "MATCH":
						pattern = string(cmd.Args[i+1])
					case "COUNT":
						parsed, err := strconv.Atoi(string(cmd.Args[i+1]))
						if err != nil {
							conn.WriteError("ERR invalid COUNT")
							return
						}
						count = parsed
					default:
						conn.WriteError("ERR unsupported option")
						return
					}
				}
			}
			keys := collectKeys(db, pattern)
			if cursor < 0 || cursor >= len(keys) {
				cursor = 0
			}
			end := cursor + count
			next := 0
			if end < len(keys) {
				next = end
			} else {
				end = len(keys)
			}
			batch := keys[cursor:end]
			conn.WriteArray(2)
			conn.WriteBulkString(strconv.Itoa(next))
			conn.WriteArray(len(batch))
			for _, k := range batch {
				conn.WriteBulk(k)
			}
		case "TTL":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for TTL")
				return
			}
			ttl := ttlForKey(db, cmd.Args[1], time.Second)
			conn.WriteInt(ttl)
		case "PTTL":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for PTTL")
				return
			}
			ttl := ttlForKey(db, cmd.Args[1], time.Millisecond)
			conn.WriteInt(ttl)
		case "EXPIRE":
			if len(cmd.Args) < 3 {
				conn.WriteError("ERR wrong number of arguments for EXPIRE")
				return
			}
			sec, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
			if err != nil {
				conn.WriteError("ERR invalid EXPIRE")
				return
			}
			conn.WriteInt(setExpire(db, cmd.Args[1], time.Duration(sec)*time.Second))
		case "PEXPIRE":
			if len(cmd.Args) < 3 {
				conn.WriteError("ERR wrong number of arguments for PEXPIRE")
				return
			}
			ms, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
			if err != nil {
				conn.WriteError("ERR invalid PEXPIRE")
				return
			}
			conn.WriteInt(setExpire(db, cmd.Args[1], time.Duration(ms)*time.Millisecond))
		case "DBSIZE":
			conn.WriteInt(db.Stats().Keys)
		case "INFO":
			stats := db.Stats()
			dead, total, ratio := db.Reclaimable()
			info := fmt.Sprintf("keys:%d\nlast_merge:%d\nbytes_total:%d\nbytes_dead:%d\nreclaimable_ratio:%f\n", stats.Keys, stats.LastMerge.Unix(), total, dead, ratio)
			conn.WriteBulkString(info)
		default:
			conn.WriteError("ERR unknown command")
		}
	}

	accept := func(conn redcon.Conn) bool { return true }
	closed := func(conn redcon.Conn, err error) {}
	log.Printf("bitgaskd listening on %s", cfg.addr)
	if err := redcon.ListenAndServe(cfg.addr, handler, accept, closed); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func collectKeys(db *bitgask.DB, pattern string) [][]byte {
	keys := make([][]byte, 0)
	for key := range db.Keys() {
		if pattern == "*" {
			keys = append(keys, key)
			continue
		}
		match, err := path.Match(pattern, string(key))
		if err != nil {
			continue
		}
		if match {
			keys = append(keys, key)
		}
	}
	return keys
}

func ttlForKey(db *bitgask.DB, key []byte, unit time.Duration) int {
	meta, err := db.Meta(key)
	if err != nil {
		if err == bitgask.ErrKeyNotFound || err == bitgask.ErrExpired {
			return -2
		}
		return -2
	}
	if meta.ExpiresAt.IsZero() {
		return -1
	}
	remaining := time.Until(meta.ExpiresAt)
	if remaining <= 0 {
		return -2
	}
	return int(remaining / unit)
}

func setExpire(db *bitgask.DB, key []byte, ttl time.Duration) int {
	val, err := db.Get(key)
	if err != nil {
		if err == bitgask.ErrKeyNotFound || err == bitgask.ErrExpired {
			return 0
		}
		return 0
	}
	if ttl <= 0 {
		_ = db.Delete(key)
		return 1
	}
	if err := db.PutWithTTL(key, val, ttl); err != nil {
		return 0
	}
	return 1
}
