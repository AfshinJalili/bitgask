package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/AfshinJalili/bitgask"
)

// Set via GoReleaser ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type commonFlags struct {
	dir            string
	maxKey         int
	maxValue       int
	defaultTTL     time.Duration
	syncWrites     bool
	syncInterval   time.Duration
	compression    string
	mergeInterval  time.Duration
	hintFiles      bool
	hintFileSync   bool
	maxDataSize    int64
	fileIDWidth    int
	validateOnOpen bool
}

func (c *commonFlags) add(fs *flag.FlagSet) {
	fs.StringVar(&c.dir, "dir", "./data", "database directory")
	fs.IntVar(&c.maxKey, "max-key", 64, "max key size in bytes")
	fs.IntVar(&c.maxValue, "max-value", 64<<10, "max value size in bytes")
	fs.DurationVar(&c.defaultTTL, "ttl", 0, "default TTL (e.g. 10s, 1m)")
	fs.BoolVar(&c.syncWrites, "sync", true, "fsync on writes")
	fs.DurationVar(&c.syncInterval, "sync-interval", 0, "sync interval for batched fsyncs")
	fs.StringVar(&c.compression, "compression", "snappy", "compression (snappy|none)")
	fs.DurationVar(&c.mergeInterval, "merge-interval", 10*time.Minute, "merge interval")
	fs.BoolVar(&c.hintFiles, "hint-files", true, "write hint files")
	fs.BoolVar(&c.hintFileSync, "hint-sync", true, "fsync hint files")
	fs.Int64Var(&c.maxDataSize, "max-data", 256<<20, "max data file size")
	fs.IntVar(&c.fileIDWidth, "file-id-width", 9, "data file id width")
	fs.BoolVar(&c.validateOnOpen, "validate-on-open", false, "validate data on open")
}

func (c *commonFlags) options() []bitgask.Option {
	opts := []bitgask.Option{
		bitgask.WithMaxKeySize(c.maxKey),
		bitgask.WithMaxValueSize(c.maxValue),
		bitgask.WithDefaultTTL(c.defaultTTL),
		bitgask.WithSyncOnPut(c.syncWrites),
		bitgask.WithSyncOnDelete(c.syncWrites),
		bitgask.WithSyncInterval(c.syncInterval),
		bitgask.WithMergeInterval(c.mergeInterval),
		bitgask.WithHintFiles(c.hintFiles),
		bitgask.WithHintFileSync(c.hintFileSync),
		bitgask.WithMaxDataFileSize(c.maxDataSize),
		bitgask.WithFileIDWidth(c.fileIDWidth),
		bitgask.WithValidateOnOpen(c.validateOnOpen),
	}
	if c.compression == "none" {
		opts = append(opts, bitgask.WithCompression(bitgask.CompressionNone))
	} else {
		opts = append(opts, bitgask.WithCompression(bitgask.CompressionSnappy))
	}
	return opts
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "get":
		err = cmdGet(args)
	case "put":
		err = cmdPut(args)
	case "del":
		err = cmdDel(args)
	case "exists":
		err = cmdExists(args)
	case "keys":
		err = cmdKeys(args)
	case "scan":
		err = cmdScan(args)
	case "stats":
		err = cmdStats(args)
	case "reclaimable":
		err = cmdReclaimable(args)
	case "merge":
		err = cmdMerge(args)
	case "gc":
		err = cmdGC(args)
	case "backup":
		err = cmdBackup(args)
	case "delete-all":
		err = cmdDeleteAll(args)
	case "reopen":
		err = cmdReopen(args)
	case "validate":
		err = cmdValidate(args)
	case "repair":
		err = cmdRepair(args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	exe := filepath.Base(os.Args[0])
	fmt.Fprintf(os.Stderr, "usage: %s <command> [args]\n", exe)
	fmt.Fprintln(os.Stderr, "commands: get, put, del, exists, keys, scan, stats, reclaimable, merge, gc, backup, delete-all, reopen, validate, repair")
}

func openDB(cfg *commonFlags) (*bitgask.DB, error) {
	return bitgask.Open(cfg.dir, cfg.options()...)
}

func cmdGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("get: key required")
	}
	key := []byte(fs.Arg(0))
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	val, err := db.Get(key)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(val)
	return err
}

func cmdPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	ttl := fs.Duration("entry-ttl", 0, "entry TTL (overrides default TTL)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return fmt.Errorf("put: key and value required")
	}
	key := []byte(fs.Arg(0))
	val := []byte(fs.Arg(1))
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if *ttl > 0 {
		return db.PutWithTTL(key, val, *ttl)
	}
	return db.Put(key, val)
}

func cmdDel(args []string) error {
	fs := flag.NewFlagSet("del", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("del: key required")
	}
	key := []byte(fs.Arg(0))
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Delete(key)
}

func cmdExists(args []string) error {
	fs := flag.NewFlagSet("exists", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("exists: key required")
	}
	key := []byte(fs.Arg(0))
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	ok, err := db.Has(key)
	if err != nil {
		return err
	}
	if ok {
		fmt.Fprintln(os.Stdout, "1")
	} else {
		fmt.Fprintln(os.Stdout, "0")
	}
	return nil
}

func cmdKeys(args []string) error {
	fs := flag.NewFlagSet("keys", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	for key := range db.Keys() {
		fmt.Fprintln(os.Stdout, string(key))
	}
	return nil
}

func cmdScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var prefix []byte
	if fs.NArg() > 0 {
		prefix = []byte(fs.Arg(0))
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Scan(prefix, func(key, _ []byte, _ bitgask.RecordMeta) bool {
		fmt.Fprintln(os.Stdout, string(key))
		return true
	})
}

func cmdStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	buf, err := json.MarshalIndent(db.Stats(), "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(buf))
	return nil
}

func cmdReclaimable(args []string) error {
	fs := flag.NewFlagSet("reclaimable", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	dead, total, ratio := db.Reclaimable()
	payload := map[string]interface{}{
		"dead_bytes":  dead,
		"total_bytes": total,
		"ratio":       ratio,
	}
	buf, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(buf))
	return nil
}

func cmdMerge(args []string) error {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	force := fs.Bool("force", false, "force merge")
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if *force {
		return db.Merge(bitgask.MergeOptions{Force: true})
	}
	return db.Merge()
}

func cmdGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	count, err := db.RunGC()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, count)
	return nil
}

func cmdBackup(args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("backup: destination required")
	}
	dst := fs.Arg(0)
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Backup(dst)
}

func cmdDeleteAll(args []string) error {
	fs := flag.NewFlagSet("delete-all", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.DeleteAll()
}

func cmdReopen(args []string) error {
	fs := flag.NewFlagSet("reopen", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Reopen()
}

func cmdValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := bitgask.Validate(cfg.dir, cfg.options()...)
	if err != nil {
		return err
	}
	buf, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(buf))
	return nil
}

func cmdRepair(args []string) error {
	fs := flag.NewFlagSet("repair", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, err := bitgask.Repair(cfg.dir, cfg.options()...)
	if err != nil {
		return err
	}
	return db.Close()
}
