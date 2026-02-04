package bitgask

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/AfshinJalili/bitgask/internal/codec"
	"github.com/AfshinJalili/bitgask/internal/file"
	"github.com/AfshinJalili/bitgask/internal/index"
	"github.com/AfshinJalili/bitgask/internal/keydir"
	"github.com/AfshinJalili/bitgask/internal/record"
)

const (
	metaFileName = "bitgask.meta"
	lockFileName = "LOCK"
)

type MergeOptions struct {
	Force bool
}

type DB struct {
	dir     string
	dataDir string
	opts    config

	lock *file.Lock

	opMu      sync.RWMutex
	mu        sync.RWMutex
	writeMu   sync.Mutex
	index     keydir.Keydir
	dataFiles map[uint32]*os.File
	active    *dataFile
	stats     Stats
	closed    bool

	mergeMu   sync.Mutex
	closeOnce sync.Once
	mergeStop chan struct{}
	mergeDone chan struct{}
	syncStop  chan struct{}
	syncDone  chan struct{}
}

type dataFile struct {
	id   uint32
	file *os.File
	hint *os.File
	size int64
}

func Open(path string, opts ...Option) (*DB, error) {
	return openDB(path, opts, false)
}

func Repair(path string, opts ...Option) (*DB, error) {
	return openDB(path, opts, true)
}

func openDB(path string, opts []Option, repair bool) (*DB, error) {
	cfg := applyOptions(opts)
	if path == "" {
		return nil, fmt.Errorf("bitgask: path required")
	}
	if err := os.MkdirAll(path, cfg.DirMode); err != nil {
		return nil, err
	}
	dataDir := filepath.Join(path, "data")
	if err := os.MkdirAll(dataDir, cfg.DirMode); err != nil {
		return nil, err
	}
	lock, err := file.AcquireLock(filepath.Join(path, lockFileName), cfg.LockTimeout)
	if err != nil {
		return nil, ErrLocked
	}

	if err := writeMeta(dataDir, cfg); err != nil {
		_ = lock.Release()
		return nil, err
	}

	var (
		idx    keydir.Keydir
		stats  Stats
		lastID uint32
	)
	if repair {
		idx, stats, lastID, err = loadFromData(dataDir, cfg, true, cfg.UseHintFiles)
	} else if cfg.ValidateOnOpen {
		idx, stats, lastID, err = loadFromData(dataDir, cfg, false, false)
	} else if cfg.UseHintFiles {
		idx, stats, lastID, err = loadFromHints(dataDir, cfg)
		if err != nil {
			idx, stats, lastID, err = loadFromData(dataDir, cfg, false, false)
		}
	} else {
		idx, stats, lastID, err = loadFromData(dataDir, cfg, false, false)
	}
	if err != nil {
		_ = lock.Release()
		return nil, err
	}

	if lastID == 0 {
		lastID = 1
	}

	active, err := openActiveFile(dataDir, lastID, cfg)
	if err != nil {
		_ = lock.Release()
		return nil, err
	}
	if stats.DataFiles == 0 {
		stats.DataFiles = 1
	}

	db := &DB{
		dir:       path,
		dataDir:   dataDir,
		opts:      cfg,
		lock:      lock,
		index:     idx,
		dataFiles: map[uint32]*os.File{active.id: active.file},
		active:    active,
		stats:     stats,
		mergeStop: make(chan struct{}),
		mergeDone: make(chan struct{}),
		syncStop:  make(chan struct{}),
		syncDone:  make(chan struct{}),
	}

	if cfg.MergeOnOpen {
		_, _ = db.CompactIfNeeded()
	}

	go db.mergeLoop()
	if cfg.SyncInterval > 0 {
		go db.syncLoop()
	} else {
		close(db.syncDone)
	}
	return db, nil
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.opMu.Lock()
	defer db.opMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.mu.Unlock()

	db.closeOnce.Do(func() {
		// stop loops
		close(db.mergeStop)
		<-db.mergeDone
		if db.opts.SyncInterval > 0 {
			close(db.syncStop)
			<-db.syncDone
		}
	})

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	defer func() {
		if db.lock != nil {
			_ = db.lock.Release()
		}
	}()

	db.mu.Lock()
	for _, f := range db.dataFiles {
		_ = f.Close()
	}
	if db.active != nil && db.active.hint != nil {
		_ = db.active.hint.Close()
	}
	db.mu.Unlock()
	return nil
}

func (db *DB) Put(key, value []byte) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	return db.putWithTTL(key, value, db.opts.DefaultTTL)
}

func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	return db.putWithTTL(key, value, ttl)
}

func (db *DB) putWithTTL(key, value []byte, ttlVal time.Duration) error {
	if db == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > db.opts.MaxKeySize || len(value) > db.opts.MaxValueSize {
		return ErrOversized
	}
	now := time.Now()
	var expiresAt int64
	if ttlVal > 0 {
		expiresAt = now.Add(ttlVal).UnixNano()
	}

	flags := uint8(0)
	codecID := uint8(0)
	encodedValue := value
	if db.opts.Compression == CompressionSnappy && len(value) >= db.opts.CompressionThreshold {
		enc, err := codec.Encode(codec.Snappy, value)
		if err != nil {
			return err
		}
		encodedValue = enc
		flags |= record.FlagCompressed
		codecID = uint8(codec.Snappy)
	}

	rec := record.Record{
		Timestamp: now.UnixNano(),
		ExpiresAt: expiresAt,
		Key:       key,
		Value:     encodedValue,
		Flags:     flags,
		Codec:     codecID,
	}
	buf, err := record.Encode(rec)
	if err != nil {
		return err
	}
	size := uint32(len(buf))

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}

	if err := db.rotateIfNeeded(int64(size)); err != nil {
		return err
	}
	offset := db.active.size
	if _, err := db.active.file.Write(buf); err != nil {
		return err
	}
	db.active.size += int64(size)

	if db.opts.UseHintFiles && db.active.hint != nil {
		hintEntry := index.HintEntry{
			Key:     append([]byte(nil), key...),
			FileID:  db.active.id,
			Offset:  offset,
			Size:    size,
			TS:      rec.Timestamp,
			Expires: rec.ExpiresAt,
			Flags:   flags,
		}
		if err := index.WriteHint(db.active.hint, hintEntry); err != nil {
			logf(db.opts.Logger, "bitgask: hint write failed: %v", err)
		} else if db.opts.HintFileSync && db.opts.SyncOnPut {
			if err := db.active.hint.Sync(); err != nil {
				logf(db.opts.Logger, "bitgask: hint sync failed: %v", err)
			}
		}
	}
	if db.opts.SyncOnPut {
		_ = db.active.file.Sync()
	}

	meta := RecordMeta{
		ExpiresAt: timeFromUnixNano(expiresAt),
		Deleted:   false,
		Timestamp: timeFromUnixNano(rec.Timestamp),
		FileID:    db.active.id,
		Offset:    offset,
		Size:      size,
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	old, ok := db.indexGet(key)
	if ok && isExpired(old, now) {
		// remove expired old record
		db.indexDelete(key)
		if !old.Deleted {
			db.stats.Keys--
		}
		db.stats.DeadBytes += int64(old.Size)
		ok = false
	}
	if ok && !old.Deleted {
		db.stats.DeadBytes += int64(old.Size)
	}
	db.stats.TotalBytes += int64(size)
	if !ok || old.Deleted {
		db.stats.Keys++
	}
	db.indexSet(key, meta)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	meta, ok := db.indexGet(key)
	db.mu.RUnlock()
	if !ok || meta.Deleted {
		return nil, ErrKeyNotFound
	}
	if isExpired(meta, now) {
		db.expireKeys([][]byte{key})
		return nil, ErrExpired
	}
	val, err := db.readValue(meta)
	if err != nil {
		if errors.Is(err, ErrExpired) {
			db.expireKeys([][]byte{key})
		}
		return nil, err
	}
	return val, nil
}

func (db *DB) Meta(key []byte) (RecordMeta, error) {
	if db == nil {
		return RecordMeta{}, ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	if len(key) == 0 {
		return RecordMeta{}, ErrInvalidKey
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return RecordMeta{}, ErrClosed
	}
	meta, ok := db.indexGet(key)
	db.mu.RUnlock()
	if !ok || meta.Deleted {
		return RecordMeta{}, ErrKeyNotFound
	}
	if isExpired(meta, now) {
		db.expireKeys([][]byte{key})
		return RecordMeta{}, ErrExpired
	}
	return meta, nil
}

func (db *DB) Delete(key []byte) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > db.opts.MaxKeySize {
		return ErrOversized
	}

	now := time.Now()
	db.mu.RLock()
	meta, ok := db.indexGet(key)
	db.mu.RUnlock()
	if !ok || meta.Deleted || isExpired(meta, now) {
		if ok && isExpired(meta, now) {
			db.expireKeys([][]byte{key})
		}
		return ErrKeyNotFound
	}

	rec := record.Record{
		Timestamp: now.UnixNano(),
		ExpiresAt: 0,
		Key:       key,
		Value:     nil,
		Flags:     record.FlagDeleted,
		Codec:     0,
	}
	buf, err := record.Encode(rec)
	if err != nil {
		return err
	}
	size := uint32(len(buf))

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if err := db.rotateIfNeeded(int64(size)); err != nil {
		return err
	}
	offset := db.active.size
	if _, err := db.active.file.Write(buf); err != nil {
		return err
	}
	db.active.size += int64(size)

	if db.opts.UseHintFiles && db.active.hint != nil {
		hintEntry := index.HintEntry{
			Key:     append([]byte(nil), key...),
			FileID:  db.active.id,
			Offset:  offset,
			Size:    size,
			TS:      rec.Timestamp,
			Expires: rec.ExpiresAt,
			Flags:   rec.Flags,
		}
		if err := index.WriteHint(db.active.hint, hintEntry); err != nil {
			logf(db.opts.Logger, "bitgask: hint write failed: %v", err)
		} else if db.opts.HintFileSync && db.opts.SyncOnDelete {
			if err := db.active.hint.Sync(); err != nil {
				logf(db.opts.Logger, "bitgask: hint sync failed: %v", err)
			}
		}
	}
	if db.opts.SyncOnDelete {
		_ = db.active.file.Sync()
	}

	deletedMeta := RecordMeta{
		ExpiresAt: time.Time{},
		Deleted:   true,
		Timestamp: timeFromUnixNano(rec.Timestamp),
		FileID:    db.active.id,
		Offset:    offset,
		Size:      size,
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// old is live value by earlier check
	db.stats.DeadBytes += int64(meta.Size) // old value becomes dead
	db.stats.DeadBytes += int64(size)      // tombstone is dead data
	db.stats.TotalBytes += int64(size)
	db.stats.Keys--
	db.indexSet(key, deletedMeta)
	return nil
}

func (db *DB) Has(key []byte) (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	if len(key) == 0 {
		return false, ErrInvalidKey
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return false, ErrClosed
	}
	meta, ok := db.indexGet(key)
	db.mu.RUnlock()
	if !ok || meta.Deleted {
		return false, nil
	}
	if isExpired(meta, now) {
		db.expireKeys([][]byte{key})
		return false, nil
	}
	return true, nil
}

func (db *DB) Keys() <-chan []byte {
	return db.KeysContext(context.Background())
}

func (db *DB) KeysContext(ctx context.Context) <-chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		if db == nil {
			return
		}
		db.opMu.RLock()
		defer db.opMu.RUnlock()
		entries, expired, err := db.snapshotEntries(nil, nil, nil, nil)
		if err != nil {
			return
		}
		if len(expired) > 0 {
			db.expireKeys(expired)
		}
		for _, entry := range entries {
			select {
			case <-ctx.Done():
				return
			case ch <- append([]byte(nil), entry.Key...):
			}
		}
	}()
	return ch
}

func (db *DB) IterKeys(ctx context.Context, fn func(key []byte) bool) error {
	if db == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	expired := make([][]byte, 0)
	var iterErr error
	db.forEachIndex(nil, func(key []byte, meta RecordMeta) bool {
		if meta.Deleted {
			return true
		}
		if isExpired(meta, now) {
			expired = append(expired, append([]byte(nil), key...))
			return true
		}
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		if !fn(append([]byte(nil), key...)) {
			return false
		}
		return true
	})
	db.mu.RUnlock()
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	return iterErr
}

func (db *DB) Iter(ctx context.Context, prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error {
	if db == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()

	now := time.Now()
	fileIDs := make(map[uint32]struct{})
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	db.forEachIndex(prefix, func(key []byte, meta RecordMeta) bool {
		if meta.Deleted || isExpired(meta, now) {
			return true
		}
		fileIDs[meta.FileID] = struct{}{}
		return true
	})
	db.mu.RUnlock()
	for id := range fileIDs {
		if _, err := db.openDataFile(id); err != nil {
			return err
		}
	}

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	expired := make([][]byte, 0)
	var iterErr error
	db.forEachIndex(prefix, func(key []byte, meta RecordMeta) bool {
		if meta.Deleted {
			return true
		}
		if isExpired(meta, now) {
			expired = append(expired, append([]byte(nil), key...))
			return true
		}
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		val, err := db.readValue(meta)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				return true
			}
			iterErr = err
			return false
		}
		if !fn(append([]byte(nil), key...), val, meta) {
			return false
		}
		return true
	})
	db.mu.RUnlock()
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	return iterErr
}

func (db *DB) Scan(prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	entries, expired, err := db.snapshotEntries(prefix, nil, nil, nil)
	if err != nil {
		return err
	}
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	for _, entry := range entries {
		val, err := db.readValue(entry.Meta)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return err
		}
		if !fn(append([]byte(nil), entry.Key...), val, entry.Meta) {
			return nil
		}
	}
	return nil
}

func (db *DB) Range(start, end []byte, fn func(key, value []byte, meta RecordMeta) bool) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	entries, expired, err := db.snapshotEntries(nil, start, end, nil)
	if err != nil {
		return err
	}
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	for _, entry := range entries {
		val, err := db.readValue(entry.Meta)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return err
		}
		if !fn(append([]byte(nil), entry.Key...), val, entry.Meta) {
			return nil
		}
	}
	return nil
}

func (db *DB) Sift(pred func(key []byte, meta RecordMeta) bool, fn func(key, value []byte, meta RecordMeta) bool) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	entries, expired, err := db.snapshotEntries(nil, nil, nil, pred)
	if err != nil {
		return err
	}
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	for _, entry := range entries {
		val, err := db.readValue(entry.Meta)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return err
		}
		if !fn(append([]byte(nil), entry.Key...), val, entry.Meta) {
			return nil
		}
	}
	return nil
}

func (db *DB) Fold(init any, fn func(any, []byte, []byte, RecordMeta) any) (any, error) {
	if db == nil {
		return init, ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	acc := init
	err := db.Scan(nil, func(key, value []byte, meta RecordMeta) bool {
		acc = fn(acc, key, value, meta)
		return true
	})
	return acc, err
}

func (db *DB) Sync() error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	return db.syncInternal()
}

func (db *DB) syncInternal() error {
	if db == nil {
		return ErrClosed
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}
	return db.syncLocked()
}

func (db *DB) syncLocked() error {
	if db.active == nil {
		return nil
	}
	if err := db.active.file.Sync(); err != nil {
		return err
	}
	if db.opts.UseHintFiles && db.active.hint != nil && db.opts.HintFileSync {
		if err := db.active.hint.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) RunGC() (int, error) {
	if db == nil {
		return 0, ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	_, expired, err := db.snapshotEntries(nil, nil, nil, nil)
	if err != nil {
		return 0, err
	}
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	return len(expired), nil
}

func (db *DB) Reopen() error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.Lock()
	defer db.opMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}

	db.stopBackground()

	db.mu.Lock()
	for _, f := range db.dataFiles {
		_ = f.Close()
	}
	if db.active != nil && db.active.hint != nil {
		_ = db.active.hint.Close()
	}
	db.mu.Unlock()

	var (
		idx    keydir.Keydir
		stats  Stats
		lastID uint32
		err    error
	)
	if db.opts.ValidateOnOpen {
		idx, stats, lastID, err = loadFromData(db.dataDir, db.opts, false, false)
	} else if db.opts.UseHintFiles {
		idx, stats, lastID, err = loadFromHints(db.dataDir, db.opts)
		if err != nil {
			idx, stats, lastID, err = loadFromData(db.dataDir, db.opts, false, false)
		}
	} else {
		idx, stats, lastID, err = loadFromData(db.dataDir, db.opts, false, false)
	}
	if err != nil {
		db.startBackground()
		return err
	}
	if lastID == 0 {
		lastID = 1
	}
	active, err := openActiveFile(db.dataDir, lastID, db.opts)
	if err != nil {
		db.startBackground()
		return err
	}
	if stats.DataFiles == 0 {
		stats.DataFiles = 1
	}

	db.mu.Lock()
	db.index = idx
	db.dataFiles = map[uint32]*os.File{active.id: active.file}
	db.active = active
	db.stats = stats
	db.mu.Unlock()

	db.startBackground()
	return nil
}

func (db *DB) Backup(dst string) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if err := db.syncLocked(); err != nil {
		return err
	}
	if err := os.MkdirAll(dst, db.opts.DirMode); err != nil {
		return err
	}
	dstData := filepath.Join(dst, "data")
	if err := os.MkdirAll(dstData, db.opts.DirMode); err != nil {
		return err
	}
	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !file.IsDataFile(name) && !file.IsHintFile(name) && name != metaFileName {
			continue
		}
		srcPath := filepath.Join(db.dataDir, name)
		dstPath := filepath.Join(dstData, name)
		if err := linkOrCopy(srcPath, dstPath, db.opts.FileMode); err != nil {
			return err
		}
	}
	if err := file.FsyncDir(dstData); err != nil {
		logf(db.opts.Logger, "bitgask: fsync backup data dir failed: %v", err)
		return err
	}
	if err := file.FsyncDir(dst); err != nil {
		logf(db.opts.Logger, "bitgask: fsync backup dir failed: %v", err)
		return err
	}
	return nil
}

func (db *DB) DeleteAll() error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}

	db.mu.Lock()
	for _, f := range db.dataFiles {
		_ = f.Close()
	}
	if db.active != nil && db.active.hint != nil {
		_ = db.active.hint.Close()
	}
	db.mu.Unlock()

	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if file.IsDataFile(name) || file.IsHintFile(name) {
			_ = os.Remove(filepath.Join(db.dataDir, name))
		}
	}

	active, err := openActiveFile(db.dataDir, 1, db.opts)
	if err != nil {
		return err
	}
	db.mu.Lock()
	db.index = keydir.NewRadix()
	db.dataFiles = map[uint32]*os.File{active.id: active.file}
	db.active = active
	db.stats = Stats{DataFiles: 1}
	db.mu.Unlock()
	if err := file.FsyncDir(db.dataDir); err != nil {
		logf(db.opts.Logger, "bitgask: fsync data dir failed after delete-all: %v", err)
		return err
	}
	if err := file.FsyncDir(db.dir); err != nil {
		logf(db.opts.Logger, "bitgask: fsync db dir failed after delete-all: %v", err)
		return err
	}
	return nil
}

func (db *DB) Reclaimable() (int64, int64, float64) {
	if db == nil {
		return 0, 0, 0
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	db.mu.RLock()
	defer db.mu.RUnlock()
	total := db.stats.TotalBytes
	dead := db.stats.DeadBytes
	if total == 0 {
		return dead, total, 0
	}
	return dead, total, float64(dead) / float64(total)
}

func (db *DB) Stats() Stats {
	if db == nil {
		return Stats{}
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.stats
}

func (db *DB) CompactIfNeeded() (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	return db.compactIfNeededInternal()
}

func (db *DB) compactIfNeededInternal() (bool, error) {
	db.mu.RLock()
	total := db.stats.TotalBytes
	dead := db.stats.DeadBytes
	db.mu.RUnlock()
	if total < db.opts.MergeMinTotal {
		return false, nil
	}
	if total == 0 {
		return false, nil
	}
	ratio := float64(dead) / float64(total)
	if ratio < db.opts.MergeTriggerRatio {
		return false, nil
	}
	if err := db.mergeInternal(); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) Merge(opts ...MergeOptions) error {
	if db == nil {
		return ErrClosed
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	return db.mergeInternal(opts...)
}

func (db *DB) mergeInternal(opts ...MergeOptions) error {
	opt := MergeOptions{}
	if len(opts) > 0 {
		opt = opts[0]
	}

	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	if !opt.Force {
		db.mu.RLock()
		total := db.stats.TotalBytes
		dead := db.stats.DeadBytes
		db.mu.RUnlock()
		if total < db.opts.MergeMinTotal {
			return nil
		}
		if total == 0 {
			return nil
		}
		if float64(dead)/float64(total) < db.opts.MergeTriggerRatio {
			return nil
		}
	}

	return db.runMerge()
}

func (db *DB) ensureOpen() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return nil
}

func (db *DB) rotateIfNeeded(nextSize int64) error {
	if db.active == nil {
		return fmt.Errorf("bitgask: no active file")
	}
	if db.active.size+nextSize <= db.opts.MaxDataFileSize {
		return nil
	}
	oldActive := db.active
	if err := oldActive.file.Sync(); err != nil {
		return err
	}
	if oldActive.hint != nil && db.opts.UseHintFiles && db.opts.HintFileSync {
		_ = oldActive.hint.Sync()
	}
	db.mu.Lock()
	delete(db.dataFiles, oldActive.id)
	db.mu.Unlock()
	_ = oldActive.file.Close()
	if oldActive.hint != nil {
		_ = oldActive.hint.Close()
	}
	newID := oldActive.id + 1
	active, err := openActiveFile(db.dataDir, newID, db.opts)
	if err != nil {
		return err
	}
	db.mu.Lock()
	db.active = active
	db.dataFiles[newID] = active.file
	db.stats.DataFiles++
	db.mu.Unlock()
	return nil
}

func (db *DB) readValue(meta RecordMeta) ([]byte, error) {
	file, err := db.openDataFile(meta.FileID)
	if err != nil {
		return nil, err
	}
	rec, _, err := record.ReadAt(file, meta.Offset)
	if err != nil {
		if errors.Is(err, record.ErrCorrupt) {
			return nil, ErrCorrupt
		}
		return nil, err
	}
	if rec.Flags&record.FlagDeleted != 0 {
		return nil, ErrKeyNotFound
	}
	if rec.ExpiresAt > 0 && time.Now().UnixNano() >= rec.ExpiresAt {
		return nil, ErrExpired
	}
	value := rec.Value
	if rec.Flags&record.FlagCompressed != 0 {
		codecType := codec.CompressionType(rec.Codec)
		decoded, err := codec.Decode(codecType, rec.Value)
		if err != nil {
			return nil, err
		}
		value = decoded
	}
	return value, nil
}

func (db *DB) openDataFile(id uint32) (*os.File, error) {
	db.mu.RLock()
	f, ok := db.dataFiles[id]
	db.mu.RUnlock()
	if ok {
		return f, nil
	}
	path := filepath.Join(db.dataDir, file.DataFileName(id, db.opts.FileIDWidth))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	db.mu.Lock()
	db.dataFiles[id] = f
	db.mu.Unlock()
	return f, nil
}

func (db *DB) indexGet(key []byte) (RecordMeta, bool) {
	if db.index == nil {
		return RecordMeta{}, false
	}
	val, ok := db.index.Get(key)
	if !ok {
		return RecordMeta{}, false
	}
	meta, ok := val.(RecordMeta)
	if !ok {
		return RecordMeta{}, false
	}
	return meta, true
}

func (db *DB) indexSet(key []byte, meta RecordMeta) {
	if db.index == nil {
		return
	}
	db.index.Set(key, meta)
}

func (db *DB) indexDelete(key []byte) {
	if db.index == nil {
		return
	}
	db.index.Delete(key)
}

func (db *DB) forEachIndex(prefix []byte, fn func(key []byte, meta RecordMeta) bool) {
	if db.index == nil {
		return
	}
	iter := func(key []byte, value interface{}) bool {
		meta, ok := value.(RecordMeta)
		if !ok {
			return true
		}
		return fn(key, meta)
	}
	if len(prefix) == 0 {
		db.index.Range(iter)
		return
	}
	db.index.Prefix(prefix, iter)
}

type keydirEntry struct {
	Key  []byte
	Meta RecordMeta
}

func (db *DB) snapshotEntries(prefix, start, end []byte, pred func(key []byte, meta RecordMeta) bool) ([]keydirEntry, [][]byte, error) {
	if db == nil {
		return nil, nil, ErrClosed
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	entries := make([]keydirEntry, 0)
	expired := make([][]byte, 0)
	db.forEachIndex(prefix, func(key []byte, meta RecordMeta) bool {
		if meta.Deleted {
			return true
		}
		if isExpired(meta, now) {
			expired = append(expired, append([]byte(nil), key...))
			return true
		}
		if start != nil && bytes.Compare(key, start) < 0 {
			return true
		}
		if end != nil && bytes.Compare(key, end) >= 0 {
			return true
		}
		if pred != nil && !pred(key, meta) {
			return true
		}
		entries = append(entries, keydirEntry{
			Key:  append([]byte(nil), key...),
			Meta: meta,
		})
		return true
	})
	db.mu.RUnlock()
	return entries, expired, nil
}

func (db *DB) snapshotAll() ([]keydirEntry, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	entries := make([]keydirEntry, 0)
	db.forEachIndex(nil, func(key []byte, meta RecordMeta) bool {
		entries = append(entries, keydirEntry{
			Key:  append([]byte(nil), key...),
			Meta: meta,
		})
		return true
	})
	db.mu.RUnlock()
	return entries, nil
}

func (db *DB) expireKeys(keys [][]byte) {
	now := time.Now()
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, k := range keys {
		m, ok := db.indexGet(k)
		if !ok {
			continue
		}
		if m.Deleted {
			continue
		}
		if !isExpired(m, now) {
			continue
		}
		db.indexDelete(k)
		db.stats.Keys--
		db.stats.DeadBytes += int64(m.Size)
	}
}

func (db *DB) mergeLoop() {
	defer close(db.mergeDone)
	ticker := time.NewTicker(db.opts.MergeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if _, err := db.compactIfNeededInternal(); err != nil {
				logf(db.opts.Logger, "bitgask: background merge failed: %v", err)
			}
		case <-db.mergeStop:
			return
		}
	}
}

func (db *DB) syncLoop() {
	defer close(db.syncDone)
	ticker := time.NewTicker(db.opts.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := db.syncInternal(); err != nil {
				logf(db.opts.Logger, "bitgask: background sync failed: %v", err)
			}
		case <-db.syncStop:
			return
		}
	}
}

func (db *DB) stopBackground() {
	close(db.mergeStop)
	<-db.mergeDone
	if db.opts.SyncInterval > 0 {
		close(db.syncStop)
		<-db.syncDone
	}
}

func (db *DB) startBackground() {
	db.mergeStop = make(chan struct{})
	db.mergeDone = make(chan struct{})
	db.syncStop = make(chan struct{})
	db.syncDone = make(chan struct{})
	go db.mergeLoop()
	if db.opts.SyncInterval > 0 {
		go db.syncLoop()
	} else {
		close(db.syncDone)
	}
}

func (db *DB) runMerge() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}

	now := time.Now()
	entries, err := db.snapshotAll()
	if err != nil {
		return err
	}

	mergeDir := filepath.Join(db.dir, "merge.tmp")
	_ = os.RemoveAll(mergeDir)
	if err := os.MkdirAll(mergeDir, db.opts.DirMode); err != nil {
		return err
	}

	writer := &mergeWriter{
		dataDir: mergeDir,
		opts:    db.opts,
		fileID:  1,
		files:   0,
	}
	if err := writer.open(); err != nil {
		return err
	}

	newIndex := keydir.NewRadix()
	var newStats Stats
	for _, entry := range entries {
		meta := entry.Meta
		if meta.Deleted {
			continue
		}
		if isExpired(meta, now) {
			continue
		}
		val, err := db.readValue(meta)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrExpired) {
				continue
			}
			return err
		}
		rec := record.Record{
			Timestamp: meta.Timestamp.UnixNano(),
			ExpiresAt: unixNano(meta.ExpiresAt),
			Key:       entry.Key,
			Value:     val,
			Flags:     0,
			Codec:     0,
		}
		buf, err := encodeWithOptions(rec, db.opts)
		if err != nil {
			return err
		}
		offset, size, err := writer.write(buf)
		if err != nil {
			return err
		}
		newMeta := RecordMeta{
			ExpiresAt: meta.ExpiresAt,
			Deleted:   false,
			Timestamp: meta.Timestamp,
			FileID:    writer.fileID,
			Offset:    offset,
			Size:      size,
		}
		newIndex.Set(entry.Key, newMeta)
		newStats.TotalBytes += int64(size)
	}
	newStats.Keys = newIndex.Len()
	newStats.DeadBytes = 0
	newStats.DataFiles = writer.files
	newStats.LastMerge = time.Now()

	if err := writer.close(); err != nil {
		return err
	}

	// Close open handles before rename (required on Windows).
	oldActiveID := uint32(0)
	db.mu.Lock()
	if db.active != nil {
		oldActiveID = db.active.id
	}
	for _, f := range db.dataFiles {
		_ = f.Close()
	}
	if db.active != nil && db.active.hint != nil {
		_ = db.active.hint.Close()
	}
	db.dataFiles = map[uint32]*os.File{}
	db.active = nil
	db.mu.Unlock()

	oldDir := filepath.Join(db.dir, "data.old")
	_ = os.RemoveAll(oldDir)
	if err := os.Rename(db.dataDir, oldDir); err != nil {
		// Best-effort reopen active file to keep DB usable
		if oldActiveID != 0 {
			if active, reopenErr := openActiveFile(db.dataDir, oldActiveID, db.opts); reopenErr == nil {
				db.mu.Lock()
				db.active = active
				db.dataFiles[active.id] = active.file
				db.mu.Unlock()
			}
		}
		return err
	}
	if err := os.Rename(mergeDir, db.dataDir); err != nil {
		_ = os.Rename(oldDir, db.dataDir)
		if oldActiveID != 0 {
			if active, reopenErr := openActiveFile(db.dataDir, oldActiveID, db.opts); reopenErr == nil {
				db.mu.Lock()
				db.active = active
				db.dataFiles[active.id] = active.file
				db.mu.Unlock()
			}
		}
		return err
	}
	_ = os.RemoveAll(oldDir)
	if err := file.FsyncDir(db.dir); err != nil {
		logf(db.opts.Logger, "bitgask: fsync db dir failed after merge: %v", err)
		return err
	}
	if err := file.FsyncDir(db.dataDir); err != nil {
		logf(db.opts.Logger, "bitgask: fsync data dir failed after merge: %v", err)
		return err
	}

	// reopen active file
	activeID := writer.fileID
	active, err := openActiveFile(db.dataDir, activeID, db.opts)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.dataFiles = map[uint32]*os.File{active.id: active.file}
	db.active = active
	db.index = newIndex
	newStats.DataFiles = writer.files
	newStats.LastMerge = time.Now()
	db.stats = newStats
	db.mu.Unlock()
	return nil
}

func encodeWithOptions(rec record.Record, opts config) ([]byte, error) {
	// apply compression based on options
	flags := rec.Flags
	codecID := rec.Codec
	value := rec.Value
	if opts.Compression == CompressionSnappy && len(value) >= opts.CompressionThreshold {
		enc, err := codec.Encode(codec.Snappy, value)
		if err != nil {
			return nil, err
		}
		value = enc
		flags |= record.FlagCompressed
		codecID = uint8(codec.Snappy)
	}
	rec.Flags = flags
	rec.Codec = codecID
	rec.Value = value
	return record.Encode(rec)
}

func openActiveFile(dataDir string, id uint32, opts config) (*dataFile, error) {
	path := filepath.Join(dataDir, file.DataFileName(id, opts.FileIDWidth))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, opts.FileMode)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var hint *os.File
	if opts.UseHintFiles {
		hintPath := filepath.Join(dataDir, file.HintFileName(id, opts.FileIDWidth))
		hint, err = os.OpenFile(hintPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, opts.FileMode)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if _, err := hint.Seek(0, io.SeekEnd); err != nil {
			_ = f.Close()
			_ = hint.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		if hint != nil {
			_ = hint.Close()
		}
		return nil, err
	}
	return &dataFile{id: id, file: f, hint: hint, size: stat.Size()}, nil
}

func writeMeta(dataDir string, opts config) error {
	metaPath := filepath.Join(dataDir, metaFileName)
	if _, err := os.Stat(metaPath); err == nil {
		return nil
	}
	type meta struct {
		Version int       `json:"version"`
		Created time.Time `json:"created"`
	}
	m := meta{Version: 1, Created: time.Now()}
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath, buf, opts.FileMode)
}

type loadEntry struct {
	key  []byte
	meta RecordMeta
}

type fileLoadResult struct {
	id         uint32
	entries    []loadEntry
	totalBytes int64
	err        error
}

func loadFromHints(dataDir string, opts config) (keydir.Keydir, Stats, uint32, error) {
	files, err := listDataFiles(dataDir)
	if err != nil {
		return nil, Stats{}, 0, err
	}
	idx := keydir.NewRadix()
	var stats Stats
	if len(files) == 0 {
		return idx, stats, 0, nil
	}
	now := time.Now()
	results, err := loadHintResults(dataDir, files, opts)
	if err != nil {
		return nil, Stats{}, 0, err
	}
	sort.Slice(results, func(i, j int) bool { return results[i].id < results[j].id })
	for _, res := range results {
		stats.TotalBytes += res.totalBytes
		for _, entry := range res.entries {
			updateIndex(idx, &stats, entry.key, entry.meta, now)
		}
	}
	stats.Keys = countLive(idx, now)
	stats.DataFiles = len(files)
	return idx, stats, files[len(files)-1], nil
}

func loadFromData(dataDir string, opts config, repair bool, rebuildHints bool) (keydir.Keydir, Stats, uint32, error) {
	files, err := listDataFiles(dataDir)
	if err != nil {
		return nil, Stats{}, 0, err
	}
	idx := keydir.NewRadix()
	var stats Stats
	if len(files) == 0 {
		return idx, stats, 0, nil
	}
	now := time.Now()
	results, err := loadDataResults(dataDir, files, opts, repair, rebuildHints)
	if err != nil {
		return nil, Stats{}, 0, err
	}
	sort.Slice(results, func(i, j int) bool { return results[i].id < results[j].id })
	for _, res := range results {
		stats.TotalBytes += res.totalBytes
		for _, entry := range res.entries {
			updateIndex(idx, &stats, entry.key, entry.meta, now)
		}
	}
	stats.Keys = countLive(idx, now)
	stats.DataFiles = len(files)
	return idx, stats, files[len(files)-1], nil
}

func loadHintResults(dataDir string, files []uint32, opts config) ([]fileLoadResult, error) {
	concurrency := opts.IndexLoadConcurrency
	if concurrency <= 1 || len(files) <= 1 {
		results := make([]fileLoadResult, 0, len(files))
		for _, id := range files {
			res := loadHintFile(dataDir, id, opts)
			if res.err != nil {
				return nil, res.err
			}
			results = append(results, res)
		}
		return results, nil
	}
	results := make([]fileLoadResult, 0, len(files))
	sem := make(chan struct{}, concurrency)
	ch := make(chan fileLoadResult, len(files))
	for _, id := range files {
		sem <- struct{}{}
		go func(fileID uint32) {
			defer func() { <-sem }()
			ch <- loadHintFile(dataDir, fileID, opts)
		}(id)
	}
	var firstErr error
	for range files {
		res := <-ch
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
		results = append(results, res)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func loadDataResults(dataDir string, files []uint32, opts config, repair bool, rebuildHints bool) ([]fileLoadResult, error) {
	concurrency := opts.IndexLoadConcurrency
	if concurrency <= 1 || len(files) <= 1 {
		results := make([]fileLoadResult, 0, len(files))
		for _, id := range files {
			res := loadDataFile(dataDir, id, opts, repair, rebuildHints)
			if res.err != nil {
				return nil, res.err
			}
			results = append(results, res)
		}
		return results, nil
	}
	results := make([]fileLoadResult, 0, len(files))
	sem := make(chan struct{}, concurrency)
	ch := make(chan fileLoadResult, len(files))
	for _, id := range files {
		sem <- struct{}{}
		go func(fileID uint32) {
			defer func() { <-sem }()
			ch <- loadDataFile(dataDir, fileID, opts, repair, rebuildHints)
		}(id)
	}
	var firstErr error
	for range files {
		res := <-ch
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
		results = append(results, res)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func loadHintFile(dataDir string, id uint32, opts config) fileLoadResult {
	hintPath := filepath.Join(dataDir, file.HintFileName(id, opts.FileIDWidth))
	hintFile, err := os.Open(hintPath)
	if err != nil {
		return fileLoadResult{id: id, err: err}
	}
	defer func() { _ = hintFile.Close() }()
	dataPath := filepath.Join(dataDir, file.DataFileName(id, opts.FileIDWidth))
	dataFile, err := os.Open(dataPath)
	if err != nil {
		logf(opts.Logger, "bitgask: hint validation failed opening data file %d: %v", id, err)
		return fileLoadResult{id: id, err: err}
	}
	defer func() { _ = dataFile.Close() }()
	reader := bufio.NewReader(hintFile)
	entries := make([]loadEntry, 0)
	var total int64
	for {
		entry, _, err := index.ReadHint(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			logf(opts.Logger, "bitgask: hint read failed for file %d: %v", id, err)
			return fileLoadResult{id: id, err: err}
		}
		rec, _, err := record.ReadAt(dataFile, entry.Offset)
		if err != nil {
			logf(opts.Logger, "bitgask: hint validation failed for file %d offset %d: %v", id, entry.Offset, err)
			return fileLoadResult{id: id, err: ErrCorrupt}
		}
		if !bytes.Equal(rec.Key, entry.Key) {
			logf(opts.Logger, "bitgask: hint validation failed for file %d offset %d: key mismatch", id, entry.Offset)
			return fileLoadResult{id: id, err: ErrCorrupt}
		}
		meta := RecordMeta{
			ExpiresAt: timeFromUnixNano(entry.Expires),
			Deleted:   entry.Flags&record.FlagDeleted != 0,
			Timestamp: timeFromUnixNano(entry.TS),
			FileID:    entry.FileID,
			Offset:    entry.Offset,
			Size:      entry.Size,
		}
		entries = append(entries, loadEntry{
			key:  append([]byte(nil), entry.Key...),
			meta: meta,
		})
		total += int64(entry.Size)
	}
	return fileLoadResult{id: id, entries: entries, totalBytes: total}
}

func loadDataFile(dataDir string, id uint32, opts config, repair bool, rebuildHints bool) fileLoadResult {
	path := filepath.Join(dataDir, file.DataFileName(id, opts.FileIDWidth))
	f, err := os.OpenFile(path, os.O_RDWR, opts.FileMode)
	if err != nil {
		return fileLoadResult{id: id, err: err}
	}
	defer func() { _ = f.Close() }()
	var hintFile *os.File
	if rebuildHints && opts.UseHintFiles {
		hintPath := filepath.Join(dataDir, file.HintFileName(id, opts.FileIDWidth))
		hintFile, err = os.OpenFile(hintPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, opts.FileMode)
		if err != nil {
			return fileLoadResult{id: id, err: err}
		}
		defer func() {
			_ = hintFile.Sync()
			_ = hintFile.Close()
		}()
	}
	reader := bufio.NewReader(f)
	var offset int64
	entries := make([]loadEntry, 0)
	var total int64
	for {
		rec, size, err := record.DecodeFrom(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, record.ErrCorrupt) {
				if repair {
					logf(opts.Logger, "bitgask: repair truncating file %d at offset %d", id, offset)
					_ = f.Truncate(offset)
					break
				}
				return fileLoadResult{id: id, err: ErrCorrupt}
			}
			return fileLoadResult{id: id, err: err}
		}
		meta := RecordMeta{
			ExpiresAt: timeFromUnixNano(rec.ExpiresAt),
			Deleted:   rec.Flags&record.FlagDeleted != 0,
			Timestamp: timeFromUnixNano(rec.Timestamp),
			FileID:    id,
			Offset:    offset,
			Size:      uint32(size),
		}
		entries = append(entries, loadEntry{
			key:  append([]byte(nil), rec.Key...),
			meta: meta,
		})
		total += int64(size)
		if rebuildHints && opts.UseHintFiles && hintFile != nil {
			hintEntry := index.HintEntry{
				Key:     append([]byte(nil), rec.Key...),
				FileID:  id,
				Offset:  offset,
				Size:    uint32(size),
				TS:      rec.Timestamp,
				Expires: rec.ExpiresAt,
				Flags:   rec.Flags,
			}
			if err := index.WriteHint(hintFile, hintEntry); err != nil {
				return fileLoadResult{id: id, err: err}
			}
		}
		offset += int64(size)
	}
	return fileLoadResult{id: id, entries: entries, totalBytes: total}
}

func listDataFiles(dataDir string) ([]uint32, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	ids := make([]uint32, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !file.IsDataFile(entry.Name()) {
			continue
		}
		id, ok := file.ParseFileID(entry.Name())
		if !ok {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func updateIndex(idx keydir.Keydir, stats *Stats, key []byte, meta RecordMeta, now time.Time) {
	oldVal, ok := idx.Get(key)
	var old RecordMeta
	if ok {
		cast, castOk := oldVal.(RecordMeta)
		if !castOk {
			ok = false
		} else {
			old = cast
		}
	}
	if ok && isExpired(old, now) {
		idx.Delete(key)
		if !old.Deleted {
			stats.Keys--
		}
		stats.DeadBytes += int64(old.Size)
		ok = false
	}

	if meta.Deleted {
		if ok && !old.Deleted {
			stats.DeadBytes += int64(old.Size)
			stats.Keys--
		}
		stats.DeadBytes += int64(meta.Size)
		idx.Set(key, meta)
		return
	}
	if isExpired(meta, now) {
		if ok && !old.Deleted {
			stats.DeadBytes += int64(old.Size)
			stats.Keys--
		}
		stats.DeadBytes += int64(meta.Size)
		idx.Delete(key)
		return
	}
	if ok && !old.Deleted {
		stats.DeadBytes += int64(old.Size)
	}
	idx.Set(key, meta)
	if !ok || old.Deleted {
		stats.Keys++
	}
}

func countLive(idx keydir.Keydir, now time.Time) int {
	count := 0
	idx.Range(func(_ []byte, value interface{}) bool {
		meta, ok := value.(RecordMeta)
		if !ok {
			return true
		}
		if meta.Deleted {
			return true
		}
		if isExpired(meta, now) {
			return true
		}
		count++
		return true
	})
	return count
}

func isExpired(meta RecordMeta, now time.Time) bool {
	return !meta.ExpiresAt.IsZero() && now.After(meta.ExpiresAt)
}

func timeFromUnixNano(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v)
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

type mergeWriter struct {
	dataDir string
	opts    config
	fileID  uint32
	file    *os.File
	hint    *os.File
	size    int64
	files   int
}

func linkOrCopy(src, dst string, mode os.FileMode) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func (w *mergeWriter) open() error {
	if w.file != nil {
		return nil
	}
	path := filepath.Join(w.dataDir, file.DataFileName(w.fileID, w.opts.FileIDWidth))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, w.opts.FileMode)
	if err != nil {
		return err
	}
	w.file = f
	w.size = 0
	w.files++
	if w.opts.UseHintFiles {
		hintPath := filepath.Join(w.dataDir, file.HintFileName(w.fileID, w.opts.FileIDWidth))
		hint, err := os.OpenFile(hintPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, w.opts.FileMode)
		if err != nil {
			_ = f.Close()
			return err
		}
		w.hint = hint
	}
	return nil
}

func (w *mergeWriter) rotateIfNeeded(next int64) error {
	if w.size+next <= w.opts.MergeMaxFileSize {
		return nil
	}
	if err := w.close(); err != nil {
		return err
	}
	w.fileID++
	w.file = nil
	w.hint = nil
	return w.open()
}

func (w *mergeWriter) write(buf []byte) (int64, uint32, error) {
	size := uint32(len(buf))
	if err := w.rotateIfNeeded(int64(size)); err != nil {
		return 0, 0, err
	}
	offset := w.size
	if _, err := w.file.Write(buf); err != nil {
		return 0, 0, err
	}
	w.size += int64(size)
	if w.opts.UseHintFiles && w.hint != nil {
		rec, _, err := record.ReadAt(w.file, offset)
		if err != nil {
			return 0, 0, err
		}
		hintEntry := index.HintEntry{
			Key:     append([]byte(nil), rec.Key...),
			FileID:  w.fileID,
			Offset:  offset,
			Size:    size,
			TS:      rec.Timestamp,
			Expires: rec.ExpiresAt,
			Flags:   rec.Flags,
		}
		if err := index.WriteHint(w.hint, hintEntry); err != nil {
			return 0, 0, err
		}
	}
	return offset, size, nil
}

func (w *mergeWriter) close() error {
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if w.hint != nil {
		if err := w.hint.Sync(); err != nil {
			return err
		}
		_ = w.hint.Close()
		w.hint = nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
