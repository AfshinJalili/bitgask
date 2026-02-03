package bitgask

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/AfshinJalili/bitgask/internal/codec"
	"github.com/AfshinJalili/bitgask/internal/file"
	"github.com/AfshinJalili/bitgask/internal/index"
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
	opts    Options

	lock *file.Lock

	mu        sync.RWMutex
	writeMu   sync.Mutex
	index     map[string]RecordMeta
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

func Open(path string, opts Options) (*DB, error) {
	return openDB(path, opts, false)
}

func Repair(path string, opts Options) (*DB, error) {
	return openDB(path, opts, true)
}

func openDB(path string, opts Options, repair bool) (*DB, error) {
	opts = opts.withDefaults()
	if path == "" {
		return nil, fmt.Errorf("bitgask: path required")
	}
	if err := os.MkdirAll(path, opts.DirMode); err != nil {
		return nil, err
	}
	dataDir := filepath.Join(path, "data")
	if err := os.MkdirAll(dataDir, opts.DirMode); err != nil {
		return nil, err
	}

	lock, err := file.AcquireLock(filepath.Join(dataDir, lockFileName), opts.LockTimeout)
	if err != nil {
		return nil, ErrLocked
	}

	if err := writeMeta(dataDir, opts); err != nil {
		_ = lock.Release()
		return nil, err
	}

	var (
		idx    map[string]RecordMeta
		stats  Stats
		lastID uint32
	)
	if repair {
		idx, stats, lastID, err = loadFromData(dataDir, opts, true, opts.UseHintFiles)
	} else if opts.ValidateOnOpen {
		idx, stats, lastID, err = loadFromData(dataDir, opts, false, false)
	} else if opts.UseHintFiles {
		idx, stats, lastID, err = loadFromHints(dataDir, opts)
		if err != nil {
			idx, stats, lastID, err = loadFromData(dataDir, opts, false, false)
		}
	} else {
		idx, stats, lastID, err = loadFromData(dataDir, opts, false, false)
	}
	if err != nil {
		_ = lock.Release()
		return nil, err
	}

	if lastID == 0 {
		lastID = 1
	}

	active, err := openActiveFile(dataDir, lastID, opts)
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
		opts:      opts,
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

	if opts.MergeOnOpen {
		_, _ = db.CompactIfNeeded()
	}

	go db.mergeLoop()
	if opts.SyncInterval > 0 {
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
		_ = db.lock.Release()
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

func (db *DB) Put(key, value []byte, ttl ...time.Duration) error {
	if db == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	if len(key) > db.opts.MaxKeySize || len(value) > db.opts.MaxValueSize {
		return ErrOversized
	}

	var ttlVal time.Duration
	if len(ttl) > 0 {
		ttlVal = ttl[0]
	} else {
		ttlVal = db.opts.DefaultTTL
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
			return err
		}
		if db.opts.HintFileSync && db.opts.SyncOnPut {
			_ = db.active.hint.Sync()
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
	old, ok := db.index[string(key)]
	if ok && isExpired(old, now) {
		// remove expired old record
		delete(db.index, string(key))
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
	db.index[string(key)] = meta
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	meta, ok := db.index[string(key)]
	db.mu.RUnlock()
	if !ok || meta.Deleted {
		return nil, ErrKeyNotFound
	}
	if isExpired(meta, now) {
		db.expireKeys([]string{string(key)})
		return nil, ErrExpired
	}
	val, err := db.readValue(meta)
	if err != nil {
		if errors.Is(err, ErrExpired) {
			db.expireKeys([]string{string(key)})
		}
		return nil, err
	}
	return val, nil
}

func (db *DB) Delete(key []byte) error {
	if db == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	if len(key) > db.opts.MaxKeySize {
		return ErrOversized
	}

	now := time.Now()
	db.mu.RLock()
	meta, ok := db.index[string(key)]
	db.mu.RUnlock()
	if !ok || meta.Deleted || isExpired(meta, now) {
		if ok && isExpired(meta, now) {
			db.expireKeys([]string{string(key)})
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
			return err
		}
		if db.opts.HintFileSync && db.opts.SyncOnDelete {
			_ = db.active.hint.Sync()
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
	db.index[string(key)] = deletedMeta
	return nil
}

func (db *DB) Has(key []byte) (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return false, ErrClosed
	}
	meta, ok := db.index[string(key)]
	db.mu.RUnlock()
	if !ok || meta.Deleted {
		return false, nil
	}
	if isExpired(meta, now) {
		db.expireKeys([]string{string(key)})
		return false, nil
	}
	return true, nil
}

func (db *DB) Keys() ([][]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	keys := make([][]byte, 0, len(db.index))
	expired := make([]string, 0)
	for k, m := range db.index {
		if m.Deleted {
			continue
		}
		if isExpired(m, now) {
			expired = append(expired, k)
			continue
		}
		keys = append(keys, []byte(k))
	}
	db.mu.RUnlock()
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	return keys, nil
}

func (db *DB) Scan(fn func(key, value []byte, meta RecordMeta) bool) error {
	if db == nil {
		return ErrClosed
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	keys := make([]string, 0, len(db.index))
	metas := make([]RecordMeta, 0, len(db.index))
	expired := make([]string, 0)
	for k, m := range db.index {
		if m.Deleted {
			continue
		}
		if isExpired(m, now) {
			expired = append(expired, k)
			continue
		}
		keys = append(keys, k)
		metas = append(metas, m)
	}
	db.mu.RUnlock()
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	for i, k := range keys {
		val, err := db.readValue(metas[i])
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return err
		}
		if !fn([]byte(k), val, metas[i]) {
			return nil
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db == nil {
		return ErrClosed
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.active != nil {
		if err := db.active.file.Sync(); err != nil {
			return err
		}
		if db.opts.UseHintFiles && db.active.hint != nil && db.opts.HintFileSync {
			if err := db.active.hint.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) Stats() Stats {
	if db == nil {
		return Stats{}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.stats
}

func (db *DB) CompactIfNeeded() (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
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
	if err := db.Merge(); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) Merge(opts ...MergeOptions) error {
	if db == nil {
		return ErrClosed
	}
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
	if err := db.active.file.Sync(); err != nil {
		return err
	}
	if db.active.hint != nil && db.opts.UseHintFiles && db.opts.HintFileSync {
		_ = db.active.hint.Sync()
	}
	_ = db.active.file.Close()
	if db.active.hint != nil {
		_ = db.active.hint.Close()
	}
	newID := db.active.id + 1
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

func (db *DB) expireKeys(keys []string) {
	now := time.Now()
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, k := range keys {
		m, ok := db.index[k]
		if !ok {
			continue
		}
		if m.Deleted {
			continue
		}
		if !isExpired(m, now) {
			continue
		}
		delete(db.index, k)
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
			_, _ = db.CompactIfNeeded()
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
			_ = db.Sync()
		case <-db.syncStop:
			return
		}
	}
}

func (db *DB) runMerge() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.ensureOpen(); err != nil {
		return err
	}

	now := time.Now()
	// snapshot index
	db.mu.RLock()
	snapshot := make(map[string]RecordMeta, len(db.index))
	for k, v := range db.index {
		snapshot[k] = v
	}
	db.mu.RUnlock()

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

	newIndex := make(map[string]RecordMeta, len(snapshot))
	var newStats Stats
	for k, meta := range snapshot {
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
			Key:       []byte(k),
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
		newIndex[k] = newMeta
		newStats.TotalBytes += int64(size)
	}
	newStats.Keys = len(newIndex)
	newStats.DeadBytes = 0
	newStats.DataFiles = writer.files
	newStats.LastMerge = time.Now()

	if err := writer.close(); err != nil {
		return err
	}

	oldDir := filepath.Join(db.dir, "data.old")
	_ = os.RemoveAll(oldDir)
	if err := os.Rename(db.dataDir, oldDir); err != nil {
		return err
	}
	if err := os.Rename(mergeDir, db.dataDir); err != nil {
		_ = os.Rename(oldDir, db.dataDir)
		return err
	}
	_ = os.RemoveAll(oldDir)

	// reopen active file
	activeID := writer.fileID
	active, err := openActiveFile(db.dataDir, activeID, db.opts)
	if err != nil {
		return err
	}

	db.mu.Lock()
	for _, f := range db.dataFiles {
		_ = f.Close()
	}
	if db.active != nil && db.active.hint != nil {
		_ = db.active.hint.Close()
	}
	db.dataFiles = map[uint32]*os.File{active.id: active.file}
	db.active = active
	db.index = newIndex
	newStats.DataFiles = writer.files
	newStats.LastMerge = time.Now()
	db.stats = newStats
	db.mu.Unlock()
	return nil
}

func encodeWithOptions(rec record.Record, opts Options) ([]byte, error) {
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

func openActiveFile(dataDir string, id uint32, opts Options) (*dataFile, error) {
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

func writeMeta(dataDir string, opts Options) error {
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

func loadFromHints(dataDir string, opts Options) (map[string]RecordMeta, Stats, uint32, error) {
	files, err := listDataFiles(dataDir)
	if err != nil {
		return nil, Stats{}, 0, err
	}
	idx := make(map[string]RecordMeta)
	var stats Stats
	var lastID uint32
	now := time.Now()
	for _, id := range files {
		lastID = id
		hintPath := filepath.Join(dataDir, file.HintFileName(id, opts.FileIDWidth))
		hintFile, err := os.Open(hintPath)
		if err != nil {
			return nil, Stats{}, 0, err
		}
		reader := bufio.NewReader(hintFile)
		for {
			entry, _, err := index.ReadHint(reader)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				_ = hintFile.Close()
				return nil, Stats{}, 0, err
			}
			stats.TotalBytes += int64(entry.Size)
			meta := RecordMeta{
				ExpiresAt: timeFromUnixNano(entry.Expires),
				Deleted:   entry.Flags&record.FlagDeleted != 0,
				Timestamp: timeFromUnixNano(entry.TS),
				FileID:    entry.FileID,
				Offset:    entry.Offset,
				Size:      entry.Size,
			}
			updateIndex(idx, &stats, entry.Key, meta, now)
		}
		_ = hintFile.Close()
	}
	stats.Keys = countLive(idx, now)
	stats.DataFiles = len(files)
	return idx, stats, lastID, nil
}

func loadFromData(dataDir string, opts Options, repair bool, rebuildHints bool) (map[string]RecordMeta, Stats, uint32, error) {
	files, err := listDataFiles(dataDir)
	if err != nil {
		return nil, Stats{}, 0, err
	}
	idx := make(map[string]RecordMeta)
	var stats Stats
	var lastID uint32
	now := time.Now()
	for _, id := range files {
		lastID = id
		path := filepath.Join(dataDir, file.DataFileName(id, opts.FileIDWidth))
		f, err := os.OpenFile(path, os.O_RDWR, opts.FileMode)
		if err != nil {
			return nil, Stats{}, 0, err
		}
		var hintFile *os.File
		if rebuildHints && opts.UseHintFiles {
			hintPath := filepath.Join(dataDir, file.HintFileName(id, opts.FileIDWidth))
			hintFile, err = os.OpenFile(hintPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, opts.FileMode)
			if err != nil {
				_ = f.Close()
				return nil, Stats{}, 0, err
			}
		}
		reader := bufio.NewReader(f)
		var offset int64
		for {
			rec, size, err := record.DecodeFrom(reader)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					if repair {
						_ = f.Truncate(offset)
						break
					}
					_ = f.Close()
					if hintFile != nil {
						_ = hintFile.Close()
					}
					return nil, Stats{}, 0, ErrCorrupt
				}
				if errors.Is(err, record.ErrCorrupt) {
					if repair {
						_ = f.Truncate(offset)
						break
					}
					_ = f.Close()
					if hintFile != nil {
						_ = hintFile.Close()
					}
					return nil, Stats{}, 0, ErrCorrupt
				}
				_ = f.Close()
				if hintFile != nil {
					_ = hintFile.Close()
				}
				return nil, Stats{}, 0, err
			}
			meta := RecordMeta{
				ExpiresAt: timeFromUnixNano(rec.ExpiresAt),
				Deleted:   rec.Flags&record.FlagDeleted != 0,
				Timestamp: timeFromUnixNano(rec.Timestamp),
				FileID:    id,
				Offset:    offset,
				Size:      uint32(size),
			}
			stats.TotalBytes += int64(size)
			updateIndex(idx, &stats, rec.Key, meta, now)
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
					_ = f.Close()
					_ = hintFile.Close()
					return nil, Stats{}, 0, err
				}
			}
			offset += int64(size)
		}
		if hintFile != nil {
			_ = hintFile.Sync()
			_ = hintFile.Close()
		}
		_ = f.Close()
	}
	stats.Keys = countLive(idx, now)
	stats.DataFiles = len(files)
	return idx, stats, lastID, nil
}

func listDataFiles(dataDir string) ([]uint32, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
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

func updateIndex(idx map[string]RecordMeta, stats *Stats, key []byte, meta RecordMeta, now time.Time) {
	k := string(key)
	old, ok := idx[k]
	if ok && isExpired(old, now) {
		delete(idx, k)
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
		idx[k] = meta
		return
	}
	if isExpired(meta, now) {
		if ok && !old.Deleted {
			stats.DeadBytes += int64(old.Size)
			stats.Keys--
		}
		stats.DeadBytes += int64(meta.Size)
		delete(idx, k)
		return
	}
	if ok && !old.Deleted {
		stats.DeadBytes += int64(old.Size)
	}
	idx[k] = meta
	if !ok || old.Deleted {
		stats.Keys++
	}
}

func countLive(idx map[string]RecordMeta, now time.Time) int {
	count := 0
	for _, m := range idx {
		if m.Deleted {
			continue
		}
		if isExpired(m, now) {
			continue
		}
		count++
	}
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
	opts    Options
	fileID  uint32
	file    *os.File
	hint    *os.File
	size    int64
	files   int
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
