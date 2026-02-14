package bitgask

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"time"

	"github.com/AfshinJalili/bitgask/internal/index"
	"github.com/AfshinJalili/bitgask/internal/keydir"
	"github.com/AfshinJalili/bitgask/internal/record"
)

type Txn struct {
	db       *DB
	snapshot keydir.Snapshot
	writes   map[string]txnEntry
	order    []string
	closed   bool
}

var txnSyncHook func()

type txnEntry struct {
	key       []byte
	value     []byte
	deleted   bool
	expiresAt time.Time
	timestamp time.Time
}

type emptySnapshot struct{}

func (emptySnapshot) Get(_ []byte) (interface{}, bool)   { return nil, false }
func (emptySnapshot) Len() int                           { return 0 }
func (emptySnapshot) Range(_ keydir.Iterator)            {}
func (emptySnapshot) Prefix(_ []byte, _ keydir.Iterator) {}

func (db *DB) Transaction() *Txn {
	if db == nil {
		return &Txn{closed: true}
	}
	db.opMu.RLock()
	defer db.opMu.RUnlock()
	if err := db.ensureOpen(); err != nil {
		return &Txn{db: db, closed: true}
	}
	db.mu.RLock()
	var snap keydir.Snapshot
	if db.index == nil {
		snap = emptySnapshot{}
	} else {
		snap = db.index.Snapshot()
	}
	db.mu.RUnlock()
	return &Txn{
		db:       db,
		snapshot: snap,
		writes:   make(map[string]txnEntry),
	}
}

func (t *Txn) ensureOpen() error {
	if t == nil || t.closed || t.db == nil {
		return ErrClosed
	}
	return t.db.ensureOpen()
}

func (t *Txn) Discard() {
	if t == nil {
		return
	}
	t.closed = true
}

func (t *Txn) Put(key, value []byte) error {
	if t == nil {
		return ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	return t.putWithTTL(key, value, t.db.opts.DefaultTTL)
}

func (t *Txn) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if t == nil {
		return ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	return t.putWithTTL(key, value, ttl)
}

func (t *Txn) putWithTTL(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > t.db.opts.MaxKeySize || len(value) > t.db.opts.MaxValueSize {
		return ErrOversized
	}
	now := time.Now()
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	entry := txnEntry{
		key:       append([]byte(nil), key...),
		value:     append([]byte(nil), value...),
		deleted:   false,
		expiresAt: expiresAt,
		timestamp: now,
	}
	t.storeWrite(entry)
	return nil
}

func (t *Txn) Delete(key []byte) error {
	if t == nil {
		return ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > t.db.opts.MaxKeySize {
		return ErrOversized
	}
	now := time.Now()
	if entry, ok := t.writes[string(key)]; ok {
		if entry.deleted || entry.isExpired(now) {
			return ErrKeyNotFound
		}
	} else {
		meta, ok := t.snapshotMeta(key)
		if !ok || meta.Deleted || isExpired(meta, now) {
			return ErrKeyNotFound
		}
	}
	entry := txnEntry{
		key:       append([]byte(nil), key...),
		deleted:   true,
		timestamp: now,
	}
	t.storeWrite(entry)
	return nil
}

func (t *Txn) Has(key []byte) (bool, error) {
	if t == nil {
		return false, ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return false, err
	}
	if len(key) == 0 {
		return false, ErrInvalidKey
	}
	now := time.Now()
	if entry, ok := t.writes[string(key)]; ok {
		if entry.deleted || entry.isExpired(now) {
			return false, nil
		}
		return true, nil
	}
	meta, ok := t.snapshotMeta(key)
	if !ok || meta.Deleted || isExpired(meta, now) {
		return false, nil
	}
	return true, nil
}

func (t *Txn) Get(key []byte) ([]byte, error) {
	if t == nil {
		return nil, ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}
	now := time.Now()
	if entry, ok := t.writes[string(key)]; ok {
		if entry.deleted {
			return nil, ErrKeyNotFound
		}
		if entry.isExpired(now) {
			return nil, ErrExpired
		}
		return append([]byte(nil), entry.value...), nil
	}
	meta, ok := t.snapshotMeta(key)
	if !ok || meta.Deleted {
		return nil, ErrKeyNotFound
	}
	if isExpired(meta, now) {
		return nil, ErrExpired
	}
	val, err := t.db.readValue(meta)
	if errors.Is(err, ErrExpired) {
		return nil, ErrExpired
	}
	return val, err
}

func (t *Txn) IterKeys(ctx context.Context, fn func(key []byte) bool) error {
	if t == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	entries := t.collectEntries(nil, nil, nil, nil)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !fn(append([]byte(nil), entry.Key...)) {
			return nil
		}
	}
	return nil
}

func (t *Txn) Iter(ctx context.Context, prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error {
	if t == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	entries := t.collectEntries(prefix, nil, nil, nil)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		val, err := t.valueForEntry(entry)
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

func (t *Txn) Scan(prefix []byte, fn func(key, value []byte, meta RecordMeta) bool) error {
	if t == nil {
		return ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	entries := t.collectEntries(prefix, nil, nil, nil)
	for _, entry := range entries {
		val, err := t.valueForEntry(entry)
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

func (t *Txn) Range(start, end []byte, fn func(key, value []byte, meta RecordMeta) bool) error {
	if t == nil {
		return ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	entries := t.collectEntries(nil, start, end, nil)
	for _, entry := range entries {
		val, err := t.valueForEntry(entry)
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

func (t *Txn) Commit() error {
	if t == nil {
		return ErrClosed
	}
	t.db.opMu.RLock()
	defer t.db.opMu.RUnlock()
	if err := t.ensureOpen(); err != nil {
		return err
	}
	if len(t.order) == 0 {
		t.closed = true
		return nil
	}

	t.db.writeMu.Lock()
	defer t.db.writeMu.Unlock()
	if err := t.db.ensureOpen(); err != nil {
		return err
	}

	entries := make([]commitEntry, 0, len(t.order))
	needsPutSync := false
	needsDeleteSync := false
	for _, key := range t.order {
		entry := t.writes[key]
		if entry.deleted {
			needsDeleteSync = true
		} else {
			needsPutSync = true
		}
		rec := record.Record{
			Timestamp: entry.timestamp.UnixNano(),
			ExpiresAt: 0,
			Key:       entry.key,
			Value:     entry.value,
			Flags:     0,
			Codec:     0,
		}
		if !entry.expiresAt.IsZero() {
			rec.ExpiresAt = entry.expiresAt.UnixNano()
		}
		if entry.deleted {
			rec.Flags = record.FlagDeleted
			rec.Value = nil
		}
		buf, err := encodeWithOptions(rec, t.db.opts)
		if err != nil {
			return err
		}
		size := uint32(len(buf))
		if err := t.db.rotateIfNeeded(int64(size)); err != nil {
			return err
		}
		offset := t.db.active.size
		if _, err := t.db.active.file.Write(buf); err != nil {
			return err
		}
		t.db.active.size += int64(size)
		if t.db.opts.UseHintFiles && t.db.active.hint != nil {
			hintEntry := index.HintEntry{
				Key:     append([]byte(nil), entry.key...),
				FileID:  t.db.active.id,
				Offset:  offset,
				Size:    size,
				TS:      rec.Timestamp,
				Expires: rec.ExpiresAt,
				Flags:   rec.Flags,
			}
			if err := index.WriteHint(t.db.active.hint, hintEntry); err != nil {
				logf(t.db.opts.Logger, "bitgask: hint write failed: %v", err)
			}
		}
		meta := RecordMeta{
			ExpiresAt: timeFromUnixNano(rec.ExpiresAt),
			Deleted:   entry.deleted,
			Timestamp: timeFromUnixNano(rec.Timestamp),
			FileID:    t.db.active.id,
			Offset:    offset,
			Size:      size,
		}
		entries = append(entries, commitEntry{
			key:  entry.key,
			meta: meta,
			size: size,
		})
	}

	needSync := (needsPutSync && t.db.opts.SyncOnPut) || (needsDeleteSync && t.db.opts.SyncOnDelete)
	if t.db.opts.UseHintFiles && t.db.active.hint != nil && t.db.opts.HintFileSync && needSync {
		if err := t.db.active.hint.Sync(); err != nil {
			logf(t.db.opts.Logger, "bitgask: hint sync failed: %v", err)
		}
	}
	if needSync {
		if err := syncDataFile(t.db.active.file); err != nil {
			return err
		}
		if txnSyncHook != nil {
			txnSyncHook()
		}
	}

	now := time.Now()
	t.db.mu.Lock()
	for i := range entries {
		ce := &entries[i]
		old, ok := t.db.indexGet(ce.key)
		if ok && isExpired(old, now) {
			t.db.indexDelete(ce.key)
			if !old.Deleted {
				t.db.stats.Keys--
			}
			t.db.stats.DeadBytes += int64(old.Size)
			ok = false
		}
		if ce.meta.Deleted {
			if ok && !old.Deleted {
				t.db.stats.DeadBytes += int64(old.Size)
				t.db.stats.Keys--
			}
			t.db.stats.DeadBytes += int64(ce.size)
			t.db.stats.TotalBytes += int64(ce.size)
			t.db.indexSet(ce.key, ce.meta)
			continue
		}
		if ok && !old.Deleted {
			t.db.stats.DeadBytes += int64(old.Size)
		}
		if !ok || old.Deleted {
			t.db.stats.Keys++
		}
		t.db.stats.TotalBytes += int64(ce.size)
		t.db.indexSet(ce.key, ce.meta)
	}
	t.db.mu.Unlock()

	t.closed = true
	return nil
}

type commitEntry struct {
	key  []byte
	meta RecordMeta
	size uint32
}

type txnViewEntry struct {
	Key       []byte
	Meta      RecordMeta
	Value     []byte
	FromWrite bool
}

func (t *Txn) collectEntries(prefix, start, end []byte, pred func(key []byte, meta RecordMeta) bool) []txnViewEntry {
	now := time.Now()
	writeKeys := make(map[string]struct{}, len(t.writes))
	for k := range t.writes {
		writeKeys[k] = struct{}{}
	}

	entries := make([]txnViewEntry, 0, t.snapshot.Len()+len(t.writes))
	iter := func(key []byte, value interface{}) bool {
		if _, ok := writeKeys[string(key)]; ok {
			return true
		}
		meta, ok := value.(RecordMeta)
		if !ok {
			return true
		}
		if meta.Deleted || isExpired(meta, now) {
			return true
		}
		if !matchKey(key, prefix, start, end) {
			return true
		}
		if pred != nil && !pred(key, meta) {
			return true
		}
		entries = append(entries, txnViewEntry{
			Key:  append([]byte(nil), key...),
			Meta: meta,
		})
		return true
	}
	if len(prefix) > 0 {
		t.snapshot.Prefix(prefix, iter)
	} else {
		t.snapshot.Range(iter)
	}

	for _, key := range t.order {
		entry := t.writes[key]
		if entry.deleted || entry.isExpired(now) {
			continue
		}
		if !matchKey(entry.key, prefix, start, end) {
			continue
		}
		meta := RecordMeta{
			ExpiresAt: entry.expiresAt,
			Deleted:   false,
			Timestamp: entry.timestamp,
		}
		if pred != nil && !pred(entry.key, meta) {
			continue
		}
		entries = append(entries, txnViewEntry{
			Key:       append([]byte(nil), entry.key...),
			Meta:      meta,
			Value:     append([]byte(nil), entry.value...),
			FromWrite: true,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})
	return entries
}

func (t *Txn) valueForEntry(entry txnViewEntry) ([]byte, error) {
	if entry.FromWrite {
		return append([]byte(nil), entry.Value...), nil
	}
	return t.db.readValue(entry.Meta)
}

func (t *Txn) storeWrite(entry txnEntry) {
	key := string(entry.key)
	if _, ok := t.writes[key]; !ok {
		t.order = append(t.order, key)
	}
	t.writes[key] = entry
}

func (t *Txn) snapshotMeta(key []byte) (RecordMeta, bool) {
	val, ok := t.snapshot.Get(key)
	if !ok {
		return RecordMeta{}, false
	}
	meta, ok := val.(RecordMeta)
	if !ok {
		return RecordMeta{}, false
	}
	return meta, true
}

func (e txnEntry) isExpired(now time.Time) bool {
	if e.expiresAt.IsZero() {
		return false
	}
	return !e.expiresAt.After(now)
}

func matchKey(key, prefix, start, end []byte) bool {
	if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
		return false
	}
	if start != nil && bytes.Compare(key, start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}
