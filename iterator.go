package bitgask

import (
	"errors"
	"time"
)

type Iterator struct {
	db     *DB
	keys   []string
	metas  []RecordMeta
	idx    int
	key    []byte
	val    []byte
	meta   RecordMeta
	err    error
	closed bool
}

func (db *DB) NewIterator() (*Iterator, error) {
	if db == nil {
		return nil, ErrClosed
	}
	now := time.Now()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
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
	return &Iterator{db: db, keys: keys, metas: metas, idx: -1}, nil
}

func (it *Iterator) Next() bool {
	if it == nil || it.closed {
		return false
	}
	for {
		it.idx++
		if it.idx >= len(it.keys) {
			return false
		}
		key := it.keys[it.idx]
		meta := it.metas[it.idx]
		val, err := it.db.readValue(meta)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				continue
			}
			it.err = err
			return false
		}
		it.key = []byte(key)
		it.val = val
		it.meta = meta
		return true
	}
}

func (it *Iterator) Key() []byte {
	if it == nil {
		return nil
	}
	return append([]byte(nil), it.key...)
}

func (it *Iterator) Value() []byte {
	if it == nil {
		return nil
	}
	return append([]byte(nil), it.val...)
}

func (it *Iterator) Meta() RecordMeta {
	if it == nil {
		return RecordMeta{}
	}
	return it.meta
}

func (it *Iterator) Err() error {
	if it == nil {
		return ErrClosed
	}
	return it.err
}

func (it *Iterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed = true
	it.keys = nil
	it.metas = nil
	return nil
}
