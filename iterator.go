package bitgask

import (
	"errors"
)

type Iterator struct {
	db      *DB
	entries []keydirEntry
	idx     int
	key     []byte
	val     []byte
	meta    RecordMeta
	err     error
	closed  bool
}

func (db *DB) NewIterator(prefix []byte) (*Iterator, error) {
	entries, expired, err := db.snapshotEntries(prefix, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(expired) > 0 {
		db.expireKeys(expired)
	}
	return &Iterator{db: db, entries: entries, idx: -1}, nil
}

func (it *Iterator) Next() bool {
	if it == nil || it.closed {
		return false
	}
	for {
		it.idx++
		if it.idx >= len(it.entries) {
			return false
		}
		entry := it.entries[it.idx]
		val, err := it.db.readValue(entry.Meta)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrKeyNotFound) {
				continue
			}
			it.err = err
			return false
		}
		it.key = entry.Key
		it.val = val
		it.meta = entry.Meta
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
	it.entries = nil
	return nil
}
