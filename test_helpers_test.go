package bitgask

import (
	"fmt"
	"testing"
)

func OpenTestDB(tb testing.TB, dir string, opts ...Option) *DB {
	tb.Helper()
	if dir == "" {
		if t, ok := tb.(interface{ TempDir() string }); ok {
			dir = t.TempDir()
		} else {
			tb.Fatalf("temp dir required")
		}
	}
	db, err := Open(dir, opts...)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	return db
}

func WriteN(tb testing.TB, db *DB, n int, value []byte) {
	tb.Helper()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := db.Put(key, value); err != nil {
			tb.Fatalf("put: %v", err)
		}
	}
}
