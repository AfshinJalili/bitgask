package bitgask

import (
	"path/filepath"
	"testing"
	"time"
)

func collectKeys(ch <-chan []byte) [][]byte {
	keys := make([][]byte, 0)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

func TestInvalidKeyErrors(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put(nil, []byte("v")); err != ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
	if _, err := db.Get(nil); err != ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
	if err := db.Delete(nil); err != ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
	if _, err := db.Has(nil); err != ErrInvalidKey {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestKeysScanRangeSiftFold(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("a1"), []byte("v1"))
	_ = db.Put([]byte("a2"), []byte("v2"))
	_ = db.Put([]byte("b1"), []byte("v3"))

	keys := collectKeys(db.Keys())
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	count := 0
	err = db.Scan([]byte("a"), func(key, _ []byte, _ RecordMeta) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 keys with prefix, got %d", count)
	}

	count = 0
	err = db.Range([]byte("a1"), []byte("b"), func(key, _ []byte, _ RecordMeta) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 keys in range, got %d", count)
	}

	count = 0
	err = db.Sift(func(key []byte, _ RecordMeta) bool {
		return string(key) == "b1"
	}, func(_ []byte, _ []byte, _ RecordMeta) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("sift: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 key in sift, got %d", count)
	}

	foldedAny, err := db.Fold(0, func(acc any, _ []byte, _ []byte, _ RecordMeta) any {
		return acc.(int) + 1
	})
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	folded := foldedAny.(int)
	if folded != 3 {
		t.Fatalf("expected fold count 3, got %d", folded)
	}
}

func TestRunGC(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.PutWithTTL([]byte("k"), []byte("v"), 20*time.Millisecond); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	count, err := db.RunGC()
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 expired key, got %d", count)
	}
}

func TestReopenBackupDeleteAll(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Reopen(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	val, err := db.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("expected v after reopen, got %v %s", err, string(val))
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(backupDir); err != nil {
		t.Fatalf("backup: %v", err)
	}
	backupDB, err := Open(backupDir)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	val, err = backupDB.Get([]byte("k"))
	_ = backupDB.Close()
	if err != nil || string(val) != "v" {
		t.Fatalf("expected v in backup, got %v %s", err, string(val))
	}

	if err := db.DeleteAll(); err != nil {
		t.Fatalf("delete-all: %v", err)
	}
	keys := collectKeys(db.Keys())
	if len(keys) != 0 {
		t.Fatalf("expected empty keys after delete-all, got %d", len(keys))
	}
	if err := db.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put after delete-all: %v", err)
	}
}

func TestReclaimable(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	dead, total, ratio := db.Reclaimable()
	if total == 0 || dead == 0 || ratio <= 0 {
		t.Fatalf("expected reclaimable data, got dead=%d total=%d ratio=%f", dead, total, ratio)
	}
}
