package bitgask

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBackupProducesCopy(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(backupDir); err != nil {
		t.Fatalf("backup: %v", err)
	}
	backup, err := Open(backupDir)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	defer backup.Close()
	val, err := backup.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("expected backup value, got %v %s", err, string(val))
	}
}

func TestBackupRejectsNonEmptyDestination(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := os.MkdirAll(filepath.Join(backupDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir backup data: %v", err)
	}
	stalePath := filepath.Join(backupDir, "data", "000000999.data")
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}

	err = db.Backup(backupDir)
	if err == nil || !strings.Contains(err.Error(), "backup destination must be empty") {
		t.Fatalf("expected non-empty destination error, got %v", err)
	}
}

func TestDeleteAllResets(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("k1"), []byte("v1"))
	if err := db.DeleteAll(); err != nil {
		t.Fatalf("delete-all: %v", err)
	}
	keys := collectKeys(db.Keys())
	if len(keys) != 0 {
		t.Fatalf("expected empty keys, got %d", len(keys))
	}
	stats := db.Stats()
	if stats.Keys != 0 {
		t.Fatalf("expected stats keys 0, got %d", stats.Keys)
	}
	if err := db.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put after delete-all: %v", err)
	}
}

func TestDeleteAllRestoresStateAfterRemoveFailure(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}

	errBoom := errors.New("remove failed")
	var calls int
	removeFileHook = func(path string) error {
		if calls == 0 && (strings.HasSuffix(path, ".data") || strings.HasSuffix(path, ".hint")) {
			calls++
			return errBoom
		}
		return os.Remove(path)
	}
	t.Cleanup(func() { removeFileHook = nil })

	if err := db.DeleteAll(); !errors.Is(err, errBoom) {
		t.Fatalf("expected delete-all to return remove error, got %v", err)
	}
	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("expected state restored after delete-all failure, got %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("expected restored value v1, got %q", val)
	}
}

func TestReclaimableShrinksAfterMerge(t *testing.T) {
	opts := []Option{WithMergeMinTotal(0), WithMergeTriggerRatio(0)}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("k"), []byte("v"))
	_ = db.Delete([]byte("k"))
	dead, total, ratio := db.Reclaimable()
	if total == 0 || dead == 0 || ratio <= 0 {
		t.Fatalf("expected reclaimable data, got dead=%d total=%d ratio=%f", dead, total, ratio)
	}
	if err := db.Merge(MergeOptions{Force: true}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	dead, total, ratio = db.Reclaimable()
	if dead != 0 || ratio != 0 {
		t.Fatalf("expected reclaimable reset, got dead=%d total=%d ratio=%f", dead, total, ratio)
	}
}

func TestExpireSemantics(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ok, err := db.Expire([]byte("missing"), time.Second)
	if err != nil {
		t.Fatalf("expire missing: %v", err)
	}
	if ok {
		t.Fatalf("expected missing key to return false")
	}

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	ok, err = db.Expire([]byte("k"), 30*time.Millisecond)
	if err != nil {
		t.Fatalf("expire with ttl: %v", err)
	}
	if !ok {
		t.Fatalf("expected existing key to return true")
	}
	time.Sleep(45 * time.Millisecond)
	if _, err := db.Get([]byte("k")); err != ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}

	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put second: %v", err)
	}
	ok, err = db.Expire([]byte("k"), 0)
	if err != nil {
		t.Fatalf("expire delete: %v", err)
	}
	if !ok {
		t.Fatalf("expected ttl=0 to delete existing key")
	}
	if _, err := db.Get([]byte("k")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestExpireDoesNotResurrectStaleValue(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncOnPut(false), WithSyncOnDelete(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("k")
	for i := 0; i < 200; i++ {
		if err := db.Put(key, []byte("v1")); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start
			if _, err := db.Expire(key, time.Minute); err != nil {
				t.Errorf("expire: %v", err)
			}
		}()

		go func() {
			defer wg.Done()
			<-start
			if err := db.Put(key, []byte("v2")); err != nil {
				t.Errorf("put: %v", err)
			}
		}()

		close(start)
		wg.Wait()

		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(val) != "v2" {
			t.Fatalf("iteration %d: expected latest value v2, got %q", i, val)
		}
	}
}

func TestExpireDeletePropagatesSyncFailure(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncOnDelete(true))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	errSync := fmt.Errorf("sync failed")
	syncDataFileHook = func(_ *os.File) error { return errSync }
	t.Cleanup(func() { syncDataFileHook = nil })

	ok, err := db.Expire([]byte("k"), 0)
	if !errors.Is(err, errSync) {
		t.Fatalf("expected sync error, got %v", err)
	}
	if ok {
		t.Fatalf("expected expire delete to report failure")
	}
	val, err := db.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("expected original value to remain visible, got %v %q", err, val)
	}
}

func TestExpireUpdatePropagatesSyncFailure(t *testing.T) {
	db, err := Open(t.TempDir(), WithSyncOnPut(true))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	errSync := fmt.Errorf("sync failed")
	syncDataFileHook = func(_ *os.File) error { return errSync }
	t.Cleanup(func() { syncDataFileHook = nil })

	ok, err := db.Expire([]byte("k"), time.Minute)
	if !errors.Is(err, errSync) {
		t.Fatalf("expected sync error, got %v", err)
	}
	if ok {
		t.Fatalf("expected expire update to report failure")
	}
	val, err := db.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("expected original value to remain visible, got %v %q", err, val)
	}
}

func TestScanKeysUsesCursorBudget(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, key := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(key), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	keys, cursor, err := db.ScanKeys(nil, 1, func(key []byte) bool {
		return string(key) == "c"
	})
	if err != nil {
		t.Fatalf("scan keys: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected no match on first bounded page, got %d", len(keys))
	}
	if string(cursor) != "a" {
		t.Fatalf("expected cursor to advance to first visited key, got %q", cursor)
	}
}
