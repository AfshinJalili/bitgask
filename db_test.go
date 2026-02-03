package bitgask

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func testOptions() Options {
	return Options{
		SyncOnPut:            false,
		SyncOnDelete:         false,
		SyncInterval:         time.Hour,
		MergeInterval:        time.Hour,
		MergeMinTotal:        1 << 30,
		MergeTriggerRatio:    0.9,
		Compression:          CompressionNone,
		UseHintFiles:         true,
		HintFileSync:         false,
		MaxDataFileSize:      256,
		CompressionThreshold: 256,
	}
}

func TestBasicCRUD(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("unexpected value: %s", string(val))
	}
	ok, err := db.Has([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("has: %v %v", ok, err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.Get([]byte("k")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("unexpected value: %s", string(val))
	}
}

func TestTTLExpiry(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v"), 20*time.Millisecond); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := db.Get([]byte("k")); err != ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
	ok, err := db.Has([]byte("k"))
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if ok {
		t.Fatalf("expected has=false")
	}
}

func TestRotationAndStats(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		if err := db.Put([]byte{byte('a' + i)}, []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	stats := db.Stats()
	if stats.DataFiles < 2 {
		t.Fatalf("expected multiple data files, got %d", stats.DataFiles)
	}
}

func TestMerge(t *testing.T) {
	opts := testOptions()
	opts.MergeMinTotal = 0
	opts.MergeTriggerRatio = 0
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := db.Merge(MergeOptions{Force: true}); err != nil {
		t.Fatalf("merge: %v", err)
	}

	if _, err := db.Get([]byte("a")); err != ErrKeyNotFound {
		t.Fatalf("expected a deleted, got %v", err)
	}
	val, err := db.Get([]byte("b"))
	if err != nil || string(val) != "2" {
		t.Fatalf("expected b=2, got %v %s", err, string(val))
	}
	stats := db.Stats()
	if stats.DeadBytes != 0 {
		t.Fatalf("expected no dead bytes after merge, got %d", stats.DeadBytes)
	}
}

func TestRepairTruncatesCorruptTail(t *testing.T) {
	opts := testOptions()
	opts.ValidateOnOpen = true
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// truncate tail
	dataFile := filepath.Join(dir, "data", "000000001.data")
	info, err := os.Stat(dataFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(dataFile, info.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	if _, err := Open(dir, opts); err == nil {
		t.Fatalf("expected open to fail on corrupt data")
	}

	db, err = Repair(dir, opts)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	defer db.Close()
	_, err = db.Get([]byte("k1"))
	if err != ErrKeyNotFound && err != ErrExpired {
		t.Fatalf("expected record dropped after repair, got %v", err)
	}
}

func TestLocking(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("locking is best-effort on windows")
	}
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := Open(dir, opts); err != ErrLocked {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}
