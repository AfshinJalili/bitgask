package bitgask

import (
	"testing"
	"time"
)

func TestMergeRemovesExpired(t *testing.T) {
	opts := []Option{WithMergeMinTotal(0), WithMergeTriggerRatio(0)}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.PutWithTTL([]byte("k"), []byte("v"), 10*time.Millisecond); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := db.Merge(MergeOptions{Force: true}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, err := db.Get([]byte("k")); err != ErrKeyNotFound && err != ErrExpired {
		t.Fatalf("expected expired key removed, got %v", err)
	}
}

func TestCompactIfNeededTriggers(t *testing.T) {
	opts := []Option{
		WithMergeMinTotal(1),
		WithMergeTriggerRatio(0.0001),
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("a"), []byte("1"))
	_ = db.Delete([]byte("a"))

	ran, err := db.CompactIfNeeded()
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !ran {
		t.Fatalf("expected compaction to run")
	}
	stats := db.Stats()
	if stats.DeadBytes != 0 {
		t.Fatalf("expected dead bytes to be 0 after merge")
	}
}
