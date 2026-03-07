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

func TestMergeWithConfiguredConcurrencyPreservesData(t *testing.T) {
	opts := []Option{
		WithMergeMinTotal(0),
		WithMergeTriggerRatio(0),
		WithMergeConcurrency(4),
		WithSyncOnPut(false),
		WithSyncOnDelete(false),
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 128; i++ {
		key := []byte(string(rune('a'+(i%26))) + "-" + string(rune('0'+(i%10))))
		val := []byte("value-" + string(rune('A'+(i%26))))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := db.Put([]byte("stable"), []byte("v1")); err != nil {
		t.Fatalf("put stable: %v", err)
	}
	if err := db.Put([]byte("stable"), []byte("v2")); err != nil {
		t.Fatalf("overwrite stable: %v", err)
	}
	if err := db.Put([]byte("deleted"), []byte("gone")); err != nil {
		t.Fatalf("put deleted: %v", err)
	}
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := db.Merge(MergeOptions{Force: true}); err != nil {
		t.Fatalf("merge: %v", err)
	}

	val, err := db.Get([]byte("stable"))
	if err != nil {
		t.Fatalf("get stable: %v", err)
	}
	if string(val) != "v2" {
		t.Fatalf("expected stable value v2, got %q", val)
	}
	if _, err := db.Get([]byte("deleted")); err != ErrKeyNotFound {
		t.Fatalf("expected deleted key to stay deleted, got %v", err)
	}
	if db.Stats().Keys == 0 {
		t.Fatalf("expected merged database to retain keys")
	}
}
