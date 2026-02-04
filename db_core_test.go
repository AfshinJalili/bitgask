package bitgask

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestOversizedLimits(t *testing.T) {
	db, err := Open(t.TempDir(), WithMaxKeySize(2), WithMaxValueSize(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("abc"), []byte("v")); err != ErrOversized {
		t.Fatalf("expected ErrOversized for key, got %v", err)
	}
	if err := db.Put([]byte("k"), []byte("value")); err != ErrOversized {
		t.Fatalf("expected ErrOversized for value, got %v", err)
	}
	if err := db.Delete([]byte("abc")); err != ErrOversized {
		t.Fatalf("expected ErrOversized for delete key, got %v", err)
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	opts := []Option{
		WithCompression(CompressionSnappy),
		WithCompressionThreshold(1),
		WithSyncOnPut(false),
		WithSyncOnDelete(false),
		WithSyncInterval(time.Hour),
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	payload := []byte(strings.Repeat("a", 1024))
	if err := db.Put([]byte("k"), payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != string(payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestMetaExpired(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.PutWithTTL([]byte("k"), []byte("v"), 15*time.Millisecond); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := db.Meta([]byte("k")); err != ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
	ok, err := db.Has([]byte("k"))
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if ok {
		t.Fatalf("expected has=false after expiry")
	}
}

func TestKeysContextCancel(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("a"), []byte("1"))
	_ = db.Put([]byte("b"), []byte("2"))
	_ = db.Put([]byte("c"), []byte("3"))

	ctx, cancel := context.WithCancel(context.Background())
	ch := db.KeysContext(ctx)
	<-ch
	cancel()
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("keys context did not close after cancel")
	}
}

func TestNewIteratorSnapshot(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("k1"), []byte("v1"))
	it, err := db.NewIterator(nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	_ = db.Put([]byte("k2"), []byte("v2"))

	count := 0
	for it.Next() {
		count++
		if string(it.Key()) != "k1" {
			t.Fatalf("unexpected key: %s", string(it.Key()))
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
	_ = it.Close()
	if count != 1 {
		t.Fatalf("expected 1 key, got %d", count)
	}
}
