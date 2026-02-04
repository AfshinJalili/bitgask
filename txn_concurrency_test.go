package bitgask

import (
	"bytes"
	"sync"
	"testing"
)

func TestTxnSnapshotIsolationConcurrent(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}

	txn := db.Transaction()
	defer txn.Discard()

	val, err := txn.Get([]byte("k"))
	if err != nil {
		t.Fatalf("txn get: %v", err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1 got %q", val)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = db.Put([]byte("k"), []byte("v2"))
	}()
	wg.Wait()

	val, err = txn.Get([]byte("k"))
	if err != nil {
		t.Fatalf("txn get after update: %v", err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1 after update got %q", val)
	}

	val, err = db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("db get: %v", err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected v2 got %q", val)
	}
}
