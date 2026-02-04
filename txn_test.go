package bitgask

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestTxnReadYourWrites(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}

	txn := db.Transaction()
	defer txn.Discard()

	if err := txn.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("txn put: %v", err)
	}
	val, err := txn.Get([]byte("k"))
	if err != nil {
		t.Fatalf("txn get: %v", err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected v2 got %q", val)
	}
	val, err = db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("db get: %v", err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1 got %q", val)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	val, err = db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("db get after commit: %v", err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected v2 after commit got %q", val)
	}
}

func TestTxnSnapshotIsolation(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	txn := db.Transaction()
	defer txn.Discard()

	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err := txn.Get([]byte("k"))
	if err != nil {
		t.Fatalf("txn get: %v", err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1 got %q", val)
	}
}

func TestTxnCommitVisibility(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	txn := db.Transaction()
	if err := txn.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("txn put: %v", err)
	}
	if _, err := db.Get([]byte("k")); !errorsIs(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound before commit, got %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("db get after commit: %v", err)
	}
	if !bytes.Equal(val, []byte("v")) {
		t.Fatalf("expected v got %q", val)
	}
}

func TestTxnDelete(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	txn := db.Transaction()
	if err := txn.Delete([]byte("k")); err != nil {
		t.Fatalf("txn delete: %v", err)
	}
	if _, err := txn.Get([]byte("k")); !errorsIs(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound in txn, got %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.Get([]byte("k")); !errorsIs(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after commit, got %v", err)
	}
}

func TestTxnTTL(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	txn := db.Transaction()
	if err := txn.PutWithTTL([]byte("k"), []byte("v"), 30*time.Millisecond); err != nil {
		t.Fatalf("txn put ttl: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := db.Get([]byte("k")); !errorsIs(err, ErrExpired) {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
}

func TestTxnConflictLastWins(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	txn1 := db.Transaction()
	txn2 := db.Transaction()
	if err := txn1.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("txn1 put: %v", err)
	}
	if err := txn2.Put([]byte("k"), []byte("v3")); err != nil {
		t.Fatalf("txn2 put: %v", err)
	}
	if err := txn1.Commit(); err != nil {
		t.Fatalf("txn1 commit: %v", err)
	}
	if err := txn2.Commit(); err != nil {
		t.Fatalf("txn2 commit: %v", err)
	}
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("db get: %v", err)
	}
	if !bytes.Equal(val, []byte("v3")) {
		t.Fatalf("expected v3 got %q", val)
	}
}

func TestTxnIterOverlay(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put: %v", err)
	}

	txn := db.Transaction()
	if err := txn.Delete([]byte("a")); err != nil {
		t.Fatalf("txn delete: %v", err)
	}
	if err := txn.Put([]byte("b"), []byte("3")); err != nil {
		t.Fatalf("txn put: %v", err)
	}
	if err := txn.Put([]byte("c"), []byte("4")); err != nil {
		t.Fatalf("txn put: %v", err)
	}

	got := map[string]string{}
	err := txn.Iter(context.Background(), nil, func(key, value []byte, _ RecordMeta) bool {
		got[string(key)] = string(value)
		return true
	})
	if err != nil {
		t.Fatalf("txn iter: %v", err)
	}
	if _, ok := got["a"]; ok {
		t.Fatalf("expected a to be deleted")
	}
	if got["b"] != "3" || got["c"] != "4" || len(got) != 2 {
		t.Fatalf("unexpected iter result: %#v", got)
	}
}

func TestTxnCommitSync(t *testing.T) {
	db := OpenTestDB(t, "", WithSyncOnPut(true), WithSyncOnDelete(true))
	defer db.Close()

	calls := 0
	txnSyncHook = func() { calls++ }
	defer func() { txnSyncHook = nil }()

	txn := db.Transaction()
	if err := txn.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("txn put: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected sync hook once, got %d", calls)
	}
}

func TestTxnOverwriteSequence(t *testing.T) {
	t.Run("PutPut", func(t *testing.T) {
		db := OpenTestDB(t, "")
		defer db.Close()

		txn := db.Transaction()
		if err := txn.Put([]byte("k"), []byte("v1")); err != nil {
			t.Fatalf("txn put v1: %v", err)
		}
		if err := txn.Put([]byte("k"), []byte("v2")); err != nil {
			t.Fatalf("txn put v2: %v", err)
		}
		val, err := txn.Get([]byte("k"))
		if err != nil {
			t.Fatalf("txn get: %v", err)
		}
		if !bytes.Equal(val, []byte("v2")) {
			t.Fatalf("expected v2 got %q", val)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		val, err = db.Get([]byte("k"))
		if err != nil {
			t.Fatalf("db get: %v", err)
		}
		if !bytes.Equal(val, []byte("v2")) {
			t.Fatalf("expected v2 got %q", val)
		}
	})

	t.Run("PutDelete", func(t *testing.T) {
		db := OpenTestDB(t, "")
		defer db.Close()

		txn := db.Transaction()
		if err := txn.Put([]byte("k"), []byte("v1")); err != nil {
			t.Fatalf("txn put v1: %v", err)
		}
		if err := txn.Delete([]byte("k")); err != nil {
			t.Fatalf("txn delete: %v", err)
		}
		if _, err := txn.Get([]byte("k")); !errorsIs(err, ErrKeyNotFound) {
			t.Fatalf("expected ErrKeyNotFound in txn, got %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if _, err := db.Get([]byte("k")); !errorsIs(err, ErrKeyNotFound) {
			t.Fatalf("expected ErrKeyNotFound after commit, got %v", err)
		}
	})

	t.Run("DeletePut", func(t *testing.T) {
		db := OpenTestDB(t, "")
		defer db.Close()

		if err := db.Put([]byte("k"), []byte("v0")); err != nil {
			t.Fatalf("seed put: %v", err)
		}
		txn := db.Transaction()
		if err := txn.Delete([]byte("k")); err != nil {
			t.Fatalf("txn delete: %v", err)
		}
		if err := txn.Put([]byte("k"), []byte("v1")); err != nil {
			t.Fatalf("txn put: %v", err)
		}
		val, err := txn.Get([]byte("k"))
		if err != nil {
			t.Fatalf("txn get: %v", err)
		}
		if !bytes.Equal(val, []byte("v1")) {
			t.Fatalf("expected v1 got %q", val)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		val, err = db.Get([]byte("k"))
		if err != nil {
			t.Fatalf("db get: %v", err)
		}
		if !bytes.Equal(val, []byte("v1")) {
			t.Fatalf("expected v1 got %q", val)
		}
	})
}

func TestTxnRangeBoundaries(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		if err := db.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	txn := db.Transaction()
	var got []string
	if err := txn.Range([]byte("b"), []byte("d"), func(key, _ []byte, _ RecordMeta) bool {
		got = append(got, string(key))
		return true
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	assertStringSlice(t, got, []string{"b", "c"})
}

func TestTxnScanPrefix(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	if err := db.Put([]byte("b1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("b2"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("x1"), []byte("v3")); err != nil {
		t.Fatalf("put: %v", err)
	}

	txn := db.Transaction()
	if err := txn.Put([]byte("b3"), []byte("v4")); err != nil {
		t.Fatalf("txn put: %v", err)
	}

	var got []string
	if err := txn.Scan([]byte("b"), func(key, _ []byte, _ RecordMeta) bool {
		got = append(got, string(key))
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	assertStringSlice(t, got, []string{"b1", "b2", "b3"})
}

func TestTxnOversizedKeyValue(t *testing.T) {
	db := OpenTestDB(t, "", WithMaxKeySize(2), WithMaxValueSize(2))
	defer db.Close()

	txn := db.Transaction()
	if err := txn.Put([]byte("key"), []byte("v")); !errorsIs(err, ErrOversized) {
		t.Fatalf("expected ErrOversized for key, got %v", err)
	}
	if err := txn.PutWithTTL([]byte("k"), []byte("val"), time.Second); !errorsIs(err, ErrOversized) {
		t.Fatalf("expected ErrOversized for value, got %v", err)
	}
}

func TestTxnExpiryAtCommit(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	txn := db.Transaction()
	if err := txn.PutWithTTL([]byte("k"), []byte("v"), 10*time.Millisecond); err != nil {
		t.Fatalf("txn put ttl: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.Get([]byte("k")); !errorsIs(err, ErrExpired) {
		t.Fatalf("expected ErrExpired after commit, got %v", err)
	}
}

func TestTxnIterOverlayOrder(t *testing.T) {
	db := OpenTestDB(t, "")
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	txn := db.Transaction()
	if err := txn.Delete([]byte("b")); err != nil {
		t.Fatalf("txn delete: %v", err)
	}
	if err := txn.Put([]byte("aa"), []byte("1")); err != nil {
		t.Fatalf("txn put: %v", err)
	}
	if err := txn.Put([]byte("d"), []byte("2")); err != nil {
		t.Fatalf("txn put: %v", err)
	}

	var got []string
	if err := txn.Iter(context.Background(), nil, func(key, _ []byte, _ RecordMeta) bool {
		got = append(got, string(key))
		return true
	}); err != nil {
		t.Fatalf("iter: %v", err)
	}
	assertStringSlice(t, got, []string{"a", "aa", "c", "d"})
}

func errorsIs(err, target error) bool {
	return err != nil && errors.Is(err, target)
}

func assertStringSlice(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}
