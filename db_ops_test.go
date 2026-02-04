package bitgask

import (
	"path/filepath"
	"testing"
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
