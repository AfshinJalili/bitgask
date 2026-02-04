package bitgask

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/AfshinJalili/bitgask/internal/testutil"
)

func TestValidateDetectsCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = db.Close()

	dataPath := filepath.Join(dir, "data", "000000001.data")
	info, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("stat data: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("unexpected empty data file")
	}
	f, err := os.OpenFile(dataPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open data: %v", err)
	}
	buf := []byte{0}
	off := info.Size() - 1
	if _, err := f.ReadAt(buf, off); err != nil {
		_ = f.Close()
		t.Fatalf("read: %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, off); err != nil {
		_ = f.Close()
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	report, err := Validate(dir)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if report.CorruptRecords == 0 {
		t.Fatalf("expected corrupt records > 0")
	}
}

func TestValidateOnOpenSkipsHints(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = db.Close()

	testutil.CorruptHint(t, testutil.DataDir(dir))

	db, err = Open(dir, WithValidateOnOpen(true))
	if err != nil {
		t.Fatalf("open validate: %v", err)
	}
	defer db.Close()
	val, err := db.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("expected value after validate-on-open, got %v %s", err, string(val))
	}
}

func TestValidateDetectsHintMismatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = db.Close()

	testutil.CorruptHint(t, testutil.DataDir(dir))
	report, err := Validate(dir)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(report.Errors) == 0 {
		t.Fatalf("expected hint validation error")
	}
}
