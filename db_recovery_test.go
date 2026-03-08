package bitgask

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
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

func TestValidateDetectsUnexpectedEOFWithoutHints(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithHintFiles(false))
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
	if err := os.Truncate(dataPath, info.Size()-1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	report, err := Validate(dir, WithHintFiles(false))
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if report.CorruptRecords == 0 {
		t.Fatalf("expected corrupt records > 0")
	}
	if !report.HasErrors() {
		t.Fatalf("expected report to have errors")
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

func TestValidateDetectsHintMetadataMismatch(t *testing.T) {
	tests := []struct {
		name   string
		offset func(keyLen uint32) int64
		want   string
	}{
		{
			name: "size",
			offset: func(keyLen uint32) int64 {
				return int64(4 + keyLen + 12)
			},
			want: "size mismatch",
		},
		{
			name: "timestamp",
			offset: func(keyLen uint32) int64 {
				return int64(4 + keyLen + 16)
			},
			want: "timestamp mismatch",
		},
		{
			name: "expires",
			offset: func(keyLen uint32) int64 {
				return int64(4 + keyLen + 24)
			},
			want: "expires mismatch",
		},
		{
			name: "flags",
			offset: func(keyLen uint32) int64 {
				return int64(4 + keyLen + 32)
			},
			want: "flags mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := db.Put([]byte("k"), []byte("value")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			hintPath := filepath.Join(dir, "data", "000000001.hint")
			f, err := os.OpenFile(hintPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open hint: %v", err)
			}

			var lenBuf [4]byte
			if _, err := f.ReadAt(lenBuf[:], 0); err != nil {
				_ = f.Close()
				t.Fatalf("read hint length: %v", err)
			}
			keyLen := binary.LittleEndian.Uint32(lenBuf[:])
			b := []byte{0}
			offset := tt.offset(keyLen)
			if _, err := f.ReadAt(b, offset); err != nil {
				_ = f.Close()
				t.Fatalf("read hint byte: %v", err)
			}
			b[0] ^= 0x01
			if _, err := f.WriteAt(b, offset); err != nil {
				_ = f.Close()
				t.Fatalf("write hint byte: %v", err)
			}
			_ = f.Close()

			report, err := Validate(dir)
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			if !report.HasErrors() {
				t.Fatalf("expected validation errors")
			}
			if !validationErrorsContain(report.Errors, tt.want) {
				t.Fatalf("expected validation error containing %q, got %v", tt.want, report.Errors)
			}
		})
	}
}

func validationErrorsContain(errs []error, want string) bool {
	for _, err := range errs {
		if strings.Contains(err.Error(), want) {
			return true
		}
	}
	return false
}
