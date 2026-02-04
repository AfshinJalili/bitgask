//go:build !windows

package file

import (
	"os"
	"testing"
)

func TestFsyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/test", []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := FsyncDir(dir); err != nil {
		t.Fatalf("fsync: %v", err)
	}
}
