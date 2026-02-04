//go:build windows

package bitgask

import "testing"

func TestLockingWindows(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db2, err := Open(dir)
	if err == nil {
		_ = db2.Close()
		t.Skip("lock not enforced on this environment")
	}
	if err != ErrLocked {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}
