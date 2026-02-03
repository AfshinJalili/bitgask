//go:build windows

package file

import (
	"os"
	"time"
)

type Lock struct {
	file *os.File
}

// AcquireLock is a best-effort lock on Windows. It keeps the lock file open
// for the duration of the DB lifecycle.
func AcquireLock(path string, _ time.Duration) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &Lock{file: f}, nil
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
}
