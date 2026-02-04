//go:build windows

package file

import (
	"os"
	"time"

	"golang.org/x/sys/windows"
)

type Lock struct {
	file *os.File
}

func AcquireLock(path string, timeout time.Duration) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		err = lockFile(f)
		if err == nil {
			return &Lock{file: f}, nil
		}
		if err == windows.ERROR_LOCK_VIOLATION {
			if timeout == 0 || time.Now().After(deadline) {
				_ = f.Close()
				return nil, err
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		_ = f.Close()
		return nil, err
	}
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	var ol windows.Overlapped
	err := windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, &ol)
	_ = l.file.Close()
	return err
}

func lockFile(f *os.File) error {
	var ol windows.Overlapped
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&ol,
	)
}
