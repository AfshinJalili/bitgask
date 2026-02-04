//go:build !windows

package file

import "os"

// FsyncDir best-effort fsync of a directory for durability after renames.
func FsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
