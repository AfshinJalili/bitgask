//go:build windows

package file

// FsyncDir is a no-op on Windows.
func FsyncDir(string) error {
	return nil
}
