package testutil

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AfshinJalili/bitgask/internal/file"
	"github.com/AfshinJalili/bitgask/internal/record"
)

func CorruptTail(tb testing.TB, dataDir string, trim int64) {
	tb.Helper()
	filePath, err := lastDataFile(dataDir)
	if err != nil {
		tb.Fatalf("last data file: %v", err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		tb.Fatalf("stat: %v", err)
	}
	if info.Size() <= trim {
		tb.Fatalf("file too small to truncate")
	}
	if err := os.Truncate(filePath, info.Size()-trim); err != nil {
		tb.Fatalf("truncate: %v", err)
	}
}

func CorruptHint(tb testing.TB, dataDir string) {
	tb.Helper()
	hintPath, err := lastHintFile(dataDir)
	if err != nil {
		tb.Fatalf("hint file: %v", err)
	}
	f, err := os.OpenFile(hintPath, os.O_RDWR, 0)
	if err != nil {
		tb.Fatalf("open hint: %v", err)
	}
	defer func() { _ = f.Close() }()
	buf := []byte{0}
	if _, err := f.ReadAt(buf, 4); err != nil {
		if err == io.EOF {
			return
		}
		tb.Fatalf("read hint: %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, 4); err != nil {
		tb.Fatalf("write hint: %v", err)
	}
}

func CollectKeys(ch <-chan []byte) [][]byte {
	keys := make([][]byte, 0)
	for k := range ch {
		keys = append(keys, append([]byte(nil), k...))
	}
	return keys
}

func WaitForExpiry(d time.Duration) {
	if d <= 0 {
		return
	}
	time.Sleep(d + 5*time.Millisecond)
}

func ReadRecordAt(tb testing.TB, dataDir string, fileID uint32, offset int64, width int) (record.Record, int, error) {
	tb.Helper()
	path := filepath.Join(dataDir, file.DataFileName(fileID, width))
	f, err := os.Open(path)
	if err != nil {
		return record.Record{}, 0, err
	}
	defer func() { _ = f.Close() }()
	return record.ReadAt(f, offset)
}

func DataDir(dir string) string {
	return filepath.Join(dir, "data")
}

func ActiveFileID(dataDir string) (uint32, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return 0, err
	}
	var maxID uint32
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !file.IsDataFile(entry.Name()) {
			continue
		}
		id, ok := file.ParseFileID(entry.Name())
		if !ok {
			continue
		}
		if id > maxID {
			maxID = id
		}
	}
	if maxID == 0 {
		return 0, fmt.Errorf("no data files")
	}
	return maxID, nil
}

func lastDataFile(dataDir string) (string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return "", err
	}
	var maxID uint32
	var name string
	for _, entry := range entries {
		if entry.IsDir() || !file.IsDataFile(entry.Name()) {
			continue
		}
		id, ok := file.ParseFileID(entry.Name())
		if !ok {
			continue
		}
		if id > maxID {
			maxID = id
			name = entry.Name()
		}
	}
	if name == "" {
		return "", fmt.Errorf("no data files")
	}
	return filepath.Join(dataDir, name), nil
}

func lastHintFile(dataDir string) (string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return "", err
	}
	var maxID uint32
	var name string
	for _, entry := range entries {
		if entry.IsDir() || !file.IsHintFile(entry.Name()) {
			continue
		}
		id, ok := file.ParseFileID(entry.Name())
		if !ok {
			continue
		}
		if id > maxID {
			maxID = id
			name = entry.Name()
		}
	}
	if name == "" {
		return "", fmt.Errorf("no hint files")
	}
	return filepath.Join(dataDir, name), nil
}

func BytesRepeat(b byte, n int) []byte {
	return bytes.Repeat([]byte{b}, n)
}
