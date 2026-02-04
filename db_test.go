package bitgask

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func testOptions() []Option {
	return []Option{
		WithSyncOnPut(false),
		WithSyncOnDelete(false),
		WithSyncInterval(time.Hour),
		WithMergeInterval(time.Hour),
		WithMergeMinTotal(1 << 30),
		WithMergeTriggerRatio(0.9),
		WithCompression(CompressionNone),
		WithHintFiles(true),
		WithHintFileSync(false),
		WithMaxDataFileSize(256),
		WithCompressionThreshold(256),
	}
}

type bufferLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *bufferLogger) Printf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}

func (l *bufferLogger) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, msg := range l.logs {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

func TestBasicCRUD(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("unexpected value: %s", string(val))
	}
	ok, err := db.Has([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("has: %v %v", ok, err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.Get([]byte("k")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dir, opts...)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("unexpected value: %s", string(val))
	}
}

func TestTTLExpiry(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.PutWithTTL([]byte("k"), []byte("v"), 20*time.Millisecond); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := db.Get([]byte("k")); err != ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
	ok, err := db.Has([]byte("k"))
	if err != nil {
		t.Fatalf("has: %v", err)
	}
	if ok {
		t.Fatalf("expected has=false")
	}
}

func TestRotationAndStats(t *testing.T) {
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		if err := db.Put([]byte{byte('a' + i)}, []byte("value")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	stats := db.Stats()
	if stats.DataFiles < 2 {
		t.Fatalf("expected multiple data files, got %d", stats.DataFiles)
	}
}

func TestRotationReadOldFile(t *testing.T) {
	opts := append(testOptions(), WithMaxDataFileSize(100))
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k1"), []byte("value-value-value-value-value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("k2"), []byte("value-value-value-value-value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "value-value-value-value-value" {
		t.Fatalf("unexpected value: %s", string(val))
	}
}

func TestHintWriteFailureBestEffort(t *testing.T) {
	logger := &bufferLogger{}
	opts := append(testOptions(), WithLogger(logger))
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if db.active == nil || db.active.hint == nil {
		t.Fatalf("expected hint file to be open")
	}
	_ = db.active.hint.Close()

	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("unexpected value: %s", string(val))
	}
	if !logger.contains("hint write failed") {
		t.Fatalf("expected hint write failure to be logged")
	}
}

func TestHintCorruptionFallback(t *testing.T) {
	logger := &bufferLogger{}
	dir := t.TempDir()
	db, err := Open(dir, WithLogger(logger))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
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
		t.Fatalf("read hint: %v", err)
	}
	keyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if keyLen > 0 {
		b := []byte{0}
		if _, err := f.ReadAt(b, 4); err != nil {
			_ = f.Close()
			t.Fatalf("read key byte: %v", err)
		}
		b[0] ^= 0xFF
		if _, err := f.WriteAt(b, 4); err != nil {
			_ = f.Close()
			t.Fatalf("write key byte: %v", err)
		}
	}
	_ = f.Close()

	db, err = Open(dir, WithLogger(logger))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("unexpected value: %s", string(val))
	}
	if !logger.contains("hint validation failed") {
		t.Fatalf("expected hint validation failure to be logged")
	}
}

func TestIterKeysAndIter(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_ = db.Put([]byte("a1"), []byte("v1"))
	_ = db.Put([]byte("a2"), []byte("v2"))
	_ = db.Put([]byte("b1"), []byte("v3"))

	count := 0
	if err := db.IterKeys(context.Background(), func(_ []byte) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("iterkeys: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 keys, got %d", count)
	}

	count = 0
	if err := db.Iter(context.Background(), []byte("a"), func(_ []byte, _ []byte, _ RecordMeta) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("iter: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 keys with prefix, got %d", count)
	}

	count = 0
	if err := db.IterKeys(context.Background(), func(_ []byte) bool {
		count++
		return false
	}); err != nil {
		t.Fatalf("iterkeys: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected early stop at 1, got %d", count)
	}
}

func TestMerge(t *testing.T) {
	opts := append(testOptions(), WithMergeMinTotal(0), WithMergeTriggerRatio(0))
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := db.Merge(MergeOptions{Force: true}); err != nil {
		t.Fatalf("merge: %v", err)
	}

	if _, err := db.Get([]byte("a")); err != ErrKeyNotFound {
		t.Fatalf("expected a deleted, got %v", err)
	}
	val, err := db.Get([]byte("b"))
	if err != nil || string(val) != "2" {
		t.Fatalf("expected b=2, got %v %s", err, string(val))
	}
	stats := db.Stats()
	if stats.DeadBytes != 0 {
		t.Fatalf("expected no dead bytes after merge, got %d", stats.DeadBytes)
	}
}

func TestRepairTruncatesCorruptTail(t *testing.T) {
	opts := append(testOptions(), WithValidateOnOpen(true))
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// truncate tail
	dataFile := filepath.Join(dir, "data", "000000001.data")
	info, err := os.Stat(dataFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(dataFile, info.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	if _, err := Open(dir, opts...); err == nil {
		t.Fatalf("expected open to fail on corrupt data")
	}

	db, err = Repair(dir, opts...)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	defer db.Close()
	_, err = db.Get([]byte("k1"))
	if err != ErrKeyNotFound && err != ErrExpired {
		t.Fatalf("expected record dropped after repair, got %v", err)
	}
}

func TestLocking(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("locking is best-effort on windows")
	}
	opts := testOptions()
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	exitCode := runOpenHelper(t, dir)
	if exitCode != 10 {
		t.Fatalf("expected ErrLocked from helper, got exit code %d", exitCode)
	}
}

func TestMergeKeepsLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("locking semantics vary on windows")
	}
	opts := append(testOptions(), WithMergeMinTotal(0), WithMergeTriggerRatio(0))
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Merge(MergeOptions{Force: true}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	exitCode := runOpenHelper(t, dir)
	if exitCode != 10 {
		t.Fatalf("expected ErrLocked after merge, got exit code %d", exitCode)
	}
}

func TestReopenConcurrentReads(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			_, _ = db.Get([]byte("k"))
		}
		close(done)
	}()

	if err := db.Reopen(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	<-done
}

func TestDisableHintFiles(t *testing.T) {
	opts := append(testOptions(), WithHintFiles(false))
	dir := t.TempDir()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".hint" {
			t.Fatalf("expected no hint files, found %s", entry.Name())
		}
	}
}

func TestOpenHelperProcess(t *testing.T) {
	if os.Getenv("BITGASK_HELPER_PROCESS") != "1" {
		return
	}
	dir := os.Getenv("BITGASK_HELPER_DIR")
	_, err := Open(dir, testOptions()...)
	if err == nil {
		os.Exit(0)
	}
	if err == ErrLocked {
		os.Exit(10)
	}
	os.Exit(20)
}

func runOpenHelper(t *testing.T, dir string) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestOpenHelperProcess")
	cmd.Env = append(os.Environ(),
		"BITGASK_HELPER_PROCESS=1",
		"BITGASK_HELPER_DIR="+dir,
	)
	err := cmd.Run()
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode()
	}
	t.Fatalf("helper run failed: %v", err)
	return -1
}
