package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AfshinJalili/bitgask"
)

func TestCommonFlagsDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dir != "./data" {
		t.Fatalf("dir: expected ./data, got %s", cfg.dir)
	}
	if cfg.maxKey != 64 {
		t.Fatalf("maxKey: expected 64, got %d", cfg.maxKey)
	}
	if cfg.maxValue != 64<<10 {
		t.Fatalf("maxValue: expected %d, got %d", 64<<10, cfg.maxValue)
	}
	if cfg.defaultTTL != 0 {
		t.Fatalf("defaultTTL: expected 0, got %v", cfg.defaultTTL)
	}
	if !cfg.syncWrites {
		t.Fatalf("syncWrites: expected true")
	}
	if cfg.syncInterval != 0 {
		t.Fatalf("syncInterval: expected 0, got %v", cfg.syncInterval)
	}
	if cfg.compression != "snappy" {
		t.Fatalf("compression: expected snappy, got %s", cfg.compression)
	}
	if cfg.mergeInterval != 10*time.Minute {
		t.Fatalf("mergeInterval: expected 10m, got %v", cfg.mergeInterval)
	}
	if !cfg.hintFiles {
		t.Fatalf("hintFiles: expected true")
	}
	if !cfg.hintFileSync {
		t.Fatalf("hintFileSync: expected true")
	}
	if cfg.maxDataSize != 256<<20 {
		t.Fatalf("maxDataSize: expected %d, got %d", 256<<20, cfg.maxDataSize)
	}
	if cfg.fileIDWidth != 9 {
		t.Fatalf("fileIDWidth: expected 9, got %d", cfg.fileIDWidth)
	}
	if cfg.validateOnOpen {
		t.Fatalf("validateOnOpen: expected false")
	}
}

func TestCommonFlagsOverrides(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	args := []string{
		"-dir", "custom",
		"-max-key", "1",
		"-max-value", "2",
		"-ttl", "5s",
		"-sync=false",
		"-sync-interval", "1s",
		"-compression", "none",
		"-merge-interval", "2s",
		"-hint-files=false",
		"-hint-sync=false",
		"-max-data", "1024",
		"-file-id-width", "7",
		"-validate-on-open=true",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dir != "custom" || cfg.maxKey != 1 || cfg.maxValue != 2 {
		t.Fatalf("basic overrides failed: %#v", cfg)
	}
	if cfg.defaultTTL != 5*time.Second {
		t.Fatalf("ttl: expected 5s got %v", cfg.defaultTTL)
	}
	if cfg.syncWrites {
		t.Fatalf("syncWrites: expected false")
	}
	if cfg.syncInterval != time.Second {
		t.Fatalf("syncInterval: expected 1s got %v", cfg.syncInterval)
	}
	if cfg.compression != "none" {
		t.Fatalf("compression: expected none got %s", cfg.compression)
	}
	if cfg.mergeInterval != 2*time.Second {
		t.Fatalf("mergeInterval: expected 2s got %v", cfg.mergeInterval)
	}
	if cfg.hintFiles || cfg.hintFileSync {
		t.Fatalf("hint flags expected false")
	}
	if cfg.maxDataSize != 1024 {
		t.Fatalf("maxDataSize: expected 1024 got %d", cfg.maxDataSize)
	}
	if cfg.fileIDWidth != 7 {
		t.Fatalf("fileIDWidth: expected 7 got %d", cfg.fileIDWidth)
	}
	if !cfg.validateOnOpen {
		t.Fatalf("validateOnOpen: expected true")
	}
}

func TestCommonFlagsOptionsMapping(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := &commonFlags{}
	cfg.add(fs)
	args := []string{
		"-max-key", "1",
		"-max-value", "1",
		"-ttl", "25ms",
		"-compression", "none",
		"-sync=false",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	db, err := bitgask.Open(t.TempDir(), cfg.options()...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("long"), []byte("")); err != bitgask.ErrOversized {
		t.Fatalf("expected ErrOversized for key, got %v", err)
	}
	if err := db.Put([]byte("k"), []byte("long")); err != bitgask.ErrOversized {
		t.Fatalf("expected ErrOversized for value, got %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := db.Get([]byte("k")); err != bitgask.ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
}

func TestBuildInfoIncludesVersionMetadata(t *testing.T) {
	oldVersion, oldCommit, oldDate := version, commit, date
	version, commit, date = "1.2.3", "abc123", "2026-03-08"
	t.Cleanup(func() {
		version, commit, date = oldVersion, oldCommit, oldDate
	})

	got := buildInfo()
	want := "version=1.2.3 commit=abc123 date=2026-03-08"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestCmdVersionWritesBuildInfo(t *testing.T) {
	oldVersion, oldCommit, oldDate := version, commit, date
	version, commit, date = "9.9.9", "deadbeef", "2026-03-08"
	t.Cleanup(func() {
		version, commit, date = oldVersion, oldCommit, oldDate
	})

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = oldStdout })

	if err := cmdVersion(); err != nil {
		t.Fatalf("cmdVersion: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	want := buildInfo() + "\n"
	if string(out) != want {
		t.Fatalf("expected %q, got %q", want, out)
	}
}

func captureOutputMain(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	runErr := fn()
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return string(out), runErr
}

func TestCLIDataCommandFlow(t *testing.T) {
	dir := t.TempDir()

	if err := cmdPut([]string{"-dir", dir, "alpha", "one"}); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := cmdPut([]string{"-dir", dir, "beta", "two"}); err != nil {
		t.Fatalf("put beta: %v", err)
	}

	out, err := captureOutputMain(t, func() error {
		return cmdGet([]string{"-dir", dir, "alpha"})
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if out != "one" {
		t.Fatalf("expected get output one, got %q", out)
	}

	out, err = captureOutputMain(t, func() error {
		return cmdExists([]string{"-dir", dir, "alpha"})
	})
	if err != nil {
		t.Fatalf("exists alpha: %v", err)
	}
	if out != "1\n" {
		t.Fatalf("expected exists output 1, got %q", out)
	}

	out, err = captureOutputMain(t, func() error {
		return cmdKeys([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	keys := strings.Fields(out)
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %v", keys)
	}
	if !(keys[0] == "alpha" || keys[1] == "alpha") || !(keys[0] == "beta" || keys[1] == "beta") {
		t.Fatalf("unexpected keys output: %v", keys)
	}

	out, err = captureOutputMain(t, func() error {
		return cmdScan([]string{"-dir", dir, "alp"})
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if strings.TrimSpace(out) != "alpha" {
		t.Fatalf("expected scan output alpha, got %q", out)
	}

	if err := cmdDel([]string{"-dir", dir, "alpha"}); err != nil {
		t.Fatalf("del alpha: %v", err)
	}
	out, err = captureOutputMain(t, func() error {
		return cmdExists([]string{"-dir", dir, "alpha"})
	})
	if err != nil {
		t.Fatalf("exists alpha after delete: %v", err)
	}
	if out != "0\n" {
		t.Fatalf("expected exists output 0 after delete, got %q", out)
	}

	if err := cmdDeleteAll([]string{"-dir", dir}); err != nil {
		t.Fatalf("delete-all: %v", err)
	}
	out, err = captureOutputMain(t, func() error {
		return cmdExists([]string{"-dir", dir, "beta"})
	})
	if err != nil {
		t.Fatalf("exists beta after delete-all: %v", err)
	}
	if out != "0\n" {
		t.Fatalf("expected exists output 0 after delete-all, got %q", out)
	}
}

func TestCLIAdminCommands(t *testing.T) {
	dir := t.TempDir()

	if err := cmdPut([]string{"-dir", dir, "live", "v1"}); err != nil {
		t.Fatalf("put live: %v", err)
	}
	if err := cmdPut([]string{"-dir", dir, "live", "v2"}); err != nil {
		t.Fatalf("overwrite live: %v", err)
	}
	if err := cmdPut([]string{"-dir", dir, "deleted", "gone"}); err != nil {
		t.Fatalf("put deleted: %v", err)
	}
	if err := cmdDel([]string{"-dir", dir, "deleted"}); err != nil {
		t.Fatalf("del deleted: %v", err)
	}

	statsOut, err := captureOutputMain(t, func() error {
		return cmdStats([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	var stats bitgask.Stats
	if err := json.Unmarshal([]byte(statsOut), &stats); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}
	if stats.Keys != 1 {
		t.Fatalf("expected 1 live key, got %d", stats.Keys)
	}

	reclaimableOut, err := captureOutputMain(t, func() error {
		return cmdReclaimable([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("reclaimable: %v", err)
	}
	var reclaimable map[string]float64
	if err := json.Unmarshal([]byte(reclaimableOut), &reclaimable); err != nil {
		t.Fatalf("unmarshal reclaimable: %v", err)
	}
	if reclaimable["dead_bytes"] <= 0 {
		t.Fatalf("expected dead bytes to be positive, got %v", reclaimable["dead_bytes"])
	}

	if err := cmdReopen([]string{"-dir", dir}); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := cmdMerge([]string{"-dir", dir, "-force"}); err != nil {
		t.Fatalf("merge: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "backup")
	if err := cmdBackup([]string{"-dir", dir, dst}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	backupDB, err := bitgask.Open(dst)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	defer backupDB.Close()
	val, err := backupDB.Get([]byte("live"))
	if err != nil {
		t.Fatalf("get backup value: %v", err)
	}
	if string(val) != "v2" {
		t.Fatalf("expected backup value v2, got %q", val)
	}
}

func TestCLIGCValidateAndRepair(t *testing.T) {
	dir := t.TempDir()

	if err := cmdPut([]string{"-dir", dir, "-entry-ttl", "20ms", "temp", "value"}); err != nil {
		t.Fatalf("put ttl: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	out, err := captureOutputMain(t, func() error {
		return cmdGC([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if strings.TrimSpace(out) != "0" {
		t.Fatalf("expected gc output 0 after reopen skipped expired keys, got %q", out)
	}

	validateOut, err := captureOutputMain(t, func() error {
		return cmdValidate([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !strings.Contains(validateOut, "\"CorruptRecords\": 0") {
		t.Fatalf("expected clean validate output, got %q", validateOut)
	}

	if err := cmdRepair([]string{"-dir", dir}); err != nil {
		t.Fatalf("repair: %v", err)
	}
}

func TestCLIArgumentValidation(t *testing.T) {
	if err := cmdGet(nil); err == nil || err.Error() != "get: key required" {
		t.Fatalf("expected get arg validation error, got %v", err)
	}
	if err := cmdPut(nil); err == nil || err.Error() != "put: key and value required" {
		t.Fatalf("expected put arg validation error, got %v", err)
	}
	if err := cmdDel(nil); err == nil || err.Error() != "del: key required" {
		t.Fatalf("expected del arg validation error, got %v", err)
	}
	if err := cmdExists(nil); err == nil || err.Error() != "exists: key required" {
		t.Fatalf("expected exists arg validation error, got %v", err)
	}
	if err := cmdBackup(nil); err == nil || err.Error() != "backup: destination required" {
		t.Fatalf("expected backup arg validation error, got %v", err)
	}
}
