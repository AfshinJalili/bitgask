package main

import (
	"flag"
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
