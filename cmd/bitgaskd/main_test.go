package main

import (
	"flag"
	"testing"
	"time"

	"github.com/AfshinJalili/bitgask"
)

func TestServerFlagsDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := &serverConfig{}
	addFlags(fs, cfg)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dir != "./data" {
		t.Fatalf("dir: expected ./data, got %s", cfg.dir)
	}
	if cfg.addr != "127.0.0.1:6380" {
		t.Fatalf("addr: expected 127.0.0.1:6380, got %s", cfg.addr)
	}
	if cfg.maxKey != 64 || cfg.maxValue != 64<<10 {
		t.Fatalf("max sizes: expected 64 and %d, got %d and %d", 64<<10, cfg.maxKey, cfg.maxValue)
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
	if cfg.mergeInterval != 10*time.Minute {
		t.Fatalf("mergeInterval: expected 10m, got %v", cfg.mergeInterval)
	}
	if cfg.compression != "snappy" {
		t.Fatalf("compression: expected snappy, got %s", cfg.compression)
	}
	if !cfg.hintFiles || !cfg.hintSync {
		t.Fatalf("hint flags expected true")
	}
}

func TestServerFlagsOverrides(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := &serverConfig{}
	addFlags(fs, cfg)
	args := []string{
		"-dir", "custom",
		"-addr", "127.0.0.1:9999",
		"-max-key", "1",
		"-max-value", "2",
		"-ttl", "5s",
		"-sync=false",
		"-sync-interval", "1s",
		"-merge-interval", "2s",
		"-compression", "none",
		"-hint-files=false",
		"-hint-sync=false",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dir != "custom" || cfg.addr != "127.0.0.1:9999" {
		t.Fatalf("dir/addr overrides failed: %#v", cfg)
	}
	if cfg.maxKey != 1 || cfg.maxValue != 2 {
		t.Fatalf("max sizes overrides failed: %#v", cfg)
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
	if cfg.mergeInterval != 2*time.Second {
		t.Fatalf("mergeInterval: expected 2s got %v", cfg.mergeInterval)
	}
	if cfg.compression != "none" {
		t.Fatalf("compression: expected none got %s", cfg.compression)
	}
	if cfg.hintFiles || cfg.hintSync {
		t.Fatalf("hint flags expected false")
	}
}

func TestServerOptionsMapping(t *testing.T) {
	cfg := &serverConfig{
		maxKey:      1,
		maxValue:    1,
		defaultTTL:  25 * time.Millisecond,
		syncWrites:  false,
		compression: "none",
		hintFiles:   true,
		hintSync:    true,
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
