package main

import (
	"flag"
	"strings"
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
	if cfg.showVersion {
		t.Fatalf("showVersion: expected false")
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
		"-version=true",
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
	if !cfg.showVersion {
		t.Fatalf("showVersion: expected true")
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

func TestParseScanArgsRejectsInvalidCount(t *testing.T) {
	_, _, _, err := parseScanArgs([][]byte{
		[]byte("SCAN"),
		[]byte("0"),
		[]byte("COUNT"),
		[]byte("-1"),
	})
	if err == nil || err.Error() != "ERR invalid COUNT" {
		t.Fatalf("expected invalid COUNT, got %v", err)
	}
}

func TestParseScanArgsRejectsInvalidCursor(t *testing.T) {
	_, _, _, err := parseScanArgs([][]byte{
		[]byte("SCAN"),
		[]byte("%%%"),
	})
	if err == nil || err.Error() != "ERR invalid cursor" {
		t.Fatalf("expected invalid cursor, got %v", err)
	}
}

func TestParseSetTTLOptionsRejectsNonPositive(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want string
	}{
		{
			name: "zero EX",
			args: [][]byte{[]byte("EX"), []byte("0")},
			want: "ERR invalid EX",
		},
		{
			name: "negative EX",
			args: [][]byte{[]byte("EX"), []byte("-1")},
			want: "ERR invalid EX",
		},
		{
			name: "zero PX",
			args: [][]byte{[]byte("PX"), []byte("0")},
			want: "ERR invalid PX",
		},
		{
			name: "negative PX",
			args: [][]byte{[]byte("PX"), []byte("-1")},
			want: "ERR invalid PX",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSetTTLOptions(tt.args)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("expected %q, got %v", tt.want, err)
			}
		})
	}
}

func TestParseSetTTLOptionsParsesValidTTL(t *testing.T) {
	ttl, err := parseSetTTLOptions([][]byte{[]byte("PX"), []byte("25")})
	if err != nil {
		t.Fatalf("parse PX: %v", err)
	}
	if ttl != 25*time.Millisecond {
		t.Fatalf("expected 25ms, got %v", ttl)
	}

	ttl, err = parseSetTTLOptions([][]byte{[]byte("EX"), []byte("2")})
	if err != nil {
		t.Fatalf("parse EX: %v", err)
	}
	if ttl != 2*time.Second {
		t.Fatalf("expected 2s, got %v", ttl)
	}
}

func TestParseScanArgsRejectsZeroCount(t *testing.T) {
	_, _, _, err := parseScanArgs([][]byte{
		[]byte("SCAN"),
		[]byte("0"),
		[]byte("COUNT"),
		[]byte("0"),
	})
	if err == nil || err.Error() != "ERR invalid COUNT" {
		t.Fatalf("expected invalid COUNT for zero, got %v", err)
	}
}

func TestParseScanArgsParsesOptions(t *testing.T) {
	cursor, pattern, count, err := parseScanArgs([][]byte{
		[]byte("SCAN"),
		[]byte("0"),
		[]byte("MATCH"),
		[]byte("user:*"),
		[]byte("COUNT"),
		[]byte("25"),
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cursor != "0" {
		t.Fatalf("expected cursor 0, got %q", cursor)
	}
	if pattern != "user:*" {
		t.Fatalf("expected pattern user:*, got %s", pattern)
	}
	if count != 25 {
		t.Fatalf("expected count 25, got %d", count)
	}
}

func TestSetExpireBehavior(t *testing.T) {
	db, err := bitgask.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if got, err := setExpire(db, []byte("missing"), time.Second); err != nil || got != 0 {
		t.Fatalf("expected missing key expire to return 0")
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got, err := setExpire(db, []byte("k"), 30*time.Millisecond); err != nil || got != 1 {
		t.Fatalf("expected expire to return 1")
	}
	time.Sleep(45 * time.Millisecond)
	if _, err := db.Get([]byte("k")); err != bitgask.ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}

	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got, err := setExpire(db, []byte("k"), 0); err != nil || got != 1 {
		t.Fatalf("expected ttl=0 expire to return 1")
	}
	if _, err := db.Get([]byte("k")); err != bitgask.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
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

func TestCollectAllKeysFiltersPattern(t *testing.T) {
	db, err := bitgask.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, key := range []string{"user:1", "user:2", "order:1"} {
		if err := db.Put([]byte(key), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	keys := collectAllKeys(db, "user:*")
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	for _, key := range keys {
		if !strings.HasPrefix(string(key), "user:") {
			t.Fatalf("unexpected key %q", key)
		}
	}
}

func TestScanKeysUsesOpaqueCursor(t *testing.T) {
	db, err := bitgask.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, key := range []string{"user:1", "user:2", "user:3"} {
		if err := db.Put([]byte(key), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	keys, cursor, err := scanKeys(db, "0", "user:*", 1)
	if err != nil {
		t.Fatalf("scan first page: %v", err)
	}
	if len(keys) != 1 || cursor == "0" {
		t.Fatalf("expected one key and non-zero cursor, got keys=%d cursor=%q", len(keys), cursor)
	}

	keys, next, err := scanKeys(db, cursor, "user:*", 1)
	if err != nil {
		t.Fatalf("scan second page: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected one key on second page, got %d", len(keys))
	}
	if next == cursor {
		t.Fatalf("expected cursor to advance, got %q", next)
	}
}

func TestScanKeysBoundsWorkByCount(t *testing.T) {
	db, err := bitgask.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, key := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(key), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	keys, cursor, err := scanKeys(db, "0", "c", 1)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected no match on first bounded page, got %d", len(keys))
	}
	if cursor == "0" {
		t.Fatalf("expected non-zero cursor after bounded scan")
	}
}

func TestTTLForKeyReturnCodes(t *testing.T) {
	db, err := bitgask.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if got := ttlForKey(db, []byte("missing"), time.Second); got != -2 {
		t.Fatalf("expected missing TTL -2, got %d", got)
	}
	if err := db.Put([]byte("persist"), []byte("v")); err != nil {
		t.Fatalf("put persist: %v", err)
	}
	if got := ttlForKey(db, []byte("persist"), time.Second); got != -1 {
		t.Fatalf("expected persistent TTL -1, got %d", got)
	}
	if err := db.PutWithTTL([]byte("temp"), []byte("v"), 200*time.Millisecond); err != nil {
		t.Fatalf("put ttl: %v", err)
	}
	if got := ttlForKey(db, []byte("temp"), time.Millisecond); got <= 0 {
		t.Fatalf("expected positive ttl, got %d", got)
	}
	time.Sleep(220 * time.Millisecond)
	if got := ttlForKey(db, []byte("temp"), time.Millisecond); got != -2 {
		t.Fatalf("expected expired TTL -2, got %d", got)
	}
}
