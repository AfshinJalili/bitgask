//go:build integration

package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = w
	err = fn()
	_ = w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)
	return string(out), err
}

func TestCLICommands(t *testing.T) {
	dir := t.TempDir()

	if _, err := captureStdout(t, func() error {
		return cmdPut([]string{"-dir", dir, "k", "v"})
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	out, err := captureStdout(t, func() error {
		return cmdGet([]string{"-dir", dir, "k"})
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if strings.TrimSpace(out) != "v" {
		t.Fatalf("unexpected get output: %s", out)
	}

	out, err = captureStdout(t, func() error {
		return cmdScan([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !bytes.Contains([]byte(out), []byte("k")) {
		t.Fatalf("expected scan to include key")
	}

	out, err = captureStdout(t, func() error {
		return cmdStats([]string{"-dir", dir})
	})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if !strings.Contains(out, "\"Keys\"") {
		t.Fatalf("expected stats output")
	}
}
