//go:build integration

package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestServerBasicCommands(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	if filepath.Base(wd) != "bitgaskd" {
		wd = filepath.Join(wd, "cmd", "bitgaskd")
	}

	bin := buildServerBinary(t, wd)
	cmd := exec.CommandContext(ctx, bin, "-dir", dir, "-addr", addr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	conn := waitForConn(t, addr)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	if err := writeRESP(conn, "PING"); err != nil {
		t.Fatalf("ping write: %v", err)
	}
	if line := readLine(t, rd); line != "+PONG" {
		t.Fatalf("unexpected ping response: %s", line)
	}

	if err := writeRESP(conn, "SET", "k", "v"); err != nil {
		t.Fatalf("set write: %v", err)
	}
	if line := readLine(t, rd); line != "+OK" {
		t.Fatalf("unexpected set response: %s", line)
	}

	if err := writeRESP(conn, "GET", "k"); err != nil {
		t.Fatalf("get write: %v", err)
	}
	val := readBulk(t, rd)
	if val != "v" {
		t.Fatalf("unexpected get value: %s", val)
	}

	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func TestServerScanRejectsInvalidCountWithoutCrashing(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	if filepath.Base(wd) != "bitgaskd" {
		wd = filepath.Join(wd, "cmd", "bitgaskd")
	}

	bin := buildServerBinary(t, wd)
	cmd := exec.CommandContext(ctx, bin, "-dir", dir, "-addr", addr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	conn := waitForConn(t, addr)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	if err := writeRESP(conn, "SCAN", "0", "COUNT", "-1"); err != nil {
		t.Fatalf("scan write: %v", err)
	}
	if line := readLine(t, rd); line != "-ERR invalid COUNT" {
		t.Fatalf("unexpected scan error: %s", line)
	}

	if err := writeRESP(conn, "PING"); err != nil {
		t.Fatalf("ping write: %v", err)
	}
	if line := readLine(t, rd); line != "+PONG" {
		t.Fatalf("unexpected ping response after invalid scan: %s", line)
	}
}

func TestServerSetRejectsNonPositiveTTL(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	if filepath.Base(wd) != "bitgaskd" {
		wd = filepath.Join(wd, "cmd", "bitgaskd")
	}

	bin := buildServerBinary(t, wd)
	cmd := exec.CommandContext(ctx, bin, "-dir", dir, "-addr", addr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	conn := waitForConn(t, addr)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	if err := writeRESP(conn, "SET", "k1", "v1", "EX", "0"); err != nil {
		t.Fatalf("set EX write: %v", err)
	}
	if line := readLine(t, rd); line != "-ERR invalid EX" {
		t.Fatalf("unexpected EX error: %s", line)
	}

	if err := writeRESP(conn, "GET", "k1"); err != nil {
		t.Fatalf("get k1 write: %v", err)
	}
	if line := readLine(t, rd); line != "$-1" {
		t.Fatalf("expected null bulk for rejected EX write, got %s", line)
	}

	if err := writeRESP(conn, "SET", "k2", "v2", "PX", "-1"); err != nil {
		t.Fatalf("set PX write: %v", err)
	}
	if line := readLine(t, rd); line != "-ERR invalid PX" {
		t.Fatalf("unexpected PX error: %s", line)
	}

	if err := writeRESP(conn, "GET", "k2"); err != nil {
		t.Fatalf("get k2 write: %v", err)
	}
	if line := readLine(t, rd); line != "$-1" {
		t.Fatalf("expected null bulk for rejected PX write, got %s", line)
	}
}

func TestServerScanPaginatesWithoutFullMaterialization(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	if filepath.Base(wd) != "bitgaskd" {
		wd = filepath.Join(wd, "cmd", "bitgaskd")
	}

	bin := buildServerBinary(t, wd)
	cmd := exec.CommandContext(ctx, bin, "-dir", dir, "-addr", addr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	conn := waitForConn(t, addr)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	for _, key := range []string{"user:1", "user:2", "user:3"} {
		if err := writeRESP(conn, "SET", key, "v"); err != nil {
			t.Fatalf("set %s write: %v", key, err)
		}
		if line := readLine(t, rd); line != "+OK" {
			t.Fatalf("unexpected set response for %s: %s", key, line)
		}
	}

	if err := writeRESP(conn, "SCAN", "0", "MATCH", "user:*", "COUNT", "1"); err != nil {
		t.Fatalf("scan write: %v", err)
	}
	cursor, keys := readScanResponse(t, rd)
	if len(keys) != 1 {
		t.Fatalf("expected one key on first scan page, got %d", len(keys))
	}
	if cursor == "0" {
		t.Fatalf("expected non-zero cursor after first bounded scan")
	}

	if err := writeRESP(conn, "SCAN", cursor, "MATCH", "user:*", "COUNT", "1"); err != nil {
		t.Fatalf("scan second write: %v", err)
	}
	next, keys := readScanResponse(t, rd)
	if len(keys) != 1 {
		t.Fatalf("expected one key on second scan page, got %d", len(keys))
	}
	if next == cursor {
		t.Fatalf("expected cursor to advance, got %q", next)
	}
}

func buildServerBinary(t *testing.T, wd string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "bitgaskd")
	cmd := exec.Command("go", "build", "-o", out, ".")
	cmd.Dir = wd
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build server: %v", err)
	}
	return out
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitForConn(t *testing.T, addr string) net.Conn {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			return conn
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server did not start: %s", addr)
	return nil
}

func writeRESP(conn net.Conn, args ...string) error {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := conn.Write([]byte(b.String()))
	return err
}

func readLine(t *testing.T, rd *bufio.Reader) string {
	line, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

func readBulk(t *testing.T, rd *bufio.Reader) string {
	line := readLine(t, rd)
	if !strings.HasPrefix(line, "$") {
		t.Fatalf("unexpected bulk header: %s", line)
	}
	n, err := strconv.Atoi(strings.TrimPrefix(line, "$"))
	if err != nil {
		t.Fatalf("parse bulk len: %v", err)
	}
	buf := make([]byte, n+2)
	if _, err := rd.Read(buf); err != nil {
		t.Fatalf("read bulk: %v", err)
	}
	return string(buf[:n])
}

func readArrayLen(t *testing.T, rd *bufio.Reader) int {
	line := readLine(t, rd)
	if !strings.HasPrefix(line, "*") {
		t.Fatalf("unexpected array header: %s", line)
	}
	n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil {
		t.Fatalf("parse array len: %v", err)
	}
	return n
}

func readScanResponse(t *testing.T, rd *bufio.Reader) (string, []string) {
	if got := readArrayLen(t, rd); got != 2 {
		t.Fatalf("expected top-level scan array len 2, got %d", got)
	}
	cursor := readBulk(t, rd)
	count := readArrayLen(t, rd)
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, readBulk(t, rd))
	}
	return cursor, keys
}
