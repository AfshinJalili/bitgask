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
