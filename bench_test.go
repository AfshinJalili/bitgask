package bitgask

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

const (
	benchIterKeyCount  = 10000
	benchIterValueSize = 128
	benchIterKeyCountLarge  = 100000
	benchIterValueSizeLarge = 1024
	benchIterValueSizeXL    = 4096
)

var (
	benchCountSink int
)

func benchPopulate(b *testing.B, count int, valueSize int) *DB {
	opts := benchOptions()
	opts = append(opts, WithMaxDataFileSize(1<<30))
	db, err := Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	value := bytes.Repeat([]byte("a"), valueSize)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := db.Put(key, value); err != nil {
			_ = db.Close()
			b.Fatalf("put: %v", err)
		}
	}
	return db
}

func validateIterKeys(b *testing.B, db *DB, expected int) {
	count := 0
	if err := db.IterKeys(context.Background(), func(_ []byte) bool {
		count++
		return true
	}); err != nil {
		b.Fatalf("iterkeys: %v", err)
	}
	if count != expected {
		b.Fatalf("iterkeys count: %d", count)
	}
}

func validateIter(b *testing.B, db *DB, expected int) {
	count := 0
	if err := db.Iter(context.Background(), nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
		count++
		return true
	}); err != nil {
		b.Fatalf("iter: %v", err)
	}
	if count != expected {
		b.Fatalf("iter count: %d", count)
	}
}

func validateKeys(b *testing.B, db *DB, expected int) {
	count := 0
	for range db.Keys() {
		count++
	}
	if count != expected {
		b.Fatalf("keys count: %d", count)
	}
}

func validateScan(b *testing.B, db *DB, expected int) {
	count := 0
	if err := db.Scan(nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
		count++
		return true
	}); err != nil {
		b.Fatalf("scan: %v", err)
	}
	if count != expected {
		b.Fatalf("scan count: %d", count)
	}
}

func benchOptions() []Option {
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
		WithMaxDataFileSize(1 << 20),
		WithCompressionThreshold(256),
	}
}

func BenchmarkPut(b *testing.B) {
	opts := benchOptions()
	db, err := Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	value := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	opts := benchOptions()
	db, err := Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	value := make([]byte, 256)
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(keys[i], value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		if _, err := db.Get(key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkMerge(b *testing.B) {
	opts := benchOptions()
	opts = append(opts, WithMergeMinTotal(0), WithMergeTriggerRatio(0))
	db, err := Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 5000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(key, []byte("value")); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
	for i := 0; i < 2500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_ = db.Delete(key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Merge(MergeOptions{Force: true}); err != nil {
			b.Fatalf("merge: %v", err)
		}
	}
}

func BenchmarkIterKeys(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCount, benchIterValueSize)
	defer db.Close()

	validateIterKeys(b, db, benchIterKeyCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.IterKeys(context.Background(), func(_ []byte) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("iterkeys: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkKeys(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCount, benchIterValueSize)
	defer db.Close()

	validateKeys(b, db, benchIterKeyCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for key := range db.Keys() {
			_ = key
			count++
		}
		benchCountSink = count
	}
}

func BenchmarkIter(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCount, benchIterValueSize)
	defer db.Close()

	validateIter(b, db, benchIterKeyCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.Iter(context.Background(), nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("iter: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkScan(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCount, benchIterValueSize)
	defer db.Close()

	validateScan(b, db, benchIterKeyCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.Scan(nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkIterKeysLarge(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeLarge)
	defer db.Close()

	validateIterKeys(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.IterKeys(context.Background(), func(_ []byte) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("iterkeys: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkKeysLarge(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeLarge)
	defer db.Close()

	validateKeys(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for range db.Keys() {
			count++
		}
		benchCountSink = count
	}
}

func BenchmarkIterLarge(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeLarge)
	defer db.Close()

	validateIter(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.Iter(context.Background(), nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("iter: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkScanLarge(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeLarge)
	defer db.Close()

	validateScan(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.Scan(nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkIterKeysXL(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeXL)
	defer db.Close()

	validateIterKeys(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.IterKeys(context.Background(), func(_ []byte) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("iterkeys: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkKeysXL(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeXL)
	defer db.Close()

	validateKeys(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for range db.Keys() {
			count++
		}
		benchCountSink = count
	}
}

func BenchmarkIterXL(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeXL)
	defer db.Close()

	validateIter(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.Iter(context.Background(), nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("iter: %v", err)
		}
		benchCountSink = count
	}
}

func BenchmarkScanXL(b *testing.B) {
	db := benchPopulate(b, benchIterKeyCountLarge, benchIterValueSizeXL)
	defer db.Close()

	validateScan(b, db, benchIterKeyCountLarge)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := db.Scan(nil, func(_ []byte, _ []byte, _ RecordMeta) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("scan: %v", err)
		}
		benchCountSink = count
	}
}
