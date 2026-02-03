package bitgask

import (
	"fmt"
	"testing"
	"time"
)

func benchOptions() Options {
	return Options{
		SyncOnPut:            false,
		SyncOnDelete:         false,
		SyncInterval:         time.Hour,
		MergeInterval:        time.Hour,
		MergeMinTotal:        1 << 30,
		MergeTriggerRatio:    0.9,
		Compression:          CompressionNone,
		UseHintFiles:         true,
		HintFileSync:         false,
		MaxDataFileSize:      1 << 20,
		CompressionThreshold: 256,
	}
}

func BenchmarkPut(b *testing.B) {
	opts := benchOptions()
	db, err := Open(b.TempDir(), opts)
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
	db, err := Open(b.TempDir(), opts)
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
	opts.MergeMinTotal = 0
	opts.MergeTriggerRatio = 0
	db, err := Open(b.TempDir(), opts)
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
