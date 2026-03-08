package compare_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	bitgask "github.com/AfshinJalili/bitgask"
	bitcask "go.mills.io/bitcask/v2"
)

type benchSize struct {
	name      string
	count     int
	valueSize int
}

type benchProfile struct {
	name       string
	bitgaskOpt []bitgask.Option
	bitcaskOpt []bitcask.Option
}

var benchSizes = []benchSize{
	{name: "Small", count: 10000, valueSize: 128},
	{name: "Large", count: 100000, valueSize: 1024},
	{name: "XL", count: 100000, valueSize: 4096},
}

var benchProfiles = []benchProfile{
	{
		name: "Fast",
		bitgaskOpt: []bitgask.Option{
			bitgask.WithSyncOnPut(false),
			bitgask.WithSyncOnDelete(false),
			bitgask.WithSyncInterval(time.Hour),
			bitgask.WithCompression(bitgask.CompressionNone),
			bitgask.WithHintFiles(false),
			bitgask.WithHintFileSync(false),
			bitgask.WithMergeInterval(time.Hour),
			bitgask.WithMergeMinTotal(1 << 30),
			bitgask.WithMergeTriggerRatio(0.99),
			bitgask.WithMaxDataFileSize(1 << 30),
		},
		bitcaskOpt: []bitcask.Option{
			bitcask.WithSyncWrites(false),
			bitcask.WithMaxDatafileSize(1 << 30),
		},
	},
	{
		name: "Durable",
		bitgaskOpt: []bitgask.Option{
			bitgask.WithSyncOnPut(true),
			bitgask.WithSyncOnDelete(true),
			bitgask.WithSyncInterval(0),
			bitgask.WithCompression(bitgask.CompressionNone),
			bitgask.WithHintFiles(false),
			bitgask.WithHintFileSync(false),
			bitgask.WithMergeInterval(time.Hour),
			bitgask.WithMergeMinTotal(1 << 30),
			bitgask.WithMergeTriggerRatio(0.99),
			bitgask.WithMaxDataFileSize(1 << 30),
		},
		bitcaskOpt: []bitcask.Option{
			bitcask.WithSyncWrites(true),
			bitcask.WithMaxDatafileSize(1 << 30),
		},
	},
}

func openBitgask(b *testing.B, dir string, opts []bitgask.Option) *bitgask.DB {
	b.Helper()
	db, err := bitgask.Open(dir, opts...)
	if err != nil {
		b.Fatalf("bitgask open: %v", err)
	}
	return db
}

func openBitcask(b *testing.B, dir string, opts []bitcask.Option) *bitcask.Bitcask {
	b.Helper()
	db, err := bitcask.Open(dir, opts...)
	if err != nil {
		b.Fatalf("bitcask open: %v", err)
	}
	return db
}

func populateBitgask(b *testing.B, db *bitgask.DB, count int, value []byte) [][]byte {
	b.Helper()
	keys := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("bitgask put: %v", err)
		}
		keys = append(keys, key)
	}
	return keys
}

func populateBitcask(b *testing.B, db *bitcask.Bitcask, count int, value []byte) []bitcask.Key {
	b.Helper()
	keys := make([]bitcask.Key, 0, count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := db.Put(bitcask.Key(key), bitcask.Value(value)); err != nil {
			b.Fatalf("bitcask put: %v", err)
		}
		keys = append(keys, bitcask.Key(key))
	}
	return keys
}

func warmupGetBitgask(b *testing.B, db *bitgask.DB, keys [][]byte) {
	b.Helper()
	limit := 1000
	if len(keys) < limit {
		limit = len(keys)
	}
	for i := 0; i < limit; i++ {
		if _, err := db.Get(keys[i]); err != nil {
			b.Fatalf("bitgask warmup get: %v", err)
		}
	}
}

func warmupGetBitcask(b *testing.B, db *bitcask.Bitcask, keys []bitcask.Key) {
	b.Helper()
	limit := 1000
	if len(keys) < limit {
		limit = len(keys)
	}
	for i := 0; i < limit; i++ {
		if _, err := db.Get(keys[i]); err != nil {
			b.Fatalf("bitcask warmup get: %v", err)
		}
	}
}

func BenchmarkComparePut(b *testing.B) {
	for _, profile := range benchProfiles {
		profile := profile
		b.Run(profile.name, func(b *testing.B) {
			for _, size := range benchSizes {
				size := size
				b.Run(size.name, func(b *testing.B) {
					value := bytes.Repeat([]byte("a"), size.valueSize)
					b.Run("bitgask", func(b *testing.B) {
						db := openBitgask(b, b.TempDir(), profile.bitgaskOpt)
						defer db.Close()
						b.ReportAllocs()
						b.SetBytes(int64(size.valueSize))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							key := []byte(fmt.Sprintf("key-%06d", i))
							if err := db.Put(key, value); err != nil {
								b.Fatalf("bitgask put: %v", err)
							}
						}
					})
					b.Run("bitcask", func(b *testing.B) {
						db := openBitcask(b, b.TempDir(), profile.bitcaskOpt)
						defer db.Close()
						b.ReportAllocs()
						b.SetBytes(int64(size.valueSize))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							key := []byte(fmt.Sprintf("key-%06d", i))
							if err := db.Put(bitcask.Key(key), bitcask.Value(value)); err != nil {
								b.Fatalf("bitcask put: %v", err)
							}
						}
					})
				})
			}
		})
	}
}

func BenchmarkCompareGet(b *testing.B) {
	for _, profile := range benchProfiles {
		profile := profile
		b.Run(profile.name, func(b *testing.B) {
			for _, size := range benchSizes {
				size := size
				b.Run(size.name, func(b *testing.B) {
					value := bytes.Repeat([]byte("b"), size.valueSize)
					b.Run("bitgask", func(b *testing.B) {
						db := openBitgask(b, b.TempDir(), profile.bitgaskOpt)
						defer db.Close()
						keys := populateBitgask(b, db, size.count, value)
						rng := rand.New(rand.NewSource(1))
						b.ReportAllocs()
						b.SetBytes(int64(size.valueSize))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							key := keys[rng.Intn(len(keys))]
							if _, err := db.Get(key); err != nil {
								b.Fatalf("bitgask get: %v", err)
							}
						}
					})
					b.Run("bitcask", func(b *testing.B) {
						db := openBitcask(b, b.TempDir(), profile.bitcaskOpt)
						defer db.Close()
						keys := populateBitcask(b, db, size.count, value)
						rng := rand.New(rand.NewSource(1))
						b.ReportAllocs()
						b.SetBytes(int64(size.valueSize))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							key := keys[rng.Intn(len(keys))]
							if _, err := db.Get(key); err != nil {
								b.Fatalf("bitcask get: %v", err)
							}
						}
					})
				})
			}
		})
	}
}

func BenchmarkCompareDelete(b *testing.B) {
	for _, profile := range benchProfiles {
		profile := profile
		b.Run(profile.name, func(b *testing.B) {
			for _, size := range benchSizes {
				size := size
				b.Run(size.name, func(b *testing.B) {
					value := bytes.Repeat([]byte("c"), size.valueSize)
					b.Run("bitgask", func(b *testing.B) {
						db := openBitgask(b, b.TempDir(), profile.bitgaskOpt)
						defer db.Close()
						b.StopTimer()
						keys := populateBitgask(b, db, b.N, value)
						b.ReportAllocs()
						b.StartTimer()
						for i := 0; i < b.N; i++ {
							if err := db.Delete(keys[i]); err != nil {
								b.Fatalf("bitgask delete: %v", err)
							}
						}
					})
					b.Run("bitcask", func(b *testing.B) {
						db := openBitcask(b, b.TempDir(), profile.bitcaskOpt)
						defer db.Close()
						b.StopTimer()
						keys := populateBitcask(b, db, b.N, value)
						b.ReportAllocs()
						b.StartTimer()
						for i := 0; i < b.N; i++ {
							if err := db.Delete(keys[i]); err != nil {
								b.Fatalf("bitcask delete: %v", err)
							}
						}
					})
				})
			}
		})
	}
}

func BenchmarkCompareForEach(b *testing.B) {
	for _, profile := range benchProfiles {
		profile := profile
		b.Run(profile.name, func(b *testing.B) {
			for _, size := range benchSizes {
				size := size
				b.Run(size.name, func(b *testing.B) {
					value := bytes.Repeat([]byte("d"), size.valueSize)
					b.Run("bitgask", func(b *testing.B) {
						db := openBitgask(b, b.TempDir(), profile.bitgaskOpt)
						defer db.Close()
						populateBitgask(b, db, size.count, value)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							count := 0
							if err := db.IterKeys(context.Background(), func(_ []byte) bool {
								count++
								return true
							}); err != nil {
								b.Fatalf("bitgask iterkeys: %v", err)
							}
							_ = count
						}
					})
					b.Run("bitcask", func(b *testing.B) {
						db := openBitcask(b, b.TempDir(), profile.bitcaskOpt)
						defer db.Close()
						populateBitcask(b, db, size.count, value)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							count := 0
							if err := db.ForEach(func(_ bitcask.Key) error {
								count++
								return nil
							}); err != nil {
								b.Fatalf("bitcask foreach: %v", err)
							}
							_ = count
						}
					})
				})
			}
		})
	}
}

func BenchmarkCompareMerge(b *testing.B) {
	for _, profile := range benchProfiles {
		profile := profile
		b.Run(profile.name, func(b *testing.B) {
			for _, size := range benchSizes {
				size := size
				b.Run(size.name, func(b *testing.B) {
					value := bytes.Repeat([]byte("e"), size.valueSize)
					b.Run("bitgask", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							b.StopTimer()
							db := openBitgask(b, b.TempDir(), profile.bitgaskOpt)
							keys := populateBitgask(b, db, size.count, value)
							for j := 0; j < len(keys); j += 2 {
								if err := db.Delete(keys[j]); err != nil {
									b.Fatalf("bitgask delete: %v", err)
								}
							}
							b.StartTimer()
							if err := db.Merge(bitgask.MergeOptions{Force: true}); err != nil {
								b.Fatalf("bitgask merge: %v", err)
							}
							b.StopTimer()
							_ = db.Close()
						}
					})
					b.Run("bitcask", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							b.StopTimer()
							db := openBitcask(b, b.TempDir(), profile.bitcaskOpt)
							keys := populateBitcask(b, db, size.count, value)
							for j := 0; j < len(keys); j += 2 {
								if err := db.Delete(keys[j]); err != nil {
									b.Fatalf("bitcask delete: %v", err)
								}
							}
							b.StartTimer()
							if err := db.Merge(); err != nil {
								b.Fatalf("bitcask merge: %v", err)
							}
							b.StopTimer()
							_ = db.Close()
						}
					})
				})
			}
		})
	}
}
