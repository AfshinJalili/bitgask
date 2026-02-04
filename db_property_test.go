package bitgask

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"testing/quick"
	"time"
)

func TestPropertyRandomOps(t *testing.T) {
	var lastErr error
	f := func(seed uint64) bool {
		dir, err := os.MkdirTemp("", "bitgask-prop-")
		if err != nil {
			lastErr = err
			return false
		}
		defer os.RemoveAll(dir)
		opts := []Option{
			WithSyncOnPut(false),
			WithSyncOnDelete(false),
			WithSyncInterval(time.Hour),
			WithMergeInterval(time.Hour),
		}
		db, err := Open(dir, opts...)
		if err != nil {
			lastErr = err
			return false
		}
		defer db.Close()

		model := make(map[string][]byte)
		r := rand.New(rand.NewSource(int64(seed)))
		for i := 0; i < 200; i++ {
			key := []byte{byte('a' + r.Intn(5))}
			op := r.Intn(4)
			switch op {
			case 0: // Put
				val := make([]byte, r.Intn(16)+1)
				_, _ = r.Read(val)
				if err := db.Put(key, val); err != nil {
					lastErr = err
					return false
				}
				model[string(key)] = append([]byte(nil), val...)
			case 1: // Delete
				err := db.Delete(key)
				if err != nil && err != ErrKeyNotFound {
					lastErr = err
					return false
				}
				delete(model, string(key))
			case 2: // Get
				val, err := db.Get(key)
				modelVal, ok := model[string(key)]
				if !ok {
					if err != ErrKeyNotFound {
						lastErr = err
						return false
					}
					continue
				}
				if err != nil {
					lastErr = err
					return false
				}
				if !bytes.Equal(val, modelVal) {
					lastErr = err
					return false
				}
			case 3: // Has
				ok, err := db.Has(key)
				if err != nil {
					lastErr = err
					return false
				}
				if ok != (model[string(key)] != nil) {
					lastErr = err
					return false
				}
			}
		}
		return true
	}
	cfg := &quick.Config{
		MaxCount: 50,
		Rand:     rand.New(rand.NewSource(1)),
	}
	if err := quick.Check(f, cfg); err != nil {
		if lastErr != nil {
			t.Fatalf("property failed: %v", lastErr)
		}
		t.Fatalf("property failed: %v", err)
	}
}
