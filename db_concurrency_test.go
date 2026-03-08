package bitgask

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentOps(t *testing.T) {
	opts := []Option{
		WithSyncOnPut(false),
		WithSyncOnDelete(false),
		WithSyncInterval(time.Hour),
		WithMergeInterval(time.Hour),
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	worker := func(id int) {
		defer wg.Done()
		r := rand.New(rand.NewSource(int64(id + 1)))
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("k%d", r.Intn(20)))
			op := r.Intn(4)
			switch op {
			case 0:
				if err := db.Put(key, []byte("v")); err != nil {
					errCh <- err
					return
				}
			case 1:
				err := db.Delete(key)
				if err != nil && err != ErrKeyNotFound {
					errCh <- err
					return
				}
			case 2:
				_, err := db.Get(key)
				if err != nil && err != ErrKeyNotFound && err != ErrExpired {
					errCh <- err
					return
				}
			case 3:
				_, err := db.Has(key)
				if err != nil {
					errCh <- err
					return
				}
			}
		}
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go worker(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	default:
	}
}

func TestConcurrentMergeWithReads(t *testing.T) {
	opts := []Option{
		WithMergeMinTotal(0),
		WithMergeTriggerRatio(0),
		WithSyncOnPut(false),
		WithSyncOnDelete(false),
		WithSyncInterval(time.Hour),
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("k%d", i%100))
			_, err := db.Get(key)
			if err != nil && err != ErrKeyNotFound && err != ErrExpired {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := db.Merge(MergeOptions{Force: true}); err != nil {
			errCh <- err
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	default:
	}
}

func TestOpenDataFileConcurrentMissSingleFlight(t *testing.T) {
	opts := []Option{
		WithMaxDataFileSize(96),
		WithSyncOnPut(false),
		WithSyncOnDelete(false),
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	payload := make([]byte, 40)
	for i := range payload {
		payload[i] = 'x'
	}
	if err := db.Put([]byte("a"), payload); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := db.Put([]byte("b"), payload); err != nil {
		t.Fatalf("put b: %v", err)
	}

	db.mu.RLock()
	if _, ok := db.dataFiles[1]; ok {
		db.mu.RUnlock()
		t.Fatalf("expected historical file 1 to require lazy open")
	}
	db.mu.RUnlock()

	var opens int32
	openReadonlyDataFileHook = func(path string) (*os.File, error) {
		atomic.AddInt32(&opens, 1)
		time.Sleep(20 * time.Millisecond)
		return os.Open(path)
	}
	t.Cleanup(func() { openReadonlyDataFileHook = nil })

	start := make(chan struct{})
	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f, err := db.openDataFile(1)
			if err != nil {
				errCh <- err
				return
			}
			if f == nil {
				errCh <- fmt.Errorf("nil file handle")
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("openDataFile: %v", err)
	}

	if got := atomic.LoadInt32(&opens); got != 1 {
		t.Fatalf("expected exactly one open call, got %d", got)
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.dataFiles[1] == nil {
		t.Fatalf("expected historical file 1 to be cached after open")
	}
}
