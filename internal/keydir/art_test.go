package keydir

import "testing"

func TestARTBasicOps(t *testing.T) {
	k := NewART()
	if k.Len() != 0 {
		t.Fatalf("expected empty")
	}
	k.Set([]byte("a"), 1)
	k.Set([]byte("ab"), 2)
	k.Set([]byte("b"), 3)
	if k.Len() != 3 {
		t.Fatalf("expected len 3, got %d", k.Len())
	}
	if v, ok := k.Get([]byte("ab")); !ok || v.(int) != 2 {
		t.Fatalf("expected get ab=2")
	}
	k.Delete([]byte("ab"))
	if _, ok := k.Get([]byte("ab")); ok {
		t.Fatalf("expected ab deleted")
	}
}

func TestARTPrefixAndRange(t *testing.T) {
	k := NewART()
	k.Set([]byte("a1"), 1)
	k.Set([]byte("a2"), 2)
	k.Set([]byte("b1"), 3)

	count := 0
	k.Range(func(_ []byte, _ interface{}) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("expected 3 keys, got %d", count)
	}

	prefixCount := 0
	k.Prefix([]byte("a"), func(key []byte, _ interface{}) bool {
		if len(key) == 0 {
			return true
		}
		if key[0] != 'a' {
			t.Fatalf("unexpected prefix key: %s", string(key))
		}
		prefixCount++
		return true
	})
	if prefixCount < 2 {
		t.Fatalf("expected at least 2 prefix keys, got %d", prefixCount)
	}
}
