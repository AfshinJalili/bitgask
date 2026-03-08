package keydir

import "testing"

func TestRadixLowerBound(t *testing.T) {
	k := NewRadix()
	for i, key := range []string{"a", "b", "c"} {
		k.Set([]byte(key), i)
	}

	var got []string
	k.LowerBound([]byte("b"), func(key []byte, _ interface{}) bool {
		got = append(got, string(key))
		return true
	})

	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("unexpected lower bound result: %#v", got)
	}

	got = got[:0]
	snap := k.Snapshot()
	snap.LowerBound([]byte("c"), func(key []byte, _ interface{}) bool {
		got = append(got, string(key))
		return true
	})
	if len(got) != 1 || got[0] != "c" {
		t.Fatalf("unexpected snapshot lower bound result: %#v", got)
	}
}
