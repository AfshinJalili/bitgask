package index

import (
	"bytes"
	"testing"
)

func TestHintRoundTrip(t *testing.T) {
	entry := HintEntry{
		Key:     []byte("k"),
		FileID:  2,
		Offset:  128,
		Size:    64,
		TS:      1234,
		Expires: 5678,
		Flags:   1,
	}
	buf := &bytes.Buffer{}
	if err := WriteHint(buf, entry); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, _, err := ReadHint(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got.Key) != string(entry.Key) || got.FileID != entry.FileID || got.Offset != entry.Offset || got.Size != entry.Size {
		t.Fatalf("mismatch")
	}
}

func TestHintVariousKeyLengths(t *testing.T) {
	lengths := []int{0, 1, 8, 32, 128}
	for _, n := range lengths {
		key := bytes.Repeat([]byte{byte('a' + n%26)}, n)
		entry := HintEntry{
			Key:     key,
			FileID:  1,
			Offset:  10,
			Size:    20,
			TS:      30,
			Expires: 40,
			Flags:   0,
		}
		buf := &bytes.Buffer{}
		if err := WriteHint(buf, entry); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, _, err := ReadHint(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got.Key) != string(entry.Key) || got.FileID != entry.FileID {
			t.Fatalf("mismatch for len %d", n)
		}
	}
}
