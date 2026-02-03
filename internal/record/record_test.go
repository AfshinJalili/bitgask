package record

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	rec := Record{
		Timestamp: 123456789,
		ExpiresAt: 987654321,
		Key:       []byte("hello"),
		Value:     []byte("world"),
		Flags:     0,
		Codec:     0,
	}
	buf, err := Encode(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, _, err := DecodeFrom(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Timestamp != rec.Timestamp || got.ExpiresAt != rec.ExpiresAt {
		t.Fatalf("timestamps mismatch")
	}
	if string(got.Key) != string(rec.Key) || string(got.Value) != string(rec.Value) {
		t.Fatalf("payload mismatch")
	}
}

func TestDecodeDetectsCorruption(t *testing.T) {
	rec := Record{Timestamp: 1, Key: []byte("k"), Value: []byte("v")}
	buf, err := Encode(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	buf[len(buf)-1] ^= 0xFF
	_, _, err = DecodeFrom(bytes.NewReader(buf))
	if err == nil {
		t.Fatalf("expected corruption error")
	}
	if err != ErrCorrupt {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}
