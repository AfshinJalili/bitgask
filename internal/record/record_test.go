package record

import (
	"bytes"
	"math/rand"
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

func TestReadAtRoundTrip(t *testing.T) {
	rec := Record{
		Timestamp: 42,
		ExpiresAt: 84,
		Key:       []byte("alpha"),
		Value:     []byte("beta"),
		Flags:     0,
		Codec:     0,
	}
	buf, err := Encode(rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	reader := bytes.NewReader(buf)
	got, _, err := ReadAt(reader, 0)
	if err != nil {
		t.Fatalf("readat: %v", err)
	}
	if string(got.Key) != string(rec.Key) || string(got.Value) != string(rec.Value) {
		t.Fatalf("readat mismatch")
	}
}

func TestEncodeDecodeRandom(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		keyLen := r.Intn(64)
		valLen := r.Intn(256)
		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		_, _ = r.Read(key)
		_, _ = r.Read(val)
		rec := Record{
			Timestamp: r.Int63(),
			ExpiresAt: r.Int63(),
			Key:       key,
			Value:     val,
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
		if string(got.Key) != string(rec.Key) || string(got.Value) != string(rec.Value) {
			t.Fatalf("random mismatch")
		}
	}
}
