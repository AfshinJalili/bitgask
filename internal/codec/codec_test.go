package codec

import "testing"

func TestCodecRoundTrip(t *testing.T) {
	input := []byte("the quick brown fox jumps over the lazy dog")
	enc, err := Encode(Snappy, input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := Decode(Snappy, enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(dec) != string(input) {
		t.Fatalf("round trip mismatch")
	}
}

func TestCodecNone(t *testing.T) {
	input := []byte("plain")
	enc, err := Encode(None, input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := Decode(None, enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(dec) != string(input) {
		t.Fatalf("round trip mismatch")
	}
}
