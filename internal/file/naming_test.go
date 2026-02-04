package file

import "testing"

func TestFileNamingAndParsing(t *testing.T) {
	name := DataFileName(42, 6)
	if name != "000042.data" {
		t.Fatalf("unexpected data file name: %s", name)
	}
	hint := HintFileName(7, 4)
	if hint != "0007.hint" {
		t.Fatalf("unexpected hint file name: %s", hint)
	}
	if !IsDataFile(name) || IsHintFile(name) {
		t.Fatalf("expected data file detection")
	}
	if !IsHintFile(hint) || IsDataFile(hint) {
		t.Fatalf("expected hint file detection")
	}
	id, ok := ParseFileID(name)
	if !ok || id != 42 {
		t.Fatalf("parse id failed")
	}
	id, ok = ParseFileID(hint)
	if !ok || id != 7 {
		t.Fatalf("parse id failed")
	}
}
