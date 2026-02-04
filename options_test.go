package bitgask

import "testing"

func TestDefaultConfigValues(t *testing.T) {
	cfg := applyOptions(nil)
	if !cfg.SyncOnPut || !cfg.SyncOnDelete {
		t.Fatalf("expected sync on by default")
	}
	if cfg.DirMode == 0 || cfg.FileMode == 0 {
		t.Fatalf("expected default file modes")
	}
	if cfg.FileIDWidth != 9 {
		t.Fatalf("expected default FileIDWidth 9, got %d", cfg.FileIDWidth)
	}
	if cfg.MaxDataFileSize != 256<<20 {
		t.Fatalf("unexpected MaxDataFileSize: %d", cfg.MaxDataFileSize)
	}
	if cfg.MaxKeySize != 64 || cfg.MaxValueSize != 64<<10 {
		t.Fatalf("unexpected key/value defaults")
	}
	if cfg.MergeTriggerRatio != 0.6 || cfg.MergeMinTotal != 64<<20 {
		t.Fatalf("unexpected merge defaults")
	}
	if cfg.Compression != CompressionSnappy {
		t.Fatalf("expected default compression snappy")
	}
	if cfg.CompressionThreshold != 256 {
		t.Fatalf("unexpected compression threshold")
	}
	if cfg.Logger == nil {
		t.Fatalf("expected default logger")
	}
}

func TestConfigClampsInvalidValues(t *testing.T) {
	cfg := applyOptions([]Option{
		WithFileIDWidth(0),
		WithMaxDataFileSize(0),
		WithIndexLoadConcurrency(0),
		WithMaxKeySize(0),
		WithMaxValueSize(0),
		WithMergeTriggerRatio(0),
		WithMergeMinTotal(0),
		WithMergeInterval(0),
		WithMergeConcurrency(0),
		WithMergeMaxFileSize(0),
		WithCompressionThreshold(0),
		WithCompression(CompressionType(99)),
		WithLogger(nil),
	})
	if cfg.FileIDWidth != 9 {
		t.Fatalf("expected clamp FileIDWidth to 9")
	}
	if cfg.MaxDataFileSize != 256<<20 {
		t.Fatalf("expected clamp MaxDataFileSize")
	}
	if cfg.MaxKeySize != 64 || cfg.MaxValueSize != 64<<10 {
		t.Fatalf("expected clamp key/value limits")
	}
	if cfg.MergeTriggerRatio != 0.6 || cfg.MergeMinTotal != 64<<20 {
		t.Fatalf("expected clamp merge thresholds")
	}
	if cfg.MergeInterval <= 0 {
		t.Fatalf("expected merge interval default")
	}
	if cfg.MergeConcurrency != 1 {
		t.Fatalf("expected merge concurrency default")
	}
	if cfg.MergeMaxFileSize != cfg.MaxDataFileSize {
		t.Fatalf("expected merge max file size default")
	}
	if cfg.CompressionThreshold != 256 {
		t.Fatalf("expected compression threshold default")
	}
	if cfg.Compression != CompressionSnappy {
		t.Fatalf("expected invalid compression to default to snappy")
	}
	if cfg.Logger == nil {
		t.Fatalf("expected default logger when nil")
	}
}
