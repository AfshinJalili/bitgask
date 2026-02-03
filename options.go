package bitgask

import (
	"io/fs"
	"runtime"
	"time"
)

type CompressionType uint8

const (
	CompressionSnappy CompressionType = 0
	CompressionNone   CompressionType = 1
)

type Logger interface {
	Printf(format string, args ...interface{})
}

type Options struct {
	SyncOnPut    bool
	SyncOnDelete bool
	SyncInterval time.Duration

	DirMode              fs.FileMode
	FileMode             fs.FileMode
	FileIDWidth          int
	MaxDataFileSize      int64
	IndexLoadConcurrency int

	MaxKeySize   int
	MaxValueSize int

	DefaultTTL time.Duration

	MergeTriggerRatio float64
	MergeMinTotal     int64
	MergeInterval     time.Duration
	MergeOnOpen       bool
	MergeConcurrency  int
	MergeMaxFileSize  int64

	UseHintFiles bool
	HintFileSync bool

	Compression          CompressionType
	CompressionThreshold int

	LockTimeout time.Duration

	Logger Logger

	ValidateOnOpen bool
}

func (o Options) withDefaults() Options {
	if !o.SyncOnPut && !o.SyncOnDelete && o.SyncInterval == 0 {
		o.SyncOnPut = true
		o.SyncOnDelete = true
	}
	if o.SyncInterval < 0 {
		o.SyncInterval = 0
	}
	if !o.UseHintFiles {
		o.UseHintFiles = true
	}
	if o.UseHintFiles && !o.HintFileSync {
		o.HintFileSync = true
	}
	if o.DirMode == 0 {
		o.DirMode = 0o755
	}
	if o.FileMode == 0 {
		o.FileMode = 0o644
	}
	if o.FileIDWidth <= 0 {
		o.FileIDWidth = 9
	}
	if o.MaxDataFileSize <= 0 {
		o.MaxDataFileSize = 256 << 20
	}
	if o.IndexLoadConcurrency <= 0 {
		o.IndexLoadConcurrency = runtime.GOMAXPROCS(0)
	}
	if o.MaxKeySize <= 0 {
		o.MaxKeySize = 1 << 20
	}
	if o.MaxValueSize <= 0 {
		o.MaxValueSize = 16 << 20
	}
	if o.MergeTriggerRatio <= 0 {
		o.MergeTriggerRatio = 0.6
	}
	if o.MergeMinTotal <= 0 {
		o.MergeMinTotal = 64 << 20
	}
	if o.MergeInterval <= 0 {
		o.MergeInterval = 10 * time.Minute
	}
	if o.MergeConcurrency <= 0 {
		o.MergeConcurrency = 1
	}
	if o.MergeMaxFileSize <= 0 {
		o.MergeMaxFileSize = o.MaxDataFileSize
	}
	if o.CompressionThreshold <= 0 {
		o.CompressionThreshold = 256
	}
	if o.Compression != CompressionNone && o.Compression != CompressionSnappy {
		o.Compression = CompressionSnappy
	}
	if o.Logger == nil {
		o.Logger = noopLogger{}
	}
	return o
}

type noopLogger struct{}

func (noopLogger) Printf(string, ...interface{}) {}
