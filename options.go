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

type Option func(*config)

type config struct {
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

func defaultConfig() config {
	return config{
		SyncOnPut:            true,
		SyncOnDelete:         true,
		SyncInterval:         0,
		DirMode:              0o755,
		FileMode:             0o644,
		FileIDWidth:          9,
		MaxDataFileSize:      256 << 20,
		IndexLoadConcurrency: runtime.GOMAXPROCS(0),
		MaxKeySize:           64,
		MaxValueSize:         64 << 10,
		DefaultTTL:           0,
		MergeTriggerRatio:    0.6,
		MergeMinTotal:        64 << 20,
		MergeInterval:        10 * time.Minute,
		MergeOnOpen:          false,
		MergeConcurrency:     1,
		MergeMaxFileSize:     0,
		UseHintFiles:         true,
		HintFileSync:         true,
		Compression:          CompressionSnappy,
		CompressionThreshold: 256,
		LockTimeout:          0,
		Logger:               noopLogger{},
		ValidateOnOpen:       false,
	}
}

func applyOptions(opts []Option) config {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.SyncInterval < 0 {
		cfg.SyncInterval = 0
	}
	if cfg.DirMode == 0 {
		cfg.DirMode = 0o755
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = 0o644
	}
	if cfg.FileIDWidth <= 0 {
		cfg.FileIDWidth = 9
	}
	if cfg.MaxDataFileSize <= 0 {
		cfg.MaxDataFileSize = 256 << 20
	}
	if cfg.IndexLoadConcurrency <= 0 {
		cfg.IndexLoadConcurrency = runtime.GOMAXPROCS(0)
	}
	if cfg.MaxKeySize <= 0 {
		cfg.MaxKeySize = 64
	}
	if cfg.MaxValueSize <= 0 {
		cfg.MaxValueSize = 64 << 10
	}
	if cfg.MergeTriggerRatio <= 0 {
		cfg.MergeTriggerRatio = 0.6
	}
	if cfg.MergeMinTotal <= 0 {
		cfg.MergeMinTotal = 64 << 20
	}
	if cfg.MergeInterval <= 0 {
		cfg.MergeInterval = 10 * time.Minute
	}
	if cfg.MergeConcurrency <= 0 {
		cfg.MergeConcurrency = 1
	}
	if cfg.MergeMaxFileSize <= 0 {
		cfg.MergeMaxFileSize = cfg.MaxDataFileSize
	}
	if cfg.CompressionThreshold <= 0 {
		cfg.CompressionThreshold = 256
	}
	if cfg.Compression != CompressionNone && cfg.Compression != CompressionSnappy {
		cfg.Compression = CompressionSnappy
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	return cfg
}

func WithSyncOnPut(v bool) Option              { return func(c *config) { c.SyncOnPut = v } }
func WithSyncOnDelete(v bool) Option           { return func(c *config) { c.SyncOnDelete = v } }
func WithSyncInterval(v time.Duration) Option  { return func(c *config) { c.SyncInterval = v } }
func WithDirMode(v fs.FileMode) Option         { return func(c *config) { c.DirMode = v } }
func WithFileMode(v fs.FileMode) Option        { return func(c *config) { c.FileMode = v } }
func WithFileIDWidth(v int) Option             { return func(c *config) { c.FileIDWidth = v } }
func WithMaxDataFileSize(v int64) Option       { return func(c *config) { c.MaxDataFileSize = v } }
func WithIndexLoadConcurrency(v int) Option    { return func(c *config) { c.IndexLoadConcurrency = v } }
func WithMaxKeySize(v int) Option              { return func(c *config) { c.MaxKeySize = v } }
func WithMaxValueSize(v int) Option            { return func(c *config) { c.MaxValueSize = v } }
func WithDefaultTTL(v time.Duration) Option    { return func(c *config) { c.DefaultTTL = v } }
func WithMergeTriggerRatio(v float64) Option   { return func(c *config) { c.MergeTriggerRatio = v } }
func WithMergeMinTotal(v int64) Option         { return func(c *config) { c.MergeMinTotal = v } }
func WithMergeInterval(v time.Duration) Option { return func(c *config) { c.MergeInterval = v } }
func WithMergeOnOpen(v bool) Option            { return func(c *config) { c.MergeOnOpen = v } }
func WithMergeConcurrency(v int) Option        { return func(c *config) { c.MergeConcurrency = v } }
func WithMergeMaxFileSize(v int64) Option      { return func(c *config) { c.MergeMaxFileSize = v } }
func WithHintFiles(v bool) Option              { return func(c *config) { c.UseHintFiles = v } }
func WithHintFileSync(v bool) Option           { return func(c *config) { c.HintFileSync = v } }
func WithCompression(v CompressionType) Option { return func(c *config) { c.Compression = v } }
func WithCompressionThreshold(v int) Option    { return func(c *config) { c.CompressionThreshold = v } }
func WithLockTimeout(v time.Duration) Option   { return func(c *config) { c.LockTimeout = v } }
func WithLogger(v Logger) Option               { return func(c *config) { c.Logger = v } }
func WithValidateOnOpen(v bool) Option         { return func(c *config) { c.ValidateOnOpen = v } }
