package bitgask

import "time"

type Stats struct {
	TotalBytes int64
	DeadBytes  int64
	Keys       int
	DataFiles  int
	LastMerge  time.Time
}

type RecordMeta struct {
	ExpiresAt time.Time
	Deleted   bool
	Timestamp time.Time
	FileID    uint32
	Offset    int64
	Size      uint32
}
