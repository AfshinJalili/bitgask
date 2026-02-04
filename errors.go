package bitgask

import "errors"

var (
	ErrKeyNotFound = errors.New("bitgask: key not found")
	ErrInvalidKey  = errors.New("bitgask: invalid key")
	ErrClosed      = errors.New("bitgask: db closed")
	ErrCorrupt     = errors.New("bitgask: corrupt data")
	ErrLocked      = errors.New("bitgask: db is locked")
	ErrOversized   = errors.New("bitgask: entry exceeds max size")
	ErrExpired     = errors.New("bitgask: key expired")
)
