package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	FlagDeleted    uint8 = 1 << 0
	FlagCompressed uint8 = 1 << 1
)

const HeaderSize = 32

var ErrCorrupt = errors.New("record: corrupt")

// Record represents a single log entry.
type Record struct {
	Timestamp int64
	ExpiresAt int64
	Key       []byte
	Value     []byte
	Flags     uint8
	Codec     uint8
}

func Size(keyLen, valueLen int) int {
	return HeaderSize + keyLen + valueLen
}

func Encode(rec Record) ([]byte, error) {
	keyLen := len(rec.Key)
	valLen := len(rec.Value)
	buf := make([]byte, HeaderSize+keyLen+valLen)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(rec.Timestamp))
	binary.LittleEndian.PutUint64(buf[12:20], uint64(rec.ExpiresAt))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(keyLen))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(valLen))
	buf[28] = rec.Flags
	buf[29] = rec.Codec
	// buf[30:32] reserved zeros
	copy(buf[HeaderSize:], rec.Key)
	copy(buf[HeaderSize+keyLen:], rec.Value)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	return buf, nil
}

func DecodeFrom(r io.Reader) (Record, int, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, 0, err
	}
	keyLen := int(binary.LittleEndian.Uint32(header[20:24]))
	valLen := int(binary.LittleEndian.Uint32(header[24:28]))
	total := HeaderSize + keyLen + valLen
	payload := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Record{}, 0, err
	}
	buf := make([]byte, total)
	copy(buf, header)
	copy(buf[HeaderSize:], payload)
	if crc32.ChecksumIEEE(buf[4:]) != binary.LittleEndian.Uint32(buf[0:4]) {
		return Record{}, 0, ErrCorrupt
	}
	rec := Record{
		Timestamp: int64(binary.LittleEndian.Uint64(header[4:12])),
		ExpiresAt: int64(binary.LittleEndian.Uint64(header[12:20])),
		Key:       append([]byte(nil), payload[:keyLen]...),
		Value:     append([]byte(nil), payload[keyLen:]...),
		Flags:     header[28],
		Codec:     header[29],
	}
	return rec, total, nil
}

func ReadAt(r io.ReaderAt, offset int64) (Record, int, error) {
	header := make([]byte, HeaderSize)
	if _, err := r.ReadAt(header, offset); err != nil {
		return Record{}, 0, err
	}
	keyLen := int(binary.LittleEndian.Uint32(header[20:24]))
	valLen := int(binary.LittleEndian.Uint32(header[24:28]))
	total := HeaderSize + keyLen + valLen
	buf := make([]byte, total)
	copy(buf, header)
	if _, err := r.ReadAt(buf[HeaderSize:], offset+HeaderSize); err != nil {
		return Record{}, 0, err
	}
	if crc32.ChecksumIEEE(buf[4:]) != binary.LittleEndian.Uint32(buf[0:4]) {
		return Record{}, 0, ErrCorrupt
	}
	payload := buf[HeaderSize:]
	rec := Record{
		Timestamp: int64(binary.LittleEndian.Uint64(header[4:12])),
		ExpiresAt: int64(binary.LittleEndian.Uint64(header[12:20])),
		Key:       append([]byte(nil), payload[:keyLen]...),
		Value:     append([]byte(nil), payload[keyLen:]...),
		Flags:     header[28],
		Codec:     header[29],
	}
	return rec, total, nil
}
