package index

import (
	"encoding/binary"
	"io"
)

const HintFixedSize = 44

type HintEntry struct {
	Key     []byte
	FileID  uint32
	Offset  int64
	Size    uint32
	TS      int64
	Expires int64
	Flags   uint8
}

func WriteHint(w io.Writer, e HintEntry) error {
	buf := make([]byte, HintFixedSize+len(e.Key))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(e.Key)))
	copy(buf[4:], e.Key)
	pos := 4 + len(e.Key)
	binary.LittleEndian.PutUint32(buf[pos:pos+4], e.FileID)
	pos += 4
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(e.Offset))
	pos += 8
	binary.LittleEndian.PutUint32(buf[pos:pos+4], e.Size)
	pos += 4
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(e.TS))
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(e.Expires))
	pos += 8
	buf[pos] = e.Flags
	// reserved 7 bytes already zero
	_, err := w.Write(buf)
	return err
}

func ReadHint(r io.Reader) (HintEntry, int, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return HintEntry{}, 0, err
	}
	keyLen := int(binary.LittleEndian.Uint32(header))
	body := make([]byte, keyLen+HintFixedSize-4)
	if _, err := io.ReadFull(r, body); err != nil {
		return HintEntry{}, 0, err
	}
	key := append([]byte(nil), body[:keyLen]...)
	pos := keyLen
	fileID := binary.LittleEndian.Uint32(body[pos : pos+4])
	pos += 4
	offset := int64(binary.LittleEndian.Uint64(body[pos : pos+8]))
	pos += 8
	size := binary.LittleEndian.Uint32(body[pos : pos+4])
	pos += 4
	ts := int64(binary.LittleEndian.Uint64(body[pos : pos+8]))
	pos += 8
	expires := int64(binary.LittleEndian.Uint64(body[pos : pos+8]))
	pos += 8
	flags := body[pos]
	return HintEntry{
		Key:     key,
		FileID:  fileID,
		Offset:  offset,
		Size:    size,
		TS:      ts,
		Expires: expires,
		Flags:   flags,
	}, HintFixedSize + keyLen, nil
}
