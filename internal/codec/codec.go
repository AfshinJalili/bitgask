package codec

import (
	"github.com/golang/snappy"
)

type CompressionType uint8

const (
	None   CompressionType = 0
	Snappy CompressionType = 1
)

func Encode(codec CompressionType, src []byte) ([]byte, error) {
	switch codec {
	case None:
		return append([]byte(nil), src...), nil
	case Snappy:
		return snappy.Encode(nil, src), nil
	default:
		return append([]byte(nil), src...), nil
	}
}

func Decode(codec CompressionType, src []byte) ([]byte, error) {
	switch codec {
	case None:
		return append([]byte(nil), src...), nil
	case Snappy:
		return snappy.Decode(nil, src)
	default:
		return append([]byte(nil), src...), nil
	}
}
