package cold

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
)

const (
	RecordMagic       uint32 = 0x544B5631 // "TKV1"
	HeaderSize               = 40
	FlagDeleted       uint16 = 1 << 0
	FlagCompressed    uint16 = 1 << 1
)

var (
	ErrInvalidMagic = errors.New("cold: invalid record magic number")
	ErrCorruptCRC   = errors.New("cold: CRC checksum mismatch")
)

// RecordHeader describes the fixed 40-byte header prepending every cold record.
type RecordHeader struct {
	Magic             uint32
	Flags             uint16
	Compression       uint8
	Reserved          uint8
	Checksum          uint32
	KeyLength         uint32
	StoredValueLength uint64
	RawValueLength    uint64
	Timestamp         int64
}

// EncodeHeader serializes the header into a 40-byte slice.
func EncodeHeader(h *RecordHeader, buf []byte) {
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint16(buf[4:6], h.Flags)
	buf[6] = h.Compression
	buf[7] = h.Reserved
	binary.BigEndian.PutUint32(buf[8:12], h.Checksum)
	binary.BigEndian.PutUint32(buf[12:16], h.KeyLength)
	binary.BigEndian.PutUint64(buf[16:24], h.StoredValueLength)
	binary.BigEndian.PutUint64(buf[24:32], h.RawValueLength)
	binary.BigEndian.PutUint64(buf[32:40], uint64(h.Timestamp))
}

// DecodeHeader deserializes the header from a 40-byte slice.
func DecodeHeader(buf []byte) (*RecordHeader, error) {
	magic := binary.BigEndian.Uint32(buf[0:4])
	if magic != RecordMagic {
		return nil, ErrInvalidMagic
	}
	return &RecordHeader{
		Magic:             magic,
		Flags:             binary.BigEndian.Uint16(buf[4:6]),
		Compression:       buf[6],
		Reserved:          buf[7],
		Checksum:          binary.BigEndian.Uint32(buf[8:12]),
		KeyLength:         binary.BigEndian.Uint32(buf[12:16]),
		StoredValueLength: binary.BigEndian.Uint64(buf[16:24]),
		RawValueLength:    binary.BigEndian.Uint64(buf[24:32]),
		Timestamp:         int64(binary.BigEndian.Uint64(buf[32:40])),
	}, nil
}

// CalculateChecksum computes IEEE CRC32 of key + stored payload.
func CalculateChecksum(key []byte, storedPayload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(key)
	h.Write(storedPayload)
	return h.Sum32()
}

func compressionToByte(c core.CompressionType) uint8 {
	switch c {
	case core.CompressionSnappy:
		return 1
	case core.CompressionZstd:
		return 2
	default:
		return 0
	}
}

func byteToCompression(b uint8) core.CompressionType {
	switch b {
	case 1:
		return core.CompressionSnappy
	case 2:
		return core.CompressionZstd
	default:
		return core.CompressionNone
	}
}

// StoredRecord holds the decoded record information read from a cold log.
type StoredRecord struct {
	Header         RecordHeader
	Key            string
	CompressedData []byte
	Offset         int64
	TotalLength    int64
	CreatedAt      time.Time
}

// ReadRecordAt reads a single record at a specified offset using io.ReaderAt.
func ReadRecordAt(ra io.ReaderAt, offset int64) (*StoredRecord, error) {
	hdrBuf := make([]byte, HeaderSize)
	if _, err := ra.ReadAt(hdrBuf, offset); err != nil {
		return nil, err
	}

	hdr, err := DecodeHeader(hdrBuf)
	if err != nil {
		return nil, err
	}

	bodyLen := int64(hdr.KeyLength) + int64(hdr.StoredValueLength)
	bodyBuf := make([]byte, bodyLen)
	if _, err := ra.ReadAt(bodyBuf, offset+HeaderSize); err != nil {
		return nil, err
	}

	keyBytes := bodyBuf[:hdr.KeyLength]
	valBytes := bodyBuf[hdr.KeyLength:]

	actualCRC := CalculateChecksum(keyBytes, valBytes)
	if actualCRC != hdr.Checksum {
		return nil, ErrCorruptCRC
	}

	return &StoredRecord{
		Header:         *hdr,
		Key:            string(keyBytes),
		CompressedData: valBytes,
		Offset:         offset,
		TotalLength:    HeaderSize + bodyLen,
		CreatedAt:      time.Unix(0, hdr.Timestamp),
	}, nil
}
