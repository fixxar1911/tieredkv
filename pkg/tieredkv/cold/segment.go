package cold

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
	"github.com/Fixxar/tieredkv/pkg/tieredkv/compress"
)

// Segment represents a single append-only log file in the cold tier.
type Segment struct {
	id       uint32
	path     string
	file     *os.File
	mu       sync.RWMutex
	size     int64
	readOnly bool
}

// OpenSegment opens or creates a segment file with the given ID.
func OpenSegment(dir string, id uint32, readOnly bool) (*Segment, error) {
	filename := fmt.Sprintf("segment_%05d.log", id)
	filePath := filepath.Join(dir, filename)

	var flag int
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	}

	file, err := os.OpenFile(filePath, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", filePath, err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Segment{
		id:       id,
		path:     filePath,
		file:     file,
		size:     fi.Size(),
		readOnly: readOnly,
	}, nil
}

// ID returns the segment ID.
func (s *Segment) ID() uint32 {
	return s.id
}

// Size returns the current size of the segment file.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Append writes a key and payload to the end of the segment.
func (s *Segment) Append(key string, rawVal []byte, compression core.CompressionType, isDeleted bool) (offset int64, length int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readOnly {
		return 0, 0, fmt.Errorf("segment %d is read-only", s.id)
	}

	compressor, err := compress.GetCompressor(compression)
	if err != nil {
		return 0, 0, err
	}

	compressedVal, err := compressor.Compress(rawVal)
	if err != nil {
		return 0, 0, fmt.Errorf("compression failed: %w", err)
	}

	var flags uint16
	if isDeleted {
		flags |= FlagDeleted
	}
	if compression != core.CompressionNone && len(compressedVal) < len(rawVal) {
		flags |= FlagCompressed
	} else {
		// If compression did not reduce size, store uncompressed
		compressedVal = rawVal
		compression = core.CompressionNone
	}

	keyBytes := []byte(key)
	crc := CalculateChecksum(keyBytes, compressedVal)

	hdr := RecordHeader{
		Magic:             RecordMagic,
		Flags:             flags,
		Compression:       compressionToByte(compression),
		Checksum:          crc,
		KeyLength:         uint32(len(keyBytes)),
		StoredValueLength: uint64(len(compressedVal)),
		RawValueLength:    uint64(len(rawVal)),
		Timestamp:         time.Now().UnixNano(),
	}

	hdrBuf := make([]byte, HeaderSize)
	EncodeHeader(&hdr, hdrBuf)

	totalLen := int64(HeaderSize + len(keyBytes) + len(compressedVal))
	writeOffset := s.size

	// Write sequentially to disk
	if _, err := s.file.Write(hdrBuf); err != nil {
		return 0, 0, err
	}
	if _, err := s.file.Write(keyBytes); err != nil {
		return 0, 0, err
	}
	if _, err := s.file.Write(compressedVal); err != nil {
		return 0, 0, err
	}

	s.size += totalLen
	return writeOffset, totalLen, nil
}

// ReadRecord reads and decodes a record at the specified offset.
func (s *Segment) ReadRecord(offset int64) ([]byte, *RecordHeader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, err := ReadRecordAt(s.file, offset)
	if err != nil {
		return nil, nil, err
	}

	compType := byteToCompression(rec.Header.Compression)
	compressor, err := compress.GetCompressor(compType)
	if err != nil {
		return nil, nil, err
	}

	rawVal, err := compressor.Decompress(rec.CompressedData)
	if err != nil {
		return nil, nil, fmt.Errorf("decompression failed: %w", err)
	}

	return rawVal, &rec.Header, nil
}

// ReadStream returns an io.ReadCloser streaming the uncompressed value directly.
func (s *Segment) ReadStream(offset int64) (io.ReadCloser, *RecordHeader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hdrBuf := make([]byte, HeaderSize)
	if _, err := s.file.ReadAt(hdrBuf, offset); err != nil {
		return nil, nil, err
	}

	hdr, err := DecodeHeader(hdrBuf)
	if err != nil {
		return nil, nil, err
	}

	// Read key to verify checksum if needed, or skip directly to payload
	valueOffset := offset + HeaderSize + int64(hdr.KeyLength)
	sectionReader := io.NewSectionReader(s.file, valueOffset, int64(hdr.StoredValueLength))

	compType := byteToCompression(hdr.Compression)
	compressor, err := compress.GetCompressor(compType)
	if err != nil {
		return nil, nil, err
	}

	stream, err := compressor.DecompressStream(sectionReader)
	if err != nil {
		return nil, nil, err
	}

	return stream, hdr, nil
}

// Sync forces disk flush.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Sync()
}

// Close closes the underlying file handle.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
