package compress

import (
	"bytes"
	"fmt"
	"io"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// Compressor defines operations for compressing and decompressing byte slices.
type Compressor interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
	CompressStream(w io.Writer) io.WriteCloser
	DecompressStream(r io.Reader) (io.ReadCloser, error)
	Type() core.CompressionType
}

// GetCompressor returns the Compressor implementation for the given algorithm.
func GetCompressor(t core.CompressionType) (Compressor, error) {
	switch t {
	case core.CompressionNone, "":
		return &noneCompressor{}, nil
	case core.CompressionSnappy:
		return &snappyCompressor{}, nil
	case core.CompressionZstd:
		return newZstdCompressor()
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", t)
	}
}

// --- None Compressor ---
type noneCompressor struct{}

func (n *noneCompressor) Compress(src []byte) ([]byte, error) {
	return src, nil
}

func (n *noneCompressor) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func (n *noneCompressor) CompressStream(w io.Writer) io.WriteCloser {
	return nopWriteCloser{Writer: w}
}

func (n *noneCompressor) DecompressStream(r io.Reader) (io.ReadCloser, error) {
	if rc, ok := r.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(r), nil
}

func (n *noneCompressor) Type() core.CompressionType {
	return core.CompressionNone
}

// --- Snappy Compressor ---
type snappyCompressor struct{}

func (s *snappyCompressor) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := snappy.NewBufferedWriter(&buf)
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *snappyCompressor) Decompress(src []byte) ([]byte, error) {
	r := snappy.NewReader(bytes.NewReader(src))
	return io.ReadAll(r)
}

func (s *snappyCompressor) CompressStream(w io.Writer) io.WriteCloser {
	return snappy.NewBufferedWriter(w)
}

func (s *snappyCompressor) DecompressStream(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(snappy.NewReader(r)), nil
}

func (s *snappyCompressor) Type() core.CompressionType {
	return core.CompressionSnappy
}

// --- Zstd Compressor ---
type zstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func newZstdCompressor() (Compressor, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &zstdCompressor{encoder: enc, decoder: dec}, nil
}

func (z *zstdCompressor) Compress(src []byte) ([]byte, error) {
	return z.encoder.EncodeAll(src, make([]byte, 0, len(src)/2)), nil
}

func (z *zstdCompressor) Decompress(src []byte) ([]byte, error) {
	return z.decoder.DecodeAll(src, nil)
}

func (z *zstdCompressor) CompressStream(w io.Writer) io.WriteCloser {
	zw, _ := zstd.NewWriter(w)
	return zw
}

func (z *zstdCompressor) DecompressStream(r io.Reader) (io.ReadCloser, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return zr.IOReadCloser(), nil
}

func (z *zstdCompressor) Type() core.CompressionType {
	return core.CompressionZstd
}

