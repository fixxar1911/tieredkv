package core

import (
	"errors"
	"time"
)

// Common errors
var (
	ErrKeyNotFound    = errors.New("tieredkv: key not found")
	ErrKeyTooLarge    = errors.New("tieredkv: key exceeds maximum allowed length")
	ErrValueTooLarge  = errors.New("tieredkv: value exceeds maximum allowed length")
	ErrCorruptData    = errors.New("tieredkv: data checksum mismatch or corrupted segment")
	ErrDBClosed       = errors.New("tieredkv: database is closed")
	ErrInvalidOptions = errors.New("tieredkv: invalid database options")
	ErrReadOnly       = errors.New("tieredkv: database is in read-only mode")
)

// TierType denotes whether a key/value currently resides in Hot or Cold storage.
type TierType string

const (
	TierHot  TierType = "HOT"
	TierCold TierType = "COLD"
)

// CompressionType defines the algorithm used for cold storage compression.
type CompressionType string

const (
	CompressionNone   CompressionType = "NONE"
	CompressionSnappy CompressionType = "SNAPPY"
	CompressionZstd   CompressionType = "ZSTD"
)

// Metadata stores runtime and persistent attributes for a stored key-value pair.
type Metadata struct {
	Key             string          `json:"key"`
	Size            int64           `json:"size"`            // Raw uncompressed value size in bytes
	CompressedSize  int64           `json:"compressed_size"` // Physical stored size
	ContentType     string          `json:"content_type,omitempty"`
	Tier            TierType        `json:"tier"`
	Compression     CompressionType `json:"compression"`
	Checksum        uint32          `json:"checksum"` // CRC32 IEEE checksum
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	LastAccessedAt  time.Time       `json:"last_accessed_at"`
	AccessCount     int64           `json:"access_count"`
	Pinned          bool            `json:"pinned"` // If true, never evicted to cold tier

	// Cold storage location pointers
	SegmentID uint32 `json:"segment_id,omitempty"`
	Offset    int64  `json:"offset,omitempty"`
	Length    int64  `json:"length,omitempty"` // stored record length

	// Inline hot payload (for small values <= InlineThreshold)
	InlineValue []byte `json:"-"`
}

// Clone creates a shallow/safe copy of Metadata.
func (m *Metadata) Clone() *Metadata {
	if m == nil {
		return nil
	}
	cp := *m
	return &cp
}

// Stats captures live runtime operational statistics of the tiered KV store.
type Stats struct {
	TotalKeys       int64 `json:"total_keys"`
	HotKeys         int64 `json:"hot_keys"`
	ColdKeys        int64 `json:"cold_keys"`
	HotBytes        int64 `json:"hot_bytes"`
	ColdBytes       int64 `json:"cold_bytes"`
	HotHits         int64 `json:"hot_hits"`
	ColdHits        int64 `json:"cold_hits"`
	Misses          int64 `json:"misses"`
	EvictionsToCold int64 `json:"evictions_to_cold"`
	PromotionsToHot int64 `json:"promotions_to_hot"`
}
