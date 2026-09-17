package tieredkv

import (
	"path/filepath"
	"time"
)

// Options specifies configuration parameters for opening a tiered KV store.
type Options struct {
	// DataDir is the root directory path where cold data segments and index manifests are persisted.
	DataDir string

	// HotTierMaxBytes is the soft limit for RAM utilized by the hot tier cache.
	// When exceeded, least-recently-used items are migrated to the cold tier. Default: 64MB.
	HotTierMaxBytes int64

	// InlineThresholdBytes specifies the maximum size of values that can be kept inline
	// within the memory index. Default: 2048 (2KB).
	InlineThresholdBytes int64

	// ColdThresholdBytes specifies the size above which new writes bypass hot cache directly
	// to cold storage to prevent large BLOBs from evicting smaller hot records. Default: 65536 (64KB).
	ColdThresholdBytes int64

	// PromoteOnAccess determines whether cold tier lookups get promoted back to the hot tier.
	// Default: true.
	PromoteOnAccess bool

	// PromoteAccessThreshold is the number of accesses a cold item must receive before being
	// promoted to hot tier. Setting to 1 promotes immediately upon first cold read. Default: 1.
	PromoteAccessThreshold int64

	// Compression specifies the compression algorithm for cold storage. Default: CompressionSnappy.
	Compression CompressionType

	// ColdSegmentSizeLimit is the target file size for cold append-only log segments before rotating.
	// Default: 32MB.
	ColdSegmentSizeLimit int64

	// SyncOnWrite forces fsync after appending to cold log/WAL. Default: false (relies on OS page cache & periodic flush).
	SyncOnWrite bool

	// AutoCompactInterval specifies periodic background compaction interval for cold segments.
	// If 0, background compaction is disabled. Default: 0.
	AutoCompactInterval time.Duration

	// MaxKeyLength is the maximum permitted key length in bytes. Default: 1024 (1KB).
	MaxKeyLength int

	// MaxValueLength is the maximum permitted value size in bytes. Default: 2GB.
	MaxValueLength int64
}

// DefaultOptions returns a balanced configuration suitable for mixed small and large workloads.
func DefaultOptions(dataDir string) Options {
	return Options{
		DataDir:                filepath.Clean(dataDir),
		HotTierMaxBytes:        64 * 1024 * 1024, // 64 MB
		InlineThresholdBytes:   2 * 1024,         // 2 KB
		ColdThresholdBytes:     64 * 1024,        // 64 KB
		PromoteOnAccess:        true,
		PromoteAccessThreshold: 1,
		Compression:            CompressionSnappy,
		ColdSegmentSizeLimit:   32 * 1024 * 1024, // 32 MB
		SyncOnWrite:            false,
		AutoCompactInterval:    0,
		MaxKeyLength:           1024,
		MaxValueLength:         2 * 1024 * 1024 * 1024, // 2 GB
	}
}
