package tieredkv

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/cold"
	"github.com/Fixxar/tieredkv/pkg/tieredkv/hot"
	"github.com/Fixxar/tieredkv/pkg/tieredkv/index"
)

// DB represents an open instance of the tiered key-value database.
type DB struct {
	opts      Options
	hotCache  *hot.Cache
	coldStore *cold.Store
	index     *index.Index

	mu       sync.RWMutex
	isClosed bool

	// Atomic operational counters
	hotHits         int64
	coldHits        int64
	misses          int64
	evictionsToCold int64
	promotionsToHot int64
}

// Open initializes and opens a tiered key-value store with the provided options.
func Open(opts Options) (*DB, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("%w: DataDir cannot be empty", ErrInvalidOptions)
	}
	if opts.HotTierMaxBytes <= 0 {
		opts.HotTierMaxBytes = 64 * 1024 * 1024
	}
	if opts.ColdSegmentSizeLimit <= 0 {
		opts.ColdSegmentSizeLimit = 32 * 1024 * 1024
	}
	if opts.MaxKeyLength <= 0 {
		opts.MaxKeyLength = 1024
	}
	if opts.MaxValueLength <= 0 {
		opts.MaxValueLength = 2 * 1024 * 1024 * 1024
	}

	coldDir := filepath.Join(opts.DataDir, "cold")
	if err := os.MkdirAll(coldDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	coldStore, err := cold.OpenStore(coldDir, opts.ColdSegmentSizeLimit, opts.Compression, opts.SyncOnWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cold store: %w", err)
	}

	db := &DB{
		opts:      opts,
		coldStore: coldStore,
		index:     index.NewIndex(),
	}

	// Replay cold segments to reconstruct index
	if err := db.replayColdLog(); err != nil {
		coldStore.Close()
		return nil, fmt.Errorf("cold log recovery failed: %w", err)
	}

	// Initialize hot cache with eviction callback that persists evicted items to cold store
	db.hotCache = hot.NewCache(opts.HotTierMaxBytes, func(entry *hot.CacheEntry) {
		db.handleHotEviction(entry)
	})

	return db, nil
}

// replayColdLog iterates cold records from oldest to newest to restore index state.
func (db *DB) replayColdLog() error {
	return db.coldStore.IterateAllRecords(func(rec *cold.StoredRecord, segID uint32) error {
		if rec.Header.Flags&cold.FlagDeleted != 0 {
			db.index.Delete(rec.Key)
			return nil
		}

		compType := tieredkvCompression(rec.Header.Compression)
		meta := &Metadata{
			Key:            rec.Key,
			Size:           int64(rec.Header.RawValueLength),
			CompressedSize: int64(rec.Header.StoredValueLength),
			Tier:           TierCold,
			Compression:    compType,
			Checksum:       rec.Header.Checksum,
			CreatedAt:      rec.CreatedAt,
			UpdatedAt:      rec.CreatedAt,
			LastAccessedAt: rec.CreatedAt,
			AccessCount:    0,
			SegmentID:      segID,
			Offset:         rec.Offset,
			Length:         rec.TotalLength,
		}

		db.index.Set(rec.Key, meta)
		return nil
	})
}

func tieredkvCompression(b uint8) CompressionType {
	switch b {
	case 1:
		return CompressionSnappy
	case 2:
		return CompressionZstd
	default:
		return CompressionNone
	}
}

// handleHotEviction migrates an item evicted from hot cache into cold storage.
func (db *DB) handleHotEviction(entry *hot.CacheEntry) {
	// If database is closed, skip
	db.mu.RLock()
	closed := db.isClosed
	db.mu.RUnlock()
	if closed {
		return
	}

	// Persist to cold log
	segID, offset, length, err := db.coldStore.Append(entry.Key, entry.Value, false)
	if err != nil {
		return
	}

	meta := entry.Metadata
	meta.Tier = TierCold
	meta.SegmentID = segID
	meta.Offset = offset
	meta.Length = length
	meta.InlineValue = nil
	meta.UpdatedAt = time.Now()

	db.index.Set(entry.Key, meta)
	atomic.AddInt64(&db.evictionsToCold, 1)
}

// Put inserts or updates a key-value pair.
func (db *DB) Put(key string, value []byte) error {
	return db.PutWithMeta(key, value, "")
}

// PutWithMeta inserts or updates a key-value pair with optional content type or metadata.
func (db *DB) PutWithMeta(key string, value []byte, contentType string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return ErrDBClosed
	}

	if len(key) == 0 {
		return fmt.Errorf("%w: key cannot be empty", ErrInvalidOptions)
	}
	if len(key) > db.opts.MaxKeyLength {
		return ErrKeyTooLarge
	}
	valLen := int64(len(value))
	if valLen > db.opts.MaxValueLength {
		return ErrValueTooLarge
	}

	now := time.Now()
	crc := crc32.ChecksumIEEE(append([]byte(key), value...))

	meta := &Metadata{
		Key:            key,
		Size:           valLen,
		ContentType:    contentType,
		Checksum:       crc,
		CreatedAt:      now,
		UpdatedAt:      now,
		LastAccessedAt: now,
		AccessCount:    1,
	}

	// Route based on size
	if valLen > db.opts.ColdThresholdBytes {
		// Large value: bypass hot tier to prevent cache thrashing
		segID, offset, length, err := db.coldStore.Append(key, value, false)
		if err != nil {
			return fmt.Errorf("failed to write cold value: %w", err)
		}

		meta.Tier = TierCold
		meta.Compression = db.opts.Compression
		meta.SegmentID = segID
		meta.Offset = offset
		meta.Length = length
		meta.CompressedSize = length

		// Evict from hot cache if an older version was present there
		db.hotCache.Delete(key)
		db.index.Set(key, meta)
		return nil
	}

	// Smaller value: place in Hot cache
	meta.Tier = TierHot
	if valLen <= db.opts.InlineThresholdBytes {
		meta.InlineValue = make([]byte, valLen)
		copy(meta.InlineValue, value)
	}

	db.hotCache.Set(key, value, meta)
	db.index.Set(key, meta)
	return nil
}

// PutStream stores a value provided as an io.Reader (useful for large files or streams).
func (db *DB) PutStream(key string, r io.Reader, size int64, contentType string) error {
	if size > db.opts.MaxValueLength {
		return ErrValueTooLarge
	}

	// Read stream to memory or buffer
	buf := bytes.NewBuffer(make([]byte, 0, size))
	if _, err := io.Copy(buf, r); err != nil {
		return fmt.Errorf("failed to read stream: %w", err)
	}

	return db.PutWithMeta(key, buf.Bytes(), contentType)
}

// Get retrieves a key's value and metadata.
func (db *DB) Get(key string) ([]byte, *Metadata, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return nil, nil, ErrDBClosed
	}

	meta, found := db.index.Get(key)
	if !found {
		atomic.AddInt64(&db.misses, 1)
		return nil, nil, ErrKeyNotFound
	}

	// If currently in Hot Tier:
	if meta.Tier == TierHot {
		val, hotMeta, ok := db.hotCache.Get(key)
		if ok {
			atomic.AddInt64(&db.hotHits, 1)
			return val, hotMeta, nil
		}
		// If not in cache (e.g. was just demoted), fall back to cold
	}

	// Cold tier lookup:
	val, _, err := db.coldStore.Read(meta.SegmentID, meta.Offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read cold record: %w", err)
	}

	atomic.AddInt64(&db.coldHits, 1)
	meta.LastAccessedAt = time.Now()
	meta.AccessCount++

	// Check Promotion Policy
	if db.opts.PromoteOnAccess &&
		meta.AccessCount >= db.opts.PromoteAccessThreshold &&
		meta.Size <= db.opts.ColdThresholdBytes {
		// Promote back to Hot Cache!
		meta.Tier = TierHot
		db.hotCache.Set(key, val, meta)
		atomic.AddInt64(&db.promotionsToHot, 1)
	}

	db.index.Set(key, meta)
	return val, meta.Clone(), nil
}

// GetStream returns an io.ReadCloser streaming the value for the given key.
func (db *DB) GetStream(key string) (io.ReadCloser, *Metadata, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return nil, nil, ErrDBClosed
	}

	meta, found := db.index.Get(key)
	if !found {
		atomic.AddInt64(&db.misses, 1)
		return nil, nil, ErrKeyNotFound
	}

	if meta.Tier == TierHot {
		val, hotMeta, ok := db.hotCache.Get(key)
		if ok {
			atomic.AddInt64(&db.hotHits, 1)
			return io.NopCloser(bytes.NewReader(val)), hotMeta, nil
		}
	}

	// Stream directly from cold segment without buffering the whole file in RAM
	stream, _, err := db.coldStore.ReadStream(meta.SegmentID, meta.Offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open cold stream: %w", err)
	}

	atomic.AddInt64(&db.coldHits, 1)
	meta.LastAccessedAt = time.Now()
	meta.AccessCount++
	db.index.Set(key, meta)

	return stream, meta.Clone(), nil
}

// Delete removes a key and appends a tombstone to cold storage.
func (db *DB) Delete(key string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return ErrDBClosed
	}

	if !db.index.Has(key) {
		return ErrKeyNotFound
	}

	db.hotCache.Delete(key)
	db.index.Delete(key)

	// Append tombstone record to cold store for persistence
	_, _, _, err := db.coldStore.Append(key, nil, true)
	return err
}

// Has returns true if the key exists.
func (db *DB) Has(key string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.Has(key)
}

// GetMeta retrieves only the metadata for a key without reading the payload.
func (db *DB) GetMeta(key string) (*Metadata, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.Get(key)
}

// ListKeys returns all keys matching the given prefix.
func (db *DB) ListKeys(prefix string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.Keys(prefix)
}

// Promote manually moves a cold key into the hot tier.
func (db *DB) Promote(key string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	meta, found := db.index.Get(key)
	if !found {
		return ErrKeyNotFound
	}
	if meta.Tier == TierHot {
		return nil // already hot
	}

	val, _, err := db.coldStore.Read(meta.SegmentID, meta.Offset)
	if err != nil {
		return err
	}

	meta.Tier = TierHot
	db.hotCache.Set(key, val, meta)
	db.index.Set(key, meta)
	atomic.AddInt64(&db.promotionsToHot, 1)
	return nil
}

// Demote manually moves a hot key into the cold tier.
func (db *DB) Demote(key string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	meta, found := db.index.Get(key)
	if !found {
		return ErrKeyNotFound
	}
	if meta.Tier == TierCold {
		return nil // already cold
	}

	val, _, ok := db.hotCache.Peek(key)
	if !ok {
		return ErrKeyNotFound
	}

	segID, offset, length, err := db.coldStore.Append(key, val, false)
	if err != nil {
		return err
	}

	meta.Tier = TierCold
	meta.SegmentID = segID
	meta.Offset = offset
	meta.Length = length
	meta.CompressedSize = length
	meta.InlineValue = nil

	db.hotCache.Delete(key)
	db.index.Set(key, meta)
	atomic.AddInt64(&db.evictionsToCold, 1)
	return nil
}

// Pin prevents a key from ever being evicted to the cold tier.
func (db *DB) Pin(key string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	meta, found := db.index.Get(key)
	if !found {
		return ErrKeyNotFound
	}

	meta.Pinned = true
	db.index.Set(key, meta)
	db.hotCache.SetPinned(key, true)
	if meta.Tier == TierCold {
		return db.Promote(key)
	}
	return nil
}

// Stats returns a snapshot of database operational statistics.
func (db *DB) Stats() Stats {
	totalKeys, hotKeys, coldKeys, hotBytes, coldBytes := db.index.Stats()

	return Stats{
		TotalKeys:       totalKeys,
		HotKeys:         hotKeys,
		ColdKeys:        coldKeys,
		HotBytes:        hotBytes,
		ColdBytes:       coldBytes,
		HotHits:         atomic.LoadInt64(&db.hotHits),
		ColdHits:        atomic.LoadInt64(&db.coldHits),
		Misses:          atomic.LoadInt64(&db.misses),
		EvictionsToCold: atomic.LoadInt64(&db.evictionsToCold),
		PromotionsToHot: atomic.LoadInt64(&db.promotionsToHot),
	}
}

// Flush persists all currently hot items to cold storage so they survive restarts.
func (db *DB) Flush() error {
	entries := db.hotCache.AllEntries()
	for _, entry := range entries {
		// Only write if not already backed by cold store
		if entry.Metadata.SegmentID == 0 {
			segID, offset, length, err := db.coldStore.Append(entry.Key, entry.Value, false)
			if err != nil {
				return err
			}
			entry.Metadata.SegmentID = segID
			entry.Metadata.Offset = offset
			entry.Metadata.Length = length
			entry.Metadata.CompressedSize = length
			db.index.Set(entry.Key, entry.Metadata)
		}
	}
	return db.coldStore.Sync()
}

// Close gracefully flushes data and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return nil
	}
	db.isClosed = true

	_ = db.Flush()
	return db.coldStore.Close()
}
