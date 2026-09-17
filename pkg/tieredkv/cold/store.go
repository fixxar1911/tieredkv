package cold

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
)

// Store manages multiple log segments for cold storage.
type Store struct {
	dir              string
	segmentSizeLimit int64
	compression      core.CompressionType
	syncOnWrite      bool

	mu            sync.RWMutex
	segments      map[uint32]*Segment
	activeSegment *Segment
	nextSegmentID uint32
	totalBytes    int64
}

// OpenStore opens or initializes the cold store in the specified directory.
func OpenStore(dir string, segmentSizeLimit int64, comp core.CompressionType, syncOnWrite bool) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cold store directory: %w", err)
	}

	store := &Store{
		dir:              dir,
		segmentSizeLimit: segmentSizeLimit,
		compression:      comp,
		syncOnWrite:      syncOnWrite,
		segments:         make(map[uint32]*Segment),
	}

	if err := store.loadSegments(); err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

// loadSegments scans the cold directory and loads existing log segments.
func (cs *Store) loadSegments() error {
	entries, err := os.ReadDir(cs.dir)
	if err != nil {
		return err
	}

	var segIDs []uint32
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".log") {
			idStr := strings.TrimSuffix(strings.TrimPrefix(name, "segment_"), ".log")
			if id, err := strconv.ParseUint(idStr, 10, 32); err == nil {
				segIDs = append(segIDs, uint32(id))
			}
		}
	}

	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	var total int64
	for i, id := range segIDs {
		// All prior segments are read-only; the last one is active (unless empty/rotating)
		isLast := (i == len(segIDs)-1)
		seg, err := OpenSegment(cs.dir, id, !isLast)
		if err != nil {
			return err
		}
		cs.segments[id] = seg
		total += seg.Size()

		if isLast {
			cs.activeSegment = seg
			cs.nextSegmentID = id + 1
		}
	}

	cs.totalBytes = total

	// If no segments existed, create segment 1
	if cs.activeSegment == nil {
		seg, err := OpenSegment(cs.dir, 1, false)
		if err != nil {
			return err
		}
		cs.segments[1] = seg
		cs.activeSegment = seg
		cs.nextSegmentID = 2
	}

	return nil
}

// Append writes a value to the active cold segment, rotating if necessary.
func (cs *Store) Append(key string, val []byte, isDeleted bool) (segID uint32, offset int64, length int64, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Check if active segment needs rotation
	if cs.activeSegment.Size() >= cs.segmentSizeLimit {
		if err := cs.rotateSegmentLocked(); err != nil {
			return 0, 0, 0, err
		}
	}

	segID = cs.activeSegment.ID()
	offset, length, err = cs.activeSegment.Append(key, val, cs.compression, isDeleted)
	if err != nil {
		return 0, 0, 0, err
	}

	if cs.syncOnWrite {
		_ = cs.activeSegment.Sync()
	}

	cs.totalBytes += length
	return segID, offset, length, nil
}

func (cs *Store) rotateSegmentLocked() error {
	// Sync current active segment
	_ = cs.activeSegment.Sync()

	newID := cs.nextSegmentID
	cs.nextSegmentID++

	newSeg, err := OpenSegment(cs.dir, newID, false)
	if err != nil {
		return fmt.Errorf("failed to rotate to new segment %d: %w", newID, err)
	}

	cs.segments[newID] = newSeg
	cs.activeSegment = newSeg
	return nil
}

// Read retrieves a record from cold storage given segment ID and offset.
func (cs *Store) Read(segID uint32, offset int64) ([]byte, *RecordHeader, error) {
	cs.mu.RLock()
	seg, exists := cs.segments[segID]
	cs.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("segment %d not found", segID)
	}

	return seg.ReadRecord(offset)
}

// ReadStream returns an io.ReadCloser streaming the record payload without reading everything to RAM.
func (cs *Store) ReadStream(segID uint32, offset int64) (io.ReadCloser, *RecordHeader, error) {
	cs.mu.RLock()
	seg, exists := cs.segments[segID]
	cs.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("segment %d not found", segID)
	}

	return seg.ReadStream(offset)
}

// IterateAllRecords scans all segments sequentially. Used for index recovery on startup.
func (cs *Store) IterateAllRecords(fn func(rec *StoredRecord, segID uint32) error) error {
	cs.mu.RLock()
	// Get sorted segment IDs
	var ids []uint32
	for id := range cs.segments {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	cs.mu.RUnlock()

	for _, id := range ids {
		cs.mu.RLock()
		seg := cs.segments[id]
		cs.mu.RUnlock()

		segSize := seg.Size()
		var offset int64 = 0

		for offset < segSize {
			rec, err := ReadRecordAt(seg.file, offset)
			if err != nil {
				if err == io.EOF || errorsIsCorruptOrTruncated(err) {
					// Hit EOF or truncated partial write at end of segment
					break
				}
				return fmt.Errorf("error reading segment %d at offset %d: %w", id, offset, err)
			}

			if err := fn(rec, id); err != nil {
				return err
			}

			offset += rec.TotalLength
		}
	}

	return nil
}

func errorsIsCorruptOrTruncated(err error) bool {
	return err == io.ErrUnexpectedEOF || err == ErrInvalidMagic || err == ErrCorruptCRC
}

// TotalBytes returns total bytes occupied across all cold segments.
func (cs *Store) TotalBytes() int64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.totalBytes
}

// Sync flushes all unwritten buffers to disk.
func (cs *Store) Sync() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.activeSegment != nil {
		return cs.activeSegment.Sync()
	}
	return nil
}

// Close gracefully closes all segment files.
func (cs *Store) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var firstErr error
	for _, seg := range cs.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
