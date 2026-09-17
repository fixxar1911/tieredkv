package index

import (
	"strings"
	"sync"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
)

// Index provides thread-safe metadata indexing for all keys in the database.
type Index struct {
	mu      sync.RWMutex
	entries map[string]*core.Metadata
}

// NewIndex creates a new in-memory index.
func NewIndex() *Index {
	return &Index{
		entries: make(map[string]*core.Metadata),
	}
}

// Get retrieves metadata for a key.
func (idx *Index) Get(key string) (*core.Metadata, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	meta, exists := idx.entries[key]
	if !exists {
		return nil, false
	}
	return meta.Clone(), true
}

// Set stores or updates metadata for a key.
func (idx *Index) Set(key string, meta *core.Metadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entries[key] = meta.Clone()
}

// Delete removes a key's metadata from the index.
func (idx *Index) Delete(key string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	_, exists := idx.entries[key]
	if exists {
		delete(idx.entries, key)
	}
	return exists
}

// Has checks whether a key exists in the index.
func (idx *Index) Has(key string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, exists := idx.entries[key]
	return exists
}

// Len returns the total count of keys in the index.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Keys returns all keys matching the given prefix. If prefix is empty, returns all keys.
func (idx *Index) Keys(prefix string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var keys []string
	for k := range idx.entries {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Snapshot returns a copy of all metadata in the index.
func (idx *Index) Snapshot() []*core.Metadata {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	list := make([]*core.Metadata, 0, len(idx.entries))
	for _, m := range idx.entries {
		list = append(list, m.Clone())
	}
	return list
}

// Stats computes summary counts and byte sizes for hot vs cold items in the index.
func (idx *Index) Stats() (totalKeys, hotKeys, coldKeys, hotBytes, coldBytes int64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	totalKeys = int64(len(idx.entries))
	for _, m := range idx.entries {
		if m.Tier == core.TierHot {
			hotKeys++
			hotBytes += m.Size
		} else {
			coldKeys++
			coldBytes += m.CompressedSize
		}
	}
	return
}
