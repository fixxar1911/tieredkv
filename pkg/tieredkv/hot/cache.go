package hot

import (
	"container/list"
	"sync"
	"time"

	"github.com/Fixxar/tieredkv/pkg/tieredkv/core"
)

// CacheEntry represents an item held in the hot in-memory tier.
type CacheEntry struct {
	Key      string
	Value    []byte
	Metadata *core.Metadata
	element  *list.Element
	cost     int64
}

// EvictCallback is invoked whenever an item is dropped from the hot cache due to size limits.
type EvictCallback func(entry *CacheEntry)

// Cache is a thread-safe, size-bounded LRU cache for hot key-value pairs.
type Cache struct {
	mu           sync.RWMutex
	maxBytes     int64
	currentBytes int64
	items        map[string]*CacheEntry
	lruList      *list.List
	onEvict      EvictCallback
}

// NewCache creates an in-memory hot cache with the specified capacity limit in bytes.
func NewCache(maxBytes int64, onEvict EvictCallback) *Cache {
	return &Cache{
		maxBytes: maxBytes,
		items:    make(map[string]*CacheEntry),
		lruList:  list.New(),
		onEvict:  onEvict,
	}
}

// Get retrieves a hot entry and marks it recently accessed.
func (c *Cache) Get(key string) ([]byte, *core.Metadata, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, found := c.items[key]
	if !found {
		return nil, nil, false
	}

	c.lruList.MoveToFront(entry.element)
	entry.Metadata.LastAccessedAt = time.Now()
	entry.Metadata.AccessCount++

	// Return a copy of value slice to avoid caller mutations corrupting cache
	valCopy := make([]byte, len(entry.Value))
	copy(valCopy, entry.Value)
	return valCopy, entry.Metadata.Clone(), true
}

// Peek returns the value and metadata without updating LRU position or access metrics.
func (c *Cache) Peek(key string) ([]byte, *core.Metadata, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, found := c.items[key]
	if !found {
		return nil, nil, false
	}

	valCopy := make([]byte, len(entry.Value))
	copy(valCopy, entry.Value)
	return valCopy, entry.Metadata.Clone(), true
}

// Set adds or updates an entry in the hot cache, evicting cold/old entries if budget is exceeded.
func (c *Cache) Set(key string, val []byte, meta *core.Metadata) {
	c.mu.Lock()

	cost := calculateCost(key, val)

	if existing, found := c.items[key]; found {
		c.currentBytes -= existing.cost
		existing.Value = val
		existing.Metadata = meta
		existing.cost = cost
		c.currentBytes += cost
		c.lruList.MoveToFront(existing.element)
	} else {
		entry := &CacheEntry{
			Key:      key,
			Value:    val,
			Metadata: meta,
			cost:     cost,
		}
		elem := c.lruList.PushFront(entry)
		entry.element = elem
		c.items[key] = entry
		c.currentBytes += cost
	}

	// Check if evictions are required
	var evicted []*CacheEntry
	for c.currentBytes > c.maxBytes && c.lruList.Len() > 0 {
		oldestElem := c.lruList.Back()
		if oldestElem == nil {
			break
		}
		candidate := oldestElem.Value.(*CacheEntry)
		if candidate.Metadata.Pinned {
			// Find next non-pinned candidate if any
			curr := oldestElem.Prev()
			foundUnpinned := false
			for curr != nil {
				cand := curr.Value.(*CacheEntry)
				if !cand.Metadata.Pinned {
					candidate = cand
					foundUnpinned = true
					break
				}
				curr = curr.Prev()
			}
			if !foundUnpinned {
				// All items are pinned, cannot evict further
				break
			}
		}

		c.lruList.Remove(candidate.element)
		delete(c.items, candidate.Key)
		c.currentBytes -= candidate.cost
		evicted = append(evicted, candidate)
	}
	c.mu.Unlock()

	// Notify evictions outside mutex lock to prevent deadlocks
	if c.onEvict != nil {
		for _, e := range evicted {
			c.onEvict(e)
		}
	}
}

// Delete removes a key from the hot cache.
func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, found := c.items[key]
	if !found {
		return false
	}

	c.lruList.Remove(entry.element)
	delete(c.items, key)
	c.currentBytes -= entry.cost
	return true
}

// Contains checks whether key is present in hot cache.
func (c *Cache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, found := c.items[key]
	return found
}

// SetPinned marks an entry in the hot cache as pinned or unpinned.
func (c *Cache) SetPinned(key string, pinned bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, found := c.items[key]; found {
		entry.Metadata.Pinned = pinned
		return true
	}
	return false
}

// AllEntries returns a snapshot slice of all active entries in the hot cache.
func (c *Cache) AllEntries() []*CacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries := make([]*CacheEntry, 0, len(c.items))
	for _, entry := range c.items {
		entries = append(entries, entry)
	}
	return entries
}

// Len returns the number of keys stored in the hot cache.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// BytesUsed returns the total memory bytes tracked by the hot cache.
func (c *Cache) BytesUsed() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentBytes
}

// Clear flushes all entries from the hot cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*CacheEntry)
	c.lruList.Init()
	c.currentBytes = 0
}

func calculateCost(key string, val []byte) int64 {
	// Base struct overhead + string buffer + byte slice buffer
	return int64(len(key)) + int64(len(val)) + 128
}
