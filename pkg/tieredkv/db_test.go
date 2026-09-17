package tieredkv_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/Fixxar/tieredkv/pkg/tieredkv"
)

func newTestDB(t *testing.T, optsFn func(*tieredkv.Options)) (*tieredkv.DB, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "tieredkv-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	opts := tieredkv.DefaultOptions(dir)
	if optsFn != nil {
		optsFn(&opts)
	}

	db, err := tieredkv.Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open test db: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	})

	return db, dir
}

func TestBasicPutGetDelete(t *testing.T) {
	db, _ := newTestDB(t, nil)

	key := "user:1001"
	val := []byte(`{"name":"Alice","role":"admin"}`)

	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !db.Has(key) {
		t.Fatalf("expected key %s to exist", key)
	}

	got, meta, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch. got %s, want %s", string(got), string(val))
	}
	if meta.Tier != tieredkv.TierHot {
		t.Fatalf("expected small key to be in TierHot, got %s", meta.Tier)
	}

	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if db.Has(key) {
		t.Fatalf("expected key %s to be deleted", key)
	}

	_, _, err = db.Get(key)
	if err != tieredkv.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestHotToColdEviction(t *testing.T) {
	// Hot tier budget of only 4KB
	db, _ := newTestDB(t, func(opts *tieredkv.Options) {
		opts.HotTierMaxBytes = 4 * 1024
		opts.ColdThresholdBytes = 10 * 1024 // allows items up to 10KB in hot tier
	})

	// Insert 10 items of 1KB each (total ~10KB > 4KB budget)
	itemCount := 10
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("item:%02d", i)
		val := make([]byte, 1024)
		for j := range val {
			val[j] = byte(i)
		}
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	stats := db.Stats()
	if stats.EvictionsToCold == 0 {
		t.Fatalf("expected evictions to cold storage, got %d", stats.EvictionsToCold)
	}
	if stats.ColdKeys == 0 {
		t.Fatalf("expected cold keys > 0, got %d", stats.ColdKeys)
	}

	// Verify all items are still intact and can be retrieved
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("item:%02d", i)
		got, _, err := db.Get(key)
		if err != nil {
			t.Fatalf("failed to retrieve key %s: %v", key, err)
		}
		if len(got) != 1024 || got[0] != byte(i) {
			t.Fatalf("data corrupted for key %s", key)
		}
	}
}

func TestColdToHotPromotion(t *testing.T) {
	db, _ := newTestDB(t, func(opts *tieredkv.Options) {
		opts.HotTierMaxBytes = 64 * 1024
		opts.ColdThresholdBytes = 1024
		opts.PromoteOnAccess = true
		opts.PromoteAccessThreshold = 1
	})

	key := "doc:report"
	val := []byte("Small enough to promote but initially placed cold")

	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Force demote to cold tier
	if err := db.Demote(key); err != nil {
		t.Fatalf("Demote failed: %v", err)
	}

	meta, found := db.GetMeta(key)
	if !found || meta.Tier != tieredkv.TierCold {
		t.Fatalf("expected key to be in TierCold, got %+v", meta)
	}

	// Now read it - it should automatically promote back to TierHot
	got, metaAfter, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch")
	}
	if metaAfter.Tier != tieredkv.TierHot {
		t.Fatalf("expected key to be promoted to TierHot, got %s", metaAfter.Tier)
	}

	stats := db.Stats()
	if stats.PromotionsToHot < 1 {
		t.Fatalf("expected at least 1 promotion to hot tier, got %d", stats.PromotionsToHot)
	}
}

func TestLargePayloadStreaming(t *testing.T) {
	db, _ := newTestDB(t, func(opts *tieredkv.Options) {
		opts.ColdThresholdBytes = 16 * 1024 // > 16KB goes cold
	})

	// Generate 2MB random payload
	payloadSize := 2 * 1024 * 1024
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	key := "large:blob:1"
	if err := db.PutStream(key, bytes.NewReader(payload), int64(payloadSize), "application/octet-stream"); err != nil {
		t.Fatalf("PutStream failed: %v", err)
	}

	meta, found := db.GetMeta(key)
	if !found {
		t.Fatalf("key not found in index")
	}
	if meta.Tier != tieredkv.TierCold {
		t.Fatalf("expected large blob to be stored in TierCold, got %s", meta.Tier)
	}
	if meta.Size != int64(payloadSize) {
		t.Fatalf("size mismatch: got %d, want %d", meta.Size, payloadSize)
	}

	// Stream back
	stream, metaStream, err := db.GetStream(key)
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	defer stream.Close()

	readBuf := make([]byte, payloadSize)
	n, err := io.ReadFull(stream, readBuf)
	if err != nil {
		t.Fatalf("failed reading stream: %v", err)
	}
	if n != payloadSize {
		t.Fatalf("read length %d != payloadSize %d", n, payloadSize)
	}
	if !bytes.Equal(readBuf, payload) {
		t.Fatalf("streamed data does not match original payload")
	}
	if metaStream.ContentType != "application/octet-stream" {
		t.Fatalf("content type mismatch: got %s", metaStream.ContentType)
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "tieredkv-persist-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := tieredkv.DefaultOptions(dir)
	opts.HotTierMaxBytes = 2 * 1024
	opts.ColdThresholdBytes = 512

	// Open DB 1
	db1, err := tieredkv.Open(opts)
	if err != nil {
		t.Fatalf("Open db1 failed: %v", err)
	}

	k1 := "key:persisted:1"
	v1 := []byte("value-1")
	k2 := "key:persisted:2"
	v2 := bytes.Repeat([]byte("LargePayload"), 200) // 2400 bytes, goes to cold

	if err := db1.Put(k1, v1); err != nil {
		t.Fatalf("Put k1 failed: %v", err)
	}
	if err := db1.Put(k2, v2); err != nil {
		t.Fatalf("Put k2 failed: %v", err)
	}

	// Explicitly demote k1 so both exist on cold log
	if err := db1.Demote(k1); err != nil {
		t.Fatalf("Demote k1 failed: %v", err)
	}

	// Delete a key to test tombstone replay
	kDel := "key:deleted"
	_ = db1.Put(kDel, []byte("to be deleted"))
	_ = db1.Demote(kDel)
	if err := db1.Delete(kDel); err != nil {
		t.Fatalf("Delete kDel failed: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("Close db1 failed: %v", err)
	}

	// Reopen DB 2 from same directory
	db2, err := tieredkv.Open(opts)
	if err != nil {
		t.Fatalf("Open db2 failed: %v", err)
	}
	defer db2.Close()

	if db2.Has(kDel) {
		t.Fatalf("expected deleted key %s to not exist after restart", kDel)
	}

	got1, _, err := db2.Get(k1)
	if err != nil {
		t.Fatalf("Get k1 after restart failed: %v", err)
	}
	if !bytes.Equal(got1, v1) {
		t.Fatalf("k1 value mismatch")
	}

	got2, _, err := db2.Get(k2)
	if err != nil {
		t.Fatalf("Get k2 after restart failed: %v", err)
	}
	if !bytes.Equal(got2, v2) {
		t.Fatalf("k2 value mismatch")
	}
}

func TestPinning(t *testing.T) {
	db, _ := newTestDB(t, func(opts *tieredkv.Options) {
		opts.HotTierMaxBytes = 2 * 1024
		opts.ColdThresholdBytes = 10 * 1024
	})

	pinnedKey := "pin:important"
	pinnedVal := make([]byte, 1024)
	_ = db.Put(pinnedKey, pinnedVal)
	if err := db.Pin(pinnedKey); err != nil {
		t.Fatalf("Pin failed: %v", err)
	}

	// Insert other keys to cause evictions
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("filler:%d", i)
		v := make([]byte, 1024)
		_ = db.Put(k, v)
	}

	// Verify pinnedKey is still in Hot Tier
	meta, found := db.GetMeta(pinnedKey)
	if !found {
		t.Fatalf("pinned key lost")
	}
	if meta.Tier != tieredkv.TierHot {
		t.Fatalf("expected pinned key to remain in TierHot, got %s", meta.Tier)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	db, _ := newTestDB(t, func(opts *tieredkv.Options) {
		opts.HotTierMaxBytes = 32 * 1024
		opts.ColdThresholdBytes = 1024
	})

	var wg sync.WaitGroup
	workers := 10
	opsPerWorker := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker:%d:key:%d", workerID, i)
				val := []byte(fmt.Sprintf("val-%d-%d", workerID, i))

				if err := db.Put(key, val); err != nil {
					t.Errorf("Put failed: %v", err)
					return
				}

				got, _, err := db.Get(key)
				if err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
				if !bytes.Equal(got, val) {
					t.Errorf("got %s, want %s", string(got), string(val))
					return
				}

				if i%5 == 0 {
					_ = db.Delete(key)
				}
			}
		}(w)
	}

	wg.Wait()
}
