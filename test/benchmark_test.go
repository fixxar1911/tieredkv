package test

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/Fixxar/tieredkv/pkg/tieredkv"
)

func BenchmarkPutSmallValues(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-small-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := tieredkv.DefaultOptions(dir)
	db, err := tieredkv.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := []byte("small-value-128-bytes-padding-to-simulate-typical-microservice-cache-record-payload-with-json-and-ids-1234567890abcdef")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k:%d", i)
		_ = db.Put(key, val)
	}
}

func BenchmarkGetHotHits(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-hot-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := tieredkv.DefaultOptions(dir)
	db, err := tieredkv.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numKeys := 1000
	val := []byte("hot-cache-record-data-sample")
	for i := 0; i < numKeys; i++ {
		_ = db.Put(fmt.Sprintf("k:%d", i), val)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k:%d", i%numKeys)
		_, _, _ = db.Get(key)
	}
}

func BenchmarkPutLargeValuesCold(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := tieredkv.DefaultOptions(dir)
	opts.ColdThresholdBytes = 16 * 1024 // > 16KB goes cold
	db, err := tieredkv.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	payloadSize := 256 * 1024 // 256KB
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("blob:%d", i)
		_ = db.Put(key, payload)
	}
}

func BenchmarkGetColdHits(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-cold-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := tieredkv.DefaultOptions(dir)
	opts.PromoteOnAccess = false // Keep in cold tier to measure cold disk lookup speed
	db, err := tieredkv.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numKeys := 500
	val := make([]byte, 4096)
	_, _ = rand.Read(val)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("cold:%d", i)
		_ = db.Put(key, val)
		_ = db.Demote(key) // force to cold storage
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("cold:%d", i%numKeys)
		_, _, _ = db.Get(key)
	}
}
