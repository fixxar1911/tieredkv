# TieredKV: Tiered Key-Value Storage Engine in Go

A high-performance, modular key-value storage engine in Go engineered to handle **small records** and **very large, complex values** (BLOBs, media, documents) seamlessly using an intelligent **Hot vs. Cold storage tiering hierarchy**.

---

## Key Highlights

- **Decoupled Key-Value Architecture**: Inspired by WiscKey. Small metadata and inline keys stay fast in memory, while heavy values are managed in segmented append-only logs (`vLog`) with zero write-amplification.
- **Intelligent Hot/Cold Tiering**:
  - **Hot Tier**: Thread-safe in-memory cache with byte-budget tracking, LRU eviction, and key pinning.
  - **Cold Tier**: Segmented append-only disk files with crash recovery, CRC32 checksums, and transparent compression (**Snappy** or **Zstd**).
  - **Adaptive Promotion/Demotion**: Cold keys accessed repeatedly are promoted to Hot RAM on-the-fly; when Hot RAM budget is reached, least recently used records are demoted to Cold disk without downtime.
- **Streaming for Large & Complex BLOBs**: Supports `io.Reader` and `io.ReadCloser` (`PutStream` / `GetStream`), streaming multi-gigabyte files directly to and from disk without ballooning memory.
- **Interactive & Scriptable CLI (`kvctl`)**: Embedded command-line tool with REPL mode, file streaming, inspection, and live benchmarking.

---

## Performance Benchmarks

*(Benchmarked on AMD Ryzen 9 7950X3D)*

| Benchmark Scenario | Throughput / Latency | Details |
| :--- | :--- | :--- |
| **Hot In-Memory Lookups (`Get`)** | **~262.8 ns / op** | **> 3.8 Million reads/sec** |
| **Hot In-Memory Writes (`Put`)** | **~6.1 µs / op** | Inline & Hot cache budget |
| **Large Value Cold Writes (`256KB`)** | **629.83 MB / sec** | Direct-to-disk bypass with compression |
| **Cold Disk Reads (`4KB + CRC`)** | **~6.9 µs / op** | Direct offset seek + decompression |

---

## Architecture Overview

```
                          +-------------------------------+
                          |        Client Application     |
                          |   (Get, Put, Stream, Delete)  |
                          +-------------------------------+
                                          |
                                          v
                          +-------------------------------+
                          |          Tier Router          |
                          |  - Size Threshold Routing     |
                          |  - Pinning & Lifecycle State  |
                          +-------------------------------+
                                  /               \
              [Size <= ColdThreshold]        [Size > ColdThreshold]
                                /                   \
                               v                     v
                +-------------------------+   +-------------------------+
                |         HOT TIER        |   |        COLD TIER        |
                | - Size-Bounded LRU RAM  |   | - Segmented Append Log  |
                | - Micro-lookups (<2KB)  |   | - Transparent Snappy/Zstd|
                | - Fast Concurrency RWMu |   | - CRC32 Verification    |
                +-------------------------+   +-------------------------+
                             |       ^                     |
                             |       |---- Read Promotion -|
                             |               (On Access)
                             |
                             +---- Demote on Eviction ---->
```

---

## Go API Usage

### 1. Opening a Database

```go
package main

import (
    "fmt"
    "log"

    "github.com/Fixxar/tieredkv/pkg/tieredkv"
)

func main() {
    opts := tieredkv.DefaultOptions("./my-data")
    opts.HotTierMaxBytes = 64 * 1024 * 1024     // 64 MB hot RAM budget
    opts.ColdThresholdBytes = 64 * 1024        // Values > 64 KB go straight to cold tier
    opts.Compression = tieredkv.CompressionSnappy
    opts.PromoteOnAccess = true                // Promote cold items upon lookup

    db, err := tieredkv.Open(opts)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. Put small record (resides in Hot Tier)
    _ = db.Put("user:101", []byte(`{"name":"Alice","tier":"hot"}`))

    // 3. Get record
    val, meta, err := db.Get("user:101")
    fmt.Printf("Got value from %s: %s\n", meta.Tier, string(val))

    // 4. Pin a key to prevent it from ever being evicted to cold tier
    _ = db.Pin("user:101")

    // 5. Query stats
    stats := db.Stats()
    fmt.Printf("Hot Keys: %d, Cold Keys: %d\n", stats.HotKeys, stats.ColdKeys)
}
```

### 2. Streaming Large Files / BLOBs

```go
// Store a 500MB video without loading it all into memory
file, _ := os.Open("render.mp4")
defer file.Close()
fi, _ := file.Stat()

err := db.PutStream("media:render.mp4", file, fi.Size(), "video/mp4")

// Stream back out directly to HTTP response or disk
stream, meta, err := db.GetStream("media:render.mp4")
defer stream.Close()

io.Copy(outputFile, stream)
```

---

## CLI Tool (`kvctl`)

A full-featured CLI is included under `cmd/kvctl`.

### Building
```bash
go build -o bin/kvctl.exe ./cmd/kvctl
```

### Commands

| Command | Example | Description |
| :--- | :--- | :--- |
| `put` | `kvctl -dir ./data put key1 "value"` | Put string key-value |
| `put-file` | `kvctl -dir ./data put-file doc:big bigfile.pdf` | Stream file into cold storage |
| `get` | `kvctl -dir ./data get key1` | Retrieve value and inspect tier |
| `get-file` | `kvctl -dir ./data get-file doc:big out.pdf` | Stream file out of database |
| `info` | `kvctl -dir ./data info key1` | Inspect JSON metadata (checksum, size, tier, offsets) |
| `promote` | `kvctl -dir ./data promote key1` | Manually promote item from Cold to Hot tier |
| `demote` | `kvctl -dir ./data demote key1` | Manually demote item from Hot to Cold tier |
| `pin` | `kvctl -dir ./data pin key1` | Pin key permanently to Hot tier |
| `list` | `kvctl -dir ./data list user:` | List keys matching prefix |
| `stats` | `kvctl -dir ./data stats` | Display runtime operational counters |
| `bench` | `kvctl -dir ./data bench 10000` | Run interactive speed benchmark |

Running `kvctl` with no arguments opens the **interactive shell** (`REPL`).

---

## Running the Tests

```bash
# Run all unit and integration tests
go test -v ./pkg/tieredkv

# Run performance benchmarks
go test -bench . github.com/Fixxar/tieredkv/test
```
