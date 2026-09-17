package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Fixxar/tieredkv/pkg/tieredkv"
)

func main() {
	dataDir := flag.String("dir", "./data", "Directory path for database storage")
	hotMB := flag.Int64("hot-mb", 64, "Hot tier memory budget in Megabytes")
	coldThresholdKB := flag.Int64("cold-kb", 64, "Size threshold in KB above which values go directly to cold tier")
	compAlg := flag.String("comp", "snappy", "Cold tier compression: none, snappy, zstd")
	flag.Parse()

	var comp tieredkv.CompressionType
	switch strings.ToLower(*compAlg) {
	case "zstd":
		comp = tieredkv.CompressionZstd
	case "none":
		comp = tieredkv.CompressionNone
	default:
		comp = tieredkv.CompressionSnappy
	}

	opts := tieredkv.DefaultOptions(*dataDir)
	opts.HotTierMaxBytes = *hotMB * 1024 * 1024
	opts.ColdThresholdBytes = *coldThresholdKB * 1024
	opts.Compression = comp

	db, err := tieredkv.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database at %s: %v\n", *dataDir, err)
		os.Exit(1)
	}
	defer db.Close()

	args := flag.Args()
	if len(args) == 0 {
		runREPL(db, *dataDir)
		return
	}

	execCommand(db, args)
}

func execCommand(db *tieredkv.DB, args []string) {
	cmd := strings.ToLower(args[0])
	switch cmd {
	case "put":
		if len(args) < 3 {
			fmt.Println("Usage: put <key> <value>")
			return
		}
		key := args[1]
		val := strings.Join(args[2:], " ")
		if err := db.Put(key, []byte(val)); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		meta, _ := db.GetMeta(key)
		fmt.Printf("OK (stored in tier: %s, size: %d bytes)\n", meta.Tier, meta.Size)

	case "put-file":
		if len(args) < 3 {
			fmt.Println("Usage: put-file <key> <filepath>")
			return
		}
		key := args[1]
		filePath := args[2]
		f, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			fmt.Printf("Error stating file: %v\n", err)
			return
		}
		start := time.Now()
		ext := filepath.Ext(filePath)
		if err := db.PutStream(key, f, fi.Size(), ext); err != nil {
			fmt.Printf("Error storing file: %v\n", err)
			return
		}
		meta, _ := db.GetMeta(key)
		elapsed := time.Since(start)
		fmt.Printf("OK: stored %d bytes in %v (Tier: %s, Compressed: %d bytes)\n",
			fi.Size(), elapsed, meta.Tier, meta.CompressedSize)

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: get <key>")
			return
		}
		key := args[1]
		val, meta, err := db.Get(key)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("[%s | %d bytes] %s\n", meta.Tier, meta.Size, string(val))

	case "get-file":
		if len(args) < 3 {
			fmt.Println("Usage: get-file <key> <outpath>")
			return
		}
		key := args[1]
		outPath := args[2]
		stream, meta, err := db.GetStream(key)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		defer stream.Close()

		outFile, err := os.Create(outPath)
		if err != nil {
			fmt.Printf("Error creating output file: %v\n", err)
			return
		}
		defer outFile.Close()

		start := time.Now()
		n, err := io.Copy(outFile, stream)
		if err != nil {
			fmt.Printf("Error writing file: %v\n", err)
			return
		}
		elapsed := time.Since(start)
		fmt.Printf("OK: streamed %d bytes from tier %s to %s in %v\n", n, meta.Tier, outPath, elapsed)

	case "del", "delete":
		if len(args) < 2 {
			fmt.Println("Usage: del <key>")
			return
		}
		key := args[1]
		if err := db.Delete(key); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("OK (deleted)")

	case "info", "meta":
		if len(args) < 2 {
			fmt.Println("Usage: info <key>")
			return
		}
		key := args[1]
		meta, found := db.GetMeta(key)
		if !found {
			fmt.Println("Key not found")
			return
		}
		b, _ := json.MarshalIndent(meta, "", "  ")
		fmt.Println(string(b))

	case "promote":
		if len(args) < 2 {
			fmt.Println("Usage: promote <key>")
			return
		}
		key := args[1]
		if err := db.Promote(key); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("OK (promoted to HOT tier)")

	case "demote":
		if len(args) < 2 {
			fmt.Println("Usage: demote <key>")
			return
		}
		key := args[1]
		if err := db.Demote(key); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("OK (demoted to COLD tier)")

	case "pin":
		if len(args) < 2 {
			fmt.Println("Usage: pin <key>")
			return
		}
		key := args[1]
		if err := db.Pin(key); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("OK (pinned to HOT tier)")

	case "list":
		prefix := ""
		if len(args) > 1 {
			prefix = args[1]
		}
		keys := db.ListKeys(prefix)
		fmt.Printf("Found %d keys (prefix '%s'):\n", len(keys), prefix)
		for _, k := range keys {
			meta, _ := db.GetMeta(k)
			fmt.Printf(" - %-25s [%s] (%d bytes)\n", k, meta.Tier, meta.Size)
		}

	case "stats":
		st := db.Stats()
		b, _ := json.MarshalIndent(st, "", "  ")
		fmt.Println(string(b))

	case "bench":
		count := 50000
		if len(args) > 1 {
			if c, err := strconv.Atoi(args[1]); err == nil && c > 0 {
				count = c
			}
		}
		runBenchmark(db, count)

	default:
		fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", cmd)
	}
}

func runREPL(db *tieredkv.DB, dir string) {
	fmt.Printf("========================================================\n")
	fmt.Printf("  TieredKV CLI Shell - Active Store: %s\n", dir)
	fmt.Printf("  Commands: put, get, del, info, promote, demote, pin, list, stats, bench, exit\n")
	fmt.Printf("========================================================\n\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("tieredkv> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			fmt.Println("Goodbye.")
			break
		}
		if line == "help" {
			printHelp()
			continue
		}
		parts := strings.Fields(line)
		execCommand(db, parts)
	}
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  put <key> <val>           - Store a key-value record")
	fmt.Println("  put-file <key> <path>     - Stream a large file into store")
	fmt.Println("  get <key>                 - Retrieve a record")
	fmt.Println("  get-file <key> <outpath>  - Stream a large file out to disk")
	fmt.Println("  del <key>                 - Delete a key")
	fmt.Println("  info <key>                - Inspect key metadata and tier status")
	fmt.Println("  promote <key>             - Move key from Cold to Hot tier")
	fmt.Println("  demote <key>              - Move key from Hot to Cold tier")
	fmt.Println("  pin <key>                 - Pin key in Hot tier permanently")
	fmt.Println("  list [prefix]             - List all keys or filter by prefix")
	fmt.Println("  stats                     - Display runtime operational statistics")
	fmt.Println("  bench [count]             - Run a live benchmark")
	fmt.Println("  exit                      - Exit the CLI")
}

func runBenchmark(db *tieredkv.DB, count int) {
	fmt.Printf("Running benchmark with %d operations...\n", count)

	payload := []byte("benchmark-sample-payload-for-tieredkv-storage-test-128-bytes-long-padding-0123456789")

	// 1. Write benchmark
	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		_ = db.Put(key, payload)
	}
	writeElapsed := time.Since(start)
	writeOps := float64(count) / writeElapsed.Seconds()
	fmt.Printf("  [PUT]  %d writes in %v (~%.0f ops/sec)\n", count, writeElapsed, writeOps)

	// 2. Read benchmark
	start = time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("bench:key:%d", i)
		_, _, _ = db.Get(key)
	}
	readElapsed := time.Since(start)
	readOps := float64(count) / readElapsed.Seconds()
	fmt.Printf("  [GET]  %d reads in %v (~%.0f ops/sec)\n", count, readElapsed, readOps)

	st := db.Stats()
	fmt.Printf("  Current Stats: TotalKeys=%d, HotKeys=%d, ColdKeys=%d, Evictions=%d, Promotions=%d\n",
		st.TotalKeys, st.HotKeys, st.ColdKeys, st.EvictionsToCold, st.PromotionsToHot)
}
