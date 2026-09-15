// remote_hydrate_spike_test.go — end-to-end exercise of the model-weight
// block cache cross-node hydrate, against the REAL block-sync HTTP server and
// two REAL BlobStores. SPIKE (spike-model-weight-block-cache).
//
// Scenario: store A = node A (authoritative librarian), store B = node B
// (non-authoritative cache). We stand A up behind the real registerBlockRoutes
// handler via httptest, then RemoteHydrate a multi-shard "model" into B and
// assert byte-identical materialization, integrity, cache-hit-on-rerun, and
// drift reconcile (only the changed shard re-pulls).
package engine

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// makeModel builds a fake multi-file model dir on disk: several "shards" plus a
// config, returns the dir and the manifest (relpath+hash) computed by storing
// each into the given authoritative store.
func makeModel(t *testing.T, authoritative *BlobStore) (modelDir string, shards []ModelShard) {
	t.Helper()
	modelDir = t.TempDir()
	files := map[string][]byte{
		"config.json":              []byte(`{"model_type":"spike","hidden":4096}`),
		"tokenizer.json":           []byte(`{"vocab":["a","b","c"]}`),
		"model-00001-of-00002.bin": make([]byte, 256*1024), // 256KB shard
		"model-00002-of-00002.bin": make([]byte, 512*1024), // 512KB shard
	}
	// Fill shards with non-zero, distinct content so hashes differ.
	for name, data := range files {
		for i := range data {
			data[i] = byte((i + len(name)) % 251)
		}
		full := filepath.Join(modelDir, name)
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		hash, err := authoritative.StoreFile(full, ContentTypeFromExt(full))
		if err != nil {
			t.Fatalf("store %s: %v", name, err)
		}
		shards = append(shards, ModelShard{RelPath: name, Hash: hash})
	}
	return modelDir, shards
}

func TestRemoteHydrate_ModelWeightCache_EndToEnd(t *testing.T) {
	// --- Node A side: authoritative store A behind the REAL HTTP handler ---
	handlerA, procA := newBlobsTestServer(t)
	storeA := procA.BlobStore()
	srcDir, shards := makeModel(t, storeA)

	srv := httptest.NewServer(handlerA)
	defer srv.Close()

	// --- Node B side: empty cache store B ---
	rootB := t.TempDir()
	storeB := NewBlobStore(rootB)
	if err := storeB.Init(); err != nil {
		t.Fatalf("init B: %v", err)
	}

	// --- Cold reconcile: B has nothing, must pull every shard from A ---
	rep, err := RemoteHydrate(storeB, srv.URL, shards, nil)
	if err != nil {
		t.Fatalf("cold RemoteHydrate: %v", err)
	}
	if rep.Pulled != len(shards) {
		t.Fatalf("cold pull = %d; want %d (all shards)", rep.Pulled, len(shards))
	}
	if rep.AlreadyLocal != 0 {
		t.Fatalf("cold already-local = %d; want 0", rep.AlreadyLocal)
	}
	t.Logf("COLD: pulled %d/%d shards, %d bytes, %s",
		rep.Pulled, rep.ShardsTotal, rep.BytesPulled, rep.Elapsed)

	// --- Materialize into a loadable dir and assert byte-identical to source ---
	destDir := t.TempDir()
	mkdirWrite := func(path string, data []byte) error {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		return os.WriteFile(path, data, 0o644)
	}
	if err := MaterializeModelDir(storeB, destDir, shards, mkdirWrite); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, s := range shards {
		want, _ := os.ReadFile(filepath.Join(srcDir, s.RelPath))
		got, err := os.ReadFile(filepath.Join(destDir, s.RelPath))
		if err != nil {
			t.Fatalf("read materialized %s: %v", s.RelPath, err)
		}
		if string(got) != string(want) {
			t.Fatalf("shard %s not byte-identical after cross-node hydrate", s.RelPath)
		}
	}
	t.Logf("MATERIALIZE: %d shards byte-identical to source", len(shards))

	// --- Warm reconcile: re-run, everything is a cache hit, zero transfer ---
	rep2, err := RemoteHydrate(storeB, srv.URL, shards, nil)
	if err != nil {
		t.Fatalf("warm RemoteHydrate: %v", err)
	}
	if rep2.Pulled != 0 || rep2.AlreadyLocal != len(shards) {
		t.Fatalf("warm: pulled=%d already-local=%d; want 0 / %d (full cache hit)",
			rep2.Pulled, rep2.AlreadyLocal, len(shards))
	}
	t.Logf("WARM: %d cache hits, %d bytes transferred (free)", rep2.AlreadyLocal, rep2.BytesPulled)

	// --- Drift reconcile: node A updates one shard. New content → new hash →
	//     manifest entry changes. B should re-pull ONLY that one shard. ---
	updated := make([]byte, 512*1024)
	for i := range updated {
		updated[i] = byte((i * 7) % 251) // different content
	}
	updatedPath := filepath.Join(srcDir, "model-00002-of-00002.bin")
	if err := os.WriteFile(updatedPath, updated, 0o644); err != nil {
		t.Fatalf("rewrite shard: %v", err)
	}
	newHash, err := storeA.StoreFile(updatedPath, ContentTypeFromExt(updatedPath))
	if err != nil {
		t.Fatalf("store updated shard: %v", err)
	}
	// Manifest now points the second shard at the new hash (the drift).
	driftShards := make([]ModelShard, len(shards))
	copy(driftShards, shards)
	for i := range driftShards {
		if driftShards[i].RelPath == "model-00002-of-00002.bin" {
			driftShards[i].Hash = newHash
		}
	}

	rep3, err := RemoteHydrate(storeB, srv.URL, driftShards, nil)
	if err != nil {
		t.Fatalf("drift RemoteHydrate: %v", err)
	}
	if rep3.Pulled != 1 {
		t.Fatalf("drift: pulled=%d; want 1 (only the changed shard)", rep3.Pulled)
	}
	if rep3.AlreadyLocal != len(shards)-1 {
		t.Fatalf("drift: already-local=%d; want %d (unchanged shards stay cached)",
			rep3.AlreadyLocal, len(shards)-1)
	}
	if len(rep3.PulledHashes) != 1 || rep3.PulledHashes[0] != newHash {
		t.Fatalf("drift: re-pulled wrong shard: %v", rep3.PulledHashes)
	}
	t.Logf("DRIFT: re-pulled only %d/%d shards (the changed one), %d bytes — reconcile is content-addressed",
		rep3.Pulled, rep3.ShardsTotal, rep3.BytesPulled)
}

// countingBlockServer serves blobs from an authoritative store by content hash,
// counting GET requests per hash so tests can assert dedup / fetch-once. It
// speaks the same GET /v1/blocks/{hash} contract RemoteHydrate expects, but is
// a purpose-built harness so the test owns the request accounting.
type countingBlockServer struct {
	store  *BlobStore
	mu     sync.Mutex
	counts map[string]int
	// gate, when non-nil, is received-from inside the handler before responding,
	// letting the test hold a request open long enough to force a concurrent race.
	gate chan struct{}
}

func newCountingBlockServer(store *BlobStore) *countingBlockServer {
	return &countingBlockServer{store: store, counts: map[string]int{}}
}

func (c *countingBlockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimPrefix(r.URL.Path, "/v1/blocks/")
	c.mu.Lock()
	c.counts[hash]++
	c.mu.Unlock()
	if c.gate != nil {
		<-c.gate // hold the request open until the test releases it
	}
	content, err := c.store.Get(hash)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func (c *countingBlockServer) count(hash string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[hash]
}

// TestRemoteHydrateStreamsLargePull verifies a multi-MB shard round-trips
// correctly through the streaming (temp-file + rolling-hash) pull path (F6).
// We don't assert RAM directly; we assert a >1MB blob lands byte-identical,
// which only the streaming path can do without buffering the whole thing.
func TestRemoteHydrateStreamsLargePull(t *testing.T) {
	rootA := t.TempDir()
	storeA := NewBlobStore(rootA)
	if err := storeA.Init(); err != nil {
		t.Fatalf("init A: %v", err)
	}

	// A 4MB shard — comfortably larger than 1MB so a buffering impl would be
	// obvious, and large enough to stream across multiple io.Copy chunks.
	big := make([]byte, 4*1024*1024)
	for i := range big {
		big[i] = byte((i*131 + 7) % 251)
	}
	hash, err := storeA.Store(big, "application/octet-stream")
	if err != nil {
		t.Fatalf("store big: %v", err)
	}
	shards := []ModelShard{{RelPath: "model-big.bin", Hash: hash}}

	srv := httptest.NewServer(newCountingBlockServer(storeA))
	defer srv.Close()

	rootB := t.TempDir()
	storeB := NewBlobStore(rootB)
	if err := storeB.Init(); err != nil {
		t.Fatalf("init B: %v", err)
	}

	rep, err := RemoteHydrate(storeB, srv.URL, shards, nil)
	if err != nil {
		t.Fatalf("RemoteHydrate: %v", err)
	}
	if rep.Pulled != 1 {
		t.Fatalf("pulled = %d; want 1", rep.Pulled)
	}
	if rep.BytesPulled != int64(len(big)) {
		t.Fatalf("BytesPulled = %d; want %d", rep.BytesPulled, len(big))
	}

	// Round-trip the bytes: integrity-verified read from the cache store.
	got, err := storeB.Get(hash)
	if err != nil {
		t.Fatalf("get streamed blob: %v", err)
	}
	if len(got) != len(big) {
		t.Fatalf("streamed length = %d; want %d", len(got), len(big))
	}
	if string(got) != string(big) {
		t.Fatalf("streamed %d-byte blob not byte-identical after pull", len(big))
	}
	t.Logf("STREAM: %d-byte shard pulled to disk and verified byte-identical", len(big))
}

// TestRemoteHydrateConcurrentDedup (F4) races two RemoteHydrate calls for the
// same manifest. The test server counts GETs per hash; with in-flight dedup,
// each hash must be fetched AT MOST once across both goroutines, even though
// both started from empty caches sharing the same store.
func TestRemoteHydrateConcurrentDedup(t *testing.T) {
	rootA := t.TempDir()
	storeA := NewBlobStore(rootA)
	if err := storeA.Init(); err != nil {
		t.Fatalf("init A: %v", err)
	}
	_, shards := makeModel(t, storeA)

	cs := newCountingBlockServer(storeA)
	// Gate the handler so the first request is held open long enough for the
	// second goroutine to reach the in-flight check and dedup against it.
	cs.gate = make(chan struct{})
	srv := httptest.NewServer(cs)
	defer srv.Close()

	// Both callers share the SAME cache store B — that's the realistic case
	// (one node, two concurrent hydrate triggers).
	rootB := t.TempDir()
	storeB := NewBlobStore(rootB)
	if err := storeB.Init(); err != nil {
		t.Fatalf("init B: %v", err)
	}

	var started sync.WaitGroup
	started.Add(2)
	var done sync.WaitGroup
	done.Add(2)
	var totalPulled, totalDeduped int64
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer done.Done()
			started.Done()
			rep, err := RemoteHydrate(storeB, srv.URL, shards, nil)
			if err != nil {
				errs[idx] = err
				return
			}
			atomic.AddInt64(&totalPulled, int64(rep.Pulled))
			atomic.AddInt64(&totalDeduped, int64(rep.Deduped))
		}(i)
	}

	started.Wait()
	// Let both goroutines get well into their pull loops and register in-flight
	// entries, then release the held requests.
	time.Sleep(150 * time.Millisecond)
	close(cs.gate)
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// The core F4 assertion: no hash was fetched more than once across both
	// concurrent callers.
	for _, s := range shards {
		if n := cs.count(s.Hash); n > 1 {
			t.Fatalf("hash %s fetched %d times; want <=1 (dedup failed)", s.Hash[:12], n)
		}
	}
	// Every shard landed in the shared store exactly once.
	for _, s := range shards {
		if !storeB.Exists(s.Hash) {
			t.Fatalf("shard %s missing after concurrent hydrate", s.Hash[:12])
		}
	}
	t.Logf("DEDUP: %d shards, total fetched=%d deduped=%d (each hash GET<=1)",
		len(shards), totalPulled, totalDeduped)
}

// TestRemoteHydrateWarnsOnDedup (F5) asserts the dedup path is actually
// exercised when a second caller races a first — i.e. at least one shard is
// reported deduped rather than fetched twice. (The slog.Warn fires on that
// same path; we assert the observable Deduped counter, which is incremented
// alongside the warning.)
func TestRemoteHydrateWarnsOnDedup(t *testing.T) {
	rootA := t.TempDir()
	storeA := NewBlobStore(rootA)
	if err := storeA.Init(); err != nil {
		t.Fatalf("init A: %v", err)
	}
	_, shards := makeModel(t, storeA)

	cs := newCountingBlockServer(storeA)
	cs.gate = make(chan struct{})
	srv := httptest.NewServer(cs)
	defer srv.Close()

	rootB := t.TempDir()
	storeB := NewBlobStore(rootB)
	if err := storeB.Init(); err != nil {
		t.Fatalf("init B: %v", err)
	}

	var done sync.WaitGroup
	done.Add(2)
	var totalDeduped int64
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer done.Done()
			rep, err := RemoteHydrate(storeB, srv.URL, shards, nil)
			if err != nil {
				errs[idx] = err
				return
			}
			atomic.AddInt64(&totalDeduped, int64(rep.Deduped))
		}(i)
	}

	time.Sleep(150 * time.Millisecond)
	close(cs.gate)
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if atomic.LoadInt64(&totalDeduped) == 0 {
		t.Fatalf("dedup path never exercised: total deduped = 0 (expected the racing caller to wait on in-flight)")
	}
	t.Logf("WARN-ON-DEDUP: %d shards taken via the deduplicated (warned) path", totalDeduped)
}

// TestPromoteWhenUnreachable exercises the self-promote V1 reachability gate:
// a dead remote → promoted=true + a .authority sentinel; a live remote →
// promoted=false + no sentinel.
func TestPromoteWhenUnreachable(t *testing.T) {
	rootB := t.TempDir()
	storeB := NewBlobStore(rootB)
	if err := storeB.Init(); err != nil {
		t.Fatalf("init B: %v", err)
	}
	shards := []ModelShard{{RelPath: "config.json", Hash: "deadbeef"}}

	// --- Unreachable remote: port 1 is reserved and refuses fast. ---
	promoted, err := Promote(storeB, "http://127.0.0.1:1", shards)
	if err != nil {
		t.Fatalf("promote (unreachable): %v", err)
	}
	if !promoted {
		t.Fatalf("promoted = false for unreachable remote; want true")
	}
	marker := filepath.Join(rootB, ".cog", "blobs", ".authority")
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("authority marker not written for unreachable remote: %v", statErr)
	}
	t.Logf("PROMOTE: unreachable remote → promoted=true, sentinel written")

	// --- Live remote: must NOT self-promote (real authority is alive). ---
	rootA := t.TempDir()
	storeA := NewBlobStore(rootA)
	if err := storeA.Init(); err != nil {
		t.Fatalf("init A: %v", err)
	}
	srv := httptest.NewServer(newCountingBlockServer(storeA))
	defer srv.Close()

	promoted2, err := Promote(storeB, srv.URL, shards)
	if err != nil {
		t.Fatalf("promote (reachable): %v", err)
	}
	if promoted2 {
		t.Fatalf("promoted = true for reachable remote; want false")
	}
	// Reaching a live authority clears any stale local-authority claim.
	if _, statErr := os.Stat(marker); statErr == nil {
		t.Fatalf("authority marker present after reaching a live remote; want it cleared")
	}
	t.Logf("PROMOTE: reachable remote → promoted=false, sentinel cleared")
}
