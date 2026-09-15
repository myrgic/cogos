package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlobStoreRoundTrip(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	bs := NewBlobStore(tmp)
	if err := bs.Init(); err != nil {
		t.Fatal("init:", err)
	}

	content := []byte("hello world this is a test blob for CogOS")
	hash, err := bs.Store(content, "text/plain", "cog://test/blob")
	if err != nil {
		t.Fatal("store:", err)
	}

	if hash == "" {
		t.Fatal("hash is empty")
	}
	if len(hash) != 64 {
		t.Errorf("hash length = %d; want 64", len(hash))
	}

	// Exists
	if !bs.Exists(hash) {
		t.Error("blob should exist after store")
	}
	if bs.Exists("0000000000000000000000000000000000000000000000000000000000000000") {
		t.Error("nonexistent hash should not exist")
	}

	// Get
	got, err := bs.Get(hash)
	if err != nil {
		t.Fatal("get:", err)
	}
	if string(got) != string(content) {
		t.Errorf("content mismatch: got %q", got)
	}

	// Idempotent store
	hash2, err := bs.Store(content, "text/plain")
	if err != nil {
		t.Fatal("second store:", err)
	}
	if hash2 != hash {
		t.Errorf("idempotent store: hash1=%s hash2=%s", hash, hash2)
	}
}

func TestBlobStoreList(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	bs := NewBlobStore(tmp)
	_ = bs.Init()

	_, _ = bs.Store([]byte("blob one"), "text/plain")
	_, _ = bs.Store([]byte("blob two"), "text/plain")

	entries, err := bs.List()
	if err != nil {
		t.Fatal("list:", err)
	}
	if len(entries) != 2 {
		t.Errorf("list count = %d; want 2", len(entries))
	}
}

func TestBlobStoreGC(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	bs := NewBlobStore(tmp)
	_ = bs.Init()

	h1, _ := bs.Store([]byte("keep me"), "text/plain")
	h2, _ := bs.Store([]byte("delete me"), "text/plain")

	refs := map[string]bool{h1: true}
	removed, freed, err := bs.GC(refs)
	if err != nil {
		t.Fatal("gc:", err)
	}
	if removed != 1 {
		t.Errorf("removed = %d; want 1", removed)
	}
	if freed <= 0 {
		t.Error("freed should be > 0")
	}

	if !bs.Exists(h1) {
		t.Error("referenced blob should survive GC")
	}
	if bs.Exists(h2) {
		t.Error("unreferenced blob should be removed by GC")
	}
}

func TestBlobStoreFile(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	bs := NewBlobStore(tmp)
	_ = bs.Init()

	// Create a test file
	testFile := filepath.Join(tmp, "test.pdf")
	_ = os.WriteFile(testFile, []byte("%PDF-1.4 fake pdf content"), 0o644)

	hash, err := bs.StoreFile(testFile, "application/pdf")
	if err != nil {
		t.Fatal("store file:", err)
	}

	got, err := bs.Get(hash)
	if err != nil {
		t.Fatal("get:", err)
	}
	if string(got) != "%PDF-1.4 fake pdf content" {
		t.Error("content mismatch")
	}
}

func TestBlobPointerWrite(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	bs := NewBlobStore(tmp)
	_ = bs.Init()

	hash, _ := bs.Store([]byte("test content"), "application/pdf")
	pointerPath := filepath.Join(tmp, "pointer.cog.md")

	err := bs.WritePointer(pointerPath, hash, 12, "application/pdf", "papers/test.pdf")
	if err != nil {
		t.Fatal("write pointer:", err)
	}

	content, err := os.ReadFile(pointerPath)
	if err != nil {
		t.Fatal("read pointer:", err)
	}

	s := string(content)
	if !strings.Contains(s, "type: blob.pointer") {
		t.Error("pointer missing type field")
	}
	if !strings.Contains(s, "hash: "+hash) {
		t.Error("pointer missing hash")
	}
	if !strings.Contains(s, "content_type: application/pdf") {
		t.Error("pointer missing content_type")
	}
}

func TestShouldRedirectToBlob(t *testing.T) {
	tests := []struct {
		path string
		size int64
		want bool
	}{
		{"audio.mp3", 100, true},        // always redirect audio
		{"model.bin", 100, true},        // always redirect binary
		{"small.pdf", 1000, false},      // small PDF OK
		{"big.pdf", 6_000_000, true},    // big PDF → blob
		{"readme.md", 500, false},       // small text OK
		{"huge.json", 15_000_000, true}, // big JSON → blob
		{"image.png", 500_000, false},   // small image OK
		{"image.png", 2_000_000, true},  // big image → blob
	}

	for _, tt := range tests {
		got := ShouldRedirectToBlob(tt.path, tt.size)
		if got != tt.want {
			t.Errorf("ShouldRedirectToBlob(%q, %d) = %v; want %v", tt.path, tt.size, got, tt.want)
		}
	}
}
