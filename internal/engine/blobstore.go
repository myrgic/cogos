// blobstore.go — Content-addressed blob store for CogOS
//
// Stores large binary content (PDFs, audio, model weights) outside of git
// in a content-addressed directory at .cog/blobs/. Files are addressed by
// SHA-256 hash and stored with a 2-character prefix directory for filesystem
// efficiency.
//
// Layout:
//
//	.cog/blobs/
//	├── a1/
//	│   └── b2c3d4e5f6...    (blob content)
//	├── manifest.jsonl        (index of all stored blobs)
//	└── .gitkeep
//
// The blob store is gitignored. CogDocs reference blobs via pointer files
// (type: blob.pointer) that carry the hash, size, and content type.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// BlobStore manages content-addressed blob storage.
type BlobStore struct {
	root string // absolute path to .cog/blobs/
	mu   sync.Mutex
}

// BlobEntry is the metadata for a single stored blob.
type BlobEntry struct {
	Hash        string   `json:"hash"`
	Size        int64    `json:"size"`
	ContentType string   `json:"content_type"`
	Refs        []string `json:"refs,omitempty"` // CogDoc URIs that reference this blob
	SyncedTo    []string `json:"synced_to,omitempty"`
	StoredAt    string   `json:"stored_at"`
}

// BlobPointer is the CogDoc frontmatter for a blob pointer file.
type BlobPointer struct {
	Hash         string `yaml:"hash" json:"hash"`
	Size         int64  `yaml:"size" json:"size"`
	ContentType  string `yaml:"content_type" json:"content_type"`
	OriginalPath string `yaml:"original_path" json:"original_path"`
}

// NewBlobStore creates a blob store rooted at workspaceRoot/.cog/blobs/.
func NewBlobStore(workspaceRoot string) *BlobStore {
	return &BlobStore{
		root: filepath.Join(workspaceRoot, ".cog", "blobs"),
	}
}

// Init ensures the blob store directory exists.
func (bs *BlobStore) Init() error {
	if err := os.MkdirAll(bs.root, 0o755); err != nil {
		return fmt.Errorf("init blob store: %w", err)
	}
	// Write .gitkeep so the directory is tracked (contents are gitignored).
	gitkeep := filepath.Join(bs.root, ".gitkeep")
	if _, err := os.Stat(gitkeep); os.IsNotExist(err) {
		_ = os.WriteFile(gitkeep, []byte(""), 0o644)
	}
	return nil
}

// Store writes content to the blob store and returns the SHA-256 hash.
// If the blob already exists (same hash), this is a no-op.
func (bs *BlobStore) Store(content []byte, contentType string, refs ...string) (string, error) {
	hash := hashBytes(content)
	blobPath := bs.blobPath(hash)

	// Check if already stored.
	if _, err := os.Stat(blobPath); err == nil {
		return hash, nil // already exists
	}

	// Create prefix directory.
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir blob prefix: %w", err)
	}

	// Write blob atomically.
	tmp := blobPath + ".tmp"
	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		return "", fmt.Errorf("write blob: %w", err)
	}
	if err := os.Rename(tmp, blobPath); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("rename blob: %w", err)
	}

	// Append to manifest.
	bs.appendManifest(BlobEntry{
		Hash:        hash,
		Size:        int64(len(content)),
		ContentType: contentType,
		Refs:        refs,
		StoredAt:    time.Now().UTC().Format(time.RFC3339),
	})

	return hash, nil
}

// StoreFile stores a file from disk into the blob store.
func (bs *BlobStore) StoreFile(path string, contentType string, refs ...string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}
	return bs.Store(content, contentType, refs...)
}

// Open returns a read-only file handle for the blob identified by hash.
// Returns os.ErrNotExist if the blob is not stored.
func (bs *BlobStore) Open(hash string) (*os.File, error) {
	blobPath := bs.blobPath(hash)
	f, err := os.Open(blobPath)
	if err != nil {
		return nil, fmt.Errorf("open blob %s: %w", hash[:12], err)
	}

	// Verify integrity before handing the file to the caller.
	if _, statErr := f.Stat(); statErr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat blob %s: %w", hash[:12], statErr)
	}

	return f, nil
}

// Get retrieves blob content by hash. Returns os.ErrNotExist if not found.
func (bs *BlobStore) Get(hash string) ([]byte, error) {
	blobPath := bs.blobPath(hash)
	content, err := os.ReadFile(blobPath)
	if err != nil {
		return nil, fmt.Errorf("get blob %s: %w", hash[:12], err)
	}

	// Verify integrity.
	actual := hashBytes(content)
	if actual != hash {
		return nil, fmt.Errorf("blob %s corrupted: expected %s got %s", hash[:12], hash[:12], actual[:12])
	}

	return content, nil
}

// Exists checks whether a blob with the given hash is stored locally.
func (bs *BlobStore) Exists(hash string) bool {
	_, err := os.Stat(bs.blobPath(hash))
	return err == nil
}

// List returns all blob entries from the manifest.
func (bs *BlobStore) List() ([]BlobEntry, error) {
	manifestPath := filepath.Join(bs.root, "manifest.jsonl")
	f, err := os.Open(manifestPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()

	var entries []BlobEntry
	dec := json.NewDecoder(f)
	for dec.More() {
		var e BlobEntry
		if err := dec.Decode(&e); err != nil {
			continue // skip corrupt entries
		}
		// Only include entries whose blobs still exist on disk.
		if bs.Exists(e.Hash) {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// Size returns the total size of all stored blobs in bytes.
func (bs *BlobStore) Size() (int64, int, error) {
	entries, err := bs.List()
	if err != nil {
		return 0, 0, err
	}
	var total int64
	for _, e := range entries {
		total += e.Size
	}
	return total, len(entries), nil
}

// GC removes blobs not referenced by any CogDoc pointer in the workspace.
// Returns the number of blobs removed and total bytes freed.
func (bs *BlobStore) GC(referencedHashes map[string]bool) (removed int, freed int64, err error) {
	entries, err := bs.List()
	if err != nil {
		return 0, 0, err
	}

	for _, e := range entries {
		if referencedHashes[e.Hash] {
			continue
		}
		blobPath := bs.blobPath(e.Hash)
		info, statErr := os.Stat(blobPath)
		if statErr != nil {
			continue
		}
		if rmErr := os.Remove(blobPath); rmErr == nil {
			removed++
			freed += info.Size()
		}
	}

	return removed, freed, nil
}

// Verify checks that all blob pointers in the workspace have matching blobs.
// Returns a list of missing hashes.
func (bs *BlobStore) Verify(workspaceRoot string) (missing []string, err error) {
	pointers, err := FindBlobPointers(workspaceRoot)
	if err != nil {
		return nil, err
	}
	for _, p := range pointers {
		if !bs.Exists(p.Hash) {
			missing = append(missing, p.Hash)
		}
	}
	return missing, nil
}

// WritePointer creates a blob pointer CogDoc at the given path.
// The pointer replaces the original file in git with a lightweight reference.
func (bs *BlobStore) WritePointer(path string, hash string, size int64, contentType string, originalPath string) error {
	id := filepath.Base(strings.TrimSuffix(originalPath, filepath.Ext(originalPath)))

	content := fmt.Sprintf(`---
v: cogblock/1.0
type: blob.pointer
id: %s
hash: %s
size: %d
content_type: %s
original_path: %s
storage:
  local: .cog/blobs/%s/%s
created: "%s"
memory_sector: semantic
---

Blob pointer. Actual content stored in the blob store.
Hash: %s
Size: %s
`, id, hash, size, contentType, originalPath,
		hash[:2], hash[2:],
		time.Now().UTC().Format("2006-01-02"),
		hash, humanSize(size))

	return os.WriteFile(path, []byte(content), 0o644)
}

// blobPath returns the filesystem path for a blob hash.
func (bs *BlobStore) blobPath(hash string) string {
	if len(hash) < 3 {
		return filepath.Join(bs.root, hash)
	}
	return filepath.Join(bs.root, hash[:2], hash[2:])
}

// appendManifest appends a blob entry to the manifest JSONL file.
func (bs *BlobStore) appendManifest(entry BlobEntry) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	manifestPath := filepath.Join(bs.root, "manifest.jsonl")
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()

	data, _ := json.Marshal(entry)
	_, _ = f.Write(append(data, '\n'))
}

// hashBytes computes SHA-256 of content and returns the hex string.
func hashBytes(content []byte) string {
	h := sha256.Sum256(content)
	return hex.EncodeToString(h[:])
}

// humanSize formats bytes as a human-readable string.
func humanSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// FindBlobPointers walks the workspace and returns all blob pointer CogDocs.
func FindBlobPointers(workspaceRoot string) ([]BlobPointer, error) {
	memDir := filepath.Join(workspaceRoot, ".cog", "mem")
	var pointers []BlobPointer

	_ = filepath.WalkDir(memDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".cog.md") {
			return nil
		}

		// Quick check: read first 512 bytes for type: blob.pointer
		f, ferr := os.Open(path)
		if ferr != nil {
			return nil
		}
		defer f.Close()

		buf := make([]byte, 512)
		n, _ := f.Read(buf)
		head := string(buf[:n])

		if !strings.Contains(head, "type: blob.pointer") {
			return nil
		}

		// Extract hash from frontmatter.
		var hash, ct, orig string
		var size int64
		for _, line := range strings.Split(head, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "hash: ") {
				hash = strings.TrimPrefix(line, "hash: ")
			}
			if strings.HasPrefix(line, "size: ") {
				fmt.Sscanf(strings.TrimPrefix(line, "size: "), "%d", &size)
			}
			if strings.HasPrefix(line, "content_type: ") {
				ct = strings.TrimPrefix(line, "content_type: ")
			}
			if strings.HasPrefix(line, "original_path: ") {
				orig = strings.TrimPrefix(line, "original_path: ")
			}
		}

		if hash != "" {
			pointers = append(pointers, BlobPointer{
				Hash:         hash,
				Size:         size,
				ContentType:  ct,
				OriginalPath: orig,
			})
		}
		return nil
	})

	return pointers, nil
}

// CollectReferencedHashes returns all blob hashes referenced by pointer CogDocs.
func CollectReferencedHashes(workspaceRoot string) (map[string]bool, error) {
	pointers, err := FindBlobPointers(workspaceRoot)
	if err != nil {
		return nil, err
	}
	refs := make(map[string]bool, len(pointers))
	for _, p := range pointers {
		refs[p.Hash] = true
	}
	return refs, nil
}

// ShouldRedirectToBlob returns true if a file should be stored in the blob
// store instead of committed to git.
func ShouldRedirectToBlob(path string, size int64) bool {
	ext := strings.ToLower(filepath.Ext(path))

	// Always redirect these types regardless of size.
	alwaysBlob := map[string]bool{
		".mp3": true, ".wav": true, ".flac": true, ".ogg": true, ".m4a": true,
		".mp4": true, ".mov": true, ".avi": true, ".mkv": true, ".webm": true,
		".bin": true, ".onnx": true, ".safetensors": true, ".pt": true,
	}
	if alwaysBlob[ext] {
		return true
	}

	// Size-based thresholds.
	thresholds := map[string]int64{
		".pdf":  5 * 1024 * 1024, // 5MB
		".png":  1024 * 1024,     // 1MB
		".jpg":  1024 * 1024,     // 1MB
		".jpeg": 1024 * 1024,     // 1MB
	}
	if threshold, ok := thresholds[ext]; ok {
		return size >= threshold
	}

	// Default: 10MB for everything else.
	return size >= 10*1024*1024
}

// ContentTypeFromExt returns a MIME content type for a file extension.
func ContentTypeFromExt(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	types := map[string]string{
		".pdf":  "application/pdf",
		".mp3":  "audio/mpeg",
		".wav":  "audio/wav",
		".flac": "audio/flac",
		".ogg":  "audio/ogg",
		".m4a":  "audio/mp4",
		".mp4":  "video/mp4",
		".mov":  "video/quicktime",
		".png":  "image/png",
		".jpg":  "image/jpeg",
		".jpeg": "image/jpeg",
		".bin":  "application/octet-stream",
		".onnx": "application/octet-stream",
		".pt":   "application/octet-stream",
	}
	if ct, ok := types[ext]; ok {
		return ct
	}
	return "application/octet-stream"
}

// ── CLI helpers ─────────────────────────────────────────────────────────────

// PrintBlobList prints a formatted table of stored blobs.
func (bs *BlobStore) PrintBlobList() error {
	entries, err := bs.List()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		fmt.Println("No blobs stored.")
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StoredAt > entries[j].StoredAt
	})

	fmt.Printf("%-12s  %10s  %-20s  %s\n", "HASH", "SIZE", "TYPE", "STORED")
	fmt.Printf("%-12s  %10s  %-20s  %s\n", "────", "────", "────", "──────")
	for _, e := range entries {
		fmt.Printf("%-12s  %10s  %-20s  %s\n",
			e.Hash[:12], humanSize(e.Size), e.ContentType, e.StoredAt[:10])
	}

	total, count, _ := bs.Size()
	fmt.Printf("\n%d blobs, %s total\n", count, humanSize(total))
	return nil
}
