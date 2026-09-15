// serve_blocks.go — HTTP endpoints for block sync protocol
//
// Phase 3 of the block sync protocol: remote blob exchange.
//
//	GET  /v1/blocks/{hash}     — retrieve a blob by hash
//	PUT  /v1/blocks/{hash}     — store a blob by hash
//	GET  /v1/blocks/manifest   — list all stored blobs (manifest exchange)
//	POST /v1/blocks/verify     — verify a list of hashes, return missing ones
//
// These endpoints enable workspace-to-workspace blob sync:
//  1. Workspace A gets B's manifest
//  2. Diffs against local manifest
//  3. GETs missing blobs by hash
//  4. Stores them locally
//
// Content is verified by hash on both read and write — the hash IS the address.
//
// ADR-084 Phase 2 digest resolution:
//
//	GET /v1/blobs/{digest}     — resolve a `sha256:<hex>` content digest to
//	                              raw blob bytes. Uses the BlobStore wired
//	                              into Process by G10; preserves manifest
//	                              Content-Type when available.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

// registerBlockRoutes wires the block sync endpoints into the mux.
func (s *Server) registerBlockRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/blocks/manifest", s.handleBlocksManifest)
	s.route(mux, "POST /v1/blocks/verify", s.handleBlocksVerify)
	s.route(mux, "GET /v1/blocks/{hash}", s.handleBlockGet)
	s.route(mux, "PUT /v1/blocks/{hash}", s.handleBlockPut)
	s.route(mux, "GET /v1/blobs/{digest}", s.handleBlobGet)
}

// handleBlockGet returns blob content by hash, streaming the file to the
// response writer without buffering the full content in memory. This is
// required for multi-GB model shards that would otherwise OOM the server.
//
//	GET /v1/blocks/{hash}
//	200 → raw blob content (application/octet-stream)
//	404 → blob not found
func (s *Server) handleBlockGet(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if hash == "" || len(hash) != 64 {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}

	bs := NewBlobStore(s.cfg.WorkspaceRoot)
	f, err := bs.Open(hash)
	if err != nil {
		http.Error(w, "blob not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		http.Error(w, "stat failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Blob-Hash", hash)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	if _, err := io.Copy(w, f); err != nil {
		// Response headers already sent; log and move on. Client will see
		// a truncated body and can retry.
		slog.Warn("blocks: stream copy error", "hash", hash[:12], "err", err)
	}
}

// handleBlobGet resolves an ADR-084 content digest to raw blob bytes via
// the BlobStore wired into Process at startup (G10).
//
//	GET /v1/blobs/{digest}
//	   where digest = "sha256:<64-hex>"
//	200 → raw blob bytes; Content-Type from manifest if known, else
//	      application/octet-stream
//	400 → malformed digest (wrong prefix, wrong hex length, non-hex chars)
//	404 → digest is well-formed but no blob is stored for it
//	500 → BlobStore unavailable or read failure unrelated to not-found
//
// BlobStore stores raw hex per G10, so the `sha256:` prefix is stripped
// before delegating to Get(). The prefix is still required on the wire
// because that is the canonical ADR-084 digest form and lets us extend to
// other hash algorithms later without a route change.
func (s *Server) handleBlobGet(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")

	hexPart, ok := parseSHA256Digest(digest)
	if !ok {
		http.Error(w, "malformed digest: want sha256:<64-hex>", http.StatusBadRequest)
		return
	}

	bs := s.process.BlobStore()
	if bs == nil {
		http.Error(w, "blob store not initialized", http.StatusInternalServerError)
		return
	}

	content, err := bs.Get(hexPart)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "blob not found", http.StatusNotFound)
			return
		}
		slog.Warn("blobs: get failed", "digest", digest, "err", err)
		http.Error(w, "blob read failed", http.StatusInternalServerError)
		return
	}

	// Recover Content-Type from the manifest if available. List() is O(N)
	// in the number of stored blobs; acceptable for the current deployment
	// scale and matches the pattern used by handleBlocksManifest. A future
	// optimization (G-series) could add a direct lookup helper.
	contentType := "application/octet-stream"
	if entries, listErr := bs.List(); listErr == nil {
		for _, e := range entries {
			if e.Hash == hexPart && e.ContentType != "" {
				contentType = e.ContentType
				break
			}
		}
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Blob-Digest", digest)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	_, _ = w.Write(content)
}

// parseSHA256Digest validates a `sha256:<64-hex>` digest string and returns
// the hex portion. Returns (hex, true) on success; ("", false) otherwise.
// The hex portion is normalized to lowercase because BlobStore stores hex in
// lowercase (hex.EncodeToString output).
func parseSHA256Digest(digest string) (string, bool) {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) {
		return "", false
	}
	hexPart := strings.ToLower(digest[len(prefix):])
	if len(hexPart) != 64 {
		return "", false
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return "", false
	}
	return hexPart, true
}

// handleBlockPut stores a blob, verifying the hash matches the content.
// The body is streamed through a rolling SHA-256 hasher via io.TeeReader
// so multi-GB model shards never need to be buffered in memory.
//
//	PUT /v1/blocks/{hash}
//	Body: raw blob content
//	201 → stored successfully
//	400 → hash mismatch
func (s *Server) handleBlockPut(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if hash == "" || len(hash) != 64 {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}

	// Storage is content-addressed; the URL-provided hash is the expected
	// hash. We stream the body into a temp file while computing SHA-256,
	// then atomically move into the BlobStore on hash match.
	tmpFile, err := os.CreateTemp("", "blob-put-*")
	if err != nil {
		http.Error(w, "temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	hasher := sha256.New()
	tee := io.TeeReader(r.Body, hasher)

	if _, err := io.Copy(tmpFile, tee); err != nil {
		tmpFile.Close()
		http.Error(w, "write error: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := tmpFile.Close(); err != nil {
		http.Error(w, "close temp: "+err.Error(), http.StatusInternalServerError)
		return
	}

	actual := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(actual, hash) {
		slog.Warn("blocks: hash mismatch on PUT", "expected", hash[:12], "actual", actual[:12])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":    "hash mismatch",
			"expected": hash,
			"actual":   actual,
		})
		return
	}

	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}

	bs := NewBlobStore(s.cfg.WorkspaceRoot)
	if err := bs.Init(); err != nil {
		http.Error(w, "init blob store: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// StoreFile does a content-addressed atomic rename; if another concurrent
	// PUT already stored this hash, it returns the existing hash. The content
	// is verified by the rolling hasher above, so we trust the bytes.
	if _, err := bs.StoreFile(tmpPath, ct); err != nil {
		http.Error(w, "store failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	info, statErr := os.Stat(tmpPath)
	size := int64(0)
	if statErr == nil {
		size = info.Size()
	}

	slog.Info("blocks: stored", "hash", hash[:12], "size", size)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"stored": true,
		"hash":   hash,
		"size":   size,
	})
}

// handleBlocksManifest returns the full blob manifest.
//
//	GET /v1/blocks/manifest
//	200 → { blobs: [...], total_size: N, count: N }
func (s *Server) handleBlocksManifest(w http.ResponseWriter, r *http.Request) {
	bs := NewBlobStore(s.cfg.WorkspaceRoot)
	entries, err := bs.List()
	if err != nil {
		http.Error(w, "list blobs: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var totalSize int64
	for _, e := range entries {
		totalSize += e.Size
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"blobs":      entries,
		"count":      len(entries),
		"total_size": totalSize,
	})
}

// handleBlocksVerify accepts a list of hashes and returns which ones are missing locally.
//
//	POST /v1/blocks/verify
//	Body: { "hashes": ["abc123...", "def456..."] }
//	200 → { "missing": ["def456..."], "present": ["abc123..."] }
func (s *Server) handleBlocksVerify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Hashes []string `json:"hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "parse body: "+err.Error(), http.StatusBadRequest)
		return
	}

	bs := NewBlobStore(s.cfg.WorkspaceRoot)
	var missing, present []string
	for _, h := range req.Hashes {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if bs.Exists(h) {
			present = append(present, h)
		} else {
			missing = append(missing, h)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"missing": missing,
		"present": present,
	})
}
