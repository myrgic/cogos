// trm_index.go — Embedding index for TRM cosine pre-filtering.
//
// Loads the binary embedding index (EMB1 format) and chunk metadata (JSON)
// exported by trm_export.py. Provides fast cosine similarity top-K search
// over the full embedding corpus.
//
// Binary format (EMB1):
//
//	4 bytes: magic "EMB1"
//	4 bytes: uint32 num_chunks
//	4 bytes: uint32 dim (384)
//	num_chunks * dim * 4 bytes: float32 data (row-major, little-endian)
package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

// ChunkMeta holds metadata for a single embedded chunk.
type ChunkMeta struct {
	DocID        string `json:"doc_id"`
	Path         string `json:"path"`
	Title        string `json:"title"`
	SectionTitle string `json:"section_title"`
	ChunkIdx     int    `json:"chunk_idx"`
	ChunkID      string `json:"chunk_id"`
	TextPreview  string `json:"text_preview"`
}

// EmbeddingIndex holds the full embedding matrix and chunk metadata.
type EmbeddingIndex struct {
	Embeddings [][]float32 // [N][dim]
	Chunks     []ChunkMeta
	Dim        int
}

// IndexResult is a single search result from the embedding index.
type IndexResult struct {
	Index     int
	Score     float32
	ChunkMeta ChunkMeta
}

// LoadEmbeddingIndex loads the binary embedding file and chunk metadata JSON.
func LoadEmbeddingIndex(embPath, chunksPath string) (*EmbeddingIndex, error) {
	// Load embeddings
	f, err := os.Open(embPath)
	if err != nil {
		return nil, fmt.Errorf("open embeddings %s: %w", embPath, err)
	}
	defer f.Close()

	// Read magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != "EMB1" {
		return nil, fmt.Errorf("bad magic: %q (want EMB1)", string(magic))
	}

	var nChunks, dim uint32
	if err := binary.Read(f, binary.LittleEndian, &nChunks); err != nil {
		return nil, fmt.Errorf("read nChunks: %w", err)
	}
	if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
		return nil, fmt.Errorf("read dim: %w", err)
	}

	// Read flat data
	flatData := make([]float32, int(nChunks)*int(dim))
	if err := binary.Read(f, binary.LittleEndian, flatData); err != nil {
		return nil, fmt.Errorf("read embeddings data: %w", err)
	}

	// Reshape into [nChunks][dim]
	embeddings := make([][]float32, nChunks)
	for i := uint32(0); i < nChunks; i++ {
		start := int(i) * int(dim)
		embeddings[i] = flatData[start : start+int(dim)]
	}

	// Load chunk metadata
	cf, err := os.Open(chunksPath)
	if err != nil {
		return nil, fmt.Errorf("open chunks %s: %w", chunksPath, err)
	}
	defer cf.Close()

	var chunks []ChunkMeta
	if err := json.NewDecoder(cf).Decode(&chunks); err != nil {
		return nil, fmt.Errorf("decode chunks: %w", err)
	}

	if len(chunks) != int(nChunks) {
		return nil, fmt.Errorf("embedding/chunk count mismatch: %d embeddings, %d chunks",
			nChunks, len(chunks))
	}

	return &EmbeddingIndex{
		Embeddings: embeddings,
		Chunks:     chunks,
		Dim:        int(dim),
	}, nil
}

// CosineTopK returns the top-K chunks by cosine similarity to the query.
// The query should already be L2-normalized (as are the stored embeddings).
func (idx *EmbeddingIndex) CosineTopK(query []float32, k int) []IndexResult {
	n := len(idx.Embeddings)
	if k > n {
		k = n
	}

	// Compute cosine similarities (dot product for normalized vectors)
	type scored struct {
		idx   int
		score float32
	}
	scores := make([]scored, n)
	for i := 0; i < n; i++ {
		scores[i] = scored{idx: i, score: cosineSim(query, idx.Embeddings[i])}
	}

	// Partial sort: we only need top-K
	sort.Slice(scores, func(a, b int) bool {
		return scores[a].score > scores[b].score
	})

	results := make([]IndexResult, k)
	for i := 0; i < k; i++ {
		results[i] = IndexResult{
			Index:     scores[i].idx,
			Score:     scores[i].score,
			ChunkMeta: idx.Chunks[scores[i].idx],
		}
	}
	return results
}

// CosineTopKIndices returns the indices and embeddings of the top-K chunks.
// Useful for pre-filtering before TRM scoring.
func (idx *EmbeddingIndex) CosineTopKIndices(query []float32, k int) ([]int, [][]float32) {
	results := idx.CosineTopK(query, k)
	indices := make([]int, len(results))
	embeddings := make([][]float32, len(results))
	for i, r := range results {
		indices[i] = r.Index
		embeddings[i] = idx.Embeddings[r.Index]
	}
	return indices, embeddings
}

// Size returns the number of chunks in the index.
func (idx *EmbeddingIndex) Size() int {
	return len(idx.Embeddings)
}

// cosineSim computes cosine similarity between two vectors.
// For pre-normalized vectors this is just the dot product.
func cosineSim(a, b []float32) float32 {
	var dot, normA, normB float64
	for i := range a {
		ai, bi := float64(a[i]), float64(b[i])
		dot += ai * bi
		normA += ai * ai
		normB += bi * bi
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0
	}
	return float32(dot / denom)
}
