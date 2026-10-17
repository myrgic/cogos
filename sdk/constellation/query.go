package constellation

import (
	"database/sql"
	"fmt"
	"strings"
)

// Node represents a document node in the constellation graph.
type Node struct {
	ID      string
	URI     string
	Type    string
	Title   string
	Path    string
	Content string
	Sector  string
	Status  string
	Rank    float64 // BM25 rank for FTS queries
}

// Search performs full-text search across all cogdocs.
func (c *Constellation) Search(query string, limit int) ([]Node, error) {
	querySQL := `
		SELECT d.id, d.path, d.title, d.type, d.sector, d.status, d.content,
		       bm25(documents_fts) AS rank
		FROM documents_fts
		JOIN documents d ON d.id = documents_fts.id
		WHERE documents_fts MATCH ?
		  AND d.status != 'deprecated'
		ORDER BY rank
		LIMIT ?
	`

	rows, err := c.db.Query(querySQL, query, limit)

	if err != nil {
		return nil, fmt.Errorf("search query failed: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		var n Node
		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &n.Sector, &n.Status, &n.Content, &n.Rank)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	// Check for errors after iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return nodes, nil
}

// QueryRelevant searches for documents relevant to anchor and goal.
// Extracts keywords from anchor/goal and performs FTS search.
func (c *Constellation) QueryRelevant(anchor, goal string, limit int) ([]Node, error) {
	// Extract keywords from anchor and goal
	keywords := extractKeywords(anchor, goal)

	// Build FTS5 query: OR-join extracted keywords, individually quoted
	// for syntax safety (see buildKeywordFTSQuery).
	query := buildKeywordFTSQuery(keywords)

	return c.Search(query, limit)
}

// SubstanceFilterConfig holds parameters for substance-aware filtering.
type SubstanceFilterConfig struct {
	MinSubstanceRatio float64 // Minimum substance ratio to include (0.0-1.0)
	PreferLeafNodes   bool    // Boost high-substance, low-ref documents
	LeafThreshold     float64 // Substance ratio to consider a "leaf node"
	LeafMaxRefs       int     // Max refs for a document to be a "leaf node"
	BM25Weight        float64 // Weight for BM25 relevance (0.0-1.0)
	SubstanceWeight   float64 // Weight for substance ratio (0.0-1.0)
}

// DefaultSubstanceFilter returns sensible defaults for substance filtering.
func DefaultSubstanceFilter() SubstanceFilterConfig {
	return SubstanceFilterConfig{
		MinSubstanceRatio: 0.5,
		PreferLeafNodes:   true,
		LeafThreshold:     0.7,
		LeafMaxRefs:       3,
		BM25Weight:        0.5,
		SubstanceWeight:   0.3,
	}
}

// NodeWithScore wraps a Node with its combined ranking score.
type NodeWithScore struct {
	Node                Node
	BM25Score           float64
	SubstanceScore      float64
	EmbeddingSimilarity float64   // Cosine similarity between query and doc embeddings
	ProbeScore          float64   // Trained probe relevance probability (Phase E)
	CombinedScore       float64
	IsLeaf              bool
	Embedding128        []float32 // Cached 128-dim embedding (avoids re-query for probe scoring)
}

// QueryRelevantWithSubstance searches for documents with substance-aware ranking.
// It fetches more candidates than needed, filters by substance ratio, and returns
// results ranked by a combined score of BM25 relevance and substance metrics.
func (c *Constellation) QueryRelevantWithSubstance(anchor, goal string, maxCandidates, maxResults int, filter SubstanceFilterConfig) ([]Node, error) {
	// Extract keywords from anchor and goal
	keywords := extractKeywords(anchor, goal)
	if len(keywords) == 0 {
		return nil, nil
	}

	// Build FTS5 query: OR-join extracted keywords, individually quoted
	// for syntax safety (see buildKeywordFTSQuery).
	ftsQuery := buildKeywordFTSQuery(keywords)

	// Query with substance metrics included
	querySQL := `
		SELECT d.id, d.path, d.title, d.type, d.sector, d.status, d.content,
		       bm25(documents_fts) AS rank,
		       COALESCE(d.substance_ratio, 0.0) AS substance_ratio,
		       COALESCE(d.ref_count, 0) AS ref_count
		FROM documents_fts
		JOIN documents d ON d.id = documents_fts.id
		WHERE documents_fts MATCH ?
		  AND d.status != 'deprecated'
		ORDER BY rank
		LIMIT ?
	`

	rows, err := c.db.Query(querySQL, ftsQuery, maxCandidates)
	if err != nil {
		return nil, fmt.Errorf("substance search failed: %w", err)
	}
	defer rows.Close()

	// Collect candidates with scores
	var candidates []NodeWithScore
	var maxBM25 float64 = 1.0 // For normalization

	for rows.Next() {
		var n Node
		var bm25Rank float64
		var substanceRatio float64
		var refCount int

		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &n.Sector, &n.Status, &n.Content,
			&bm25Rank, &substanceRatio, &refCount)
		if err != nil {
			return nil, err
		}

		// Filter by minimum substance ratio
		if substanceRatio < filter.MinSubstanceRatio {
			continue
		}

		// Track max BM25 for normalization (BM25 returns negative, closer to 0 is better)
		if bm25Rank < maxBM25 {
			maxBM25 = bm25Rank
		}

		// Determine if this is a "leaf node" (high substance, few refs)
		isLeaf := substanceRatio >= filter.LeafThreshold && refCount <= filter.LeafMaxRefs

		candidates = append(candidates, NodeWithScore{
			Node:          n,
			BM25Score:     bm25Rank,
			SubstanceScore: substanceRatio,
			IsLeaf:        isLeaf,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating substance results: %w", err)
	}

	// Calculate combined scores
	for i := range candidates {
		// Normalize BM25 (convert to 0-1 where 1 is best)
		// BM25 returns negative scores, closer to 0 is better
		normalizedBM25 := 1.0
		if maxBM25 < 0 {
			normalizedBM25 = candidates[i].BM25Score / maxBM25
		}

		// Combined score
		candidates[i].CombinedScore = filter.BM25Weight*normalizedBM25 +
			filter.SubstanceWeight*candidates[i].SubstanceScore

		// Leaf node boost (add 10% bonus)
		if filter.PreferLeafNodes && candidates[i].IsLeaf {
			candidates[i].CombinedScore *= 1.1
		}
	}

	// Sort by combined score (descending)
	SortNodesByScore(candidates)

	// Take top maxResults
	if len(candidates) > maxResults {
		candidates = candidates[:maxResults]
	}

	// Extract nodes
	nodes := make([]Node, len(candidates))
	for i, c := range candidates {
		nodes[i] = c.Node
	}

	return nodes, nil
}

// QueryRelevantWithEmbedding is like QueryRelevantWithSubstance but also computes
// embedding similarity for each candidate (shadow scoring for Phase C).
// The heuristic score still controls final ranking — embedding scores are recorded
// for shadow logging and eventual blend-in.
func (c *Constellation) QueryRelevantWithEmbedding(anchor, goal string, maxCandidates, maxResults int, filter SubstanceFilterConfig, queryEmb128 []float32) ([]NodeWithScore, error) {
	keywords := extractKeywords(anchor, goal)
	if len(keywords) == 0 {
		return nil, nil
	}

	// Build FTS5 query: OR-join extracted keywords, individually quoted
	// for syntax safety (see buildKeywordFTSQuery).
	ftsQuery := buildKeywordFTSQuery(keywords)

	// Query with substance metrics + embedding BLOBs
	querySQL := `
		SELECT d.id, d.path, d.title, d.type, d.sector, d.status, d.content,
		       bm25(documents_fts) AS rank,
		       COALESCE(d.substance_ratio, 0.0) AS substance_ratio,
		       COALESCE(d.ref_count, 0) AS ref_count,
		       d.embedding_128
		FROM documents_fts
		JOIN documents d ON d.id = documents_fts.id
		WHERE documents_fts MATCH ?
		  AND d.status != 'deprecated'
		ORDER BY rank
		LIMIT ?
	`

	rows, err := c.db.Query(querySQL, ftsQuery, maxCandidates)
	if err != nil {
		return nil, fmt.Errorf("embedding search failed: %w", err)
	}
	defer rows.Close()

	var candidates []NodeWithScore
	var maxBM25 float64 = 1.0

	for rows.Next() {
		var n Node
		var bm25Rank float64
		var substanceRatio float64
		var refCount int
		var embBlob []byte

		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &n.Sector, &n.Status, &n.Content,
			&bm25Rank, &substanceRatio, &refCount, &embBlob)
		if err != nil {
			return nil, err
		}

		if substanceRatio < filter.MinSubstanceRatio {
			continue
		}

		if bm25Rank < maxBM25 {
			maxBM25 = bm25Rank
		}

		isLeaf := substanceRatio >= filter.LeafThreshold && refCount <= filter.LeafMaxRefs

		// Compute embedding similarity if both embeddings exist
		var embSim float64
		var docEmb128 []float32
		if len(queryEmb128) > 0 && len(embBlob) > 0 {
			docEmb128 = BytesToFloat32(embBlob)
			if len(docEmb128) == len(queryEmb128) {
				embSim = CosineSimilarity(queryEmb128, docEmb128)
			}
		}

		candidates = append(candidates, NodeWithScore{
			Node:                n,
			BM25Score:           bm25Rank,
			SubstanceScore:      substanceRatio,
			EmbeddingSimilarity: embSim,
			IsLeaf:              isLeaf,
			Embedding128:        docEmb128,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating embedding results: %w", err)
	}

	// Calculate combined scores (heuristic only — embedding is shadow)
	for i := range candidates {
		normalizedBM25 := 1.0
		if maxBM25 < 0 {
			normalizedBM25 = candidates[i].BM25Score / maxBM25
		}

		candidates[i].CombinedScore = filter.BM25Weight*normalizedBM25 +
			filter.SubstanceWeight*candidates[i].SubstanceScore

		if filter.PreferLeafNodes && candidates[i].IsLeaf {
			candidates[i].CombinedScore *= 1.1
		}
	}

	// Sort by combined score (heuristic controls ranking)
	SortNodesByScore(candidates)

	if len(candidates) > maxResults {
		candidates = candidates[:maxResults]
	}

	return candidates, nil
}

// SortNodesByScore sorts candidates by combined score (descending).
func SortNodesByScore(candidates []NodeWithScore) {
	// Simple insertion sort (good enough for small lists)
	for i := 1; i < len(candidates); i++ {
		j := i
		for j > 0 && candidates[j].CombinedScore > candidates[j-1].CombinedScore {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
			j--
		}
	}
}

// SearchWithFilters performs filtered full-text search.
func (c *Constellation) SearchWithFilters(query string, types []string, sector string, limit int) ([]Node, error) {
	// Build WHERE clause
	whereClauses := []string{"documents_fts MATCH ?", "d.status != 'deprecated'"}
	args := []interface{}{query}

	if len(types) > 0 {
		placeholders := make([]string, len(types))
		for i, t := range types {
			placeholders[i] = "?"
			args = append(args, t)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("d.type IN (%s)", strings.Join(placeholders, ",")))
	}

	if sector != "" {
		whereClauses = append(whereClauses, "d.sector = ?")
		args = append(args, sector)
	}

	args = append(args, limit)

	querySQL := fmt.Sprintf(`
		SELECT d.id, d.path, d.title, d.type, d.sector, d.status, d.content,
		       bm25(documents_fts) AS rank
		FROM documents_fts
		JOIN documents d ON d.id = documents_fts.id
		WHERE %s
		ORDER BY rank
		LIMIT ?
	`, strings.Join(whereClauses, " AND "))

	rows, err := c.db.Query(querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("filtered search failed: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		var n Node
		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &n.Sector, &n.Status, &n.Content, &n.Rank)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	return nodes, nil
}

// GetRelated finds documents related to the given document ID by following edges.
func (c *Constellation) GetRelated(docID string, relType string) ([]Node, error) {
	query := `
		SELECT d.id, d.path, d.title, d.type, d.sector, d.status, d.content
		FROM doc_references r
		JOIN documents d ON d.id = r.target_id
		WHERE r.source_id = ?
	`
	args := []interface{}{docID}

	if relType != "" {
		query += " AND r.relation = ?"
		args = append(args, relType)
	}

	query += " ORDER BY d.updated DESC"

	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("get related failed: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		var n Node
		var sector, status, content sql.NullString
		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &sector, &status, &content)
		if err != nil {
			return nil, err
		}
		n.Sector = sector.String
		n.Status = status.String
		n.Content = content.String
		nodes = append(nodes, n)
	}

	return nodes, nil
}

// GetBacklinks finds documents that reference the given document ID.
func (c *Constellation) GetBacklinks(docID string) ([]Node, error) {
	rows, err := c.db.Query(`
		SELECT d.id, d.path, d.title, d.type, d.sector, d.status, r.relation
		FROM backlinks b
		JOIN documents d ON d.id = b.source_id
		LEFT JOIN doc_references r ON r.source_id = b.source_id AND r.target_id = b.target_id
		WHERE b.target_id = ?
		ORDER BY d.updated DESC
	`, docID)

	if err != nil {
		return nil, fmt.Errorf("get backlinks failed: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		var n Node
		var sector, status sql.NullString
		var relation sql.NullString
		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &sector, &status, &relation)
		if err != nil {
			return nil, err
		}
		n.Sector = sector.String
		n.Status = status.String
		nodes = append(nodes, n)
	}

	return nodes, nil
}

// GetRecentBySector returns recently updated documents in a sector.
func (c *Constellation) GetRecentBySector(sector string, limit int) ([]Node, error) {
	rows, err := c.db.Query(`
		SELECT id, path, title, type, sector, status, content, updated
		FROM documents
		WHERE sector = ?
		  AND status != 'deprecated'
		ORDER BY updated DESC
		LIMIT ?
	`, sector, limit)

	if err != nil {
		return nil, fmt.Errorf("get recent failed: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		var n Node
		var updated sql.NullString
		err := rows.Scan(&n.ID, &n.Path, &n.Title, &n.Type, &n.Sector, &n.Status, &n.Content, &updated)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	return nodes, nil
}

// SubstanceMetrics represents substance analysis for a document or aggregate.
type SubstanceMetrics struct {
	Path             string  // Document path (empty for aggregates)
	Sector           string  // Sector name
	Type             string  // Document type
	DocCount         int     // Number of documents (for aggregates)
	FrontmatterBytes int     // Total frontmatter bytes
	ContentBytes     int     // Total content bytes
	SubstanceRatio   float64 // Content / (Content + Frontmatter)
	RefCount         int     // Number of outgoing references
	RefDensity       float64 // Refs per KB of content
}

// SubstanceReport returns substance metrics aggregated by sector.
func (c *Constellation) SubstanceReport() ([]SubstanceMetrics, error) {
	rows, err := c.db.Query(`
		SELECT
			COALESCE(sector, 'unknown') as sector,
			COUNT(*) as doc_count,
			COALESCE(SUM(frontmatter_bytes), 0) as total_frontmatter,
			COALESCE(SUM(content_bytes), 0) as total_content,
			COALESCE(AVG(substance_ratio), 0) as avg_substance,
			COALESCE(SUM(ref_count), 0) as total_refs,
			COALESCE(AVG(ref_density), 0) as avg_ref_density
		FROM documents
		GROUP BY sector
		ORDER BY avg_substance DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("substance report query failed: %w", err)
	}
	defer rows.Close()

	var metrics []SubstanceMetrics
	for rows.Next() {
		var m SubstanceMetrics
		err := rows.Scan(
			&m.Sector,
			&m.DocCount,
			&m.FrontmatterBytes,
			&m.ContentBytes,
			&m.SubstanceRatio,
			&m.RefCount,
			&m.RefDensity,
		)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// SubstanceReportByType returns substance metrics aggregated by document type.
func (c *Constellation) SubstanceReportByType() ([]SubstanceMetrics, error) {
	rows, err := c.db.Query(`
		SELECT
			type,
			COUNT(*) as doc_count,
			COALESCE(SUM(frontmatter_bytes), 0) as total_frontmatter,
			COALESCE(SUM(content_bytes), 0) as total_content,
			COALESCE(AVG(substance_ratio), 0) as avg_substance,
			COALESCE(SUM(ref_count), 0) as total_refs,
			COALESCE(AVG(ref_density), 0) as avg_ref_density
		FROM documents
		GROUP BY type
		ORDER BY avg_substance DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("substance by type query failed: %w", err)
	}
	defer rows.Close()

	var metrics []SubstanceMetrics
	for rows.Next() {
		var m SubstanceMetrics
		err := rows.Scan(
			&m.Type,
			&m.DocCount,
			&m.FrontmatterBytes,
			&m.ContentBytes,
			&m.SubstanceRatio,
			&m.RefCount,
			&m.RefDensity,
		)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// FindRoutingLayers returns documents that are mostly metadata (low substance, high refs).
// These are potential "over-abstracted" documents that may be pure wiring.
func (c *Constellation) FindRoutingLayers(substanceThreshold float64, minRefs int) ([]SubstanceMetrics, error) {
	rows, err := c.db.Query(`
		SELECT
			path,
			COALESCE(sector, 'unknown'),
			type,
			frontmatter_bytes,
			content_bytes,
			substance_ratio,
			ref_count,
			ref_density
		FROM documents
		WHERE substance_ratio < ? AND ref_count >= ?
		ORDER BY ref_density DESC
	`, substanceThreshold, minRefs)
	if err != nil {
		return nil, fmt.Errorf("routing layers query failed: %w", err)
	}
	defer rows.Close()

	var metrics []SubstanceMetrics
	for rows.Next() {
		var m SubstanceMetrics
		m.DocCount = 1 // Individual document
		err := rows.Scan(
			&m.Path,
			&m.Sector,
			&m.Type,
			&m.FrontmatterBytes,
			&m.ContentBytes,
			&m.SubstanceRatio,
			&m.RefCount,
			&m.RefDensity,
		)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// FindLeafNodes returns documents with high substance and few references.
// These are "leaf" knowledge documents with actual content.
func (c *Constellation) FindLeafNodes(substanceThreshold float64, maxRefs int) ([]SubstanceMetrics, error) {
	rows, err := c.db.Query(`
		SELECT
			path,
			COALESCE(sector, 'unknown'),
			type,
			frontmatter_bytes,
			content_bytes,
			substance_ratio,
			ref_count,
			ref_density
		FROM documents
		WHERE substance_ratio >= ? AND ref_count <= ?
		ORDER BY content_bytes DESC
	`, substanceThreshold, maxRefs)
	if err != nil {
		return nil, fmt.Errorf("leaf nodes query failed: %w", err)
	}
	defer rows.Close()

	var metrics []SubstanceMetrics
	for rows.Next() {
		var m SubstanceMetrics
		m.DocCount = 1 // Individual document
		err := rows.Scan(
			&m.Path,
			&m.Sector,
			&m.Type,
			&m.FrontmatterBytes,
			&m.ContentBytes,
			&m.SubstanceRatio,
			&m.RefCount,
			&m.RefDensity,
		)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// SubstanceSummary returns overall workspace substance statistics.
func (c *Constellation) SubstanceSummary() (*SubstanceMetrics, error) {
	var m SubstanceMetrics
	err := c.db.QueryRow(`
		SELECT
			COUNT(*) as doc_count,
			COALESCE(SUM(frontmatter_bytes), 0) as total_frontmatter,
			COALESCE(SUM(content_bytes), 0) as total_content,
			COALESCE(AVG(substance_ratio), 0) as avg_substance,
			COALESCE(SUM(ref_count), 0) as total_refs,
			COALESCE(AVG(ref_density), 0) as avg_ref_density
		FROM documents
	`).Scan(
		&m.DocCount,
		&m.FrontmatterBytes,
		&m.ContentBytes,
		&m.SubstanceRatio,
		&m.RefCount,
		&m.RefDensity,
	)
	if err != nil {
		return nil, fmt.Errorf("substance summary query failed: %w", err)
	}

	m.Sector = "workspace" // Marker for aggregate
	return &m, nil
}

// extractKeywords extracts search keywords from anchor and goal strings.
func extractKeywords(anchor, goal string) []string {
	// Simple keyword extraction - split on whitespace and remove common words
	stopWords := map[string]bool{
		"the": true, "a": true, "an": true, "and": true, "or": true,
		"but": true, "in": true, "on": true, "at": true, "to": true,
		"for": true, "of": true, "with": true, "by": true, "from": true,
		"is": true, "are": true, "was": true, "were": true, "be": true,
		"been": true, "being": true, "have": true, "has": true, "had": true,
		"do": true, "does": true, "did": true, "will": true, "would": true,
		"could": true, "should": true, "may": true, "might": true, "must": true,
		"can": true, "this": true, "that": true, "these": true, "those": true,
	}

	text := strings.ToLower(anchor + " " + goal)
	words := strings.Fields(text)

	var keywords []string
	seen := make(map[string]bool)

	for _, word := range words {
		// Remove punctuation
		word = strings.Trim(word, ".,!?;:()[]{}\"'")

		// Skip stop words, short words, and duplicates
		if len(word) < 3 || stopWords[word] || seen[word] {
			continue
		}

		// FTS5 treats hyphens as NOT operator — quote terms containing
		// special characters, or split hyphenated terms into parts
		if strings.ContainsAny(word, "-+*") {
			// Split hyphenated terms and add each part
			parts := strings.FieldsFunc(word, func(r rune) bool {
				return r == '-' || r == '+' || r == '*'
			})
			for _, part := range parts {
				if len(part) >= 3 && !stopWords[part] && !seen[part] {
					keywords = append(keywords, part)
					seen[part] = true
				}
			}
		} else {
			keywords = append(keywords, word)
		}
		seen[word] = true
	}

	return keywords
}

// buildKeywordFTSQuery joins already-extracted keywords into an FTS5 MATCH
// query. Each keyword is individually double-quoted (stripping any embedded
// quote so a keyword can never inject stray FTS5 syntax), and the terms are
// OR-joined.
//
// This deliberately keeps OR rather than adopting the AND-default fix
// applied to buildFTSQuery in internal/engine/mcp_stubs.go: that fix targets
// a typed search-box query, where a specific 2-3 word phrase like "spirit
// over letter" OR-joined into effectively "spirit OR over OR letter" turns
// a precise request into a near-corpus-wide match -- a real usability bug.
// extractKeywords(anchor, goal) is not that. It dedupes every non-stopword
// token >=3 chars out of two free-text strings with no cap, so realistic
// multi-sentence anchor/goal input yields 8-15+ keywords; QueryRelevant and
// friends feed the OR-matched candidates through maxCandidates/maxResults
// and (for the *WithSubstance/*WithEmbedding variants) substance-ratio and
// embedding-similarity re-ranking -- a recall-then-rank pipeline, not a
// direct return-to-user search. AND-joining that many keywords would
// require literally all of them to co-occur in one document, collapsing
// the candidate pool toward zero for any anchor/goal text that isn't
// already a near-verbatim quote of the target document, which would starve
// the ranking stages of anything to rank. OR keeps the existing
// recall-then-rank contract intact; only the syntax-safety quoting is new.
func buildKeywordFTSQuery(keywords []string) string {
	quoted := make([]string, 0, len(keywords))
	for _, kw := range keywords {
		kw = strings.ReplaceAll(kw, `"`, "")
		if kw == "" {
			continue
		}
		quoted = append(quoted, `"`+kw+`"`)
	}
	return strings.Join(quoted, " OR ")
}
