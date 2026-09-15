// serve_attention.go — Constellation attention + fovea endpoints for CogOS v3
//
// Implements three HTTP endpoints expected by the Phase 3 OMZ plugin:
//
//	POST /v1/attention                     — emit an attention signal
//	GET  /v1/constellation/adjacent?uri=… — find adjacent nodes in attentional field
//	GET  /v1/constellation/fovea          — current fovea (top-N scoring files)
//
// Attention signals are stored in .cog/run/attention.jsonl (append-only log)
// and also update the in-process AttentionalField with a small recency boost.
// The adjacent endpoint uses the field's score map plus a simple co-access
// heuristic derived from the attention log.

package engine

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// attentionSignal is the on-wire representation for POST /v1/attention.
type attentionSignal struct {
	ParticipantID string            `json:"participant_id"`
	TargetURI     string            `json:"target_uri"`
	SignalType    string            `json:"signal_type"` // visit|read|write|search|traverse
	Context       map[string]string `json:"context,omitempty"`
	OccurredAt    string            `json:"occurred_at,omitempty"` // RFC3339; defaults to now
}

// attentionLog provides thread-safe append-only logging to a JSONL file.
type attentionLog struct {
	mu   sync.Mutex
	path string
}

func newAttentionLog(workspaceRoot string) *attentionLog {
	dir := filepath.Join(workspaceRoot, ".cog", "run")
	_ = os.MkdirAll(dir, 0755)
	return &attentionLog{path: filepath.Join(dir, "attention.jsonl")}
}

func (l *attentionLog) append(sig attentionSignal) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	return enc.Encode(sig)
}

// recentSignals returns the last N signals from the log file.
func (l *attentionLog) recentSignals(n int) []attentionSignal {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.Open(l.path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var all []attentionSignal
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var s attentionSignal
		if json.Unmarshal(sc.Bytes(), &s) == nil {
			all = append(all, s)
		}
	}
	if len(all) <= n {
		return all
	}
	return all[len(all)-n:]
}

// registerAttentionRoutes wires the constellation and observer endpoints into the mux.
// Called from NewServer after the baseline routes are registered.
func (s *Server) registerAttentionRoutes(mux *http.ServeMux) {
	s.attentionLog = newAttentionLog(s.cfg.WorkspaceRoot)

	s.route(mux, "POST /v1/attention", s.handleAttention)
	s.route(mux, "GET /v1/constellation/adjacent", s.handleConstellationAdjacent)
	s.route(mux, "GET /v1/constellation/fovea", s.handleConstellationFovea)
	s.route(mux, "GET /v1/observer/state", s.handleObserverState)
}

// handleAttention stores an attention signal and applies a recency boost to the field.
//
//	POST /v1/attention
//	Body: { "participant_id": "human:node-abc", "target_uri": "cog://mem/...",
//	        "signal_type": "traverse", "context": {"anchor": "..."} }
//	200 → { "ok": true }
func (s *Server) handleAttention(w http.ResponseWriter, r *http.Request) {
	var sig attentionSignal
	if err := json.NewDecoder(r.Body).Decode(&sig); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	if sig.OccurredAt == "" {
		sig.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	if sig.SignalType == "" {
		sig.SignalType = "visit"
	}

	// Append to attention log (async so shell hook never blocks on disk).
	go func() {
		if err := s.attentionLog.append(sig); err != nil {
			slog.Warn("attention: log append failed", "err", err)
		}
	}()

	// Apply a small recency boost to the attentional field.
	if sig.TargetURI != "" && s.process != nil {
		fsPath := uriToFSPath(s.cfg.WorkspaceRoot, sig.TargetURI)
		if fsPath != "" {
			s.process.Field().Boost(fsPath, 0.1)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// handleConstellationFovea returns the current fovea state — the top-N files
// by attentional field score, with their cog:// URIs.
//
//	GET /v1/constellation/fovea?limit=20
//	200 → { "fovea": [...], "field_size": N, "last_updated": "..." }
func (s *Server) handleConstellationFovea(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n := 0; len(v) > 0 {
			if _, err := parseInt(v, &n); err == nil && n > 0 {
				limit = n
			}
		}
	}

	fovea := s.process.Field().Fovea(limit)

	type fovealEntry struct {
		Path  string  `json:"path"`
		URI   string  `json:"uri"`
		Score float64 `json:"score"`
	}
	entries := make([]fovealEntry, len(fovea))
	for i, fs := range fovea {
		entries[i] = fovealEntry{
			Path:  fs.Path,
			URI:   fsPathToURI(s.cfg.WorkspaceRoot, fs.Path),
			Score: fs.Score,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"fovea":        entries,
		"field_size":   s.process.Field().Len(),
		"last_updated": s.process.Field().LastUpdated().Format(time.RFC3339),
		"nucleus":      s.nucleus.Name,
	})
}

// handleConstellationAdjacent returns nodes "near" a given URI in the attentional field.
// Nearness is scored by:
//
//  1. Co-access within a 5-minute window (from attention log)
//
//  2. Path proximity (same directory prefix)
//
//  3. Attentional field score
//
//     GET /v1/constellation/adjacent?uri=cog://mem/semantic/foo&limit=10
//     200 → { "uri": "cog://...", "nodes": [...] }
func (s *Server) handleConstellationAdjacent(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")
	if uri == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "uri required"})
		return
	}

	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		var n int
		if _, err := parseInt(v, &n); err == nil && n > 0 {
			limit = n
		}
	}

	targetPath := uriToFSPath(s.cfg.WorkspaceRoot, uri)

	// Collect all field scores.
	allScores := s.process.Field().AllScores()

	// Build set of co-accessed paths (accessed within 5 min of target).
	coAccessed := coAccessedPaths(s.attentionLog, uri, 5*time.Minute)

	type candidate struct {
		path  string
		score float64
	}
	var candidates []candidate

	for path, score := range allScores {
		if path == targetPath {
			continue
		}

		combined := score

		// Co-access boost: +0.3 per co-access
		if coAccessed[path] {
			combined += 0.3
		}

		// Directory proximity boost: +0.2 if same immediate parent
		if targetPath != "" && sameDir(targetPath, path) {
			combined += 0.2
		}

		candidates = append(candidates, candidate{path: path, score: combined})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	type node struct {
		Path  string  `json:"path"`
		URI   string  `json:"uri"`
		Score float64 `json:"score"`
	}
	nodes := make([]node, len(candidates))
	for i, c := range candidates {
		nodes[i] = node{
			Path:  c.path,
			URI:   fsPathToURI(s.cfg.WorkspaceRoot, c.path),
			Score: c.score,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"uri":   uri,
		"nodes": nodes,
	})
}

// coAccessedPaths returns a set of file paths that were accessed within
// window of any signal targeting uri.
func coAccessedPaths(log *attentionLog, targetURI string, window time.Duration) map[string]bool {
	if log == nil {
		return nil
	}
	recent := log.recentSignals(500)
	// Find timestamps when targetURI was accessed.
	var targetTimes []time.Time
	for _, s := range recent {
		if s.TargetURI == targetURI {
			if t, err := time.Parse(time.RFC3339, s.OccurredAt); err == nil {
				targetTimes = append(targetTimes, t)
			}
		}
	}
	if len(targetTimes) == 0 {
		return nil
	}

	coAccessed := make(map[string]bool)
	for _, s := range recent {
		if s.TargetURI == targetURI {
			continue
		}
		if t, err := time.Parse(time.RFC3339, s.OccurredAt); err == nil {
			for _, tt := range targetTimes {
				diff := t.Sub(tt)
				if diff < 0 {
					diff = -diff
				}
				if diff <= window {
					coAccessed[s.TargetURI] = true
					break
				}
			}
		}
	}
	return coAccessed
}

// uriToFSPath converts a cog:// URI to an absolute filesystem path.
func uriToFSPath(workspaceRoot, uri string) string {
	prefixes := [][2]string{
		{"cog://mem/", ".cog/mem/"},
		{"cog://docs/", ".cog/docs/"},
		{"cog://adr/", ".cog/adr/"},
		{"cog://hooks/", ".cog/hooks/"},
		{"cog://workspace/", ""},
		{"cog://claude/", ".claude/"},
	}
	for _, p := range prefixes {
		if strings.HasPrefix(uri, p[0]) {
			rel := p[1] + strings.TrimPrefix(uri, p[0])
			return filepath.Join(workspaceRoot, rel)
		}
	}
	return ""
}

// fsPathToURI converts an absolute filesystem path to a cog: URI.
// Projection references use the bare form (no //) per ADR-067.
func fsPathToURI(workspaceRoot, path string) string {
	rel := strings.TrimPrefix(path, workspaceRoot+"/")
	prefixes := [][2]string{
		{".cog/mem/", "cog:mem/"},
		{".cog/docs/", "cog:docs/"},
		{".cog/adr/", "cog:adr/"},
		{".cog/hooks/", "cog:hooks/"},
		{".claude/", "cog:claude/"},
	}
	for _, p := range prefixes {
		if strings.HasPrefix(rel, p[0]) {
			return p[1] + strings.TrimPrefix(rel, p[0])
		}
	}
	return "cog://workspace/" + rel
}

// sameDir returns true if a and b share the same immediate parent directory.
func sameDir(a, b string) bool {
	return filepath.Dir(a) == filepath.Dir(b)
}

// parseInt parses an integer from a string.
func parseInt(s string, out *int) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, &parseError{s}
		}
		n = n*10 + int(c-'0')
	}
	*out = n
	return n, nil
}

type parseError struct{ s string }

func (e *parseError) Error() string { return "invalid integer: " + e.s }

// handleObserverState returns the current state of the trajectory model.
//
//	GET /v1/observer/state
//	200 → {
//	  "cycles":        N,
//	  "mean_error":    0.42,
//	  "prediction":    ["cog://mem/...", ...],
//	  "momentum_top":  [{"uri":"...", "momentum":0.9}, ...],
//	  "surprise_threshold": 0.7
//	}
func (s *Server) handleObserverState(w http.ResponseWriter, r *http.Request) {
	obs := s.process.Observer()
	cycles, meanErr := obs.Stats()
	prediction := obs.LastPrediction()
	momentum := obs.Momentum()

	// Build top-10 momentum entries sorted descending.
	type momentumEntry struct {
		URI      string  `json:"uri"`
		Momentum float64 `json:"momentum"`
	}
	entries := make([]momentumEntry, 0, len(momentum))
	for path, m := range momentum {
		entries = append(entries, momentumEntry{
			URI:      fsPathToURI(s.cfg.WorkspaceRoot, path),
			Momentum: m,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Momentum > entries[j].Momentum
	})
	const maxMomentumEntries = 10
	if len(entries) > maxMomentumEntries {
		entries = entries[:maxMomentumEntries]
	}

	// Convert prediction FS paths to URIs.
	predURIs := make([]string, len(prediction))
	for i, p := range prediction {
		predURIs[i] = fsPathToURI(s.cfg.WorkspaceRoot, p)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"cycles":             cycles,
		"mean_error":         meanErr,
		"prediction":         predURIs,
		"momentum_top":       entries,
		"surprise_threshold": surpriseThreshold,
	})
}
