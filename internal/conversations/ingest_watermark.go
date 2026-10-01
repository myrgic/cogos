// ingest_watermark.go — delta ingest for normalized ingest sources.
//
// Before this file, any byte appended to an ingest source (one new Hermes turn,
// say) planned an `update` for the whole SOURCE, and applyIngestSource then
// re-parsed every JSONL file the source had ever written (1,421 files / 728 MB
// for hermes-cog, 2026-10-01) and rewrote the turns file of every session in it
// (3,508), to change one. A fresh process (the `cog reconcile` CLI) additionally
// re-parsed every UNCHANGED source each cycle to recount coverage, because its
// in-memory coverage cache starts cold.
//
// The watermark records, per ingest source, how far each of its files has been
// consumed (byte offset after the last complete line) plus the coverage counted
// over those bytes and the ontology fingerprint they were mapped under. With it:
//
//   - ComputePlan's drift check is "did any file appear, grow, shrink or vanish
//     since the watermark", not "does every session carry the source's latest
//     aggregate size".
//   - ApplyPlan reads only the bytes past each file's offset, merges the records
//     into the sessions they belong to (seeding dedup from the turns already
//     indexed, so observer re-emission stays a no-op), and upserts only the
//     sessions that gained a turn.
//   - A skipped source's coverage is restored from the watermark, so a cold
//     process never re-parses an unchanged source.
//
// The full re-parse stays as the fallback and the reference: it runs when there
// is no watermark yet, when the ontology/mapping fingerprint changed, or when a
// consumed file shrank or disappeared (a rewrite the append-only contract does
// not allow). A full parse writes a fresh watermark, so the next cycle is delta.
//
// The watermark is written after the index commit, atomically (tmp + rename).
// A crash between the two re-reads the same delta next cycle, which dedups
// against the turns now on disk: idempotent.
package conversations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// watermarkVersion is bumped when the file's meaning changes; a mismatch is
// treated as "no watermark" (full re-parse, then a fresh watermark).
const watermarkVersion = 1

// fileMark is how far one ingest file has been consumed.
type fileMark struct {
	// Offset is the byte offset just past the last complete ('\n'-terminated)
	// line consumed. A trailing partial line (a file still being written) is
	// left for the next cycle.
	Offset int64 `json:"offset"`
	// Size is the file size observed when Offset was recorded. Drift is any
	// change of size; a size below Offset forces a full re-parse.
	Size int64 `json:"size"`
}

// sourceWatermark is the delta-ingest state for one ingest source.
type sourceWatermark struct {
	// Fingerprint is LoadedOntology.SourceFingerprint(source) at the time the
	// consumed bytes were mapped. A different fingerprint means the mapping
	// changed and every record must be re-mapped: full re-parse.
	Fingerprint string `json:"fingerprint"`
	// Files maps file basename -> consumed mark.
	Files map[string]fileMark `json:"files"`
	// Coverage is the coverage counted over every consumed byte (the same
	// numbers a full parse of those bytes produces).
	Coverage SourceCoverage `json:"coverage"`
	// UpdatedAt is informational.
	UpdatedAt time.Time `json:"updated_at"`
}

// watermarkFile is the on-disk document.
type watermarkFile struct {
	Version int                         `json:"version"`
	Sources map[string]*sourceWatermark `json:"sources"`
}

// watermarkPath is beside the hermes observer's cursor file, deliberately NOT
// inside .cog/state/conversations/ (tools glob that directory as sessions).
func watermarkPath(root string) string {
	return filepath.Join(root, ".cog", "state", "conversations-ingest-watermarks.json")
}

// loadWatermarks returns the per-source watermarks, or an empty map when the
// file is missing, unreadable, or of another version (all mean "full parse").
func loadWatermarks(root string) map[string]*sourceWatermark {
	out := make(map[string]*sourceWatermark)
	if root == "" {
		return out
	}
	data, err := os.ReadFile(watermarkPath(root))
	if err != nil {
		return out
	}
	var wf watermarkFile
	if json.Unmarshal(data, &wf) != nil || wf.Version != watermarkVersion {
		return out
	}
	for src, wm := range wf.Sources {
		if wm != nil && wm.Files != nil {
			out[src] = wm
		}
	}
	return out
}

// saveWatermarks writes the watermarks atomically.
func saveWatermarks(root string, wms map[string]*sourceWatermark) error {
	if root == "" {
		return nil
	}
	path := watermarkPath(root)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(watermarkFile{Version: watermarkVersion, Sources: wms}, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + fmt.Sprintf(".tmp.%d", time.Now().UnixNano())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// watermarkDrift reports whether src's files differ from what wm consumed:
// a new file, a file whose size changed, or a consumed file that is gone.
func watermarkDrift(wm *sourceWatermark, src ingestSourceInfo) bool {
	seen := 0
	for _, path := range src.Files {
		mark, ok := wm.Files[filepath.Base(path)]
		if !ok {
			return true
		}
		seen++
		fi, err := os.Stat(path)
		if err != nil || fi.Size() != mark.Size {
			return true
		}
	}
	return seen != len(wm.Files)
}

// lastCompleteOffset returns the offset just past the last '\n' in the first
// size bytes of path (0 when there is none), reading backwards in chunks so a
// multi-megabyte final record costs only its own length.
func lastCompleteOffset(path string, size int64) (int64, error) {
	if size <= 0 {
		return 0, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	const chunk = 64 * 1024
	buf := make([]byte, chunk)
	end := size
	for end > 0 {
		start := end - chunk
		if start < 0 {
			start = 0
		}
		n, err := f.ReadAt(buf[:end-start], start)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if i := bytes.LastIndexByte(buf[:n], '\n'); i >= 0 {
			return start + int64(i) + 1, nil
		}
		end = start
	}
	return 0, nil
}

// watermarkFromFull builds the watermark a full parse of files leaves behind.
func watermarkFromFull(files []string, fingerprint string, cov SourceCoverage) (*sourceWatermark, error) {
	wm := &sourceWatermark{
		Fingerprint: fingerprint,
		Files:       make(map[string]fileMark, len(files)),
		Coverage:    cov,
		UpdatedAt:   time.Now().UTC(),
	}
	for _, path := range files {
		fi, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		off, err := lastCompleteOffset(path, fi.Size())
		if err != nil {
			return nil, err
		}
		wm.Files[filepath.Base(path)] = fileMark{Offset: off, Size: fi.Size()}
	}
	return wm, nil
}

// addCoverage returns a + b (the coverage of two disjoint byte ranges).
func addCoverage(a, b SourceCoverage) SourceCoverage {
	out := a
	out.Mapped += b.Mapped
	out.Degenerate += b.Degenerate
	out.Quarantined += b.Quarantined
	if len(b.UnmappedComponentCounts) > 0 {
		m := make(map[string]int, len(a.UnmappedComponentCounts)+len(b.UnmappedComponentCounts))
		for k, v := range a.UnmappedComponentCounts {
			m[k] = v
		}
		for k, v := range b.UnmappedComponentCounts {
			m[k] += v
		}
		out.UnmappedComponentCounts = m
	}
	if b.OntologyRef != "" {
		out.OntologyRef = b.OntologyRef
	}
	if b.MappingRef != "" {
		out.MappingRef = b.MappingRef
	}
	return out
}

// errNeedFull is returned by applyIngestSourceDelta when the watermark cannot
// be trusted for this source; the caller falls back to the full re-parse.
var errNeedFull = fmt.Errorf("conversations: watermark not usable, full re-parse required")

// deltaResult is what one delta apply did.
type deltaResult struct {
	Watermark     *sourceWatermark
	BytesRead     int64
	FilesRead     int
	SessionsTouch int
	TurnsAdded    int
}

// applyIngestSourceDelta ingests only the bytes of action's files past wm's
// offsets, merging new records into the sessions they belong to, and upserts
// only the sessions that gained a turn. Returns errNeedFull when the
// watermark cannot be used (fingerprint changed, a consumed file shrank or
// vanished); any other error is a real failure.
func applyIngestSourceDelta(idx *Index, action reconcile.Action, ont *LoadedOntology, qw *QuarantineWriter, wm *sourceWatermark) (deltaResult, error) {
	var res deltaResult
	if wm == nil {
		return res, errNeedFull
	}
	fingerprint := ont.SourceFingerprint(action.Name)
	if wm.Fingerprint != fingerprint {
		return res, errNeedFull
	}
	sourceDir, _ := action.Details["source_dir"].(string)
	files := stringSliceDetail(action.Details["ingest_files"])

	present := make(map[string]struct{}, len(files))
	for _, p := range files {
		present[filepath.Base(p)] = struct{}{}
	}
	for base := range wm.Files {
		if _, ok := present[base]; !ok {
			return res, errNeedFull // a consumed file vanished
		}
	}

	deltaCov := NewCoverageTracker()
	acc := newIngestAccumulator(defaultMaxTurnLen)
	acc.Ontology = ont
	acc.Quarantine = qw
	acc.Coverage = deltaCov
	acc.existing = func(key string) (SessionMeta, []Turn, bool) {
		meta, ok := idx.GetMeta(key)
		if !ok {
			return SessionMeta{}, nil, false
		}
		return meta, idx.SessionTurns(key), true
	}

	next := &sourceWatermark{
		Fingerprint: fingerprint,
		Files:       make(map[string]fileMark, len(files)),
		UpdatedAt:   time.Now().UTC(),
	}
	var totalSize int64
	var latestMtime time.Time
	for _, path := range files {
		fi, err := os.Stat(path)
		if err != nil {
			return res, fmt.Errorf("stat %s: %w", path, err)
		}
		totalSize += fi.Size()
		if fi.ModTime().After(latestMtime) {
			latestMtime = fi.ModTime()
		}
		base := filepath.Base(path)
		mark := wm.Files[base] // zero for a new file
		if fi.Size() < mark.Offset {
			return res, errNeedFull // shrank below what was consumed: rewritten
		}
		if fi.Size() == mark.Offset {
			next.Files[base] = fileMark{Offset: mark.Offset, Size: fi.Size()}
			continue
		}
		chunk, err := readFrom(path, mark.Offset, fi.Size())
		if err != nil {
			return res, err
		}
		cut := bytes.LastIndexByte(chunk, '\n') + 1 // 0 when no complete line yet
		if cut > 0 {
			if err := acc.ConsumeFile(bytes.NewReader(chunk[:cut])); err != nil {
				return res, fmt.Errorf("parse %s: %w", path, err)
			}
			res.FilesRead++
			res.BytesRead += int64(cut)
		}
		next.Files[base] = fileMark{Offset: mark.Offset + int64(cut), Size: fi.Size()}
	}

	now := time.Now().UTC()
	batch := make([]SessionAndTurns, 0)
	for _, sess := range acc.Sessions() {
		if sess.added == 0 {
			continue
		}
		sess.Meta.SourcePath = sourceDir
		sess.Meta.IndexedAt = now
		sess.Meta.SourceMtime = latestMtime
		sess.Meta.SourceSize = totalSize
		batch = append(batch, SessionAndTurns{Meta: sess.Meta, Turns: sess.Turns})
		res.SessionsTouch++
		res.TurnsAdded += sess.added
	}
	if len(batch) > 0 {
		if _, err := idx.UpsertSessions(batch); err != nil {
			return res, fmt.Errorf("upsert sessions for source %s: %w", action.Name, err)
		}
	}

	next.Coverage = addCoverage(wm.Coverage, deltaCov.All()[action.Name])
	res.Watermark = next
	return res, nil
}

// readFrom returns bytes [from, to) of path.
func readFrom(path string, from, to int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	buf := make([]byte, to-from)
	n, err := f.ReadAt(buf, from)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return buf[:n], nil
}
