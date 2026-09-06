package engine

import (
	"database/sql"
	"sort"
	"strings"
	"sync"

	// Registers the "sqlite3" driver. Imported here as well as in
	// mcp_stubs.go so the probe below is self-sufficient: if the file that
	// happens to carry the driver import today is ever split out of this
	// package, the FTS5 probe must not silently start reporting false
	// because no driver is registered.
	_ "github.com/mattn/go-sqlite3"
)

// BuildTags is the build-tag list the binary was compiled with, injected at
// build time via -ldflags -X (see the Makefile's BUILD_TAGS/LDFLAGS pair and
// .github/workflows/release.yml). It is a *claim*, not evidence: an ldflag can
// say "fts5" while the module is absent (CGO_ENABLED=0 strips it), and a build
// that forgets the ldflag entirely leaves this empty while FTS5 is very much
// compiled in. Nothing downstream may trust it on its own — see
// BuildTagsReport, which pairs every claim with a runtime probe.
//
// Ledger L01: /health previously reported nothing about compile-time features,
// so a kernel silently missing the fts5 module (C01) looked identical to a
// healthy one. The Makefile guard test added in #604 catches it at build time;
// this catches it on the live binary.
var BuildTags = ""

// fts5ProbeOnce caches the FTS5 runtime probe. The probe opens an in-memory
// SQLite database and creates a virtual table, which is cheap but not free,
// and the answer cannot change over a process's lifetime — the module is
// either linked into this binary or it is not.
var (
	fts5ProbeOnce   sync.Once
	fts5ProbeResult bool
	fts5ProbeErr    string
)

// probeFTS5 reports whether the SQLite driver linked into THIS binary actually
// provides the fts5 module, by attempting the one operation that requires it:
// CREATE VIRTUAL TABLE ... USING fts5 on an in-memory database.
//
// This is the only honest test. Checking the build tag string, grepping the
// binary for "fts5" symbols, or trusting the Makefile all answer a different
// question than "will the constellation schema load at runtime".
func probeFTS5() (bool, string) {
	fts5ProbeOnce.Do(func() {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			fts5ProbeErr = err.Error()
			return
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE VIRTUAL TABLE fts5_probe USING fts5(body)`); err != nil {
			// The canonical failure is "no such module: fts5".
			fts5ProbeErr = err.Error()
			return
		}
		fts5ProbeResult = true
	})
	return fts5ProbeResult, fts5ProbeErr
}

// parseBuildTags splits the injected BuildTags string into a normalized set.
// Accepts the Go toolchain's accepted separators (comma and space) so the same
// value can be handed to `go build -tags` and to -ldflags -X unchanged.
func parseBuildTags(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	seen := make(map[string]struct{}, len(fields))
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if _, dup := seen[f]; dup {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// BuildTagsReport is the /health "build_tags" object.
//
// Field contract, deliberately narrow so consumers cannot be subtly wrong:
//
//   - fts5      — the RUNTIME PROBE result, never the ldflags claim. A consumer
//     that reads only this field is correct.
//   - declared  — the raw -ldflags claim ("" when the build did not inject it).
//   - tags      — declared, normalized to a sorted list.
//   - fts5_declared — whether "fts5" appears in the claim.
//   - mismatch  — declared and probed disagree; the build is lying in one
//     direction or the other and someone should look.
//   - fts5_error — the probe's error string when fts5 is false ("no such
//     module: fts5" for the C01 shape). Omitted when the probe succeeded.
func BuildTagsReport() map[string]interface{} {
	tags := parseBuildTags(BuildTags)
	declaredFTS5 := false
	for _, t := range tags {
		if t == "fts5" {
			declaredFTS5 = true
			break
		}
	}
	probed, probeErr := probeFTS5()

	rep := map[string]interface{}{
		"fts5":          probed,
		"declared":      BuildTags,
		"tags":          tags,
		"fts5_declared": declaredFTS5,
		"mismatch":      declaredFTS5 != probed,
	}
	if !probed && probeErr != "" {
		rep["fts5_error"] = probeErr
	}
	return rep
}
