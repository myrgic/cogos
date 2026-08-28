package engine

// Named audit_callers_test.go rather than the ADR-093 §8 Commit 1 text's
// literal "audit_callers.go": a `testing.T`-based guard has to live in a
// _test.go file to run under `go test` and, just as importantly, to be
// excluded from production binaries — a non-test file importing "testing"
// would link the test framework into every cogos build. Functionally this
// is exactly the audit the ADR describes.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestAudit_NoNewSpawnBackgroundCallers is the ADR-093 §8 Commit 1 guard:
// "Add an audit_callers.go test that flags any caller still using
// SpawnBackground from within the kernel." SpawnBackground
// (provider_claudecode.go) is deprecated as a substrate primitive in favor
// of ManagedSession (managed_session.go), but its one pre-existing caller
// has not yet been migrated (ADR-093 §8 Commits 3-4 are a later lane) —
// so this audit is an allowlist, not a blanket ban: it fails the build
// only if a call site appears that isn't already known and accounted for,
// catching drift (a new caller added without reading the deprecation
// notice) rather than relitigating the existing one on every run.
//
// Scope: internal/engine only, matching the ADR's "from within the
// kernel" — this package is where the substrate lives; other packages
// (cmd/, tooling) are out of scope for this guard.
func TestAudit_NoNewSpawnBackgroundCallers(t *testing.T) {
	// Files allowed to reference `.SpawnBackground(` as a *call* (not the
	// method's own definition, which is exempted by name below). Update
	// this list only alongside an ADR-093 §8 Commit 3/4 migration PR that
	// either removes a caller or explains the new one in its description —
	// this is the search-before-rename / update-once-cascade discipline
	// applied to a deprecated-API call site instead of an identifier
	// rename.
	allowedCallers := map[string]bool{
		"serve_claude_code.go": true, // POST /v1/claude-code/spawn — migration pending, ADR-093 §8 Commits 3-4
	}

	callRe := regexp.MustCompile(`\.SpawnBackground\(`)
	defRe := regexp.MustCompile(`func\s*\([^)]*\)\s*SpawnBackground\(`)

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	var unexpected []string
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		base := filepath.Base(path)
		if base == "audit_callers_test.go" {
			return nil // this file's own doc comment mentions the method name
		}

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		content := string(data)

		for _, line := range strings.Split(content, "\n") {
			if defRe.MatchString(line) {
				continue // the method's own definition, not a call
			}
			if !callRe.MatchString(line) {
				continue
			}
			if allowedCallers[base] {
				continue
			}
			unexpected = append(unexpected, base)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/engine: %v", err)
	}

	if len(unexpected) > 0 {
		t.Errorf(
			"SpawnBackground is deprecated (see its doc comment in "+
				"provider_claudecode.go) — found call(s) in %v not on the "+
				"allowlist in audit_callers_test.go. New spawn paths should use "+
				"ManagedSessionRegistry (managed_session.go) instead. If this "+
				"caller is intentional migration-in-progress work, add it to "+
				"allowedCallers with a comment explaining why.",
			unexpected)
	}
}
