package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuildTagsReportProbeIsIndependentOfLdflags pins the load-bearing
// property of the report: the fts5 field comes from the runtime probe, NOT
// from the -ldflags claim. A build that forgets the ldflag still reports the
// truth, and the disagreement surfaces as mismatch=true.
func TestBuildTagsReportProbeIsIndependentOfLdflags(t *testing.T) {
	orig := BuildTags
	t.Cleanup(func() { BuildTags = orig })

	// Claim nothing. Under -tags fts5 the probe still says true.
	BuildTags = ""
	rep := BuildTagsReport()
	probed, probeErr := probeFTS5()
	if rep["fts5"] != probed {
		t.Errorf("fts5 = %v; want probe result %v (probe err %q)", rep["fts5"], probed, probeErr)
	}
	if rep["fts5_declared"] != false {
		t.Errorf("fts5_declared = %v; want false for empty BuildTags", rep["fts5_declared"])
	}
	if rep["mismatch"] != probed {
		t.Errorf("mismatch = %v; want %v (declared=false vs probed=%v)", rep["mismatch"], probed, probed)
	}

	// Claim fts5. Now declared and probed agree under -tags fts5.
	BuildTags = "fts5"
	rep = BuildTagsReport()
	if rep["fts5_declared"] != true {
		t.Errorf("fts5_declared = %v; want true", rep["fts5_declared"])
	}
	if rep["fts5"] != probed {
		t.Errorf("fts5 = %v; want probe result %v", rep["fts5"], probed)
	}
	if rep["mismatch"] != (true != probed) {
		t.Errorf("mismatch = %v; want %v", rep["mismatch"], true != probed)
	}
}

// TestParseBuildTags covers the separators the Go toolchain accepts, so the
// same string can be passed to `go build -tags` and to -ldflags -X.
func TestParseBuildTags(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"fts5", []string{"fts5"}},
		{"fts5,mcpserver", []string{"fts5", "mcpserver"}},
		{"mcpserver fts5", []string{"fts5", "mcpserver"}},
		{" fts5 ,, fts5 ", []string{"fts5"}},
	}
	for _, c := range cases {
		got := parseBuildTags(c.in)
		if len(got) != len(c.want) {
			t.Errorf("parseBuildTags(%q) = %v; want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("parseBuildTags(%q) = %v; want %v", c.in, got, c.want)
				break
			}
		}
	}
}

// jsonKeysOf lists an unmarshalled JSON object's keys for failure messages.
// Named distinctly from router_test.go's keysOf, which is typed to
// map[string]ProviderConfig.
func jsonKeysOf(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestBuildTags_EveryTaggedBuildPathDeclares — review finding on #608.
// The Dockerfile passed -tags "fts5" but did not inject
// -X .../engine.BuildTags=fts5, so every docker-compose.node.yml service
// would report build_tags.mismatch=true on an image where fts5 genuinely
// works — a false alarm from the feature built to detect false claims.
//
// This test asserts the invariant directly: any build path that passes the
// fts5 build tag must also declare it via ldflags. Repo-root relative, so a
// newly added build path fails here rather than in production telemetry.
func TestBuildTags_EveryTaggedBuildPathDeclares(t *testing.T) {
	root := "../.."
	// scripts/setup-dev.sh is here because the reviewer found it as a sibling
	// instance of exactly this defect: the dev quickstart built a binary a
	// developer then RUNS, with no tag and no declaration, and the fixed file
	// set below could not see it. A hardcoded list only covers the paths
	// someone remembered; discoverShellBuildPaths below closes that.
	paths := []string{"Dockerfile", "Makefile", ".github/workflows/release.yml", "scripts/setup-dev.sh"}
	paths = append(paths, discoverShellBuildPaths(t, root)...)
	for _, rel := range paths {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		s := string(b)
		tagged := strings.Contains(s, `-tags "fts5"`) || strings.Contains(s, "-tags fts5") ||
			strings.Contains(s, "BUILD_TAGS") || strings.Contains(s, "$tagflag")
		if !tagged {
			continue
		}
		if !strings.Contains(s, "engine.BuildTags") {
			t.Errorf("%s passes the fts5 build tag but never injects "+
				"-X github.com/myrgic/cogos/internal/engine.BuildTags — a binary from "+
				"this path reports build_tags.mismatch=true even when fts5 works", rel)
		}
	}
}

// TestBuildTags_UntaggedTargetsDeclareNothing is the converse, and the review
// finding that prompted it: the Makefile's Windows targets build
// CGO_ENABLED=0 with no -tags flag, so they CANNOT provide fts5. When they
// inherited the shared LDFLAGS they declared fts5 anyway, and /health
// reported declared=fts5 against a probe correctly saying false — flipping
// mismatch=true on every Makefile-built Windows binary and inverting the
// signal this feature exists to give.
//
// Over-claiming is the failure mode this whole row (ledger L01) is about, so
// it gets a test in both directions.
func TestBuildTags_UntaggedTargetsDeclareNothing(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.Contains(line, "$(GO) build") {
			continue
		}
		declares := strings.Contains(line, "$(LDFLAGS)") && !strings.Contains(line, "$(LDFLAGS_UNTAGGED)")
		tagged := strings.Contains(line, "$(BUILD_TAGS)") || strings.Contains(line, "-tags")
		if declares && !tagged {
			t.Errorf("Makefile build line declares BuildTags via $(LDFLAGS) but passes no "+
				"build tag, so the binary claims a module it cannot have "+
				"(mismatch=true on a healthy build): %s", strings.TrimSpace(line))
		}
	}

	// Shell build paths, same rule. My first negative control for
	// scripts/setup-dev.sh PASSED when it should have failed: I removed the
	// -tags flag and left the BuildTags declaration behind, and nothing
	// caught it, because the forward test only fires on paths that DO pass
	// the tag and this converse test only read the Makefile. The over-claim
	// direction was unguarded for exactly the file the reviewer flagged.
	for _, rel := range append([]string{filepath.Join("scripts", "setup-dev.sh")},
		discoverShellBuildPaths(t, "../..")...) {
		sb, err := os.ReadFile(filepath.Join("..", "..", rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, line := range strings.Split(string(sb), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") || !strings.Contains(line, "go build") {
				continue
			}
			if !strings.Contains(line, "./cmd/cogos") {
				continue
			}
			// The declaration may be assembled into $LDFLAGS above the build
			// line, so judge the file, not just the line, for declaration —
			// but judge the actual invocation for the tag.
			declares := strings.Contains(string(sb), "engine.BuildTags=")
			tagged := strings.Contains(line, "-tags")
			if declares && !tagged {
				t.Errorf("%s builds ./cmd/cogos and declares engine.BuildTags but passes no "+
					"-tags flag, so the binary claims a module it cannot have "+
					"(mismatch=true on a healthy build): %s", rel, trimmed)
			}
		}
	}
}

// TestHealthCLI_FailsOnFalseFTS5Probe — review finding on #608. A 200 from
// /health means the daemon answered, not that it is capable: ledger L01's
// whole point is that search degraded silently while health stayed green.
// runHealthCheckCmd exits 0 on any 200, so this asserts the decision logic
// it now applies to the body — a present-and-false probe is a failure,
// an absent field (older kernel) is not.
func TestHealthCLI_FailsOnFalseFTS5Probe(t *testing.T) {
	// Calls the REAL decision function that runHealthCheckCmd uses, not a
	// copy. Review finding on #608: this test previously reimplemented the
	// logic in a local `decide` closure, so editing cli.go would leave the
	// test green while `cog health` silently stopped failing on a broken
	// fts5 build. One implementation, tested directly.
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"probe false → unhealthy", `{"status":"ok","build_tags":{"fts5":false}}`, true},
		{"probe true → healthy", `{"status":"ok","build_tags":{"fts5":true}}`, false},
		{"field absent (older kernel) → healthy", `{"status":"ok"}`, false},
		{"build_tags present, fts5 absent → healthy", `{"status":"ok","build_tags":{"declared":""}}`, false},
		{"unparseable body → healthy (do not fail on garbage)", `not json`, false},
	}
	for _, c := range cases {
		got, msg := healthProbeFailure([]byte(c.body))
		if got != c.want {
			t.Errorf("%s: healthProbeFailure(%s) = %v, want %v", c.name, c.body, got, c.want)
		}
		if got && msg == "" {
			t.Errorf("%s: failure returned an empty message; the operator needs the cause", c.name)
		}
		if !got && msg != "" {
			t.Errorf("%s: healthy result carried a message %q", c.name, msg)
		}
	}
}

// discoverShellBuildPaths finds every tracked shell script under scripts/ that
// builds the cogos command, so the invariant above applies to paths nobody
// remembered to list.
//
// This exists because the hardcoded set missed scripts/setup-dev.sh, and a
// reviewer caught it rather than a test. The rule this repo keeps relearning:
// a check that enumerates known instances reports safety only for the ones
// someone thought of. Enumerate by PROPERTY — "builds ./cmd/cogos" — not by
// name.
func discoverShellBuildPaths(t *testing.T, root string) []string {
	t.Helper()
	dir := filepath.Join(root, "scripts")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read scripts/: %v", err)
	}
	var found []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sh") {
			continue
		}
		rel := filepath.Join("scripts", e.Name())
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		body := string(b)
		// Only scripts that actually build the kernel binary. A script that
		// merely runs `go test` or invokes an already-built cogos is not a
		// build path and must not be forced to declare build tags.
		if !strings.Contains(body, "go build") || !strings.Contains(body, "./cmd/cogos") {
			continue
		}
		if rel == filepath.Join("scripts", "setup-dev.sh") {
			continue // already in the explicit list; don't check it twice
		}
		found = append(found, rel)
	}
	return found
}
