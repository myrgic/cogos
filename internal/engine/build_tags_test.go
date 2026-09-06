package engine

import (
	"encoding/json"
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
	paths := []string{"Dockerfile", "Makefile", ".github/workflows/release.yml"}
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
}

// TestHealthCLI_FailsOnFalseFTS5Probe — review finding on #608. A 200 from
// /health means the daemon answered, not that it is capable: ledger L01's
// whole point is that search degraded silently while health stayed green.
// runHealthCheckCmd exits 0 on any 200, so this asserts the decision logic
// it now applies to the body — a present-and-false probe is a failure,
// an absent field (older kernel) is not.
func TestHealthCLI_FailsOnFalseFTS5Probe(t *testing.T) {
	decide := func(body string) (fail bool) {
		var h struct {
			BuildTags struct {
				FTS5 *bool `json:"fts5"`
			} `json:"build_tags"`
		}
		if json.Unmarshal([]byte(body), &h) != nil {
			return false
		}
		return h.BuildTags.FTS5 != nil && !*h.BuildTags.FTS5
	}
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
		if got := decide(c.body); got != c.want {
			t.Errorf("%s: decide(%s) = %v, want %v", c.name, c.body, got, c.want)
		}
	}
}
