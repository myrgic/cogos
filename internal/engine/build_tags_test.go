package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthReportsBuildTagsFTS5 is the L01 closing test.
//
// Negative control: on the pre-change code /health carries no "build_tags"
// key at all, so this test fails at the first lookup ("build_tags missing
// from /health"). It only passes once the runtime FTS5 probe is wired into
// the health payload.
//
// Under -tags fts5 the probe MUST report true. If this fails on a build that
// was supposed to have FTS5, the binary genuinely cannot create an fts5
// virtual table and the constellation index would silently fall back to grep
// (ledger C01) — that is the exact failure this test exists to make loud.
func TestHealthReportsBuildTagsFTS5(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", w.Code)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	raw, ok := body["build_tags"]
	if !ok {
		t.Fatalf("build_tags missing from /health; got keys %v", jsonKeysOf(body))
	}
	bt, ok := raw.(map[string]interface{})
	if !ok {
		t.Fatalf("build_tags = %T; want object", raw)
	}

	fts5, ok := bt["fts5"]
	if !ok {
		t.Fatalf("build_tags.fts5 missing; got %v", jsonKeysOf(bt))
	}
	if fts5 != true {
		t.Errorf("build_tags.fts5 = %v (%T); want true under -tags fts5 (probe error: %v)",
			fts5, fts5, bt["fts5_error"])
	}
}

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
