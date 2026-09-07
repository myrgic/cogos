//go:build fts5

// This file is compiled ONLY under -tags fts5, because the test below hard
// asserts that the runtime probe succeeds. An unconditional assertion would
// fail by construction on any untagged build — which is precisely the ledger
// L01 defect it documents: the tag was declared in the Makefile and enforced
// nowhere. The tagged build asserts the property; untagged builds are covered
// by TestBuildTags_EveryTaggedBuildPathDeclares, which fails with the real
// swallowed error instead.
//
// STATUS IN CI, as of this commit: #604 is MERGED (it is this branch's merge
// base), and it added -tags fts5 to every go test invocation in the repo —
// ci.yml:69, ci.yml:86, and nightly-integration.yml:59 (`-tags "integration
// fts5"`). There is no untagged test job left, so this file DOES compile and
// the assertion below DOES run in CI.
//
// That is a correction. An earlier version of this header said CI ran
// untagged, that no workflow passed -tags fts5, and that this test was "real
// but UNRUN in CI" pending #604. All three statements were true when written
// and false by the time they were read: #604 merged the same day. A reviewer
// caught the stale claim, not a test — a comment asserting a fact about
// another branch has no mechanism to notice when that branch lands. Re-verify
// against origin/main before trusting any CI claim written here.

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
