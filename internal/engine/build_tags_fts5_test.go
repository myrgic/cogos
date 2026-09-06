//go:build fts5

// This file is compiled ONLY under -tags fts5, because the test below hard
// asserts that the runtime probe succeeds. CI's default `test` job runs
// untagged (that is precisely the ledger L01 defect: the tag was declared in
// the Makefile and enforced nowhere), so an unconditional assertion here
// fails by construction on the very job that proves the point. The tagged
// build asserts the property; the untagged build is covered by
// TestBuildTags_EveryTaggedBuildPathDeclares and the #604 guard, which fail
// with the real swallowed error instead.
//
// CONSEQUENCE, stated plainly because it is easy to miss: on main today NO
// workflow passes -tags fts5 (ci.yml:69 is `go test -race -count=1 ./...`;
// nightly-integration.yml:59 is `-tags integration`). So this file does not
// compile in CI at all, and the assertion below runs only locally. The tag
// is added to CI by PR #604, which is not merged yet. Until it lands, this
// test is real but UNRUN in CI — do not read a green PR as evidence that the
// runtime probe was exercised. Verified 2026-09-06 by diffing
// origin/main:.github/workflows/ci.yml against the #604 branch (2 tagged
// test sites there, 0 on main).

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
