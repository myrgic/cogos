package engine

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRunBoardAuth_GetWithoutGrant_401 pins that /v1/runs is carved out of the
// blanket GET exemption: an unauthenticated read must be refused at the gate,
// the same way /v1/identity reads are.
func TestRunBoardAuth_GetWithoutGrant_401(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	mintTestGrant(t, srv, "run-board-reader")

	for _, path := range []string{"/v1/runs", "/v1/runs/events", "/v1/runs/run-1"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s without a grant = %d; want 401 (run board reads need a live grant)",
				path, resp.StatusCode)
		}
	}
}

// TestRunBoardAuth_PostWithoutRunsWrite_403 pins that a live grant lacking
// runs:write cannot open or end runs. The grant is fully scoped for the other
// write scopes, so only the missing runs:write can refuse it.
func TestRunBoardAuth_PostWithoutRunsWrite_403(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	token := mintScopedTestGrant(t, srv, "run-board-no-runs",
		ScopeInference, ScopeWrite, ScopeAdmin)

	for _, path := range []string{"/v1/runs", "/v1/runs/run-1/end", "/v1/runs/run-1/delivered"} {
		req, err := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewBufferString(`{}`))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(GrantHeaderName, token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("POST %s without runs:write = %d; want 403 insufficient_scope",
				path, resp.StatusCode)
		}
	}
}

// TestRunBoardAuth_PostWithRunsWrite_Passes_Gate pins that a grant carrying
// runs:write gets past the gate on POST /v1/runs. The handler is still a W1
// stub, so this currently fails with 501 from the stub, not from auth.
func TestRunBoardAuth_PostWithRunsWrite_Passes_Gate(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	token := mintScopedTestGrant(t, srv, "run-board-writer", ScopeRunsWrite)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/runs",
		bytes.NewBufferString(`{"title":"t","owner":"cog","executor":{"kind":"manual"},"origin":{"by":"chaz"}}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(GrantHeaderName, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/runs: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		t.Fatalf("POST /v1/runs with runs:write = %d; the gate refused a scoped grant", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("POST /v1/runs with runs:write = %d; want 201 (W1 body not landed: handler stub returns 501)",
			resp.StatusCode)
	}
}

// TestRunBoardAuth_ShareLinkRefusedOnRuns pins that a share-link token presented
// as a grant does not authorize the run board, on read or write.
func TestRunBoardAuth_ShareLinkRefusedOnRuns(t *testing.T) {
	t.Parallel()
	srv := newGrantAuthTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const linkToken = "tok-run-board-share"
	if _, err := srv.shareLinks.Import(shareImport(linkToken, "stage/room/r1/owner"), ""); err != nil {
		t.Fatalf("import share link: %v", err)
	}

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequest(method, ts.URL+"/v1/runs", bytes.NewBufferString(`{}`))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(GrantHeaderName, linkToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s /v1/runs: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s /v1/runs with a share-link token = %d; want 401 or 403", method, resp.StatusCode)
		}
	}
}
