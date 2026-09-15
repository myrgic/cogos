// config_write_wire_test.go — MCP tool + HTTP handler integration tests
// for the Config Mutation API (Agent O §Test plan, wire layer).
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// newMCPForTest builds an MCPServer wired to a fresh workspace + process.
// EnableConfigMutation defaults to true here since these tests exercise
// config read/write/rollback behavior itself, not the gate — mirroring
// newTestServer's HTTP-side callers (TestHandleConfigGet_IncludesDefaults
// et al.) which set srv.cfg.EnableConfigMutation = true explicitly. Gate
// behavior itself is covered by TestConfigMutationMCP_GateDisabled/
// GateEnabled in serve_config_gate_test.go.
func newMCPForTest(t *testing.T) (*MCPServer, string) {
	t.Helper()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	cfg.EnableConfigMutation = true
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))
	return NewMCPServer(cfg, makeNucleus("Cog", "tester"), process), root
}

//  15. MCP roundtrip: toolWriteConfig with a real merge patch writes + returns
//     the expected shape.
func TestToolWriteConfig_HappyPath(t *testing.T) {
	t.Parallel()
	server, root := newMCPForTest(t)
	seedKernelYAML(t, root, "port: 6931\n")

	result, _, err := server.toolWriteConfig(context.Background(), nil, writeConfigInput{
		Patch: map[string]any{"port": float64(7000)},
	})
	if err != nil {
		t.Fatalf("toolWriteConfig: %v", err)
	}
	var decoded WriteConfigResult
	decodeMCPJSON(t, result, &decoded)
	if !decoded.Written {
		t.Errorf("written = false; violations=%+v", decoded.Violations)
	}
	if decoded.EffectiveConfig["port"].(float64) != 7000 {
		t.Errorf("effective_config.port = %v; want 7000", decoded.EffectiveConfig["port"])
	}
	if !decoded.RequiresRestart {
		t.Errorf("requires_restart = false; want true")
	}
}

// 16. MCP roundtrip: toolReadConfig returns defaults when requested.
func TestToolReadConfig_IncludesDefaults(t *testing.T) {
	t.Parallel()
	server, root := newMCPForTest(t)
	seedKernelYAML(t, root, "port: 6931\n")
	result, _, err := server.toolReadConfig(context.Background(), nil, readConfigInput{
		IncludeDefaults: true,
		IncludeRawYAML:  true,
	})
	if err != nil {
		t.Fatalf("toolReadConfig: %v", err)
	}
	var decoded ReadConfigResult
	decodeMCPJSON(t, result, &decoded)
	if !decoded.Exists {
		t.Errorf("exists = false")
	}
	if decoded.Defaults == nil {
		t.Errorf("defaults missing")
	}
	if decoded.RawYAML == "" {
		t.Errorf("raw_yaml missing")
	}
}

// 17. MCP resource: cogos://config returns JSON.
func TestResourceConfig_Reads(t *testing.T) {
	t.Parallel()
	server, root := newMCPForTest(t)
	seedKernelYAML(t, root, "port: 6931\n")
	// Build a minimal request carrying the URI the resource expects.
	// The handler only reads req.Params.URI.
	fakeReq := &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "cogos://config"}}
	result, err := server.resourceConfig(context.Background(), fakeReq)
	if err != nil {
		t.Fatalf("resourceConfig: %v", err)
	}
	if len(result.Contents) != 1 {
		t.Fatalf("contents len = %d; want 1", len(result.Contents))
	}
	text := result.Contents[0].Text
	if !strings.Contains(text, "effective_config") {
		t.Errorf("resource text missing effective_config; got %s", text)
	}
	if !strings.Contains(text, "defaults") {
		t.Errorf("resource text missing defaults; got %s", text)
	}
}

// 18. HTTP: GET /v1/config with include_defaults=1 returns the expected shape.
func TestHandleConfigGet_IncludesDefaults(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	srv.cfg.EnableConfigMutation = true
	seedKernelYAML(t, srv.cfg.WorkspaceRoot, "port: 6931\n")

	req := httptest.NewRequest(http.MethodGet, "/v1/config?include_defaults=1&include_raw_yaml=1", nil)
	w := httptest.NewRecorder()
	srv.handleConfigGet(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", w.Code)
	}
	var body ReadConfigResult
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.Exists {
		t.Errorf("exists = false")
	}
	if body.Defaults == nil {
		t.Errorf("defaults missing")
	}
	if body.RawYAML == "" {
		t.Errorf("raw_yaml missing")
	}
	if body.Path != filepath.Join(srv.cfg.WorkspaceRoot, ".cog/config/kernel.yaml") {
		t.Errorf("path = %q; unexpected", body.Path)
	}
}

// 19. HTTP: PATCH /v1/config with a valid patch returns 200 + written=true.
func TestHandleConfigPatch_Success(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	srv.cfg.EnableConfigMutation = true
	seedKernelYAML(t, srv.cfg.WorkspaceRoot, "port: 6931\n")

	body := map[string]any{
		"patch": map[string]any{"port": 7000},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPatch, "/v1/config", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	srv.handleConfigPatch(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d (body=%s); want 200", w.Code, w.Body.String())
	}
	var result WriteConfigResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !result.Written {
		t.Errorf("written = false; violations = %+v", result.Violations)
	}
}

// 20. HTTP: PATCH /v1/config with an invalid port → 400 + violations.
func TestHandleConfigPatch_ValidationFailure(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t)
	srv.cfg.EnableConfigMutation = true
	seedKernelYAML(t, srv.cfg.WorkspaceRoot, "port: 6931\n")

	body := map[string]any{
		"patch": map[string]any{"port": 70000},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPatch, "/v1/config", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	srv.handleConfigPatch(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d (body=%s); want 400", w.Code, w.Body.String())
	}
	var result WriteConfigResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Written {
		t.Errorf("written = true on bad patch")
	}
	if len(result.Violations) == 0 {
		t.Errorf("expected violations; got none")
	}
}
