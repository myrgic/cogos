// serve_config_gate_test.go — outside-in httptest coverage for the
// EnableConfigMutation gate on GET/PATCH /v1/config and POST
// /v1/config/rollback (L5-HTTP-AUTHZ, ledger L5), plus the matching MCP
// transport gate on cog_read_config/cog_write_config/cog_rollback_config
// (mcp_server.go) added in the same PR after cog-review flagged that the
// REST gate alone left the MCP tool surface unprotected.
//
// Covers:
//  1. Gate — 403 for all three REST routes when EnableConfigMutation=false
//     (default).
//  2. Gated-on — 200 for all three REST routes when EnableConfigMutation=true.
//  3. Boot warning — non-loopback BindAddr with no auth token configured logs
//     a warning; loopback BindAddr does not.
//  4. MCP gate — cog_read_config/cog_write_config/cog_rollback_config return
//     an IsError result with {"error":"disabled",...} when the gate is off,
//     and reach the real handler when the gate is on — same gate, second
//     transport.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// newConfigGateTestServer returns an HTTP handler backed by a Server whose
// EnableConfigMutation gate is set as requested. Uses a real temp workspace
// root (no kernel.yaml present) so ReadConfigSnapshot/WriteConfigPatch/
// RollbackConfig have a valid root to operate against.
func newConfigGateTestServer(t *testing.T, enableConfigMutation bool) http.Handler {
	t.Helper()
	root := t.TempDir()
	cfg := makeConfig(t, root)
	cfg.EnableConfigMutation = enableConfigMutation
	nucleus := makeNucleus("Test", "tester")
	proc := NewProcess(cfg, nucleus)

	srv := NewServer(cfg, nucleus, proc)
	t.Cleanup(func() {
		if b := proc.Broker(); b != nil {
			_ = b.Close()
		}
	})
	return srv.Handler()
}

// TestConfigMutation_GateDisabled verifies all three config routes return
// 403 with a "disabled" error when EnableConfigMutation is false (the
// default) — matching the EnableSkillExec / EnableServiceControl convention.
func TestConfigMutation_GateDisabled(t *testing.T) {
	t.Parallel()
	handler := newConfigGateTestServer(t, false)

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"get", http.MethodGet, "/v1/config", ""},
		{"patch", http.MethodPatch, "/v1/config", `{"patch":{"port":7000}}`},
		{"rollback", http.MethodPost, "/v1/config/rollback", ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var body *bytes.Reader
			if tc.body != "" {
				body = bytes.NewReader([]byte(tc.body))
			} else {
				body = bytes.NewReader(nil)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s %s: status=%d body=%q; want 403", tc.method, tc.path, rec.Code, rec.Body.String())
			}
			var resp map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("%s %s: decode response: %v; body=%q", tc.method, tc.path, err, rec.Body.String())
			}
			if resp["error"] != "disabled" {
				t.Errorf("%s %s: error=%q; want \"disabled\"", tc.method, tc.path, resp["error"])
			}
			if !strings.Contains(resp["detail"], "enable_config_mutation") {
				t.Errorf("%s %s: detail=%q; want mention of enable_config_mutation", tc.method, tc.path, resp["detail"])
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("%s %s: content-type=%q; want application/json", tc.method, tc.path, ct)
			}
		})
	}
}

// TestConfigMutation_GateEnabled verifies all three config routes are
// reachable (not 403) when EnableConfigMutation is true.
func TestConfigMutation_GateEnabled(t *testing.T) {
	t.Parallel()
	handler := newConfigGateTestServer(t, true)

	t.Run("get", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/v1/config", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /v1/config: status=%d body=%q; want 200", rec.Code, rec.Body.String())
		}
		var snap map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
			t.Fatalf("decode snapshot: %v; body=%q", err, rec.Body.String())
		}
		if _, ok := snap["effective_config"]; !ok {
			t.Errorf("snapshot missing effective_config field: %v", snap)
		}
	})

	t.Run("patch", func(t *testing.T) {
		t.Parallel()
		body := bytes.NewReader([]byte(`{"patch":{"heartbeat_interval":45},"dry_run":true}`))
		req := httptest.NewRequest(http.MethodPatch, "/v1/config", body)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("PATCH /v1/config: status=%d body=%q; want 200", rec.Code, rec.Body.String())
		}
	})

	t.Run("rollback", func(t *testing.T) {
		t.Parallel()
		body := bytes.NewReader([]byte(`{"list_only":true}`))
		req := httptest.NewRequest(http.MethodPost, "/v1/config/rollback", body)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// No backups exist in a fresh temp workspace; list_only must still
		// reach the handler (not be gated) and return a structured result
		// rather than a 403.
		if rec.Code == http.StatusForbidden {
			t.Fatalf("POST /v1/config/rollback: got 403 with gate enabled; body=%q", rec.Body.String())
		}
	})
}

// ─── Boot bind-address warning ──────────────────────────────────────────────

// captureSlogTo builds a logger writing to a fresh buffer, calls fn with it,
// and returns everything logged. Unlike mutating slog.SetDefault, this never
// touches process-global state, so it's safe under t.Parallel() alongside
// any other test in the package that might also log.
func captureSlogTo(fn func(*slog.Logger)) string {
	var buf bytes.Buffer
	fn(slog.New(slog.NewTextHandler(&buf, nil)))
	return buf.String()
}

func TestWarnIfUnauthenticatedNonLoopback_WarnsOnNonLoopback(t *testing.T) {
	t.Parallel()
	cfg := &Config{BindAddr: "0.0.0.0", Port: 6931}
	out := captureSlogTo(func(l *slog.Logger) {
		warnIfUnauthenticatedNonLoopbackTo(l, cfg)
	})
	if !strings.Contains(out, "SECURITY") {
		t.Errorf("expected a SECURITY warning for non-loopback bind, got: %q", out)
	}
	if !strings.Contains(out, "0.0.0.0") {
		t.Errorf("expected the bind address in the warning, got: %q", out)
	}
}

func TestWarnIfUnauthenticatedNonLoopback_SilentOnLoopback(t *testing.T) {
	t.Parallel()
	cases := []string{"127.0.0.1", "localhost", "::1", ""}
	for _, addr := range cases {
		addr := addr
		t.Run("addr="+addr, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{BindAddr: addr, Port: 6931}
			out := captureSlogTo(func(l *slog.Logger) {
				warnIfUnauthenticatedNonLoopbackTo(l, cfg)
			})
			if strings.Contains(out, "SECURITY") {
				t.Errorf("expected no warning for loopback bind_addr=%q, got: %q", addr, out)
			}
		})
	}
}

func TestWarnIfUnauthenticatedNonLoopback_LANAddress(t *testing.T) {
	t.Parallel()
	cfg := &Config{BindAddr: "192.0.2.10", Port: 6931}
	out := captureSlogTo(func(l *slog.Logger) {
		warnIfUnauthenticatedNonLoopbackTo(l, cfg)
	})
	if !strings.Contains(out, "SECURITY") {
		t.Errorf("expected a SECURITY warning for LAN bind, got: %q", out)
	}
}

func TestIsLoopbackBindAddr(t *testing.T) {
	t.Parallel()
	loopback := []string{"127.0.0.1", "localhost", "::1", "127.5.5.5"}
	nonLoopback := []string{"0.0.0.0", "192.168.1.10", "10.0.0.5", ""}

	for _, addr := range loopback {
		if !isLoopbackBindAddr(addr) {
			t.Errorf("isLoopbackBindAddr(%q) = false; want true", addr)
		}
	}
	for _, addr := range nonLoopback {
		if isLoopbackBindAddr(addr) {
			t.Errorf("isLoopbackBindAddr(%q) = true; want false", addr)
		}
	}
}

// ── MCP transport gate ───────────────────────────────────────────────────────

// newConfigGateTestMCPServer returns an MCPServer whose EnableConfigMutation
// gate is set as requested, wired to a fresh temp workspace (no kernel.yaml
// present) so ReadConfigSnapshot/WriteConfigPatch/RollbackConfig have a valid
// root to operate against once the gate is open.
func newConfigGateTestMCPServer(t *testing.T, enableConfigMutation bool) *MCPServer {
	t.Helper()
	root := t.TempDir()
	cfg := makeConfig(t, root)
	cfg.EnableConfigMutation = enableConfigMutation
	nucleus := makeNucleus("Test", "tester")
	process := NewProcess(cfg, nucleus)
	t.Cleanup(func() {
		if b := process.Broker(); b != nil {
			_ = b.Close()
		}
	})
	return NewMCPServer(cfg, nucleus, process)
}

// TestConfigMutationMCP_GateDisabled verifies all three MCP config tools
// return an IsError result carrying {"error":"disabled",...} — the same
// shape as the REST gate's 403 body — when EnableConfigMutation is false
// (the default). Closes the gap cog-review found: the REST gate alone left
// cog_read_config/cog_write_config/cog_rollback_config reachable over the
// same HTTP bind address via POST /mcp with no check at all.
func TestConfigMutationMCP_GateDisabled(t *testing.T) {
	t.Parallel()
	server := newConfigGateTestMCPServer(t, false)

	t.Run("read", func(t *testing.T) {
		t.Parallel()
		result, _, err := server.toolReadConfig(context.Background(), nil, readConfigInput{})
		assertMCPGateDisabled(t, result, err)
	})

	t.Run("write", func(t *testing.T) {
		t.Parallel()
		result, _, err := server.toolWriteConfig(context.Background(), nil, writeConfigInput{
			Patch: map[string]any{"port": float64(7000)},
		})
		assertMCPGateDisabled(t, result, err)
	})

	t.Run("rollback", func(t *testing.T) {
		t.Parallel()
		result, _, err := server.toolRollbackConfig(context.Background(), nil, rollbackConfigInput{ListOnly: true})
		assertMCPGateDisabled(t, result, err)
	})
}

// assertMCPGateDisabled checks the shared gate-error shape returned by
// requireConfigMutationMCP.
func assertMCPGateDisabled(t *testing.T, result *mcp.CallToolResult, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || !result.IsError {
		t.Fatalf("result.IsError = false; want true (gate should reject)")
	}
	var resp map[string]string
	decodeMCPJSON(t, result, &resp)
	if resp["error"] != "disabled" {
		t.Errorf("error = %q; want \"disabled\"", resp["error"])
	}
	if !strings.Contains(resp["detail"], "enable_config_mutation") {
		t.Errorf("detail = %q; want mention of enable_config_mutation", resp["detail"])
	}
}

// TestConfigMutationMCP_GateEnabled verifies all three MCP config tools reach
// their real handlers (no IsError, normal result shape) when
// EnableConfigMutation is true.
func TestConfigMutationMCP_GateEnabled(t *testing.T) {
	t.Parallel()
	server := newConfigGateTestMCPServer(t, true)

	t.Run("read", func(t *testing.T) {
		t.Parallel()
		result, _, err := server.toolReadConfig(context.Background(), nil, readConfigInput{})
		if err != nil {
			t.Fatalf("toolReadConfig: %v", err)
		}
		if result.IsError {
			t.Fatalf("result.IsError = true; want gate-open passthrough, got %+v", result)
		}
		var decoded ReadConfigResult
		decodeMCPJSON(t, result, &decoded)
		if decoded.Path == "" {
			t.Errorf("path missing from read result")
		}
	})

	t.Run("write", func(t *testing.T) {
		t.Parallel()
		result, _, err := server.toolWriteConfig(context.Background(), nil, writeConfigInput{
			Patch:  map[string]any{"port": float64(7000)},
			DryRun: true,
		})
		if err != nil {
			t.Fatalf("toolWriteConfig: %v", err)
		}
		if result.IsError {
			t.Fatalf("result.IsError = true; want gate-open passthrough, got %+v", result)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		t.Parallel()
		result, _, err := server.toolRollbackConfig(context.Background(), nil, rollbackConfigInput{ListOnly: true})
		if err != nil {
			t.Fatalf("toolRollbackConfig: %v", err)
		}
		if result.IsError {
			t.Fatalf("result.IsError = true; want gate-open passthrough (list_only, no backups yet), got %+v", result)
		}
	})
}

// TestResourceConfigMCP_GateDisabled verifies the cogos://config MCP
// resource returns an error (mentioning "disabled") when EnableConfigMutation
// is false. Regression test for the second cog-review round on PR #460: the
// resource read the same ReadConfigSnapshot data via the untouched Resources
// API, bypassing the gate the three cog_*_config tools had just been given.
func TestResourceConfigMCP_GateDisabled(t *testing.T) {
	t.Parallel()
	server := newConfigGateTestMCPServer(t, false)
	fakeReq := &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "cogos://config"}}
	result, err := server.resourceConfig(context.Background(), fakeReq)
	if err == nil {
		t.Fatalf("resourceConfig returned no error with gate disabled; result=%+v", result)
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("error = %q; want mention of \"disabled\"", err.Error())
	}
	if !strings.Contains(err.Error(), "enable_config_mutation") {
		t.Errorf("error = %q; want mention of enable_config_mutation", err.Error())
	}
	if result != nil {
		t.Errorf("result = %+v; want nil when gated off", result)
	}
}

// TestResourceConfigMCP_GateEnabled verifies the cogos://config MCP resource
// reaches ReadConfigSnapshot (no error, effective_config present) when
// EnableConfigMutation is true.
func TestResourceConfigMCP_GateEnabled(t *testing.T) {
	t.Parallel()
	server := newConfigGateTestMCPServer(t, true)
	fakeReq := &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: "cogos://config"}}
	result, err := server.resourceConfig(context.Background(), fakeReq)
	if err != nil {
		t.Fatalf("resourceConfig: %v", err)
	}
	if len(result.Contents) != 1 {
		t.Fatalf("contents len = %d; want 1", len(result.Contents))
	}
	if !strings.Contains(result.Contents[0].Text, "effective_config") {
		t.Errorf("resource text missing effective_config; got %s", result.Contents[0].Text)
	}
}
