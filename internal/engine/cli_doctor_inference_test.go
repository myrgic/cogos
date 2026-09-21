// cli_doctor_inference_test.go — tests for the "inference surface" doctor
// group (myrgic/cogos#631). Every test that has a FAIL path is paired with
// a fixture that must FAIL (negative control), not just a happy-path OK
// fixture, per the task's own testing rule.
package engine

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// findCheckInGroup locates a check by exact name within a group; test
// helper. Named distinctly from cli_doctor_test.go's own findCheck (which
// takes a report+group+check name) to avoid colliding in this package.
func findCheckInGroup(t *testing.T, g *DoctorGroup, name string) DoctorCheck {
	t.Helper()
	for _, c := range g.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no check named %q in group %q (have: %v)", name, g.Name, checkNamesInGroup(g))
	return DoctorCheck{}
}

func checkNamesInGroup(g *DoctorGroup) []string {
	var out []string
	for _, c := range g.Checks {
		out = append(out, c.Name)
	}
	return out
}

// ---------------------------------------------------------------------------
// (a) providers.yaml endpoints
// ---------------------------------------------------------------------------

func TestDoctorProviderEndpoints_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data": []map[string]any{
				{"id": "test-model"},
			},
		})
	}))
	defer srv.Close()

	root := t.TempDir()
	writeProvidersYAML(t, root, srv.URL)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorProviderEndpoints(g, root, DoctorOptions{})

	c := findCheckInGroup(t, g, "providers.yaml endpoint: test-openai")
	if c.Status != StatusOK {
		t.Fatalf("status = %s, detail = %q; want OK", c.Status, c.Detail)
	}
}

// TestDoctorProviderEndpoints_FAIL is the negative control: an endpoint
// that refuses connections (port 1, the TCP reserved/unassigned port) must
// FAIL, not silently pass.
func TestDoctorProviderEndpoints_FAIL(t *testing.T) {
	root := t.TempDir()
	writeProvidersYAML(t, root, "http://127.0.0.1:1")

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorProviderEndpoints(g, root, DoctorOptions{})

	c := findCheckInGroup(t, g, "providers.yaml endpoint: test-openai")
	if c.Status != StatusFail {
		t.Fatalf("status = %s, detail = %q; want FAIL", c.Status, c.Detail)
	}
}

func TestDoctorProviderEndpoints_WarnsOnMissingDeclaredModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data": []map[string]any{
				{"id": "some-other-model"},
			},
		})
	}))
	defer srv.Close()

	root := t.TempDir()
	writeProvidersYAML(t, root, srv.URL)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorProviderEndpoints(g, root, DoctorOptions{})

	c := findCheckInGroup(t, g, "providers.yaml endpoint: test-openai")
	if c.Status != StatusWarn {
		t.Fatalf("status = %s, detail = %q; want WARN (declared model absent from listing)", c.Status, c.Detail)
	}
}

func TestDoctorProviderEndpoints_SkipNetwork(t *testing.T) {
	root := t.TempDir()
	writeProvidersYAML(t, root, "http://127.0.0.1:1")

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorProviderEndpoints(g, root, DoctorOptions{SkipNetwork: true})

	c := findCheckInGroup(t, g, "providers.yaml endpoint: test-openai")
	if c.Status != StatusUnknown {
		t.Fatalf("status = %s; want UNKNOWN under --skip-network", c.Status)
	}
	if !strings.Contains(c.Detail, "skip-network") {
		t.Errorf("detail = %q; want mention of --skip-network", c.Detail)
	}
}

func TestDoctorProviderEndpoints_NoProvidersYAML(t *testing.T) {
	root := t.TempDir()

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorProviderEndpoints(g, root, DoctorOptions{})

	c := findCheckInGroup(t, g, "providers.yaml endpoints")
	if c.Status != StatusUnknown {
		t.Fatalf("status = %s; want UNKNOWN when no providers.yaml present", c.Status)
	}
}

func TestLoadProvidersYAMLDoctor_LocalOverrideMergesShallow(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".cog", "config")
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		t.Fatal(err)
	}
	base := "providers:\n  p1:\n    type: openai\n    endpoint: http://base:1234\n    model: base-model\n"
	local := "providers:\n  p1:\n    endpoint: http://override:5678\n"
	if err := os.WriteFile(filepath.Join(cfgDir, "providers.yaml"), []byte(base), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "providers.local.yaml"), []byte(local), 0644); err != nil {
		t.Fatal(err)
	}

	// Doctor now uses the router's own loader (loadProvidersConfig), so the
	// overlay semantics under test are the kernel's, not a doctor re-implementation.
	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		t.Fatalf("loadProvidersConfig: %v", err)
	}
	p1 := pcfg.Providers["p1"]
	if p1.Endpoint != "http://override:5678" {
		t.Errorf("endpoint = %q; want local override", p1.Endpoint)
	}
	if p1.Model != "base-model" {
		t.Errorf("model = %q; want base value preserved (shallow merge)", p1.Model)
	}
}

func writeProvidersYAML(t *testing.T, root, endpoint string) {
	t.Helper()
	cfgDir := filepath.Join(root, ".cog", "config")
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		t.Fatal(err)
	}
	content := "providers:\n  test-openai:\n    type: openai\n    endpoint: " + endpoint + "\n    model: test-model\n    enabled: true\n"
	if err := os.WriteFile(filepath.Join(cfgDir, "providers.yaml"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func writeProvidersYAMLRaw(t *testing.T, root, content string) {
	t.Helper()
	cfgDir := filepath.Join(root, ".cog", "config")
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "providers.yaml"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// (c) provider argv vs installed CLI
// ---------------------------------------------------------------------------

// writeFakeHelpScript writes an executable shell script at dir/name that
// prints helpText to stdout when invoked with any arguments (mimicking
// `<bin> <subcmd> --help`), and returns dir so callers can point PATH at it
// via resolveCLIBinary's lookPathAll(name) call.
func writeFakeHelpScript(t *testing.T, dir, name, helpText string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake --help script fixture is POSIX-shell only")
	}
	path := filepath.Join(dir, name)
	script := "#!/bin/sh\ncat << 'HELPEOF'\n" + helpText + "\nHELPEOF\n"
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return path
}

func withPATH(t *testing.T, dir string) {
	t.Helper()
	old := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", old) })
	os.Setenv("PATH", dir+string(os.PathListSeparator)+old)
}

func TestCheckArgvContracts_OK(t *testing.T) {
	dir := t.TempDir()
	writeFakeHelpScript(t, dir, "fake-cli-ok",
		"Usage: fake-cli-ok exec [OPTIONS]\n-m, --model\n--config\n--sandbox\n--full-auto\n--skip-git-repo-check\n--json")
	withPATH(t, dir)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	checkArgvContracts(g, []cliArgvContract{{
		provider: "fake",
		bin:      "fake-cli-ok",
		subcmd:   []string{"exec"},
		flags:    []string{"-m", "--config", "--sandbox", "--full-auto", "--skip-git-repo-check", "--json"},
	}})

	c := findCheckInGroup(t, g, "argv vs CLI: fake")
	if c.Status != StatusOK {
		t.Fatalf("status = %s, detail = %q; want OK", c.Status, c.Detail)
	}
}

// TestCheckArgvContracts_FAIL is the negative control this check exists
// for: a fixture --help output that OMITS a flag the contract requires
// (mirrors the real codex --full-auto removal, #627/#628) must FAIL.
func TestCheckArgvContracts_FAIL(t *testing.T) {
	dir := t.TempDir()
	writeFakeHelpScript(t, dir, "fake-cli-missing-flag",
		"Usage: fake-cli-missing-flag exec [OPTIONS]\n-m, --model\n--config\n--sandbox\n--skip-git-repo-check\n--json")
	withPATH(t, dir)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	checkArgvContracts(g, []cliArgvContract{{
		provider: "fake",
		bin:      "fake-cli-missing-flag",
		subcmd:   []string{"exec"},
		flags:    []string{"-m", "--config", "--sandbox", "--full-auto", "--skip-git-repo-check", "--json"},
	}})

	c := findCheckInGroup(t, g, "argv vs CLI: fake")
	if c.Status != StatusFail {
		t.Fatalf("status = %s, detail = %q; want FAIL (missing --full-auto)", c.Status, c.Detail)
	}
	if !strings.Contains(c.Detail, "--full-auto") {
		t.Errorf("detail = %q; want it to name the missing flag", c.Detail)
	}
}

func TestCheckArgvContracts_UnknownWhenBinaryMissing(t *testing.T) {
	dir := t.TempDir() // empty — nothing on PATH
	withPATH(t, dir)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	checkArgvContracts(g, []cliArgvContract{{
		provider: "fake",
		bin:      "definitely-not-a-real-binary-xyz",
	}})

	c := findCheckInGroup(t, g, "argv vs CLI: fake")
	if c.Status != StatusUnknown {
		t.Fatalf("status = %s; want UNKNOWN when binary is not resolvable", c.Status)
	}
}

// ---------------------------------------------------------------------------
// (e) alias targets in live catalog
// ---------------------------------------------------------------------------

func TestDoctorAliasTargetsInCatalog_FAIL_MissingAlias(t *testing.T) {
	// A live catalog missing "local" (a promised static alias per
	// AvailableModelIDs) is the negative control: the check must FAIL, not
	// silently pass.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data":   []map[string]any{{"id": "foreground"}, {"id": "deliberation"}},
		})
	}))
	defer srv.Close()

	root := t.TempDir()
	writeKernelYAMLPort(t, root, srv.URL)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorAliasTargetsInCatalog(g, root, DoctorOptions{})

	c := findCheckInGroup(t, g, "alias targets in live catalog")
	if c.Status != StatusFail {
		t.Fatalf("status = %s, detail = %q; want FAIL (catalog missing promised aliases)", c.Status, c.Detail)
	}
}

func TestDoctorAliasTargetsInCatalog_UnknownWhenUnreachable(t *testing.T) {
	root := t.TempDir()
	writeKernelYAMLPort(t, root, "http://127.0.0.1:1")

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorAliasTargetsInCatalog(g, root, DoctorOptions{})

	c := findCheckInGroup(t, g, "alias targets in live catalog")
	if c.Status != StatusUnknown {
		t.Fatalf("status = %s, detail = %q; want UNKNOWN when kernel unreachable", c.Status, c.Detail)
	}
}

// writeKernelYAMLPort points a workspace's .cog/config/kernel.yaml at the
// port of an httptest server URL, so kernelEndpointForDoctor resolves to it
// without a running daemon/state.yaml.
func writeKernelYAMLPort(t *testing.T, root, serverURL string) {
	t.Helper()
	u, err := neturlParseDoctor(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	cfgDir := filepath.Join(root, ".cog", "config")
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		t.Fatal(err)
	}
	content := "port: " + u.Port() + "\n"
	if err := os.WriteFile(filepath.Join(cfgDir, "kernel.yaml"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// (f) external clients -> kernel: dead ports + auth
// ---------------------------------------------------------------------------

func TestDoctorExternalClientsToKernel_FAIL_InvalidGrant(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"type": "invalid_grant", "message": "grant rejected"},
		})
	}))
	defer srv.Close()

	root, home := setupExternalClientFixture(t, srv.URL+"/mcp")
	kernelYAMLFromURL(t, root, srv.URL)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorExternalClientsToKernelWithHome(g, root, DoctorOptions{}, home)

	c := findCheckInGroup(t, g, "external client: ~/.claude.json (mcpServers.cogos-kernel)")
	if c.Status != StatusFail {
		t.Fatalf("status = %s, detail = %q; want FAIL (401 invalid_grant)", c.Status, c.Detail)
	}
	if !strings.Contains(c.Detail, "invalid_grant") {
		t.Errorf("detail = %q; want error type surfaced", c.Detail)
	}
	// Redaction: the fixture's grant value must never appear verbatim.
	if strings.Contains(c.Detail, "test-grant-value") {
		t.Errorf("detail leaks raw credential: %q", c.Detail)
	}
}

func TestDoctorExternalClientsToKernel_OK_NonAuthErrorMeansAuthPassed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Any non-401 (e.g. 400 for a malformed-but-authenticated request)
		// means auth passed, per the task's own spec.
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	root, home := setupExternalClientFixture(t, srv.URL+"/mcp")
	kernelYAMLFromURL(t, root, srv.URL)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorExternalClientsToKernelWithHome(g, root, DoctorOptions{}, home)

	c := findCheckInGroup(t, g, "external client: ~/.claude.json (mcpServers.cogos-kernel)")
	if c.Status != StatusOK {
		t.Fatalf("status = %s, detail = %q; want OK (non-401 means auth passed)", c.Status, c.Detail)
	}
}

// TestDoctorExternalClientsToKernel_FAIL_DeadPort is the negative control
// for the TCP-dial half of check (f): a client config pointing at a closed
// port must FAIL as "dead port", not silently pass or hang.
func TestDoctorExternalClientsToKernel_FAIL_DeadPort(t *testing.T) {
	// Bind a listener then close it immediately: the port is very likely to
	// still be refusing connections for the extent of this test on
	// loopback, without depending on the OS-reserved port 1 trick used
	// elsewhere (kept independent so this test isn't tied to that
	// behavior).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	deadURL := "http://" + deadAddr + "/mcp"
	root, home := setupExternalClientFixture(t, deadURL)

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorExternalClientsToKernelWithHome(g, root, DoctorOptions{}, home)

	c := findCheckInGroup(t, g, "external client: ~/.claude.json (mcpServers.cogos-kernel)")
	if c.Status != StatusFail {
		t.Fatalf("status = %s, detail = %q; want FAIL (dead port)", c.Status, c.Detail)
	}
	if !strings.Contains(c.Detail, "dead port") {
		t.Errorf("detail = %q; want it to say \"dead port\"", c.Detail)
	}
}

func TestDoctorExternalClientsToKernel_SkipNetwork(t *testing.T) {
	root, home := setupExternalClientFixture(t, "http://127.0.0.1:1/mcp")

	report := &DoctorReport{}
	g := report.addGroup("inference surface")
	doctorExternalClientsToKernelWithHome(g, root, DoctorOptions{SkipNetwork: true}, home)

	c := findCheckInGroup(t, g, "external client: ~/.claude.json (mcpServers.cogos-kernel)")
	if c.Status != StatusUnknown {
		t.Fatalf("status = %s; want UNKNOWN under --skip-network", c.Status)
	}
}

// setupExternalClientFixture writes a fake HOME with a ~/.claude.json
// registering the kernel MCP target at kernelURL, and returns (workspace
// root, home).
func setupExternalClientFixture(t *testing.T, kernelURL string) (root, home string) {
	t.Helper()
	root = t.TempDir()
	home = t.TempDir()

	content := `{
  "mcpServers": {
    "cogos-kernel": {
      "type": "http",
      "url": "` + kernelURL + `",
      "headers": { "X-Cogos-Grant": "test-grant-value" }
    }
  }
}`
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return root, home
}

func kernelYAMLFromURL(t *testing.T, root, serverURL string) {
	t.Helper()
	writeKernelYAMLPort(t, root, serverURL)
}

// ---------------------------------------------------------------------------
// Config parsers against testdata/doctor/ fixtures
// ---------------------------------------------------------------------------

func TestCollectClaudeJSONTargets_Fixture(t *testing.T) {
	home := t.TempDir()
	fixture := readTestdataFixture(t, "claude-json/dot-claude.json")
	fixture = strings.ReplaceAll(fixture, "PLACEHOLDER_KERNEL_URL", "http://127.0.0.1:6931/mcp")
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(fixture), 0644); err != nil {
		t.Fatal(err)
	}

	targets := collectClaudeJSONTargets(home)
	if len(targets) != 1 {
		t.Fatalf("got %d targets; want 1 (stdio-server has no url, must be skipped)", len(targets))
	}
	if targets[0].url != "http://127.0.0.1:6931/mcp" {
		t.Errorf("url = %q", targets[0].url)
	}
	if targets[0].headers["X-Cogos-Grant"] != "test-grant-value" {
		t.Errorf("headers = %v; want X-Cogos-Grant carried through", targets[0].headers)
	}
}

func TestCollectCodexTOMLTargets_Fixture(t *testing.T) {
	home := t.TempDir()
	fixture := readTestdataFixture(t, "codex-toml/dot-codex-config.toml")
	fixture = strings.ReplaceAll(fixture, "PLACEHOLDER_KERNEL_URL", "http://127.0.0.1:6931/mcp")
	codexDir := filepath.Join(home, ".codex")
	if err := os.MkdirAll(codexDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexDir, "config.toml"), []byte(fixture), 0644); err != nil {
		t.Fatal(err)
	}

	targets := collectCodexTOMLTargets(home)

	byName := map[string]externalClientTarget{}
	for _, tg := range targets {
		byName[codexTargetName(tg.source)] = tg
	}

	// stdio-thing and bridge-thing have no url and must be skipped. A
	// sub-table ([...].http_headers, [...].env) must never register as a
	// server of its own — that phantom entry is the parse bug this guards.
	if len(targets) != 3 {
		t.Fatalf("got %d targets; want 3 (cogos-v3, cogos-kernel, early-headers): %+v", len(targets), targets)
	}
	for _, phantom := range []string{
		"cogos-v3.http_headers",
		"cogos-kernel.http_headers",
		"early-headers.http_headers",
		"bridge-thing.env",
	} {
		if _, found := byName[phantom]; found {
			t.Errorf("sub-table %q registered as a server; sub-tables are server-scoped state, not servers", phantom)
		}
	}

	// Inline shape: http_headers = { "X-Cogos-Grant" = "..." } (quoted key).
	inline, ok := byName["cogos-v3"]
	if !ok {
		t.Fatalf("cogos-v3 missing from targets: %+v", targets)
	}
	if inline.url != "http://127.0.0.1:6931/mcp" {
		t.Errorf("cogos-v3 url = %q", inline.url)
	}
	if inline.headers["X-Cogos-Grant"] != "test-grant-value" {
		t.Errorf("cogos-v3 headers = %v; want X-Cogos-Grant parsed from http_headers inline table", inline.headers)
	}

	// Sub-table shape with a BARE key — the shape ~/.codex/config.toml
	// actually writes, and the one that produced the false-positive FAIL.
	sub, ok := byName["cogos-kernel"]
	if !ok {
		t.Fatalf("cogos-kernel missing from targets: %+v", targets)
	}
	if sub.url != "http://127.0.0.1:6931/mcp" {
		t.Errorf("cogos-kernel url = %q", sub.url)
	}
	if sub.headers["X-Cogos-Grant"] != "subtable-grant-value" {
		t.Errorf("cogos-kernel headers = %v; want X-Cogos-Grant parsed from a [mcp_servers.NAME.http_headers] sub-table with a bare key", sub.headers)
	}

	// Declaration order must not matter: sub-table before its own section.
	early, ok := byName["early-headers"]
	if !ok {
		t.Fatalf("early-headers missing from targets: %+v", targets)
	}
	if early.headers["Authorization"] != "Bearer early-value" {
		t.Errorf("early-headers headers = %v; want Authorization from a sub-table declared BEFORE the server section", early.headers)
	}
}

// TestCollectCodexTOMLTargets_SubTableCredentialReachesAuthProbe is the
// regression guard for the false-positive doctor FAIL that blocked the
// pre-push hook on this branch.
//
// Observed on Darkstar before the fix:
//
//	[FAIL] external client: ~/.codex/config.toml ([mcp_servers.cogos-v3])
//	       kernel auth: HTTP 401 (missing_grant), credential(s) sent:
//
// with "credential(s) sent" EMPTY — while the very same grant, sent by hand,
// was accepted by the kernel (HTTP 400, i.e. past auth). Doctor was reporting
// a live credential as missing.
//
// The auth probe builds its request from target.headers, so an empty map is
// indistinguishable from "this client sends no credential". This asserts the
// credential survives collection, which is the precondition for the probe
// reporting truthfully.
func TestCollectCodexTOMLTargets_SubTableCredentialReachesAuthProbe(t *testing.T) {
	home := t.TempDir()
	codexDir := filepath.Join(home, ".codex")
	if err := os.MkdirAll(codexDir, 0755); err != nil {
		t.Fatal(err)
	}
	// Minimal reproduction of the real file's shape.
	content := `[mcp_servers.cogos-v3]
url = "http://127.0.0.1:6931/mcp"
transport = "http"

[mcp_servers.cogos-v3.http_headers]
X-Cogos-Grant = "live-grant-32-chars-exactly-ok!!"
`
	if err := os.WriteFile(filepath.Join(codexDir, "config.toml"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	targets := collectCodexTOMLTargets(home)
	if len(targets) != 1 {
		t.Fatalf("got %d targets; want exactly 1 (the sub-table must not register as a second server): %+v", len(targets), targets)
	}
	if got := len(targets[0].headers); got == 0 {
		t.Fatal("credential(s) sent would be EMPTY — doctor would report a live grant as missing_grant (the false-positive FAIL)")
	}
	if targets[0].headers["X-Cogos-Grant"] != "live-grant-32-chars-exactly-ok!!" {
		t.Errorf("headers = %v; want the sub-table grant carried through to the auth probe", targets[0].headers)
	}
}

func TestCollectHermesProfileTargets_Fixture(t *testing.T) {
	home := t.TempDir()
	fixture := readTestdataFixture(t, "hermes-profiles/config.yaml")
	fixture = strings.ReplaceAll(fixture, "PLACEHOLDER_KERNEL_URL_V1", "http://127.0.0.1:6931/v1")
	profDir := filepath.Join(home, ".hermes", "profiles", "testprofile")
	if err := os.MkdirAll(profDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profDir, "config.yaml"), []byte(fixture), 0644); err != nil {
		t.Fatal(err)
	}

	targets := collectHermesProfileTargets(home)
	if len(targets) != 1 {
		t.Fatalf("got %d targets; want 1", len(targets))
	}
	if targets[0].url != "http://127.0.0.1:6931/mcp" {
		t.Errorf("url = %q; want /v1 base rewritten to /mcp", targets[0].url)
	}
	if got := targets[0].headers["Authorization"]; got != "Bearer test-api-key-value" {
		t.Errorf("Authorization header = %q", got)
	}
}

func readTestdataFixture(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "doctor", rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// Negative control for cog-review finding on PR #635: a short flag that is a
// substring of a sibling long flag must NOT count as advertised. "-p" occurs
// inside "--dangerously-skip-permissions" (the hyphen before "permissions");
// naive strings.Contains reported it present after the CLI dropped it.
func TestHelpAdvertisesFlag_ShortFlagNotSatisfiedBySubstring(t *testing.T) {
	helpWithoutP := "Usage: claude [OPTIONS]\n  --dangerously-skip-permissions  Skip all prompts\n  --model <MODEL>\n  --provider <NAME>\n"
	if helpAdvertisesFlag(helpWithoutP, "-p") {
		t.Fatalf("-p reported present but only occurs inside --dangerously-skip-permissions / --provider")
	}
	if !helpAdvertisesFlag(helpWithoutP, "--model") {
		t.Fatalf("--model should be present")
	}
	helpWithP := helpWithoutP + "  -p, --print  Print and exit\n"
	if !helpAdvertisesFlag(helpWithP, "-p") {
		t.Fatalf("-p should be present when listed as its own token")
	}
	// Forms other CLIs use: "-m, --model", "[-p]", "(-p|--print)", "-p=VAL".
	for _, h := range []string{"  -m, --model", "usage: x [-p] file", "(-p|--print)", "  -p=VAL  print"} {
		flag := "-m"
		if !strings.Contains(h, "-m") {
			flag = "-p"
		}
		if !helpAdvertisesFlag(h, flag) {
			t.Fatalf("%q should advertise %s", h, flag)
		}
	}
}

// Every "--flag"/"-x" literal in a CLI provider's source that buildArgs can
// emit must appear in the derived contract. This is the guard cog-review asked
// for on #635 round 3: the derived contract silently omitted --allowedTools,
// --mcp-config, --max-budget-usd and --no-session-persistence because the
// doctor's fixture config/request did not turn those branches on.
func TestCliArgvContracts_CoverEveryBuildArgsFlag(t *testing.T) {
	// Flags a provider file mentions that are NOT emitted by buildArgs (probe
	// or version calls) — excluded explicitly so the test stays honest.
	notArgv := map[string]bool{"--version": true, "--help": true}
	want := map[string][]string{
		"codex": {"-m", "--config", "--sandbox", "--skip-git-repo-check", "--json"},
		// "--print" is only ever rewritten to "--mode" by Stream(); pi's
		// buildArgs emits "-p". "--mode" IS sent (Stream) and must be checked.
		"pi":     {"-p", "--provider", "--model", "--thinking", "--tools", "--system-prompt", "--no-session", "--no-extensions", "--mode"},
		"claude": {"-p", "--dangerously-skip-permissions", "--model", "--effort", "--append-system-prompt", "--mcp-config", "--strict-mcp-config", "--allowedTools", "--disallowedTools", "--max-budget-usd", "--no-session-persistence", "--output-format", "--verbose", "--include-partial-messages"},
	}
	got := map[string]map[string]bool{}
	for _, c := range cliArgvContracts() {
		got[c.provider] = map[string]bool{}
		for _, f := range c.flags {
			got[c.provider][f] = true
		}
	}
	for prov, flags := range want {
		for _, f := range flags {
			if notArgv[f] {
				continue
			}
			if !got[prov][f] {
				t.Errorf("%s: buildArgs can emit %s but the derived doctor contract does not include it — a fixture branch is off", prov, f)
			}
		}
	}
}

// cog-review #635 round 4: `endpoint` is overloaded as a binary PATH for
// codex/claude-code/pi (provider_codex_test.go:156 uses
// ProviderConfig{Endpoint: "/opt/codex/bin/codex"}). The HTTP probe must skip
// those by TYPE, not by empty-endpoint, or it FAILs on a valid config.
func TestDoctorProviderEndpoints_SkipsCLIProviderWithBinaryPathEndpoint(t *testing.T) {
	root := t.TempDir()
	writeProvidersYAMLRaw(t, root, `providers:
  codex:
    type: codex
    endpoint: /opt/codex/bin/codex
    enabled: true
  claude-code:
    type: claude-code
    endpoint: /usr/local/bin/claude
`)
	g := &DoctorGroup{}
	doctorProviderEndpoints(g, root, DoctorOptions{})
	for _, c := range g.Checks {
		if strings.HasPrefix(c.Name, "providers.yaml endpoint: codex") || strings.HasPrefix(c.Name, "providers.yaml endpoint: claude-code") {
			t.Fatalf("CLI provider with binary-path endpoint must not be HTTP-probed; got %s=%s %q", c.Name, c.Status, c.Detail)
		}
	}
}

// #638: the probe must send Authorization: Bearer $api_key_env exactly as
// OpenAICompatProvider does, or auth-gated endpoints report a false 401.
func TestDoctorProviderEndpoints_SendsAPIKeyEnv(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if gotAuth != "Bearer sekrit" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"m1"}]}`))
	}))
	defer srv.Close()
	t.Setenv("DOCTOR_TEST_LMS_KEY", "sekrit")
	root := t.TempDir()
	// Base declares the provider; the LOCAL overlay carries api_key_env — the
	// shape darkstar actually uses. The overlay merge must carry the field.
	writeProvidersYAMLRaw(t, root, "providers:\n  lms:\n    type: openai\n    endpoint: "+srv.URL+"\n    model: m1\n")
	if err := os.WriteFile(filepath.Join(root, ".cog", "config", "providers.local.yaml"), []byte("providers:\n  lms:\n    api_key_env: DOCTOR_TEST_LMS_KEY\n"), 0644); err != nil {
		t.Fatal(err)
	}
	g := &DoctorGroup{}
	doctorProviderEndpoints(g, root, DoctorOptions{})
	c := findCheckInGroup(t, g, "providers.yaml endpoint: lms")
	if c.Status != StatusOK {
		t.Fatalf("expected OK with api_key_env sent; got %s %q (server saw Authorization=%q)", c.Status, c.Detail, gotAuth)
	}
	if !strings.Contains(c.Detail, "$DOCTOR_TEST_LMS_KEY") || strings.Contains(c.Detail, "sekrit") {
		t.Fatalf("detail must name the env var and never the value: %q", c.Detail)
	}
}

// cog-review #635 round 5: the anthropic type authenticates with x-api-key,
// not Authorization: Bearer. Doctor must not know that — the provider does.
// This passes only because doctorProviderEndpoints delegates to the
// provider's own Ping() (provider_anthropic.go setAuthHeaders).
func TestDoctorProviderEndpoints_AnthropicUsesXAPIKeyViaProviderPing(t *testing.T) {
	var sawXAPIKey, sawBearer bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawXAPIKey = r.Header.Get("x-api-key") == "sk-test"
		sawBearer = r.Header.Get("Authorization") != ""
		if !sawXAPIKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"claude-x"}]}`))
	}))
	defer srv.Close()
	t.Setenv("DOCTOR_TEST_ANTHROPIC_KEY", "sk-test")
	root := t.TempDir()
	writeProvidersYAMLRaw(t, root, "providers:\n  anthropic:\n    type: anthropic\n    endpoint: "+srv.URL+"\n    api_key_env: DOCTOR_TEST_ANTHROPIC_KEY\n    enabled: true\n")
	g := &DoctorGroup{}
	doctorProviderEndpoints(g, root, DoctorOptions{})
	c := findCheckInGroup(t, g, "providers.yaml endpoint: anthropic")
	if c.Status != StatusOK {
		t.Fatalf("anthropic probe must succeed via x-api-key; got %s %q (x-api-key seen=%v, bearer seen=%v)", c.Status, c.Detail, sawXAPIKey, sawBearer)
	}
	if sawBearer {
		t.Fatalf("doctor sent Authorization: Bearer to an anthropic endpoint — it is re-implementing auth instead of delegating to the provider")
	}
}

// cog-review #635 rounds 6+7: a reachable endpoint that answers non-2xx must
// FAIL for EVERY HTTP-backed provider type doctor can construct. Round 6 fixed
// openai/ollama; round 7 caught anthropic/claude-oauth still passing on
// 403/500/502 (they only checked 401). Table-driven so a new provider type
// that forgets to check status fails here, not in a review round.
func TestDoctorProviderEndpoints_ConnectedButNon2xxIsFail(t *testing.T) {
	types := []struct {
		typ   string
		extra string // yaml lines needed for the provider to construct
		env   map[string]string
	}{
		{typ: "openai"},
		{typ: "ollama"},
		{typ: "anthropic", extra: "    api_key_env: DOCTOR_T_ANTHROPIC\n", env: map[string]string{"DOCTOR_T_ANTHROPIC": "sk-x"}},
	}
	for _, tc := range types {
		for _, code := range []int{403, 404, 500, 502} {
			t.Run(fmt.Sprintf("%s_%d", tc.typ, code), func(t *testing.T) {
				for k, v := range tc.env {
					t.Setenv(k, v)
				}
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(code)
				}))
				defer srv.Close()
				root := t.TempDir()
				writeProvidersYAMLRaw(t, root, "providers:\n  p:\n    type: "+tc.typ+"\n    endpoint: "+srv.URL+"\n    enabled: true\n"+tc.extra)
				g := &DoctorGroup{}
				doctorProviderEndpoints(g, root, DoctorOptions{})
				c := findCheckInGroup(t, g, "providers.yaml endpoint: p")
				if c.Status != StatusFail {
					t.Fatalf("%s HTTP %d must be FAIL; got %s %q", tc.typ, code, c.Status, c.Detail)
				}
				if !strings.Contains(c.Detail, fmt.Sprintf("HTTP %d", code)) {
					t.Fatalf("detail must name the status: %q", c.Detail)
				}
			})
		}
	}
}
