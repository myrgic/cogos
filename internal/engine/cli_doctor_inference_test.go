// cli_doctor_inference_test.go — tests for the "inference surface" doctor
// group (myrgic/cogos#631). Every test that has a FAIL path is paired with
// a fixture that must FAIL (negative control), not just a happy-path OK
// fixture, per the task's own testing rule.
package engine

import (
	"encoding/json"
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

	pcfg, found, err := loadProvidersYAMLDoctor(root)
	if !found || err != nil {
		t.Fatalf("found=%v err=%v", found, err)
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
		provider:  "fake",
		bin:       "fake-cli-ok",
		subcmd:    []string{"exec"},
		longFlags: []string{"-m", "--config", "--sandbox", "--full-auto", "--skip-git-repo-check", "--json"},
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
		provider:  "fake",
		bin:       "fake-cli-missing-flag",
		subcmd:    []string{"exec"},
		longFlags: []string{"-m", "--config", "--sandbox", "--full-auto", "--skip-git-repo-check", "--json"},
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
	if len(targets) != 1 {
		t.Fatalf("got %d targets; want 1 (stdio-thing has no url, must be skipped): %+v", len(targets), targets)
	}
	if targets[0].url != "http://127.0.0.1:6931/mcp" {
		t.Errorf("url = %q", targets[0].url)
	}
	if targets[0].headers["X-Cogos-Grant"] != "test-grant-value" {
		t.Errorf("headers = %v; want X-Cogos-Grant parsed from http_headers inline table", targets[0].headers)
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

