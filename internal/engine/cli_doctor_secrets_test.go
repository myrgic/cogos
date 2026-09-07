package engine

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The canary. Every test in this file that writes a fake credential uses THIS
// value, so the "doctor never prints a secret" test can assert on one string.
const canarySecret = "sk-live-CANARY-a1b2c3d4e5f6a7b8c9d0e1f2"

// writeSecretFile writes content to dir/name and returns the full path.
// (The package already has writeFile(t, path, content) and
// findGroup(t, report, name) in cli_doctor_test.go — reused here rather than
// shadowed.)
func writeSecretFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	writeFile(t, p, content)
	return p
}

func statusOf(g *DoctorGroup, namePrefix string) DoctorStatus {
	for _, c := range g.Checks {
		if strings.HasPrefix(c.Name, namePrefix) {
			return c.Status
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// classifyValue — the reference-vs-material discrimination
// ---------------------------------------------------------------------------

func TestClassifyValue_ReferencesAreNotMaterial(t *testing.T) {
	refs := []string{
		"cog:secret/discord/cog",
		"cog://workspace@darkstar/secret/telegram/cog",
		"${DISCORD_TOKEN}",
		"$(cat /run/secrets/tok)",
		"env:OPENAI_API_KEY",
		"file:/run/secrets/token",
		"keychain:cogos-vaultwarden-master",
		"op://vault/item/field",
		"vault:secret/data/app",
		"!secret my_value_here_long",
		"/Users/slowbro/path/to/keyfile.pem",
		"https://example.com/callback?token=abc",
	}
	for _, v := range refs {
		if classifyValue(v) {
			t.Errorf("classifyValue(%q) = true (material); want false (reference)", v)
		}
	}
}

func TestClassifyValue_PlaceholdersAreNotMaterial(t *testing.T) {
	for _, v := range []string{"", "null", "~", "none", "changeme", "TODO", "cogos", "local", "short"} {
		if classifyValue(v) {
			t.Errorf("classifyValue(%q) = true; want false (placeholder)", v)
		}
	}
}

func TestClassifyValue_RealMaterialIsDetected(t *testing.T) {
	mats := []string{
		canarySecret,
		"14ad9f8e7c6b5a4938271605f4e3d2c1b0a99887",
		`"MTE4Nzc2NDU5NzI0MjE0NDU4Ng.GaBcDe.xxxxxxxxxxxxxxxxxxxxxxxxx"`,
	}
	for _, v := range mats {
		if !classifyValue(v) {
			t.Errorf("classifyValue(%q...) = false; want true (material)", v[:12])
		}
	}
}

// ---------------------------------------------------------------------------
// scanFileForSecrets — key matching and line reporting
// ---------------------------------------------------------------------------

func TestScanFileForSecrets_FindsInlineMaterial(t *testing.T) {
	dir := t.TempDir()
	p := writeSecretFile(t, dir, "config.yaml", strings.Join([]string{
		"# a comment: api_key: not-a-real-one",
		"model: local",
		"API_SERVER_KEY: " + canarySecret,
		"discord_token: cog:secret/discord/cog",
		"timeout: 30",
	}, "\n"))

	fs, err := scanFileForSecrets(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 1 {
		t.Fatalf("got %d findings, want 1: %+v", len(fs), fs)
	}
	if fs[0].key != "API_SERVER_KEY" {
		t.Errorf("key = %q, want API_SERVER_KEY", fs[0].key)
	}
	if fs[0].line != 3 {
		t.Errorf("line = %d, want 3", fs[0].line)
	}
	if fs[0].length != len(canarySecret) {
		t.Errorf("length = %d, want %d", fs[0].length, len(canarySecret))
	}
}

func TestScanFileForSecrets_IgnoresCommentsAndReferences(t *testing.T) {
	dir := t.TempDir()
	p := writeSecretFile(t, dir, ".envspec", strings.Join([]string{
		"# @env-spec bitwarden(id=\"uuid\")",
		"DISCORD_TOKEN=cog:secret/discord/cog",
		"// api_key = commented-out-" + canarySecret,
		"VAULTWARDEN_URL=http://localhost:8222",
	}, "\n"))
	fs, err := scanFileForSecrets(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 0 {
		t.Errorf("got %d findings, want 0: %+v", len(fs), fs)
	}
}

func TestScanFileForSecrets_UnreadableReturnsError(t *testing.T) {
	_, err := scanFileForSecrets(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("want error for unreadable file; got nil (would be reported as OK — the bug this guards)")
	}
}

// Regression: the first implementation anchored its key/value regex to the
// whole line and therefore reported a CLEAN result for minified single-line
// JSON — the shape .mcp.json frequently ships in. A scanner that passes a
// one-line file full of API keys is precisely the silent failure this group
// exists to close.
func TestScanFileForSecrets_SingleLineJSON(t *testing.T) {
	dir := t.TempDir()
	p := writeSecretFile(t, dir, "min.json",
		`{"mcpServers":{"a":{"env":{"API_KEY":"`+canarySecret+`","PORT":"8080"}}}}`)
	fs, err := scanFileForSecrets(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 1 {
		t.Fatalf("minified JSON: got %d findings, want 1 (line-anchored regex regression)", len(fs))
	}
	if fs[0].key != "API_KEY" {
		t.Errorf("key = %q, want API_KEY", fs[0].key)
	}
}

// Regression: API_SERVER_KEY — the real finding that motivated this group —
// does not match `api[_-]?key`, which requires "api" and "key" to be adjacent.
// A pattern that misses the credential that prompted the check reports OK and
// is worse than no check at all.
func TestCredentialKeyPattern_MatchesSuffixStyleNames(t *testing.T) {
	for _, k := range []string{"API_SERVER_KEY", "LMS_API_TOKEN", "record_key", "AUTH_TOKEN", "openai_api_key"} {
		if !credentialKeyPattern.MatchString(k) {
			t.Errorf("credentialKeyPattern does not match %q — it would be reported OK", k)
		}
	}
	for _, k := range []string{"model", "timeout", "port", "workspace", "monkey"} {
		if credentialKeyPattern.MatchString(k) {
			t.Errorf("credentialKeyPattern falsely matches %q", k)
		}
	}
}

// ---------------------------------------------------------------------------
// THE LOAD-BEARING TEST: doctor must never print a secret value
// ---------------------------------------------------------------------------

func TestCredentialHygiene_NeverPrintsSecretValue(t *testing.T) {
	dir := t.TempDir()
	// Isolate HOME: without this the scan reaches the real machine's
	// ~/.hermes and ~/.claude configs and the test asserts on the
	// operator's actual secrets instead of the planted fixtures.
	t.Setenv("HOME", filepath.Join(dir, "fakehome"))
	writeSecretFile(t, dir, ".env", "OPENAI_API_KEY="+canarySecret+"\n")
	writeSecretFile(t, dir, ".mcp.json", `{"env":{"AUTH_TOKEN":"`+canarySecret+`"}}`)

	report := &DoctorReport{Workspace: dir}
	doctorCredentialHygiene(report, dir, DoctorOptions{})

	g := findGroup(t, report, "credential hygiene")
	if g == nil {
		t.Fatal("credential hygiene group missing")
	}

	// Walk EVERY string the group produced.
	var all strings.Builder
	all.WriteString(g.Name)
	for _, c := range g.Checks {
		all.WriteString(c.Name)
		all.WriteString(string(c.Status))
		all.WriteString(c.Detail)
	}
	if strings.Contains(all.String(), canarySecret) {
		t.Fatal("SECRET VALUE LEAKED into doctor output — the group became the leak it detects")
	}
	// Also assert no long substring of it escaped.
	if strings.Contains(all.String(), canarySecret[:20]) {
		t.Fatal("partial secret value leaked into doctor output")
	}
	// But it must still have FOUND something — a scanner that reports nothing
	// trivially passes the no-leak test.
	if len(g.Checks) == 0 {
		t.Fatal("no checks reported; the no-leak assertion above would pass vacuously")
	}
	found := false
	for _, c := range g.Checks {
		if strings.Contains(c.Name, "inline credential material") {
			found = true
		}
	}
	if !found {
		t.Error("scanner did not report the planted credentials; no-leak result is vacuous")
	}
}

// ---------------------------------------------------------------------------
// Status semantics: FAIL for committable, WARN for gitignored
// ---------------------------------------------------------------------------

func TestCredentialHygiene_GitignoredIsWarnNotFail(t *testing.T) {
	dir := t.TempDir()
	// Isolate HOME: without this the scan reaches the real machine's
	// ~/.hermes and ~/.claude configs and the test asserts on the
	// operator's actual secrets instead of the planted fixtures.
	t.Setenv("HOME", filepath.Join(dir, "fakehome"))
	if err := exec.Command("git", "-C", dir, "init", "-q").Run(); err != nil {
		t.Skip("git unavailable:", err)
	}
	writeSecretFile(t, dir, ".gitignore", ".env\n")
	writeSecretFile(t, dir, ".env", "API_KEY="+canarySecret+"\n")

	report := &DoctorReport{Workspace: dir}
	doctorCredentialHygiene(report, dir, DoctorOptions{})
	g := findGroup(t, report, "credential hygiene")

	if s := statusOf(g, "inline credential material (contained)"); s != StatusWarn {
		t.Errorf("gitignored inline secret -> %q; want WARN", s)
	}
	if s := statusOf(g, "inline credential material (exposed)"); s == StatusFail {
		t.Error("gitignored secret reported as exposed/FAIL; containment not honored")
	}
}

func TestCredentialHygiene_CommittableIsFail(t *testing.T) {
	dir := t.TempDir()
	// Isolate HOME: without this the scan reaches the real machine's
	// ~/.hermes and ~/.claude configs and the test asserts on the
	// operator's actual secrets instead of the planted fixtures.
	t.Setenv("HOME", filepath.Join(dir, "fakehome"))
	if err := exec.Command("git", "-C", dir, "init", "-q").Run(); err != nil {
		t.Skip("git unavailable:", err)
	}
	// No .gitignore -> the file is committable.
	writeSecretFile(t, dir, ".mcp.json", `{"env":{"API_KEY":"`+canarySecret+`"}}`)

	report := &DoctorReport{Workspace: dir}
	doctorCredentialHygiene(report, dir, DoctorOptions{})
	g := findGroup(t, report, "credential hygiene")

	if s := statusOf(g, "inline credential material (exposed)"); s != StatusFail {
		t.Errorf("committable inline secret -> %q; want FAIL", s)
	}
}

func TestCredentialHygiene_CleanWorkspaceReportsCoverage(t *testing.T) {
	dir := t.TempDir()
	// Isolate HOME: without this the scan reaches the real machine's
	// ~/.hermes and ~/.claude configs and the test asserts on the
	// operator's actual secrets instead of the planted fixtures.
	t.Setenv("HOME", filepath.Join(dir, "fakehome"))
	writeSecretFile(t, dir, ".envspec", "DISCORD_TOKEN=cog:secret/discord/cog\n")

	report := &DoctorReport{Workspace: dir}
	doctorCredentialHygiene(report, dir, DoctorOptions{})
	g := findGroup(t, report, "credential hygiene")

	if s := statusOf(g, "inline credential material"); s != StatusOK {
		t.Errorf("clean workspace -> %q; want OK", s)
	}
	// Positive reporting: coverage must be stated, not implied by silence.
	cov := ""
	for _, c := range g.Checks {
		if strings.HasPrefix(c.Name, "credential scan coverage") {
			cov = c.Detail
		}
	}
	if !strings.Contains(cov, "file(s) scanned") {
		t.Errorf("coverage not reported (%q); a silent pass is indistinguishable from no scan", cov)
	}
}

func TestCredentialHygiene_NoCandidatesIsUnknownNotOK(t *testing.T) {
	dir := t.TempDir() // completely empty, and HOME-based candidates won't exist under it
	report := &DoctorReport{Workspace: dir}
	t.Setenv("HOME", dir)
	doctorCredentialHygiene(report, dir, DoctorOptions{})
	g := findGroup(t, report, "credential hygiene")
	if g == nil {
		t.Fatal("group missing")
	}
	for _, c := range g.Checks {
		if c.Status == StatusOK && strings.HasPrefix(c.Name, "credential scan") &&
			!strings.Contains(c.Detail, "0 file(s)") {
			continue
		}
	}
	// The contract: nothing scanned must never present as a clean bill of health.
	if len(g.Checks) == 1 && g.Checks[0].Status == StatusOK {
		t.Error("empty scan reported as OK; must be UNKNOWN (a check that could not run learned nothing)")
	}
}

// Regression: `access_token_env: HERMES_TOKEN` names WHERE a secret lives; it
// is not the secret. The first real run against the operator's machine flagged
// four of these as findings. Confident false positives are how a security check
// teaches people to ignore it.
func TestClassifyValue_EnvVarNamesAreNotMaterial(t *testing.T) {
	for _, v := range []string{"HERMES_ACCESS_TOKEN", "OPENAI_API_KEY", "MY_LONG_ENV_VAR_NAME"} {
		if classifyValue(v) {
			t.Errorf("classifyValue(%q) = true; an env-var NAME is not material", v)
		}
	}
	for _, k := range []string{"access_token_env", "api_key_file", "secret_path", "token_ref", "client_secret_name"} {
		if !isEnvIndirectionKey(k) {
			t.Errorf("isEnvIndirectionKey(%q) = false; want true (names indirection, not material)", k)
		}
	}
	for _, k := range []string{"API_SERVER_KEY", "api_key", "auth_token"} {
		if isEnvIndirectionKey(k) {
			t.Errorf("isEnvIndirectionKey(%q) = true; want false (this DOES hold material)", k)
		}
	}
}
