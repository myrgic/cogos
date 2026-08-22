// cli_doctor_secrets.go — "credential hygiene" check group for `cogos doctor`.
//
// # Why this group exists
//
// Every other doctor group closes a failure that is silent. This one closes a
// failure that is silent AND irreversible: a credential committed, synced, or
// shipped in plaintext cannot be un-leaked. Rotation is the only remedy, and
// rotation only happens if somebody notices. Nobody notices, because a config
// file with an inline API key works perfectly — it is indistinguishable, at
// runtime, from one that resolves the same value out of a vault.
//
// The node already has the machinery to do this correctly. ADR-node-secret-provider
// specifies a NodeSecretProvider contract (`ensure_secret` / `get_secret_into`,
// generate-into-sink, values never returned) and a `cog:secret/<domain>/<app>`
// projection that desugars to a SecretRef. Two conformant resolvers ship: the
// Python bw_bridge and the Go envspec.BitwardenResolver. What was missing is the
// part that notices when something bypasses all of it. A contract nothing audits
// is a convention, not a control.
//
// # The core discrimination
//
// For each credential-shaped key this group answers one question: is the VALUE a
// reference, or is it the secret itself?
//
//	cog:secret/discord/cog     -> REFERENCE. Resolution is deferred, gated, revocable.
//	${DISCORD_TOKEN}           -> REFERENCE (indirection through env).
//	MTE4Nzc2...                -> MATERIAL. The credential is at rest in this file.
//
// Only the third case is a finding. This is deliberately a shape test, not a
// value test — see "What this group refuses to do" below.
//
// # What this group refuses to do
//
// It never reads, prints, logs, stores, or transmits a secret value, and it never
// contacts a vault. A doctor that dumps credentials to stdout to prove they exist
// has become the leak it was written to detect. Every finding is reported as
// (file, line, key-name, classification, length) — enough to act on, insufficient
// to abuse. Detail strings are built only from key names and lengths; the value is
// used solely to compute a boolean and a length, then dropped.
//
// It also does not decrypt, does not walk into .git objects, and does not follow
// symlinks out of the scanned roots.
//
// # Status semantics (per the package output contract)
//
//	FAIL    — a credential-shaped key holds inline material in a file that is
//	          TRACKED BY GIT or not ignored. This is the irreversible case: it is
//	          committed, or one `git add` away from being committed.
//	WARN    — inline material in a file that is gitignored or outside a repo. Real,
//	          but contained: it leaks on a disk image or a backup, not on a push.
//	UNKNOWN — a candidate file could not be read or parsed. Never OK: an unreadable
//	          config has taught us nothing about whether it holds a secret.
//	OK      — files were scanned and every credential-shaped key resolved to a
//	          reference (or there were none).
//
// The OK case reports HOW MANY files were scanned. A silent pass is
// indistinguishable from a scan that never ran, and this codebase treats
// proof-by-silence as a defect.
package engine

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// credentialKeyPattern matches key names that conventionally hold credentials.
// Matching is on the KEY, never the value — a heuristic over value entropy would
// have to look at secrets to work, which this group will not do.
//
// The trailing `key` alternation is load-bearing and was added after the first
// test run: the real finding that motivated this group, `API_SERVER_KEY`, does
// NOT match `api[_-]?key` (that wants "api_key" adjacently). A pattern that
// misses the credential that prompted the check is worse than no pattern, since
// it reports OK.
var credentialKeyPattern = regexp.MustCompile(
	`(?i)(api[_-]?key|auth[_-]?token|access[_-]?token|refresh[_-]?token|secret|password|passwd|credential|private[_-]?key|client[_-]?secret|bearer|[_-]key$|^key$|[_-]token$|^token$)`)

// referencePrefixes mark a value as an indirection rather than material.
// A value carrying one of these is compliant with ADR-node-secret-provider.
var referencePrefixes = []string{
	"cog:secret/", // canonical projection
	"cog://",      // pinned cross-node form
	"${",          // shell / env interpolation
	"$(",          // command substitution
	"env:",        // varlock-style provider
	"file:",       // docker secret / tmpfs
	"keychain:",   // OS-trusted store
	"op://",       // 1Password
	"vault:",      // HashiCorp
	"!",           // YAML tag (e.g. !secret)
}

// placeholderValues are non-secrets that merely look credential-shaped.
var placeholderValues = map[string]bool{
	"": true, "null": true, "~": true, "none": true, "false": true, "true": true,
	"changeme": true, "your-api-key-here": true, "xxx": true, "todo": true,
	"placeholder": true, "unset": true, "local": true, "cogos": true,
}

// minMaterialLength is the shortest value treated as plausible credential
// material. Below this, a value is far more likely to be a mode string
// ("local", "auto") than a key. Chosen to sit under the shortest real token
// shape we care about while excluding common enum values.
const minMaterialLength = 16

// secretFinding is one credential-shaped key with an inline value.
type secretFinding struct {
	file    string
	line    int
	key     string
	length  int
	tracked bool // known to git (committed or staged)
	ignored bool // matched by .gitignore
}

// envIndirectionKeySuffixes mark keys whose value is the NAME of an
// environment variable rather than a credential. `access_token_env: FOO_TOKEN`
// names where the secret lives; it is not the secret. Treating these as
// material produces confident false positives, which is how a security check
// trains people to ignore it.
var envIndirectionKeySuffixes = []string{"_env", "_envvar", "_env_var", "_var", "_name", "_file", "_path", "_ref", "_id"}

// classifyValue reports whether v is credential MATERIAL (as opposed to a
// reference or a placeholder). The value is inspected but never retained.
func classifyValue(v string) bool {
	t := strings.TrimSpace(v)
	t = strings.Trim(t, `"'`)
	if placeholderValues[strings.ToLower(t)] {
		return false
	}
	for _, p := range referencePrefixes {
		if strings.HasPrefix(t, p) {
			return false
		}
	}
	if len(t) < minMaterialLength {
		return false
	}
	// A path is a locator, not a secret.
	if strings.HasPrefix(t, "/") || strings.HasPrefix(t, "~/") ||
		strings.HasPrefix(t, "http://") || strings.HasPrefix(t, "https://") {
		return false
	}
	// A bare SHOUTY_SNAKE_CASE token is an env-var NAME, not material.
	if t == strings.ToUpper(t) && !strings.ContainsAny(t, " /+=") &&
		strings.Contains(t, "_") && !strings.ContainsAny(t, "0123456789") {
		return false
	}
	return true
}

// isEnvIndirectionKey reports whether the key names an env var rather than
// holding a credential.
func isEnvIndirectionKey(k string) bool {
	lk := strings.ToLower(k)
	for _, s := range envIndirectionKeySuffixes {
		if strings.HasSuffix(lk, s) {
			return true
		}
	}
	return false
}

// kvPairPattern matches KEY: value / KEY=value pairs ANYWHERE in a line, not
// just as the whole line.
//
// This is deliberately not anchored. The first version anchored to the full
// line (`^\s*KEY\s*[:=]\s*(.+)$`) and silently missed every credential in a
// minified or single-line JSON file — the exact shape `.mcp.json` often ships
// in. A scanner that reports OK on a one-line JSON file full of API keys is
// the failure mode this whole group exists to prevent, so the unanchored form
// is load-bearing rather than merely convenient.
//
// The bare-value alternation MUST exclude `{` and `"`. Without that exclusion
// the bare branch matches `{"a":{"env":{"API_KEY":"…` as the *value* of the
// outer key and consumes the nested credential before it can ever be seen —
// a second silent-miss found only because the single-line-JSON regression
// test above was written.
var kvPairPattern = regexp.MustCompile(
	`"?([A-Za-z_][A-Za-z0-9_.\-]*)"?\s*[:=]\s*(?:"([^"]*)"|'([^']*)'|([^\s,;{}"'\[\]]+))`)

// scanFileForSecrets reads one file line-by-line and returns findings.
// Returns an error only when the file cannot be read — the caller reports
// that as UNKNOWN rather than assuming the file was clean.
func scanFileForSecrets(path string) ([]secretFinding, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []secretFinding
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
			continue
		}
		for _, m := range kvPairPattern.FindAllStringSubmatch(line, -1) {
			key := m[1]
			if !credentialKeyPattern.MatchString(key) || isEnvIndirectionKey(key) {
				continue
			}
			// Whichever alternation captured: quoted, single-quoted, or bare.
			val := m[2]
			if val == "" {
				val = m[3]
			}
			if val == "" {
				val = m[4]
			}
			if !classifyValue(val) {
				continue
			}
			out = append(out, secretFinding{
				file:   path,
				line:   lineNo,
				key:    key,
				length: len(strings.Trim(strings.TrimSpace(val), `"'`)),
			})
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// gitTracked reports whether path is tracked by the git repo containing it.
func gitTracked(path string) bool {
	dir := filepath.Dir(path)
	cmd := exec.Command("git", "-C", dir, "ls-files", "--error-unmatch", filepath.Base(path))
	return cmd.Run() == nil
}

// gitIgnored reports whether path is matched by a .gitignore rule.
func gitIgnored(path string) bool {
	dir := filepath.Dir(path)
	cmd := exec.Command("git", "-C", dir, "check-ignore", "-q", filepath.Base(path))
	return cmd.Run() == nil
}

// doctorCredentialHygiene is check group 6: inline credential material.
func doctorCredentialHygiene(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("credential hygiene")
	home, _ := os.UserHomeDir()

	// If the workspace itself does not exist, this group has nothing to say.
	// Scanning $HOME anyway would report the operator's real machine state
	// under a report nominally about a nonexistent workspace — and, worse,
	// would turn "this workspace is absent" into a FAIL. Absence of a
	// workspace is UNKNOWN, never a proven defect. (Enforced by
	// TestDoctorAgainstNonexistentWorkspaceNeverReportsOK.)
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		g.add("credential scan", StatusUnknown,
			"workspace root does not exist — no credential scan performed")
		return
	}

	// Candidate files: the same config surfaces the config-coherence group
	// knows about, plus env-style files, plus anything the caller named.
	var candidates []string
	if home != "" {
		candidates = append(candidates,
			filepath.Join(home, ".claude", "settings.json"),
			filepath.Join(home, ".claude", "settings.local.json"),
		)
		candidates = append(candidates, globQuiet(filepath.Join(home, ".claude", "*.mcp.json"))...)
		candidates = append(candidates, globQuiet(filepath.Join(home, ".hermes", "profiles", "*", "config.yaml"))...)
	}
	candidates = append(candidates,
		filepath.Join(root, ".mcp.json"),
		filepath.Join(root, ".envspec"),
		filepath.Join(root, ".env"),
	)
	candidates = append(candidates, globQuiet(filepath.Join(root, "*.env"))...)
	candidates = append(candidates, globQuiet(filepath.Join(root, ".cog", "config", "*.env"))...)
	candidates = append(candidates, globQuiet(filepath.Join(root, ".cog", "config", "**", "*.yaml"))...)
	candidates = append(candidates, opts.ExtraConfigFiles...)

	files := dedupeExisting(candidates)
	if len(files) == 0 {
		g.add("credential scan", StatusUnknown,
			"no candidate config/env files found at the known locations — nothing was scanned")
		return
	}

	var findings []secretFinding
	unreadable := 0
	for _, f := range files {
		fs, err := scanFileForSecrets(f)
		if err != nil {
			unreadable++
			g.add("scan "+f, StatusUnknown, fmt.Sprintf("could not read: %v", err))
			continue
		}
		for _, fd := range fs {
			fd.tracked = gitTracked(fd.file)
			fd.ignored = gitIgnored(fd.file)
			findings = append(findings, fd)
		}
	}

	// Positive report: say how many files were actually scanned. A pass that
	// cannot distinguish itself from "never ran" is not evidence.
	scanned := len(files) - unreadable
	g.add("credential scan coverage", StatusOK,
		fmt.Sprintf("%d file(s) scanned for inline credential material", scanned))

	if len(findings) == 0 {
		g.add("inline credential material", StatusOK,
			"no credential-shaped key holds an inline value; all resolve to references")
		return
	}

	// Split by reversibility AND by scope.
	//
	// Scope matters for the exit code: doctor is pointed at a workspace, and its
	// FAIL verdict should describe THAT workspace. A credential in a machine-wide
	// config under $HOME is a real finding and is always reported — but it is not
	// evidence that the workspace under test is broken, and letting it drive the
	// exit code makes `cogos doctor` on a pristine checkout fail because of an
	// unrelated file elsewhere on the machine. (This surfaced immediately:
	// TestDoctorLintExitCodesEndToEnd began failing against a clean temp
	// workspace because the scan reached the operator's real ~/.hermes config.)
	inWorkspace := func(p string) bool {
		rel, err := filepath.Rel(root, p)
		return err == nil && !strings.HasPrefix(rel, "..")
	}

	var exposed, contained, external []secretFinding
	for _, f := range findings {
		switch {
		case !inWorkspace(f.file):
			external = append(external, f)
		case f.tracked || !f.ignored:
			exposed = append(exposed, f)
		default:
			contained = append(contained, f)
		}
	}

	if len(exposed) > 0 {
		var lines []string
		for _, f := range exposed {
			state := "not ignored"
			if f.tracked {
				state = "TRACKED BY GIT"
			}
			lines = append(lines, fmt.Sprintf("%s:%d  %s  (%d chars, %s)",
				f.file, f.line, f.key, f.length, state))
		}
		g.add("inline credential material (exposed)", StatusFail,
			fmt.Sprintf("%d credential-shaped key(s) hold inline values in files that are committed or committable\n%s\nmigrate to cog:secret/<domain>/<app> — see ADR-node-secret-provider",
				len(exposed), strings.Join(lines, "\n")))
	}

	if len(external) > 0 {
		var lines []string
		for _, f := range external {
			lines = append(lines, fmt.Sprintf("%s:%d  %s  (%d chars)",
				f.file, f.line, f.key, f.length))
		}
		g.add("inline credential material (machine-wide config)", StatusWarn,
			fmt.Sprintf("%d credential-shaped key(s) hold inline values in configs OUTSIDE this workspace\n%s\nreal findings, but not scoped to the workspace under test — they do not drive the exit code",
				len(external), strings.Join(lines, "\n")))
	}

	if len(contained) > 0 {
		var lines []string
		for _, f := range contained {
			lines = append(lines, fmt.Sprintf("%s:%d  %s  (%d chars, gitignored)",
				f.file, f.line, f.key, f.length))
		}
		g.add("inline credential material (contained)", StatusWarn,
			fmt.Sprintf("%d credential-shaped key(s) hold inline values in gitignored files\n%s\nnot committable, but still at rest in plaintext on disk",
				len(contained), strings.Join(lines, "\n")))
	}
}
