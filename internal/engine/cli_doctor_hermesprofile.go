// cli_doctor_hermesprofile.go: Hermes profile parsing and ${env:VAR}
// resolution shared by the grant-provenance, inherited-env, model-probe and
// placement checks.
//
// Resolution mirrors Hermes (agent/secret_sources/command.py): a value like
// "${env:COGOS_HERMES_GRANT}" is filled from the process environment, else
// from the output of the profile's secrets.command helper (run via /bin/sh -c
// with an empty HERMES_SECRET_KEY, 3s timeout, KEY=VALUE lines). With
// secrets.command.override_existing: true the helper wins over the process
// environment; the default (false) is the reverse. Resolved values live in
// memory only and are never printed: callers report grantFingerprint().
package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	hermesHelperTimeout = 3 * time.Second
	hermesHelperMaxOut  = 1 << 20 // Hermes caps helper output at 1 MiB
)

// hermesHelperUnderTest is set from a _test.go init so production code does
// not import the testing package; see cli_doctor_testguard_test.go.
var hermesHelperUnderTest bool

var hermesEnvRefRE = regexp.MustCompile(`\$\{(?:env:)?([A-Za-z_][A-Za-z0-9_]*)\}`)

// hermesProfile is the subset of a Hermes profile config.yaml doctor reads.
type hermesProfile struct {
	Name             string
	Path             string
	Providers        map[string]hermesProvider
	SecretsEnabled   bool
	SecretsCommand   string
	OverrideExisting bool
	HelperTimeout    time.Duration // secrets.command.helper_timeout_seconds; 0 = default
	ModelDefault     string
	ModelProvider    string // model.provider ("" when model is a bare string)
	ModelBaseURL     string // model.base_url
}

type hermesProvider struct {
	BaseURL string
	APIKey  string // raw config text, e.g. "${env:COGOS_HERMES_GRANT}"
}

func (p hermesProfile) label() string { return "~/.hermes/profiles/" + p.Name + "/config.yaml" }

// loadHermesProfiles parses every ~/.hermes/profiles/*/config.yaml under home.
func loadHermesProfiles(home string) []hermesProfile {
	if home == "" {
		return nil
	}
	var out []hermesProfile
	for _, path := range globQuiet(filepath.Join(home, ".hermes", "profiles", "*", "config.yaml")) {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var doc struct {
			Providers map[string]struct {
				BaseURL string `yaml:"base_url"`
				APIKey  string `yaml:"api_key"`
			} `yaml:"providers"`
			Secrets struct {
				Command struct {
					Enabled          bool    `yaml:"enabled"`
					Command          string  `yaml:"command"`
					OverrideExisting bool    `yaml:"override_existing"`
					HelperTimeoutSec float64 `yaml:"helper_timeout_seconds"`
				} `yaml:"command"`
			} `yaml:"secrets"`
			Model yaml.Node `yaml:"model"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			continue
		}
		p := hermesProfile{
			Name:             filepath.Base(filepath.Dir(path)),
			Path:             path,
			Providers:        map[string]hermesProvider{},
			SecretsEnabled:   doc.Secrets.Command.Enabled,
			SecretsCommand:   doc.Secrets.Command.Command,
			OverrideExisting: doc.Secrets.Command.OverrideExisting,
		}
		if t := doc.Secrets.Command.HelperTimeoutSec; t > 0 {
			p.HelperTimeout = time.Duration(t * float64(time.Second))
		}
		for n, pr := range doc.Providers {
			p.Providers[n] = hermesProvider{BaseURL: pr.BaseURL, APIKey: pr.APIKey}
		}
		switch doc.Model.Kind {
		case yaml.ScalarNode:
			p.ModelDefault = strings.TrimSpace(doc.Model.Value)
		case yaml.MappingNode:
			for i := 0; i+1 < len(doc.Model.Content); i += 2 {
				v := strings.TrimSpace(doc.Model.Content[i+1].Value)
				switch doc.Model.Content[i].Value {
				case "default":
					p.ModelDefault = v
				case "provider":
					p.ModelProvider = v
				case "base_url":
					if v != "null" && v != "None" {
						p.ModelBaseURL = v
					}
				}
			}
		}
		out = append(out, p)
	}
	return out
}

// envRefs lists the distinct env var names referenced by providers.*.api_key,
// as (provider, var) pairs sorted for stable output.
func (p hermesProfile) envRefs() [][2]string {
	var out [][2]string
	for name, pr := range p.Providers {
		for _, m := range hermesEnvRefRE.FindAllStringSubmatch(pr.APIKey, -1) {
			out = append(out, [2]string{name, m[1]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

// hermesProfileCache memoizes secrets.command helper runs for one doctor run,
// so a vault-backed helper is spawned once per profile, not once per check.
type hermesProfileCache struct {
	mu     sync.Mutex
	helper map[string]helperResult
	// run is the helper runner seam; nil = real /bin/sh -c execution.
	run func(command string, timeout time.Duration) (map[string]string, error)
	// getenv is the process-env seam; nil = os.Getenv.
	getenv func(string) string
	// skipHelper makes helperValues report errHelperSkipped instead of
	// running the helper (--skip-network).
	skipHelper bool
}

type helperResult struct {
	vals map[string]string
	err  error
}

func newHermesProfileCache() *hermesProfileCache {
	return &hermesProfileCache{helper: map[string]helperResult{}}
}

func (o DoctorOptions) hermesCache() *hermesProfileCache {
	c := o.hermes
	if c == nil {
		c = newHermesProfileCache()
	}
	if o.SkipNetwork {
		c.skipHelper = true
	}
	return c
}

var errHelperSkipped = fmt.Errorf("secrets.command helper not run under --skip-network (it may contact a vault)")

// redactKeyRef renders a raw api_key config value as its ${env:VAR}
// references only; literal text (which may be key material, alone or mixed
// with a reference) is never echoed.
func redactKeyRef(raw string) string {
	refs := hermesEnvRefRE.FindAllStringSubmatch(raw, -1)
	if len(refs) == 0 {
		return "<literal api_key, not shown>"
	}
	var parts []string
	for _, m := range refs {
		parts = append(parts, "${env:"+m[1]+"}")
	}
	out := strings.Join(parts, "+")
	if strings.TrimSpace(hermesEnvRefRE.ReplaceAllString(raw, "")) != "" {
		out += " (plus literal text, not shown)"
	}
	return out
}

func (c *hermesProfileCache) env(k string) string {
	if c.getenv != nil {
		return c.getenv(k)
	}
	return os.Getenv(k)
}

func (c *hermesProfileCache) helperValues(p hermesProfile) (map[string]string, error) {
	if !p.SecretsEnabled || strings.TrimSpace(p.SecretsCommand) == "" {
		return nil, nil
	}
	if c.skipHelper {
		return nil, errHelperSkipped
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.helper[p.Path]; ok {
		return r.vals, r.err
	}
	run := c.run
	if run == nil {
		run = runHermesSecretsHelper
		if hermesHelperUnderTest && !underTempDir(p.Path) {
			// A test that reaches here without a runner seam and without a
			// temp HOME would execute the operator's real ~/.hermes
			// secrets.command (vault) helper.
			err := fmt.Errorf("secrets.command helpers outside a temp dir are not run under go test; set hermesProfileCache.run or a temp HOME")
			c.helper[p.Path] = helperResult{nil, err}
			return nil, err
		}
	}
	vals, err := run(p.SecretsCommand, p.HelperTimeout)
	c.helper[p.Path] = helperResult{vals, err}
	return vals, err
}

// profileDotenv returns the profile's own <profile dir>/.env. Hermes
// (hermes_cli/env_loader.py) loads it with override=True before any secret
// source runs, so its values replace the inherited process environment.
func (p hermesProfile) profileDotenv() map[string]string {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(p.Path), ".env"))
	if err != nil {
		return nil
	}
	return parseDotenvMap(string(data))
}

// resolve expands every ${env:VAR} in raw the way Hermes would for profile p.
// Precedence (Hermes load_hermes_dotenv, then agent/secret_sources/command.py):
// the helper wins only with override_existing; otherwise the profile .env, then
// the process env, then the helper. how names the source of the (last)
// substituted value; unresolved lists vars nothing provides. The helper error
// (if any) is returned in note so the caller can say why.
func (c *hermesProfileCache) resolve(p hermesProfile, raw string) (value, how string, unresolved []string, note string) {
	value, how, unresolved, note, _ = c.resolveX(p, raw, false)
	return
}

// resolveX is resolve plus two controls. ignoreProcEnv drops the doctor
// process's own environment from the lookup (a service such as Hermes under
// systemd/launchd does not inherit the operator's shell). procShadowed is true
// when a process-env value was used although the helper offered a different
// one, i.e. the verdict depends on the caller's shell.
func (c *hermesProfileCache) resolveX(p hermesProfile, raw string, ignoreProcEnv bool) (value, how string, unresolved []string, note string, procShadowed bool) {
	if !hermesEnvRefRE.MatchString(raw) {
		return raw, "literal", nil, "", false
	}
	helperVals, herr := c.helperValues(p)
	if herr != nil {
		note = "secrets.command helper failed: " + herr.Error()
		if herr == errHelperSkipped {
			note = herr.Error()
		}
	}
	dotenv := p.profileDotenv()
	value = hermesEnvRefRE.ReplaceAllStringFunc(raw, func(ref string) string {
		name := hermesEnvRefRE.FindStringSubmatch(ref)[1]
		fromEnv := ""
		if !ignoreProcEnv {
			fromEnv = c.env(name)
		}
		fromDotenv := strings.TrimSpace(dotenv[name])
		fromHelper := strings.TrimSpace(helperVals[name])
		switch {
		case p.OverrideExisting && fromHelper != "":
			how = "secrets.command helper (override_existing)"
			return fromHelper
		case fromDotenv != "":
			how = ".env (" + filepath.Join("~/.hermes/profiles", p.Name, ".env") + ")"
			return fromDotenv
		case fromEnv != "":
			how = "process env"
			if fromHelper != "" && fromHelper != fromEnv {
				procShadowed = true
			}
			return fromEnv
		case fromHelper != "":
			how = "secrets.command helper"
			return fromHelper
		}
		unresolved = append(unresolved, name)
		return ref
	})
	return value, how, unresolved, note, procShadowed
}

// runHermesSecretsHelper runs a Hermes secrets.command helper exactly as
// Hermes does at startup and returns its KEY=VALUE map. Output is held in
// memory only; stderr is discarded. POSIX only.
func runHermesSecretsHelper(command string, timeout time.Duration) (map[string]string, error) {
	if runtime.GOOS == "windows" {
		return nil, fmt.Errorf("secrets.command needs /bin/sh (POSIX only)")
	}
	if timeout <= 0 {
		timeout = hermesHelperTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Env = append(os.Environ(), "HERMES_SECRET_KEY=")
	bindHelperLifetime(cmd)
	cmd.WaitDelay = time.Second // do not wait on pipes a stray grandchild still holds
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("cannot create pipe")
	}
	cmd.Stdout = pw
	if err := cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		return nil, fmt.Errorf("exit error (output withheld)")
	}
	pw.Close()
	var out bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&out, io.LimitReader(pr, hermesHelperMaxOut))
		_, _ = io.Copy(io.Discard, pr) // keep draining so the helper never blocks on a full pipe
		close(done)
	}()
	werr := cmd.Wait()
	pr.Close() // unblocks the reader even if a grandchild still holds the write end
	<-done
	if werr != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("timed out after %s", timeout)
		}
		return nil, fmt.Errorf("exit error (output withheld)")
	}
	return parseDotenvMap(out.String()), nil
}

var dotenvLineRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)=(.*)$`)

// parseDotenvMap parses KEY=VALUE lines (comments and non-matching lines
// skipped, one layer of matching quotes stripped), like Hermes' helper parser.
func parseDotenvMap(s string) map[string]string {
	out := map[string]string{}
	for _, raw := range strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if m := dotenvLineRE.FindStringSubmatch(line); m != nil {
			v := strings.TrimSpace(m[2])
			if len(v) >= 2 && ((v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'')) {
				v = v[1 : len(v)-1]
			}
			out[m[1]] = v
		}
	}
	return out
}

// underTempDir reports whether path lies under the OS temp dir (symlinks
// resolved), i.e. is a test fixture rather than the operator's real profile.
func underTempDir(path string) bool {
	real := func(p string) string {
		if r, err := filepath.EvalSymlinks(p); err == nil {
			return r
		}
		return p
	}
	tmp := real(os.TempDir())
	rel, err := filepath.Rel(tmp, real(path))
	return err == nil && !strings.HasPrefix(rel, "..")
}
