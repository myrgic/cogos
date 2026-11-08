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

const hermesHelperTimeout = 3 * time.Second

var hermesEnvRefRE = regexp.MustCompile(`\$\{(?:env:)?([A-Za-z_][A-Za-z0-9_]*)\}`)

// hermesProfile is the subset of a Hermes profile config.yaml doctor reads.
type hermesProfile struct {
	Name             string
	Path             string
	Providers        map[string]hermesProvider
	SecretsEnabled   bool
	SecretsCommand   string
	OverrideExisting bool
	ModelDefault     string
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
					Enabled          bool   `yaml:"enabled"`
					Command          string `yaml:"command"`
					OverrideExisting bool   `yaml:"override_existing"`
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
		for n, pr := range doc.Providers {
			p.Providers[n] = hermesProvider{BaseURL: pr.BaseURL, APIKey: pr.APIKey}
		}
		switch doc.Model.Kind {
		case yaml.ScalarNode:
			p.ModelDefault = strings.TrimSpace(doc.Model.Value)
		case yaml.MappingNode:
			for i := 0; i+1 < len(doc.Model.Content); i += 2 {
				if doc.Model.Content[i].Value == "default" {
					p.ModelDefault = strings.TrimSpace(doc.Model.Content[i+1].Value)
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
	run func(command string) (map[string]string, error)
	// getenv is the process-env seam; nil = os.Getenv.
	getenv func(string) string
}

type helperResult struct {
	vals map[string]string
	err  error
}

func newHermesProfileCache() *hermesProfileCache {
	return &hermesProfileCache{helper: map[string]helperResult{}}
}

func (o DoctorOptions) hermesCache() *hermesProfileCache {
	if o.hermes != nil {
		return o.hermes
	}
	return newHermesProfileCache()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.helper[p.Path]; ok {
		return r.vals, r.err
	}
	run := c.run
	if run == nil {
		run = runHermesSecretsHelper
	}
	vals, err := run(p.SecretsCommand)
	c.helper[p.Path] = helperResult{vals, err}
	return vals, err
}

// resolve expands every ${env:VAR} in raw the way Hermes would for profile p.
// how names the source of the (last) substituted value; unresolved lists vars
// that neither the process env nor the helper provides. The helper error (if
// any) is returned in note so the caller can say why.
func (c *hermesProfileCache) resolve(p hermesProfile, raw string) (value, how string, unresolved []string, note string) {
	if !hermesEnvRefRE.MatchString(raw) {
		return raw, "literal", nil, ""
	}
	helperVals, herr := c.helperValues(p)
	if herr != nil {
		note = "secrets.command helper failed: " + herr.Error()
	}
	value = hermesEnvRefRE.ReplaceAllStringFunc(raw, func(ref string) string {
		name := hermesEnvRefRE.FindStringSubmatch(ref)[1]
		fromEnv := c.env(name)
		fromHelper := strings.TrimSpace(helperVals[name])
		switch {
		case p.OverrideExisting && fromHelper != "":
			how = "secrets.command helper (override_existing)"
			return fromHelper
		case fromEnv != "":
			how = "process env"
			return fromEnv
		case fromHelper != "":
			how = "secrets.command helper"
			return fromHelper
		}
		unresolved = append(unresolved, name)
		return ref
	})
	return value, how, unresolved, note
}

// runHermesSecretsHelper runs a Hermes secrets.command helper exactly as
// Hermes does at startup and returns its KEY=VALUE map. Output is held in
// memory only; stderr is discarded. POSIX only.
func runHermesSecretsHelper(command string) (map[string]string, error) {
	if runtime.GOOS == "windows" {
		return nil, fmt.Errorf("secrets.command needs /bin/sh (POSIX only)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), hermesHelperTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Env = append(os.Environ(), "HERMES_SECRET_KEY=")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("timed out after %s", hermesHelperTimeout)
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
