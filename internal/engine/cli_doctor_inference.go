// cli_doctor_inference.go — "inference surface" doctor group (myrgic/cogos#631).
//
// Split out from cli_doctor.go to keep that file from growing further (it
// was already 2294 lines before this group existed). Same package, same
// report/group/check plumbing (DoctorReport.addGroup, DoctorGroup.add),
// same DoctorOptions.SkipNetwork honor-network-skip convention as every
// other group in cli_doctor.go.
//
// Four mechanical checks, added as the sixth call in RunDoctor:
//
//	(a) providers.yaml endpoints    — every enabled provider's configured
//	    endpoint actually answers GET <endpoint>/v1/models (or
//	    options.health_path), and if the listing is OpenAI-shaped, the
//	    declared model id is actually among the ids served.
//	(c) provider argv vs installed CLI — each provider's buildArgs()
//	    contract (provider_codex.go, provider_pi.go,
//	    provider_claudecode.go) is checked against what the installed CLI's
//	    own `<bin> <subcmd> --help` actually advertises, so upstream CLI
//	    flag drift (the #627/#628 codex --full-auto removal) is caught
//	    mechanically instead of by a broken subprocess call at request time.
//	(e) alias targets in live catalog — every model id resolve.go's alias
//	    tables promise is actually present in the live GET /v1/models
//	    response.
//	(f) external clients -> kernel — every localhost MCP/API target an
//	    external client config (~/.claude.json, ~/.codex/config.toml,
//	    ~/.hermes/profiles/*/config.yaml) declares is (1) a live TCP port,
//	    not a dead one, and (2) actually authenticates against the kernel's
//	    grant gate with that client's own configured credential.
//
// Two checks from the original #631 issue are intentionally NOT here:
// ModelLister-based per-provider live enumeration and --probe-inference.
// Both depend on sibling PRs (#628, #630) not yet landed; see the PR body
// for #631 for the split rationale. #632 tracks them as a follow-up.
package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// doctorInferenceSurface runs the four inference-surface mechanical checks
// as the sixth (and final) group added by RunDoctor.
func doctorInferenceSurface(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("inference surface")

	doctorProviderEndpoints(g, root, opts)
	doctorProviderArgvVsCLI(g, opts)
	doctorAliasTargetsInCatalog(g, root, opts)
	doctorExternalClientsToKernel(g, root, opts)
}

// ---------------------------------------------------------------------------
// (a) providers.yaml endpoints
// ---------------------------------------------------------------------------

// providersYAMLShape mirrors just enough of ProviderConfig (provider.go) to
// parse providers.yaml/providers.local.yaml without importing router.go's
// full BuildRouter machinery (which additionally probes local backends,
// auto-registers claude-oauth, etc. — more than a read-only doctor check
// should trigger as a side effect).
type providersYAMLShape struct {
	Providers map[string]struct {
		Type     string                 `yaml:"type"`
		Endpoint string                 `yaml:"endpoint"`
		Model    string                 `yaml:"model"`
		Enabled  *bool                  `yaml:"enabled"`
		Options  map[string]interface{} `yaml:"options"`
	} `yaml:"providers"`
}

func (p providersYAMLShape) isEnabled(name string) bool {
	pc := p.Providers[name]
	if pc.Enabled == nil {
		return true
	}
	return *pc.Enabled
}

// loadProvidersYAMLDoctor parses .cog/config/providers.yaml, deep-merging
// providers.local.yaml on top when present. Per the task's scope note this
// is a SIMPLE merge — local provider keys fully override same-named base
// keys (no field-by-field merge) — deliberately simpler than
// mergeProvidersConfig in router.go, which this doctor check does not call
// because that merge is tied to the full ProviderConfig/router type and
// would pull in more of router.go's surface than a read-only check needs.
func loadProvidersYAMLDoctor(root string) (providersYAMLShape, bool, error) {
	basePath := filepath.Join(root, ".cog", "config", "providers.yaml")
	data, err := os.ReadFile(basePath)
	if err != nil {
		return providersYAMLShape{}, false, err
	}
	var base providersYAMLShape
	if err := yaml.Unmarshal(data, &base); err != nil {
		return providersYAMLShape{}, true, fmt.Errorf("parse providers.yaml: %w", err)
	}
	if base.Providers == nil {
		base.Providers = map[string]struct {
			Type     string                 `yaml:"type"`
			Endpoint string                 `yaml:"endpoint"`
			Model    string                 `yaml:"model"`
			Enabled  *bool                  `yaml:"enabled"`
			Options  map[string]interface{} `yaml:"options"`
		}{}
	}

	localPath := filepath.Join(root, ".cog", "config", "providers.local.yaml")
	if localData, lerr := os.ReadFile(localPath); lerr == nil {
		var local providersYAMLShape
		if perr := yaml.Unmarshal(localData, &local); perr == nil {
			for name, pc := range local.Providers {
				// Simple override: local's key entirely replaces base's key
				// UNLESS local only set a subset of fields, in which case we
				// still want the base entry's other fields. Emulate a
				// shallow per-field override on top of any existing base
				// entry so a local.yaml that only overrides `endpoint`
				// doesn't blank out `model`/`type` from providers.yaml.
				merged := base.Providers[name]
				if pc.Type != "" {
					merged.Type = pc.Type
				}
				if pc.Endpoint != "" {
					merged.Endpoint = pc.Endpoint
				}
				if pc.Model != "" {
					merged.Model = pc.Model
				}
				if pc.Enabled != nil {
					merged.Enabled = pc.Enabled
				}
				if pc.Options != nil {
					merged.Options = pc.Options
				}
				base.Providers[name] = merged
			}
		}
	}
	return base, true, nil
}

// openAIModelsListShape is the minimal GET /v1/models response shape this
// check needs to extract ids from an OpenAI-compatible listing.
type openAIModelsListShape struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

func doctorProviderEndpoints(g *DoctorGroup, root string, opts DoctorOptions) {
	pcfg, found, err := loadProvidersYAMLDoctor(root)
	if !found {
		g.add("providers.yaml endpoints", StatusUnknown, fmt.Sprintf("no providers.yaml at .cog/config: %v", err))
		return
	}
	if err != nil {
		g.add("providers.yaml endpoints", StatusFail, err.Error())
		return
	}

	// Deterministic order.
	names := make([]string, 0, len(pcfg.Providers))
	for name := range pcfg.Providers {
		names = append(names, name)
	}
	sort.Strings(names)

	checked := 0
	for _, name := range names {
		pc := pcfg.Providers[name]
		if !pcfg.isEnabled(name) {
			continue
		}
		if pc.Endpoint == "" {
			// Subprocess-CLI providers (claude-code, codex) have no HTTP
			// endpoint to probe — not a finding, just nothing to check here.
			continue
		}
		checked++

		if opts.SkipNetwork {
			g.add("providers.yaml endpoint: "+name, StatusUnknown, "skipped (--skip-network)")
			continue
		}

		healthPath := "/v1/models"
		if hp, ok := pc.Options["health_path"].(string); ok && hp != "" {
			healthPath = hp
		}
		url := strings.TrimRight(pc.Endpoint, "/") + healthPath

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if rerr != nil {
			cancel()
			g.add("providers.yaml endpoint: "+name, StatusFail, fmt.Sprintf("bad request for %s: %v", url, rerr))
			continue
		}
		resp, herr := http.DefaultClient.Do(req)
		if herr != nil {
			cancel()
			g.add("providers.yaml endpoint: "+name, StatusFail, fmt.Sprintf("%s: %v", url, herr))
			continue
		}
		body, _ := readLimited(resp.Body, 1<<20)
		resp.Body.Close()
		cancel()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			g.add("providers.yaml endpoint: "+name, StatusFail, fmt.Sprintf("%s: HTTP %d", url, resp.StatusCode))
			continue
		}

		var listing openAIModelsListShape
		if jerr := json.Unmarshal(body, &listing); jerr == nil && len(listing.Data) > 0 {
			if pc.Model != "" {
				declaredPresent := false
				for _, m := range listing.Data {
					if m.ID == pc.Model {
						declaredPresent = true
						break
					}
				}
				if !declaredPresent {
					g.add("providers.yaml endpoint: "+name, StatusWarn,
						fmt.Sprintf("%s answered (%d models) but declared model %q is not among them", url, len(listing.Data), pc.Model))
					continue
				}
			}
			g.add("providers.yaml endpoint: "+name, StatusOK, fmt.Sprintf("%s answered, %d model(s), declared model present", url, len(listing.Data)))
			continue
		}
		g.add("providers.yaml endpoint: "+name, StatusOK, fmt.Sprintf("%s answered (non-OpenAI-shaped or empty listing)", url))
	}

	if checked == 0 {
		g.add("providers.yaml endpoints", StatusUnknown, "no enabled provider declares an HTTP endpoint to check")
	}
}

func readLimited(r io.Reader, max int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, max))
}

// ---------------------------------------------------------------------------
// (c) provider argv vs installed CLI
// ---------------------------------------------------------------------------

// cliArgvContract describes one provider's buildArgs() contract: the binary
// name, the subcommand whose --help output is authoritative, and the long
// flags that provider's buildArgs() unconditionally emits (i.e. present on
// every request, not gated behind optional per-request fields) and which
// MUST therefore appear in that subcommand's --help output for the
// subprocess call to succeed.
//
// MUST track buildArgs() in provider_<x>.go: any long flag buildArgs()
// unconditionally passes belongs here, and any flag removed from buildArgs()
// should be removed here too, or this check drifts from what actually ships.
type cliArgvContract struct {
	// provider names the check for reporting; bin/subcmd/longFlags source
	// the exact provider_<x>.go this contract mirrors.
	provider  string
	bin       string
	subcmd    []string // args appended before --help, e.g. []string{"exec"}
	longFlags []string
}

// cliArgvContracts is intentionally doctor-local (not exported from the
// provider_*.go files) — see cli_doctor_inference.go's package doc. Update
// this table whenever a provider's buildArgs() unconditional flag set
// changes.
var cliArgvContracts = []cliArgvContract{
	{
		// provider_codex.go buildArgs(), ~line 212-224.
		provider: "codex",
		bin:      "codex",
		subcmd:   []string{"exec"},
		longFlags: []string{
			"-m", "--config", "--sandbox", "--full-auto",
			"--skip-git-repo-check", "--json",
		},
	},
	{
		// provider_pi.go buildArgs(), ~line 378-390. "-p" and "--no-session"
		// are unconditional; "--provider"/"--model" are always emitted with
		// a value (provider/model are always non-empty on a configured
		// provider); "--thinking"/"--tools"/"--system-prompt" are
		// conditional on per-request fields and deliberately excluded here.
		provider: "pi",
		bin:      "pi",
		subcmd:   nil,
		longFlags: []string{
			"-p", "--provider", "--model", "--no-session",
		},
	},
	{
		// provider_claudecode.go buildArgs(), ~line 466+. --effort,
		// --append-system-prompt, --mcp-config/--strict-mcp-config,
		// --allowedTools/--disallowedTools are all conditional on optional
		// per-provider config and deliberately excluded; only what's
		// genuinely unconditional is checked.
		provider: "claude",
		bin:      "claude",
		subcmd:   nil,
		longFlags: []string{
			"-p", "--dangerously-skip-permissions", "--model",
		},
	},
}

// cliHelpTimeout bounds each `<bin> <subcmd> --help` subprocess call.
const cliHelpTimeout = 5 * time.Second

func doctorProviderArgvVsCLI(g *DoctorGroup, opts DoctorOptions) {
	checkArgvContracts(g, cliArgvContracts)
}

// checkArgvContracts is the testable core of doctorProviderArgvVsCLI: it
// takes the contract list as a parameter so tests can supply a fixture
// contract pointing at a fake `--help` script instead of depending on the
// real codex/pi/claude binaries being installed in CI.
func checkArgvContracts(g *DoctorGroup, contracts []cliArgvContract) {
	for _, c := range contracts {
		paths := resolveCLIBinary(c.bin)
		if len(paths) == 0 {
			g.add("argv vs CLI: "+c.provider, StatusUnknown, fmt.Sprintf("%s not found on PATH or ~/.nvm/versions/node/*/bin", c.bin))
			continue
		}
		binPath := paths[0]

		args := append(append([]string{}, c.subcmd...), "--help")
		ctx, cancel := context.WithTimeout(context.Background(), cliHelpTimeout)
		cmd := exec.CommandContext(ctx, binPath, args...)
		out, runErr := cmd.CombinedOutput()
		cancel()
		if runErr != nil && len(out) == 0 {
			g.add("argv vs CLI: "+c.provider, StatusUnknown, fmt.Sprintf("%s %s failed: %v", binPath, strings.Join(args, " "), runErr))
			continue
		}

		helpText := string(out)
		var missing []string
		for _, flag := range c.longFlags {
			if !strings.Contains(helpText, flag) {
				missing = append(missing, flag)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			g.add("argv vs CLI: "+c.provider, StatusFail,
				fmt.Sprintf("%s: buildArgs() emits flag(s) not advertised by `%s %s --help`: %s",
					binPath, c.bin, strings.Join(c.subcmd, " "), strings.Join(missing, ", ")))
			continue
		}
		g.add("argv vs CLI: "+c.provider, StatusOK,
			fmt.Sprintf("%s: every buildArgs() flag present in `%s %s --help`", binPath, c.bin, strings.Join(c.subcmd, " ")))
	}
}

// resolveCLIBinary resolves name against PATH (via lookPathAll) plus every
// ~/.nvm/versions/node/*/bin directory, since launchd's PATH for a kernel
// daemon commonly differs from an interactive shell's PATH and misses
// nvm-installed binaries (codex, pi) entirely.
func resolveCLIBinary(name string) []string {
	var out []string
	if matches, err := lookPathAll(name); err == nil {
		out = append(out, matches...)
	}
	home, _ := os.UserHomeDir()
	if home != "" {
		nvmGlob := filepath.Join(home, ".nvm", "versions", "node", "*", "bin", name)
		matches, _ := filepathGlobQuiet(nvmGlob)
		for _, m := range matches {
			info, err := os.Stat(m)
			if err != nil || info.IsDir() {
				continue
			}
			out = append(out, m)
		}
	}
	return dedupeStrings(out)
}

func filepathGlobQuiet(pattern string) ([]string, error) {
	return globQuiet(pattern), nil
}

// ---------------------------------------------------------------------------
// (e) alias targets in live catalog
// ---------------------------------------------------------------------------

// aliasCatalogEntry is the minimal GET /v1/models entry shape this check
// needs.
type aliasCatalogEntryList struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// doctorAliasTargetsInCatalog asserts that every model id resolve.go's
// static alias tables promise the caller (AvailableModelIDs — the exported
// function resolve.go already uses to build the 400 "unknown model" error
// body, so this check does not duplicate resolve.go's literal alias list)
// is actually present in the live GET /v1/models response. A gap here means
// a client selecting a documented/advertised alias would get served
// something the kernel never actually lists — the inverse of the
// admission-parity invariant resolve.go's own doc comments describe.
func doctorAliasTargetsInCatalog(g *DoctorGroup, root string, opts DoctorOptions) {
	if opts.SkipNetwork {
		g.add("alias targets in live catalog", StatusUnknown, "skipped (--skip-network)")
		return
	}

	endpoint := kernelEndpointForDoctor(root)
	url := endpoint + "/v1/models"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if rerr != nil {
		cancel()
		g.add("alias targets in live catalog", StatusFail, fmt.Sprintf("bad request for %s: %v", url, rerr))
		return
	}
	resp, herr := http.DefaultClient.Do(req)
	if herr != nil {
		cancel()
		g.add("alias targets in live catalog", StatusUnknown, fmt.Sprintf("kernel not reachable at %s: %v (start the kernel to check alias parity)", url, herr))
		return
	}
	body, _ := readLimited(resp.Body, 4<<20)
	resp.Body.Close()
	cancel()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		g.add("alias targets in live catalog", StatusFail, fmt.Sprintf("%s: HTTP %d", url, resp.StatusCode))
		return
	}

	var listing aliasCatalogEntryList
	if err := json.Unmarshal(body, &listing); err != nil {
		g.add("alias targets in live catalog", StatusFail, fmt.Sprintf("%s: could not parse response: %v", url, err))
		return
	}
	live := make(map[string]bool, len(listing.Data))
	for _, m := range listing.Data {
		live[m.ID] = true
	}

	// AvailableModelIDs needs a Router; nil is valid for this doctor check
	// per its own doc comment ("Without a live router only the static alias
	// table is knowable") — we are only checking the STATIC alias ids here,
	// the same subset AvailableModelIDs(nil) yields without ever touching a
	// live router: intentAliases keys plus "local", excluding registered
	// provider names (which are not alias promises this check is about).
	promised := AvailableModelIDs(nil)

	var missing []string
	for _, id := range promised {
		if !live[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		g.add("alias targets in live catalog", StatusFail,
			fmt.Sprintf("%s: alias(es) not present in live catalog (%d entries): %s", url, len(listing.Data), strings.Join(missing, ", ")))
		return
	}
	g.add("alias targets in live catalog", StatusOK,
		fmt.Sprintf("%s: all %d promised alias(es) present in live catalog (%d entries)", url, len(promised), len(listing.Data)))
}

// kernelEndpointForDoctor mirrors resolveClientEndpoint's precedence
// (daemon.state.yaml recorded endpoint, else .cog/config/kernel.yaml port,
// else the 6931 default) via LoadConfig + endpointForPort, without needing
// a running daemon just to compute the URL to probe.
func kernelEndpointForDoctor(root string) string {
	if cfg, err := LoadConfig(root, 0); err == nil {
		if state, serr := loadDaemonState(cfg.WorkspaceRoot); serr == nil && state != nil && state.Endpoint != "" {
			return state.Endpoint
		}
		if cfg.Port != 0 {
			return endpointForPort(cfg.Port)
		}
	}
	return endpointForPort(6931)
}

// ---------------------------------------------------------------------------
// (f) external clients -> kernel: dead ports + auth
// ---------------------------------------------------------------------------

// externalClientTarget is one localhost-or-not HTTP target an external
// client config declares, with whatever credential (if any) that client
// would send on a request to it.
type externalClientTarget struct {
	source  string // e.g. "~/.claude.json (mcpServers.cogos-kernel)"
	url     string
	headers map[string]string // header name -> value (redacted at print time, never stored redacted so the real value is still usable for the auth probe)
}

func doctorExternalClientsToKernel(g *DoctorGroup, root string, opts DoctorOptions) {
	home, _ := os.UserHomeDir()
	doctorExternalClientsToKernelWithHome(g, root, opts, home)
}

// doctorExternalClientsToKernelWithHome is the testable core of
// doctorExternalClientsToKernel: home is a parameter (rather than resolved
// internally via os.UserHomeDir) so tests can point it at a fixture
// directory instead of the real ~/.claude.json etc. on the machine running
// the test.
func doctorExternalClientsToKernelWithHome(g *DoctorGroup, root string, opts DoctorOptions, home string) {
	if home == "" {
		g.add("external clients -> kernel", StatusUnknown, "could not resolve home directory")
		return
	}

	var targets []externalClientTarget
	targets = append(targets, collectClaudeJSONTargets(home)...)
	targets = append(targets, collectCodexTOMLTargets(home)...)
	targets = append(targets, collectHermesProfileTargets(home)...)

	if len(targets) == 0 {
		g.add("external clients -> kernel", StatusUnknown, "no external client config found (~/.claude.json, ~/.codex/config.toml, ~/.hermes/profiles/*/config.yaml)")
		return
	}

	kernelEndpoint := kernelEndpointForDoctor(root)
	kernelHost, kernelPort := splitHostPort(kernelEndpoint)

	localCount := 0
	for _, t := range targets {
		host, port := splitHostPort(t.url)
		if !isLocalHost(host) {
			continue // out of scope for this check per #631's spec
		}
		localCount++
		checkName := "external client: " + t.source

		if opts.SkipNetwork {
			g.add(checkName, StatusUnknown, "skipped (--skip-network)")
			continue
		}

		// (1) TCP dial the port.
		addr := net.JoinHostPort(host, port)
		conn, derr := net.DialTimeout("tcp", addr, 1*time.Second)
		if derr != nil {
			g.add(checkName, StatusFail, fmt.Sprintf("dead port: %s unreachable: %v (target %s)", addr, derr, redactMCPTarget(t.url)))
			continue
		}
		conn.Close()

		// (2) If this is the kernel's own /mcp endpoint, verify the
		// client's configured credential actually authenticates.
		u, uerr := neturlParseDoctor(t.url)
		isKernelMCP := uerr == nil && u.Path == "/mcp" && port == kernelPort && sameHost(host, kernelHost)
		if !isKernelMCP {
			g.add(checkName, StatusOK, fmt.Sprintf("port open: %s (%s)", addr, redactMCPTarget(t.url)))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, rerr := http.NewRequestWithContext(ctx, http.MethodPost, t.url, strings.NewReader("{}"))
		if rerr != nil {
			cancel()
			g.add(checkName, StatusUnknown, fmt.Sprintf("bad request for %s: %v", redactMCPTarget(t.url), rerr))
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		var credDesc []string
		for hk, hv := range t.headers {
			req.Header.Set(hk, hv)
			credDesc = append(credDesc, fmt.Sprintf("%s(len=%d)", hk, len(hv)))
		}
		sort.Strings(credDesc)
		resp, herr := http.DefaultClient.Do(req)
		if herr != nil {
			cancel()
			g.add(checkName, StatusFail, fmt.Sprintf("dead port: POST %s: %v", redactMCPTarget(t.url), herr))
			continue
		}
		respBody, _ := readLimited(resp.Body, 1<<16)
		resp.Body.Close()
		cancel()

		if resp.StatusCode == http.StatusUnauthorized {
			errType := grantErrorType(respBody)
			g.add(checkName, StatusFail,
				fmt.Sprintf("kernel auth: %s: HTTP 401 (%s), credential(s) sent: %s", redactMCPTarget(t.url), errType, strings.Join(credDesc, ", ")))
			continue
		}
		g.add(checkName, StatusOK,
			fmt.Sprintf("kernel auth: %s: HTTP %d (non-401 = credential accepted), credential(s) sent: %s", redactMCPTarget(t.url), resp.StatusCode, strings.Join(credDesc, ", ")))
	}

	if localCount == 0 {
		g.add("external clients -> kernel", StatusUnknown, fmt.Sprintf("%d client target(s) found, none point at localhost/127.0.0.1", len(targets)))
	}
}

// grantErrorType extracts error.type from the kernel's 401 JSON body
// ("missing_grant" | "invalid_grant"), falling back to a truncated raw body
// when the shape doesn't match (still useful for diagnosis, still bounded).
func grantErrorType(body []byte) string {
	var parsed struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil && parsed.Error.Type != "" {
		return parsed.Error.Type
	}
	s := string(body)
	if len(s) > 120 {
		s = s[:120] + "…"
	}
	return "unrecognized 401 body: " + s
}

// collectClaudeJSONTargets reads ~/.claude.json's top-level mcpServers plus
// every project's mcpServers block, extracting http/url-shaped entries only
// (stdio command-based entries have no network target to check).
func collectClaudeJSONTargets(home string) []externalClientTarget {
	path := filepath.Join(home, ".claude.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var doc struct {
		McpServers map[string]claudeJSONMCPEntry            `json:"mcpServers"`
		Projects   map[string]struct {
			McpServers map[string]claudeJSONMCPEntry `json:"mcpServers"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil
	}

	var out []externalClientTarget
	for name, entry := range doc.McpServers {
		if t, ok := entry.target("~/.claude.json (mcpServers." + name + ")"); ok {
			out = append(out, t)
		}
	}
	for proj, p := range doc.Projects {
		for name, entry := range p.McpServers {
			if t, ok := entry.target(fmt.Sprintf("~/.claude.json (project %s, mcpServers.%s)", proj, name)); ok {
				out = append(out, t)
			}
		}
	}
	return out
}

type claudeJSONMCPEntry struct {
	Type    string            `json:"type"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

func (e claudeJSONMCPEntry) target(source string) (externalClientTarget, bool) {
	if e.URL == "" {
		return externalClientTarget{}, false
	}
	return externalClientTarget{source: source, url: e.URL, headers: e.Headers}, true
}

// codexMCPServerHeaderRe matches one `"key" = "value"` pair inside a TOML
// inline table, used to parse http_headers = { "X-Cogos-Grant" = "..." }
// without a TOML dependency.
var codexMCPServerHeaderRe = regexp.MustCompile(`"([^"]+)"\s*=\s*"([^"]*)"`)

var codexSectionHeaderRe = regexp.MustCompile(`^\[mcp_servers\.(?:"([^"]+)"|([^\]]+))\]$`)

// collectCodexTOMLTargets scans ~/.codex/config.toml line-by-line for
// [mcp_servers.NAME] sections carrying a `url` and, on the same
// (possibly-multiline-folded-into-one-line) or later lines within the
// section, an `http_headers` inline table. Deliberately NOT a general TOML
// parser: only the exact shape provider configs in this repo actually use
// (see provider_codex.go's own config surface).
func collectCodexTOMLTargets(home string) []externalClientTarget {
	path := filepath.Join(home, ".codex", "config.toml")
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var out []externalClientTarget
	var curName string
	var curURL string
	var curHeaders map[string]string
	var curEnabled bool
	inSection := false

	flush := func() {
		if inSection && curURL != "" && curEnabled {
			out = append(out, externalClientTarget{
				source:  fmt.Sprintf("~/.codex/config.toml ([mcp_servers.%s])", curName),
				url:     curURL,
				headers: curHeaders,
			})
		}
		curName, curURL, curHeaders, curEnabled, inSection = "", "", nil, true, false
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			flush()
			if m := codexSectionHeaderRe.FindStringSubmatch(line); m != nil {
				name := m[1]
				if name == "" {
					name = m[2]
				}
				curName = name
				curEnabled = true
				inSection = true
			}
			continue
		}
		if !inSection {
			continue
		}
		switch {
		case strings.HasPrefix(line, "url"):
			if eq := strings.Index(line, "="); eq >= 0 {
				curURL = strings.Trim(strings.TrimSpace(line[eq+1:]), `"`)
			}
		case strings.HasPrefix(line, "enabled"):
			if eq := strings.Index(line, "="); eq >= 0 {
				v := strings.TrimSpace(line[eq+1:])
				curEnabled = v != "false"
			}
		case strings.HasPrefix(line, "http_headers"):
			curHeaders = map[string]string{}
			for _, m := range codexMCPServerHeaderRe.FindAllStringSubmatch(line, -1) {
				curHeaders[m[1]] = m[2]
			}
		}
	}
	flush()
	return out
}

// collectHermesProfileTargets reads every ~/.hermes/profiles/*/config.yaml
// for a top-level providers.cogos entry (base_url + api_key), per #631's
// scope. Sibling provider entries (darkstar-lms, eclipse-lms, etc.) are out
// of scope for this check — it targets client->kernel auth specifically.
func collectHermesProfileTargets(home string) []externalClientTarget {
	pattern := filepath.Join(home, ".hermes", "profiles", "*", "config.yaml")
	matches := globQuiet(pattern)

	var out []externalClientTarget
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var doc struct {
			Providers map[string]struct {
				BaseURL string `yaml:"base_url"`
				APIKey  string `yaml:"api_key"`
			} `yaml:"providers"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			continue
		}
		cogos, ok := doc.Providers["cogos"]
		if !ok || cogos.BaseURL == "" {
			continue
		}
		// base_url is typically ".../v1"; the kernel's /mcp endpoint lives
		// at the host root, not under /v1 — derive it from the same
		// scheme+host rather than assuming a suffix to strip.
		u, uerr := neturlParseDoctor(cogos.BaseURL)
		if uerr != nil {
			continue
		}
		u.Path = "/mcp"
		u.RawQuery = ""
		headers := map[string]string{}
		if cogos.APIKey != "" {
			headers["Authorization"] = "Bearer " + cogos.APIKey
		}
		out = append(out, externalClientTarget{
			source:  fmt.Sprintf("%s (providers.cogos)", hermesProfileLabel(path)),
			url:     u.String(),
			headers: headers,
		})
	}
	return out
}

func hermesProfileLabel(configPath string) string {
	profile := filepath.Base(filepath.Dir(configPath))
	return "~/.hermes/profiles/" + profile + "/config.yaml"
}

// splitHostPort extracts host and port from a URL string. Port defaults to
// "80"/"443" per scheme when absent (matches net/url.Port() semantics via a
// manual default since url.Port() returns "" when the URL omits it).
func splitHostPort(rawURL string) (host, port string) {
	u, err := neturlParseDoctor(rawURL)
	if err != nil {
		return "", ""
	}
	host = u.Hostname()
	port = u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return host, port
}

func sameHost(a, b string) bool {
	// Treat localhost/127.0.0.1/::1 as interchangeable — a client
	// registering "localhost:6931" and a kernel resolved as
	// "127.0.0.1:6931" (or vice versa) name the same endpoint. isLocalHost
	// is shared with provider_lms_model_state.go's own localhost gating.
	return isLocalHost(a) && isLocalHost(b)
}

// neturlParseDoctor parses a URL string, delegating to net/url.Parse
// (imported here as neturl, matching cli_doctor.go's own import alias so
// both files refer to the same package the same way).
func neturlParseDoctor(raw string) (*neturl.URL, error) {
	return neturl.Parse(raw)
}

