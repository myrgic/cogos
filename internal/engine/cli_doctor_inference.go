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
// cliProviderTypes overload ProviderConfig.Endpoint as a binary PATH, not an
// HTTP URL (provider_codex.go:156, provider_claudecode.go:88 "abuse Endpoint
// field for binary path", provider_pi.go:75). They have no HTTP surface to
// probe; check (c) validates them via their real CLI instead.
var cliProviderTypes = map[string]bool{"codex": true, "claude-code": true, "pi": true}

// doctorProviderEndpoints asks each enabled HTTP-backed provider to probe
// ITSELF. It loads providers.yaml + providers.local.yaml through the same
// loadProvidersConfig the router uses, constructs each provider with the
// same makeProvider the router uses, and calls the provider's own Ping()
// and (when implemented) ListModels(). The provider already knows its auth
// scheme (Authorization: Bearer for OpenAI-compat, x-api-key for Anthropic),
// its health path, and its listing shape — doctor re-implementing any of
// that is exactly the drift cog-review caught three rounds running on #635
// (no auth → 401; Bearer-only → wrong header for anthropic; endpoint-as-path
// for CLI types). One probe implementation per provider, owned by the provider.
func doctorProviderEndpoints(g *DoctorGroup, root string, opts DoctorOptions) {
	cfg, err := LoadConfig(root, 0)
	if err != nil {
		g.add("providers.yaml endpoints", StatusUnknown, fmt.Sprintf("workspace config not loadable: %v", err))
		return
	}
	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		if os.IsNotExist(err) {
			g.add("providers.yaml endpoints", StatusUnknown, fmt.Sprintf("no providers.yaml at .cog/config: %v", err))
		} else {
			g.add("providers.yaml endpoints", StatusFail, err.Error())
		}
		return
	}

	names := make([]string, 0, len(pcfg.Providers))
	for name := range pcfg.Providers {
		names = append(names, name)
	}
	sort.Strings(names)

	checked := 0
	for _, name := range names {
		pc := pcfg.Providers[name]
		if !pc.IsEnabled() {
			continue
		}
		typ := pc.Type
		if typ == "" {
			typ = name
		}
		if pc.Endpoint == "" || cliProviderTypes[typ] {
			continue
		}
		checked++

		if opts.SkipNetwork {
			g.add("providers.yaml endpoint: "+name, StatusUnknown, "skipped (--skip-network)")
			continue
		}

		prov, perr := makeProvider(name, pc, nil)
		if perr != nil {
			g.add("providers.yaml endpoint: "+name, StatusUnknown, fmt.Sprintf("cannot construct provider (type %q): %v", typ, perr))
			continue
		}

		authNote := ""
		if pc.APIKeyEnv != "" {
			if key := os.Getenv(pc.APIKeyEnv); key != "" {
				authNote = fmt.Sprintf(" (auth: $%s, len=%d)", pc.APIKeyEnv, len(key))
			} else {
				authNote = fmt.Sprintf(" (auth: $%s UNSET in this environment)", pc.APIKeyEnv)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		latency, pingErr := prov.Ping(ctx)
		if pingErr != nil {
			cancel()
			g.add("providers.yaml endpoint: "+name, StatusFail, fmt.Sprintf("%s: %v%s", pc.Endpoint, pingErr, authNote))
			continue
		}

		// Declared-model check via the provider's own lister when it has one.
		if lister, ok := prov.(ModelLister); ok && pc.Model != "" {
			ids, lerr := lister.ListModels(ctx)
			cancel()
			if lerr != nil {
				g.add("providers.yaml endpoint: "+name, StatusWarn,
					fmt.Sprintf("%s answered ping in %s but ListModels failed: %v%s", pc.Endpoint, latency.Round(time.Millisecond), lerr, authNote))
				continue
			}
			present := false
			for _, id := range ids {
				if id == pc.Model {
					present = true
					break
				}
			}
			if !present {
				g.add("providers.yaml endpoint: "+name, StatusWarn,
					fmt.Sprintf("%s answered (%d models) but declared model %q is not among them%s", pc.Endpoint, len(ids), pc.Model, authNote))
				continue
			}
			g.add("providers.yaml endpoint: "+name, StatusOK,
				fmt.Sprintf("%s answered in %s, %d model(s), declared model present%s", pc.Endpoint, latency.Round(time.Millisecond), len(ids), authNote))
			continue
		}
		cancel()
		g.add("providers.yaml endpoint: "+name, StatusOK,
			fmt.Sprintf("%s answered ping in %s%s", pc.Endpoint, latency.Round(time.Millisecond), authNote))
	}
	if checked == 0 {
		g.add("providers.yaml endpoints", StatusOK, "no enabled HTTP-backed providers declared")
	}
}

func readLimited(r io.Reader, max int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, max))
}

// ---------------------------------------------------------------------------
// (c) provider argv vs installed CLI
// ---------------------------------------------------------------------------

// cliArgvContract describes one provider's buildArgs() contract: the binary
// name, the subcommand whose --help output is authoritative, and the flags
// that provider's buildArgs() emits for a representative request — every one
// of which MUST appear in that subcommand's --help output for the subprocess
// call to succeed.
//
// The flag list is DERIVED by calling the real buildArgs() on a throwaway
// provider with a fully-populated CompletionRequest, not hand-copied. A
// hand-maintained table is exactly the class of drift this check exists to
// catch (cog-review on #635 found "--no-extensions" missing from the pi
// contract on the first round); deriving from buildArgs() cannot miss a flag.
type cliArgvContract struct {
	provider string
	bin      string
	subcmd   []string // args appended before --help, e.g. []string{"exec"}
	flags    []string // every "-x"/"--xyz" token buildArgs() emitted
}

// argvFlagTokens extracts the flag tokens ("-m", "--json", "--config=x" → "--config")
// from a buildArgs() result, in first-seen order, deduplicated.
func argvFlagTokens(args []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range args {
		if !strings.HasPrefix(a, "-") || a == "-" || a == "--" {
			continue
		}
		if i := strings.IndexByte(a, '='); i > 0 {
			a = a[:i]
		}
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// doctorArgvRequest is a request with every request-gated field populated so
// conditional flags are exercised. Config-gated flags are turned on by the
// ProviderConfig in cliArgvContracts. Between the two, every branch of each
// buildArgs() is live, so the derived contract is the SUPERSET of what any
// real request can emit — any flag the provider CAN emit must be one the
// installed CLI accepts. TestCliArgvContracts_CoverEveryBuildArgsFlag pins
// this by diffing against the flag literals in each provider file.
func doctorArgvRequest() *CompletionRequest {
	cost := 1.0
	return &CompletionRequest{
		SystemPrompt:  "doctor",
		Messages:      []ProviderMessage{{Role: "user", Content: "doctor"}},
		ModelOverride: "doctor-model",
		Metadata: RequestMetadata{
			RequestID:  "doctor", // → --no-session-persistence (claude-code)
			MaxCostUSD: &cost,    // → --max-budget-usd (claude-code)
		},
	}
}

// cliArgvContracts builds the three CLI providers' contracts from their real
// buildArgs(). Providers are constructed with a representative ProviderConfig
// so every config-gated branch that affects argv is on.
func cliArgvContracts() []cliArgvContract {
	req := doctorArgvRequest()
	codex := NewCodexProvider("doctor-codex", ProviderConfig{Model: "doctor-model", Options: map[string]any{"effort": "medium", "sandbox": "read-only"}})
	pi := NewPiProvider("doctor-pi", ProviderConfig{Model: "doctor-model", Options: map[string]any{"provider": "ollama", "thinking": "medium", "tools": "read"}}, nil)
	cc := NewClaudeCodeProvider("doctor-claude", ProviderConfig{Model: "sonnet", Options: map[string]any{
		"effort":           "medium",
		"mcp_config":       "/dev/null", // → --mcp-config + --strict-mcp-config
		"allowed_tools":    "Read",      // → --allowedTools
		"disallowed_tools": "Bash",      // → --disallowedTools
	}}, nil)
	// Complete()/Stream() append transport flags AFTER buildArgs() — the
	// claude-code stream-json trio (provider_claudecode.go Complete+Stream)
	// and pi's --print → --mode json rewrite (provider_pi.go Stream). Those
	// are part of the real argv too; a derived contract that stopped at
	// buildArgs() would miss them (found by TestCliArgvContracts_CoverEveryBuildArgsFlag).
	ccArgv := append(cc.buildArgs(req), "--output-format", "stream-json", "--verbose", "--include-partial-messages")
	piArgv := append(pi.buildArgs(req), "--mode", "json")
	return []cliArgvContract{
		{provider: "codex", bin: "codex", subcmd: []string{"exec"}, flags: argvFlagTokens(codex.buildArgs(req))},
		{provider: "pi", bin: "pi", subcmd: nil, flags: argvFlagTokens(piArgv)},
		{provider: "claude", bin: "claude", subcmd: nil, flags: argvFlagTokens(ccArgv)},
	}
}

// cliHelpTimeout bounds each `<bin> <subcmd> --help` subprocess call.
const cliHelpTimeout = 5 * time.Second

func doctorProviderArgvVsCLI(g *DoctorGroup, opts DoctorOptions) {
	if opts.SkipNetwork {
		// --help spawns subprocesses; treat like the other external probes.
		g.add("argv vs CLI", StatusUnknown, "skipped (--skip-network)")
		return
	}
	checkArgvContracts(g, cliArgvContracts())
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
		for _, flag := range c.flags {
			if !helpAdvertisesFlag(helpText, flag) {
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

// helpAdvertisesFlag reports whether --help output lists flag as a flag token,
// not merely as a substring. A short flag like "-p" is a literal substring of
// "--dangerously-skip-permissions" and "--provider" (the hyphen before the
// word), so strings.Contains would report it present after the CLI dropped it —
// exactly the silent drift this check exists to catch. A flag token is
// delimited by start/end of text, whitespace, or the punctuation clap/cobra/flag
// put around flags: , | ( ) [ ] < = /
func helpAdvertisesFlag(helpText, flag string) bool {
	for start := 0; ; {
		i := strings.Index(helpText[start:], flag)
		if i < 0 {
			return false
		}
		i += start
		end := i + len(flag)
		before := byte(' ')
		if i > 0 {
			before = helpText[i-1]
		}
		after := byte(' ')
		if end < len(helpText) {
			after = helpText[end]
		}
		if isFlagBoundary(before) && isFlagBoundary(after) {
			return true
		}
		start = i + 1
	}
}

func isFlagBoundary(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', ',', '|', '(', ')', '[', ']', '<', '=', '/':
		return true
	}
	return false
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

	// An alias is a resolver, not a catalog entry: "sonnet" is never in
	// /v1/models, but its ModelOverride target ("claude-sonnet-5") must be.
	// Collect every static alias whose resolution names a concrete model and
	// assert THAT id is live. Aliases with no ModelOverride (provider-only
	// routing such as "codex", "local") have nothing to check here; the
	// provider-registration check covers them.
	promised := map[string]string{} // target id → alias(es) that promise it
	for _, table := range []map[string]ModelResolution{intentAliases, dispatchFrontierAliases} {
		for alias, res := range table {
			if res.ModelOverride == "" {
				continue
			}
			if prev := promised[res.ModelOverride]; prev != "" {
				promised[res.ModelOverride] = prev + "," + alias
			} else {
				promised[res.ModelOverride] = alias
			}
		}
	}

	var missing []string
	for target, aliases := range promised {
		if !live[target] {
			missing = append(missing, fmt.Sprintf("%s (via %s)", target, aliases))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		g.add("alias targets in live catalog", StatusFail,
			fmt.Sprintf("%s: alias target(s) not present in live catalog (%d entries): %s", url, len(listing.Data), strings.Join(missing, ", ")))
		return
	}
	g.add("alias targets in live catalog", StatusOK,
		fmt.Sprintf("%s: all %d alias target(s) present in live catalog (%d entries)", url, len(promised), len(listing.Data)))
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
		McpServers map[string]claudeJSONMCPEntry `json:"mcpServers"`
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
