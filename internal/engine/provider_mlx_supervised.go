// provider_mlx_supervised.go — kernel-supervised mlx_lm.server inference provider.
//
// MLXSupervisedProvider implements both Provider (dispatch) and
// reconcile.Reconcilable (health + lifecycle). A single struct, two interfaces,
// no config-schema divergence.
//
// Architecture
//
//   - Config is declared in providers(.local).yaml under a key whose type field
//     is "mlx-supervised". No other type values are affected.
//   - The Reconcilable half manages a launchd plist at
//     ~/Library/LaunchAgents/<launchd_label>.plist, using the existing
//     LaunchctlController / ObserverSupervisor abstraction (service_supervisor.go).
//   - The Provider half wraps OpenAICompatProvider for the actual HTTP dispatch —
//     mlx_lm.server speaks OpenAI-compat /v1/chat/completions.
//   - Available() consults both launchd-reported service state and the
//     /v1/models HTTP probe so the router gets an accurate signal.
//   - Health() is cheap: it reads cached state (last launchd probe + last HTTP
//     probe) without blocking. The Reconcile() path updates that cache.
//   - Because Health() is non-blocking, it is safe inside buildHealthBlock's
//     200 ms per-provider timeout.
//
// # Plist template
//
// The kernel writes a minimal launchd plist that runs:
//
//	<binary> --model <model_path> --port <port> --host 127.0.0.1
//
// Additional args are supported via the "args" Options key ([]string).
// The plist uses KeepAlive=true so launchd restarts the server on crash.
//
// Config schema (providers.local.yaml example)
//
//	mlx-gemma-supervised:
//	  type: mlx-supervised
//	  endpoint: http://localhost:1235
//	  model: /Volumes/2TBSD/AI_Models/…/gemma-4-E4B-…-mlx-4bit
//	  context_window: 32768
//	  timeout: 300
//	  options:
//	    binary: /usr/local/bin/mlx_lm.server     # default: mlx_lm.server (PATH)
//	    launchd_label: com.cogos.mlx-gemma        # default: com.cogos.mlx-<name>
//	    args: []                                  # extra CLI args after model+port
//
// # Lifetime under launchd
//
// launchd is the supervisor. The kernel only writes the plist and tells launchd
// to load/unload it. It does NOT keep the process address in memory or attempt
// in-process restart. This matches the established service_supervisor pattern.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// mlxSupervisedType is the value of ProviderConfig.Type that activates this driver.
const mlxSupervisedType = "mlx-supervised"

// mlxDefaultLaunchdPrefix is prepended to the provider name when no explicit
// launchd_label is supplied in Options.
const mlxDefaultLaunchdPrefix = "com.cogos.mlx-"

// MLXSupervisedProvider implements Provider (dispatch) and reconcile.Reconcilable
// (health + lifecycle) for a kernel-supervised mlx_lm.server instance.
type MLXSupervisedProvider struct {
	// --- identity ---
	name string // provider name as declared in providers.yaml

	// --- dispatch (OpenAI-compat wrapper) ---
	inner *OpenAICompatProvider

	// --- supervision ---
	supervisor   ServiceSupervisor
	launchdLabel string   // e.g. "com.cogos.mlx-gemma"
	plistPath    string   // ~/Library/LaunchAgents/<label>.plist
	binary       string   // path/name of mlx_lm.server binary
	modelPath    string   // full model path passed to --model
	port         int      // port mlx_lm.server should bind
	extraArgs    []string // additional CLI args

	// --- cached health state (updated by Reconcile / Available) ---
	mu         sync.RWMutex
	lastStatus *ServiceStatus
	lastHTTP   bool // last /v1/models probe result
	lastProbed time.Time

	// --- reconcile metadata ---
	workspaceRoot string // injected at daemon boot
}

// newMLXSupervisedProvider constructs an MLXSupervisedProvider from ProviderConfig.
// It wraps OpenAICompatProvider for dispatch, so all HTTP logic is reused.
func newMLXSupervisedProvider(name string, cfg ProviderConfig, supervisor ServiceSupervisor) (*MLXSupervisedProvider, error) {
	// Resolve launchd label.
	label := optStr(cfg.Options, "launchd_label")
	if label == "" {
		label = mlxDefaultLaunchdPrefix + name
	}

	// Resolve binary path.
	binary := optStr(cfg.Options, "binary")
	if binary == "" {
		binary = "mlx_lm.server"
	}

	// Resolve port from endpoint.
	port := extractPort(cfg.Endpoint, 1235)

	// Extra CLI args.
	var extraArgs []string
	if raw, ok := cfg.Options["args"]; ok {
		switch v := raw.(type) {
		case []interface{}:
			for _, a := range v {
				extraArgs = append(extraArgs, fmt.Sprint(a))
			}
		case []string:
			extraArgs = v
		}
	}

	// Plist path.
	home, err := homeDir()
	if err != nil {
		return nil, fmt.Errorf("mlx-supervised %q: cannot resolve home dir for plist path: %w", name, err)
	}
	plistPath := filepath.Join(home, "Library", "LaunchAgents", label+".plist")

	// Inner OpenAI-compat provider for dispatch.
	inner := NewOpenAICompatProvider(name, cfg)

	return &MLXSupervisedProvider{
		name:         name,
		inner:        inner,
		supervisor:   supervisor,
		launchdLabel: label,
		plistPath:    plistPath,
		binary:       binary,
		modelPath:    cfg.Model,
		port:         port,
		extraArgs:    extraArgs,
	}, nil
}

// ── Provider interface ─────────────────────────────────────────────────────────

// Name returns the provider name.
func (p *MLXSupervisedProvider) Name() string { return p.name }

// Model returns the configured model path.
func (p *MLXSupervisedProvider) Model() string { return p.inner.Model() }

// Complete dispatches via the inner OpenAI-compat provider.
func (p *MLXSupervisedProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	return p.inner.Complete(ctx, req)
}

// Stream dispatches via the inner OpenAI-compat provider.
func (p *MLXSupervisedProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	return p.inner.Stream(ctx, req)
}

// Available returns true when the supervised service is running and the
// configured model is present at /v1/models. It updates the cached HTTP probe
// result as a side-effect (so Health() can reflect it without blocking).
func (p *MLXSupervisedProvider) Available(ctx context.Context) bool {
	// First check launchd state from the cache (cheap, updated by Reconcile).
	p.mu.RLock()
	lastStatus := p.lastStatus
	p.mu.RUnlock()

	if lastStatus != nil && !lastStatus.Running {
		// Launchd says not running — short-circuit without HTTP probe.
		return false
	}

	// HTTP probe.
	ok := p.inner.Available(ctx)
	p.mu.Lock()
	p.lastHTTP = ok
	p.lastProbed = time.Now()
	p.mu.Unlock()
	return ok
}

// Capabilities delegates to the inner provider.
func (p *MLXSupervisedProvider) Capabilities() ProviderCapabilities {
	caps := p.inner.Capabilities()
	caps.IsLocal = true
	return caps
}

// Ping delegates to the inner provider.
func (p *MLXSupervisedProvider) Ping(ctx context.Context) (time.Duration, error) {
	return p.inner.Ping(ctx)
}

// ── reconcile.Reconcilable interface ──────────────────────────────────────────

// Type returns the reconcilable type identifier.
func (p *MLXSupervisedProvider) Type() string { return mlxSupervisedType }

// LoadConfig loads declared configuration from the workspace.
// For MLXSupervisedProvider this is a no-op: config is already parsed at
// construction time from ProviderConfig. The daemon uses this path for stub
// providers; the full provider is constructed by BuildRouter.
func (p *MLXSupervisedProvider) LoadConfig(root string) (any, error) {
	p.mu.Lock()
	p.workspaceRoot = root
	p.mu.Unlock()
	return nil, nil
}

// FetchLive returns the current launchd + HTTP state for this service.
func (p *MLXSupervisedProvider) FetchLive(ctx context.Context, _ any) (any, error) {
	def := p.serviceDef()
	st, err := p.supervisor.Status(ctx, p.name, def)
	if err != nil {
		return nil, fmt.Errorf("mlx-supervised %q: status: %w", p.name, err)
	}
	return st, nil
}

// ComputePlan compares declared config against live state.
// The only action this provider can take is "ensure plist exists and is loaded".
func (p *MLXSupervisedProvider) ComputePlan(config any, live any, state *reconcile.State) (*reconcile.Plan, error) {
	st, _ := live.(*ServiceStatus)
	running := st != nil && st.Running
	plistExists := false
	if _, err := os.Stat(p.plistPath); err == nil {
		plistExists = true
	}

	plan := &reconcile.Plan{ResourceType: mlxSupervisedType}
	if !plistExists {
		plan.Actions = append(plan.Actions, reconcile.Action{
			Action:       reconcile.ActionCreate,
			ResourceType: mlxSupervisedType,
			Name:         p.name + "/plist",
			Details: map[string]any{
				"path":   p.plistPath,
				"reason": "launchd plist absent — kernel must write it before starting service",
			},
		})
		plan.Summary.Creates++
	}
	if !running {
		plan.Actions = append(plan.Actions, reconcile.Action{
			Action:       reconcile.ActionUpdate,
			ResourceType: mlxSupervisedType,
			Name:         p.name + "/service",
			Details: map[string]any{
				"label":  p.launchdLabel,
				"reason": "service not running — ensure started via launchd",
			},
		})
		plan.Summary.Updates++
	}
	return plan, nil
}

// ApplyPlan writes the plist (if needed) and starts the service via launchd.
func (p *MLXSupervisedProvider) ApplyPlan(ctx context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	var results []reconcile.Result

	for _, action := range plan.Actions {
		switch {
		case strings.HasSuffix(action.Name, "/plist"):
			if err := p.writePlist(); err != nil {
				results = append(results, reconcile.Result{
					Phase:  "apply",
					Action: string(action.Action),
					Name:   action.Name,
					Status: reconcile.ApplyFailed,
					Error:  err.Error(),
				})
				return results, err
			}
			results = append(results, reconcile.Result{
				Phase:  "apply",
				Action: string(action.Action),
				Name:   action.Name,
				Status: reconcile.ApplySucceeded,
			})

		case strings.HasSuffix(action.Name, "/service"):
			def := p.serviceDef()
			st, err := p.supervisor.Start(ctx, p.name, def)
			if err != nil {
				results = append(results, reconcile.Result{
					Phase:  "apply",
					Action: string(action.Action),
					Name:   action.Name,
					Status: reconcile.ApplyFailed,
					Error:  err.Error(),
				})
				// Non-fatal: surface in results but continue.
				continue
			}
			p.mu.Lock()
			p.lastStatus = st
			p.mu.Unlock()
			results = append(results, reconcile.Result{
				Phase:  "apply",
				Action: string(action.Action),
				Name:   action.Name,
				Status: reconcile.ApplySucceeded,
			})
		}
	}
	return results, nil
}

// BuildState constructs state from live data.
func (p *MLXSupervisedProvider) BuildState(_ any, live any, existing *reconcile.State) (*reconcile.State, error) {
	st, _ := live.(*ServiceStatus)
	state := &reconcile.State{}
	if existing != nil {
		state = existing
	}
	addr := "mlx-supervised/" + p.name
	res := reconcile.Resource{
		Address:    addr,
		ExternalID: p.launchdLabel,
		Name:       p.name,
		Attributes: map[string]any{
			"binary":     p.binary,
			"model":      p.modelPath,
			"port":       p.port,
			"plist_path": p.plistPath,
		},
	}
	if st != nil {
		res.Attributes["running"] = st.Running
		res.Attributes["pid"] = st.PID
	}
	state.Resources = []reconcile.Resource{res}
	return state, nil
}

// Health returns the three-axis status without blocking.
// It reads the cached launchd + HTTP probe results updated by Available()
// and FetchLive(). Designed to return in <1 ms so buildHealthBlock's
// 200 ms per-provider timeout is never triggered by this provider.
func (p *MLXSupervisedProvider) Health() reconcile.ResourceStatus {
	p.mu.RLock()
	st := p.lastStatus
	httpOK := p.lastHTTP
	probed := p.lastProbed
	p.mu.RUnlock()

	// Plist-existence check (stat is fast).
	plistExists := false
	if _, err := os.Stat(p.plistPath); err == nil {
		plistExists = true
	}

	// Not yet probed — report as progressing.
	if st == nil && probed.IsZero() {
		msg := fmt.Sprintf("mlx_lm.server not yet probed (label: %s)", p.launchdLabel)
		if !plistExists {
			msg = fmt.Sprintf("plist not found at %s — run cog apply mlx-supervised to create", p.plistPath)
			return reconcile.ResourceStatus{
				Sync:      reconcile.SyncStatusOutOfSync,
				Health:    reconcile.HealthMissing,
				Operation: reconcile.OperationIdle,
				Message:   msg,
			}
		}
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthProgressing,
			Operation: reconcile.OperationWaiting,
			Message:   msg,
		}
	}

	// Plist absent — hard missing.
	if !plistExists {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthMissing,
			Operation: reconcile.OperationIdle,
			Message:   fmt.Sprintf("launchd plist absent at %s", p.plistPath),
		}
	}

	// Launchd says not running.
	if st != nil && !st.Running {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   fmt.Sprintf("plist registered but process not running (label: %s)", p.launchdLabel),
		}
	}

	// Running but HTTP probe failed.
	if !httpOK {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthProgressing,
			Operation: reconcile.OperationWaiting,
			Message:   fmt.Sprintf("process running (launchd) but /v1/models probe failed — model may be loading (port: %d)", p.port),
		}
	}

	// All three axes healthy.
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusSynced,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
		Message:   fmt.Sprintf("mlx_lm.server healthy (label: %s, port: %d, model: %s)", p.launchdLabel, p.port, p.modelPath),
	}
}

// ── Plist management ──────────────────────────────────────────────────────────

// writePlist writes the launchd property list for this mlx_lm.server instance.
// The plist is always written fresh so config changes take effect on next reload.
func (p *MLXSupervisedProvider) writePlist() error {
	if err := os.MkdirAll(filepath.Dir(p.plistPath), 0o755); err != nil {
		return fmt.Errorf("create LaunchAgents dir: %w", err)
	}

	// Build ProgramArguments array.
	args := []string{p.binary}
	if p.modelPath != "" {
		args = append(args, "--model", p.modelPath)
	}
	args = append(args, "--port", fmt.Sprintf("%d", p.port))
	args = append(args, "--host", "127.0.0.1")
	args = append(args, p.extraArgs...)

	plist := mlxPlist{
		Label:             p.launchdLabel,
		ProgramArguments:  args,
		RunAtLoad:         true,
		KeepAlive:         true,
		StandardOutPath:   p.logPath("stdout"),
		StandardErrorPath: p.logPath("stderr"),
	}

	data, err := marshalPlist(plist)
	if err != nil {
		return fmt.Errorf("marshal plist: %w", err)
	}
	if err := os.WriteFile(p.plistPath, data, 0o644); err != nil {
		return fmt.Errorf("write plist %s: %w", p.plistPath, err)
	}
	return nil
}

// logPath returns a conventional log path for this service.
func (p *MLXSupervisedProvider) logPath(stream string) string {
	home, _ := homeDir()
	return filepath.Join(home, "Library", "Logs", "cogos", p.launchdLabel+"."+stream+".log")
}

// serviceDef builds the ServiceDef shape required by ServiceSupervisor.
func (p *MLXSupervisedProvider) serviceDef() ServiceDef {
	return ServiceDef{
		Kind:    ServiceKindManaged,
		Port:    p.port,
		Launchd: p.launchdLabel,
		Health:  fmt.Sprintf("http://127.0.0.1:%d/v1/models", p.port),
		Restart: "always",
	}
}

// UpdateCachedStatus stores a fresh launchd status snapshot (called from
// Reconcile paths so Health() can reflect it without re-probing launchd).
func (p *MLXSupervisedProvider) UpdateCachedStatus(st *ServiceStatus) {
	p.mu.Lock()
	p.lastStatus = st
	p.mu.Unlock()
}

// ── Plist XML helpers ─────────────────────────────────────────────────────────

// mlxPlist is the minimal launchd plist structure we generate.
type mlxPlist struct {
	Label             string
	ProgramArguments  []string
	RunAtLoad         bool
	KeepAlive         bool
	StandardOutPath   string
	StandardErrorPath string
}

// marshalPlist renders a mlxPlist as a launchd XML property list.
// We hand-roll the XML rather than importing a plist library to keep the
// dependency footprint zero (the format is simple and stable).
func marshalPlist(p mlxPlist) ([]byte, error) {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	sb.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	sb.WriteString(`<plist version="1.0">` + "\n")
	sb.WriteString("<dict>\n")
	plistKey(&sb, "Label", p.Label)
	sb.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	for _, arg := range p.ProgramArguments {
		sb.WriteString("\t\t<string>" + xmlEscape(arg) + "</string>\n")
	}
	sb.WriteString("\t</array>\n")
	plistBool(&sb, "RunAtLoad", p.RunAtLoad)
	plistBool(&sb, "KeepAlive", p.KeepAlive)
	if p.StandardOutPath != "" {
		plistKey(&sb, "StandardOutPath", p.StandardOutPath)
	}
	if p.StandardErrorPath != "" {
		plistKey(&sb, "StandardErrorPath", p.StandardErrorPath)
	}
	sb.WriteString("</dict>\n</plist>\n")
	return []byte(sb.String()), nil
}

func plistKey(sb *strings.Builder, key, value string) {
	sb.WriteString("\t<key>" + xmlEscape(key) + "</key>\n")
	sb.WriteString("\t<string>" + xmlEscape(value) + "</string>\n")
}

func plistBool(sb *strings.Builder, key string, value bool) {
	sb.WriteString("\t<key>" + key + "</key>\n")
	if value {
		sb.WriteString("\t<true/>\n")
	} else {
		sb.WriteString("\t<false/>\n")
	}
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

// ── Config option helpers ─────────────────────────────────────────────────────

// optStr extracts a string value from the options map.
func optStr(opts map[string]interface{}, key string) string {
	if opts == nil {
		return ""
	}
	v, ok := opts[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// extractPort parses the port from an "http://host:port" endpoint string.
// Returns defaultPort if parsing fails.
func extractPort(endpoint string, defaultPort int) int {
	if endpoint == "" {
		return defaultPort
	}
	// Strip scheme.
	s := endpoint
	if idx := strings.Index(s, "://"); idx >= 0 {
		s = s[idx+3:]
	}
	// Strip path.
	if idx := strings.Index(s, "/"); idx >= 0 {
		s = s[:idx]
	}
	// Extract port.
	if idx := strings.LastIndex(s, ":"); idx >= 0 {
		portStr := s[idx+1:]
		port := 0
		for _, c := range portStr {
			if c < '0' || c > '9' {
				return defaultPort
			}
			port = port*10 + int(c-'0')
		}
		if port > 0 {
			return port
		}
	}
	return defaultPort
}

// ── HTTP probe helper (used by daemon-side provider) ─────────────────────────

// probeMLXEndpoint performs a GET /v1/models against the given endpoint and
// returns true when the response is 200 and the body contains model data.
// Used by the daemon-side Health() stub and by Available().
func probeMLXEndpoint(ctx context.Context, endpoint, model string) bool {
	url := strings.TrimRight(endpoint, "/") + "/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return false
	}
	defer resp.Body.Close()
	if model == "" {
		return true
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false
	}
	for _, m := range result.Data {
		if m.ID == model || strings.HasPrefix(m.ID, model) || strings.HasPrefix(model, m.ID) {
			return true
		}
	}
	return false
}
