// lms_model_state.go — daemon-side health provider for the LM Studio
// model/context-state reconciler.
//
// lmsModelStateProvider implements reconcile.Reconcilable for the daemon's
// proprioception block. It scans providers(.local).yaml for any dispatch backend
// carrying an options.model_state block with manage:true. For each managed
// backend it performs a read-only GET /api/v0/models probe and reports whether
// the declared model is loaded at the declared context length.
//
// If no backend opts in (no model_state.manage:true), Health() returns
// HealthSuspended (NOT HealthMissing) — this is an opt-in feature, not a missing
// requirement. This mirrors mlx_inference.go's precedent exactly.
//
// A managed backend that is unreachable (box off, off-LAN) also reports
// HealthSuspended, not HealthDegraded — matching the engine-layer
// LMSModelStateProvider's precedent ("do NOT self-heal a box that is simply
// off or unreachable"). Only a REACHABLE backend reporting the wrong
// model/context/state is HealthDegraded. Before this split (fixed 2026-10),
// both cases folded into Degraded here, which autonomic_ticker's
// healDegradedProviders treats as actionable — so an unreachable LAN box
// (Eclipse) drove an unbounded self-heal reconcile loop (measured: ~79k
// cycles, thousands of escalations to claude-oauth) against a condition no
// self-heal action could ever resolve. See the go-cogos-kernel-patterns
// skill's "degenerate status signals" pattern.
//
// GUARDRAILS: this daemon stub is Health()-only and strictly read-only. It never
// loads or unloads a model. Full plan/apply lives in the engine-layer
// LMSModelStateProvider (constructed by BuildRouter). The daemon only exercises
// Health() through the proprioception block.
//
// Registration: init() registers this provider as "lms-model-state" so the
// providers/all import chain triggers it automatically.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// unreachableError marks a probe failure caused by the backend being off or
// off-LAN, as opposed to a reachable backend reporting the wrong model/state.
// Health() uses errors.As to route these to Suspended instead of Degraded —
// see the package doc comment and the engine-layer precedent this mirrors
// (provider_lms_model_state.go's lastErr ⇒ Suspended mapping, "do NOT
// self-heal a box that is simply off or unreachable").
type unreachableError struct{ err error }

func (e *unreachableError) Error() string { return "unreachable: " + e.err.Error() }
func (e *unreachableError) Unwrap() error { return e.err }

func init() {
	reconcile.RegisterProvider("lms-model-state", &lmsModelStateProvider{stubMethods: stubMethods{name: "lms-model-state"}})
}

// lmsModelStateProvider is the daemon-side Reconcilable for LM Studio model-state.
type lmsModelStateProvider struct {
	stubMethods
}

func (p *lmsModelStateProvider) Type() string { return "lms-model-state" }

// Health inspects all opted-in backends and returns the aggregate status.
// Called by buildHealthBlock on every foveated context generation; kept cheap.
func (p *lmsModelStateProvider) Health() reconcile.ResourceStatus {
	root, bad := resolveRoot()
	if bad != nil {
		return *bad
	}

	entries := loadModelStateEntries(root)
	if len(entries) == 0 {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthSuspended,
			Operation: reconcile.OperationIdle,
			Message:   "no backend declares options.model_state.manage:true — opt-in feature",
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	var issues []string
	var unreachable []string
	var gapNotes []string
	var anyProgressing bool
	for _, e := range entries {
		progressing, gapNote, err := probeModelStateEntry(ctx, e)
		var unreachErr *unreachableError
		switch {
		case errors.As(err, &unreachErr):
			unreachable = append(unreachable, fmt.Sprintf("%s: %v", e.name, err))
		case err != nil:
			issues = append(issues, fmt.Sprintf("%s: %v", e.name, err))
		case progressing:
			anyProgressing = true
		case gapNote != "":
			gapNotes = append(gapNotes, gapNote)
		}
	}

	if len(issues) > 0 {
		// Reachable backend, wrong model/context/state — drift a self-heal
		// action could plausibly fix. OutOfSync/Degraded is correct here.
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusOutOfSync,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   healthIssuesMessage(issues, gapNotes),
		}
	}

	if len(unreachable) > 0 {
		// Backend is off or off-LAN. No self-heal action fixes "the box is
		// off" — mirror the engine-layer provider's Suspended mapping (see
		// provider_lms_model_state.go: "do NOT self-heal a box that is
		// simply off or unreachable"). Reporting this as Degraded is exactly
		// degenerate-status-signal bug class: a status that can only
		// ever read one way isn't instrumentation, and autonomic_ticker's
		// healDegradedProviders treats Degraded as actionable, so an
		// unreachable LAN box drove a self-heal reconcile cycle roughly
		// every minute indefinitely (measured 2026-10-01: 79,140 bare
		// lms-model-state "starting reconcile cycle" lines; 4,085
		// degraded_health escalations to claude-oauth in 24h while Eclipse
		// was down). If some backends are unreachable and others merely
		// report gap notes, that's still an incomplete-but-not-actionable
		// watch — Suspended, not Degraded.
		msg := strings.Join(unreachable, "; ")
		if len(gapNotes) > 0 {
			msg += "; " + strings.Join(gapNotes, "; ")
		}
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthSuspended,
			Operation: reconcile.OperationIdle,
			Message:   msg,
		}
	}

	if anyProgressing {
		// A managed model is mid-load (state=="loading"). That is Progressing, not
		// a problem — a high-context load can take minutes. Matches the engine
		// provider's loading→Progressing mapping so proprioception doesn't false-alarm.
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthProgressing,
			Operation: reconcile.OperationWaiting,
			Message:   "model load in progress",
		}
	}

	noun := "backend"
	if len(entries) != 1 {
		noun = "backends"
	}
	msg := fmt.Sprintf("%d model_state %s at target", len(entries), noun)
	if len(gapNotes) > 0 {
		// Surface the coverage gap even on an otherwise-clean cycle: a declared
		// parallel target that this daemon cannot actually watch (remote
		// backend, or a dead/missing local lms CLI) must not present as full
		// coverage. See probeModelStateEntry's gapNote doc.
		msg += "; " + strings.Join(gapNotes, "; ")
	}
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusSynced,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
		Message:   msg,
	}
}

// healthIssuesMessage joins the Degraded-path issues and gapNotes into a
// single message. gapNotes from OTHER entries must not be dropped just
// because some entry in the same cycle reported a louder issue — otherwise a
// coverage gap on backend B (e.g. its lms CLI is missing, so its parallel
// watch is dead) goes invisible for exactly as long as backend A is also
// loudly unreachable, defeating the reason gap notes exist: never present a
// dead watch as clean/silent coverage. See probeModelStateEntry's gapNote doc.
func healthIssuesMessage(issues, gapNotes []string) string {
	msg := strings.Join(issues, "; ")
	if len(gapNotes) > 0 {
		msg += "; " + strings.Join(gapNotes, "; ")
	}
	return msg
}

// modelStateEntry is the minimal config needed for a daemon-side health probe.
type modelStateEntry struct {
	name          string
	endpoint      string
	apiKeyEnv     string
	model         string
	contextLength int
	parallel      int // 0 ⇒ don't watch parallel

	// local and lmsCLIPath gate the parallel probe (see checkParallelDrift):
	// `lms ps --json` is a local-only CLI (no --host flag), so parallel drift is
	// only observable on a localhost endpoint. Computed in loadModelStateEntries
	// from the endpoint host; test-constructed modelStateEntry{} literals may set
	// these directly for injection (no test-only global needed).
	local      bool
	lmsCLIPath string
}

// loadModelStateEntries reads providers(.local).yaml and returns entries whose
// options.model_state.manage is true. providers.local.yaml overlays the base.
func loadModelStateEntries(root string) []modelStateEntry {
	var result []modelStateEntry

	for _, filename := range []string{"providers.yaml", "providers.local.yaml"} {
		path := filepath.Join(root, ".cog", "config", filename)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		entries := parseModelStateEntriesFromYAML(data)
		existing := make(map[string]int, len(result))
		for i, e := range result {
			existing[e.name] = i
		}
		for _, e := range entries {
			if idx, ok := existing[e.name]; ok {
				result[idx] = e
			} else {
				result = append(result, e)
				existing[e.name] = len(result) - 1
			}
		}
	}

	lmsCLI := resolveLmsCLIPath()
	for i := range result {
		result[i].local = isLocalHostEndpoint(result[i].endpoint)
		result[i].lmsCLIPath = lmsCLI
	}
	return result
}

// isLocalHostEndpoint reports whether endpoint's host is loopback (or empty,
// which probeModelStateEntry defaults to localhost). Duplicates the small
// loopback check in internal/engine/provider_lms_model_state.go's isLocalHost
// rather than cross-importing engine from daemon.
//
// Host is lowercased before comparison so an uppercase scheme/host (e.g.
// "http://LOCALHOST:1234", which providers.yaml does not forbid) still
// matches — a case-sensitive miss here would silently disable the parallel
// watch on an otherwise-local backend with no annotation (the same
// invisibility class as the gap notes above guard against).
func isLocalHostEndpoint(endpoint string) bool {
	host := endpoint
	if idx := strings.Index(host, "://"); idx >= 0 {
		host = host[idx+3:]
	}
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	host = stripHostPort(host)
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1", "[::1]", "":
		return true
	}
	return false
}

// stripHostPort removes a trailing ":<port>" from host, bracket-aware for an
// IPv6 literal. A naive strings.LastIndex(host, ":") cuts inside the brackets
// on a bracketed literal with no port suffix — "[::1]" has its LAST colon at
// index 2, so a bare LastIndex split yields "[:" (not "[::1]"), silently
// failing the loopback match ("http://[::1]" and "http://[::1]/v1" would both
// report false). With a port present the naive split works only by accident,
// because the LastIndex happens to land on the port-separator colon rather
// than an internal IPv6 colon. Handling the bracketed case explicitly (locate
// the closing ']', keep everything through it) fixes both.
func stripHostPort(host string) string {
	if strings.HasPrefix(host, "[") {
		if end := strings.Index(host, "]"); end >= 0 {
			return host[:end+1]
		}
		return host // malformed literal — no closing bracket; leave as-is
	}
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		return host[:idx]
	}
	return host
}

// resolveLmsCLIPath returns the local lms CLI fast-path binary path,
// best-effort. An empty result disables the parallel probe (checkParallelDrift
// treats it as unavailable — not fatal).
func resolveLmsCLIPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".lmstudio", "bin", "lms")
}

// msProviderFileCfg is the top-level shape used for the daemon-side model_state
// parse. Only fields relevant to the probe are decoded.
type msProviderFileCfg struct {
	Providers map[string]msProviderEntryCfg `yaml:"providers"`
}

type msProviderEntryCfg struct {
	Endpoint  string       `yaml:"endpoint"`
	APIKeyEnv string       `yaml:"api_key_env"`
	Options   msOptionsCfg `yaml:"options"`
}

type msOptionsCfg struct {
	ModelState msModelStateCfg `yaml:"model_state"`
}

type msModelStateCfg struct {
	Manage        bool   `yaml:"manage"`
	Model         string `yaml:"model"`
	ContextLength int    `yaml:"context_length"`
	Parallel      int    `yaml:"parallel"`
}

// parseModelStateEntriesFromYAML returns entries with model_state.manage:true.
func parseModelStateEntriesFromYAML(data []byte) []modelStateEntry {
	var cfg msProviderFileCfg
	if err := yaml.Unmarshal(data, &cfg); err != nil || cfg.Providers == nil {
		return nil
	}
	var entries []modelStateEntry
	for name, p := range cfg.Providers {
		if !p.Options.ModelState.Manage {
			continue
		}
		entries = append(entries, modelStateEntry{
			name:          name,
			endpoint:      p.Endpoint,
			apiKeyEnv:     p.APIKeyEnv,
			model:         p.Options.ModelState.Model,
			contextLength: p.Options.ModelState.ContextLength,
			parallel:      p.Options.ModelState.Parallel,
		})
	}
	return entries
}

// probeModelStateEntry checks one backend: read /api/v0/models and report whether
// the declared model is loaded at the declared context. Read-only.
//
// Returns (progressing, gapNote, err). progressing==true means the model is
// mid-load (state=="loading"), which is Progressing — NOT a health issue: a
// high-context load can take minutes and must never read as Degraded. A
// non-nil err is a real problem (unreachable / wrong context / not loaded /
// absent). gapNote is non-empty when a parallel target is declared but this
// entry could not actually be watched for parallel drift (remote backend, or
// the local probe produced no observation) — Health() surfaces it even on an
// otherwise-clean cycle so the watch's coverage never silently degrades to
// zero while still reporting Healthy. Mirrors the engine provider's
// findModelRow (prefer the state=="loaded" row) and ComputePlan (loading is
// not drift) so the daemon proprioception matches the engine's Health().
func probeModelStateEntry(ctx context.Context, e modelStateEntry) (progressing bool, gapNote string, err error) {
	base := strings.TrimRight(e.endpoint, "/")
	if base == "" {
		base = "http://localhost:1234"
	}
	url := base + "/api/v0/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, "", fmt.Errorf("build request: %w", err)
	}
	if e.apiKeyEnv != "" {
		if tok := os.Getenv(e.apiKeyEnv); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, "", &unreachableError{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, "", fmt.Errorf("/api/v0/models returned %d", resp.StatusCode)
	}

	var out struct {
		Data []struct {
			ID                  string `json:"id"`
			State               string `json:"state"`
			LoadedContextLength *int   `json:"loaded_context_length"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, "", fmt.Errorf("decode: %w", err)
	}

	// Collect id-matching rows, preferring state=="loaded" so a not-loaded/loading
	// duplicate row cannot shadow the real loaded one (LM Studio can return both).
	loadedIdx, loadingIdx, otherIdx := -1, -1, -1
	for i := range out.Data {
		m := out.Data[i]
		if m.ID != e.model && !strings.HasPrefix(m.ID, e.model) && !strings.HasPrefix(e.model, m.ID) {
			continue
		}
		switch m.State {
		case "loaded":
			if loadedIdx == -1 {
				loadedIdx = i
			}
		case "loading":
			if loadingIdx == -1 {
				loadingIdx = i
			}
		default:
			if otherIdx == -1 {
				otherIdx = i
			}
		}
	}

	switch {
	case loadedIdx != -1:
		m := out.Data[loadedIdx]
		if e.contextLength > 0 && (m.LoadedContextLength == nil || *m.LoadedContextLength != e.contextLength) {
			got := "null"
			if m.LoadedContextLength != nil {
				got = fmt.Sprintf("%d", *m.LoadedContextLength)
			}
			return false, "", fmt.Errorf("model %q loaded at context %s (want %d)", e.model, got, e.contextLength)
		}
		if e.parallel > 0 {
			if !e.local {
				gapNote = fmt.Sprintf("%s: parallel target declared but backend is remote — not observable via lms ps", e.name)
			} else {
				observed, perr := checkParallelDrift(ctx, e, m.ID)
				if perr != nil {
					return false, "", perr
				}
				if !observed {
					gapNote = fmt.Sprintf("%s: parallel target declared but not observed — lms ps probe failed, lms CLI missing, or no matching row", e.name)
				}
			}
		}
		return false, gapNote, nil
	case loadingIdx != -1:
		return true, "", nil // mid-load — Progressing, not an issue
	case otherIdx != -1:
		return false, "", fmt.Errorf("model %q state=%q (want loaded)", e.model, out.Data[otherIdx].State)
	default:
		return false, "", fmt.Errorf("model %q not present", e.model)
	}
}

// msPsRow mirrors `lms ps --json`'s row shape (the local-only lms CLI — no
// --host flag, so remote backends cannot be probed this way). Confirmed live:
// {"identifier":"example-35b",...,"parallel":1}.
//
// Parallel is a pointer, mirroring lmsPsRow in the engine copy: an older lms
// CLI (or any future shape change) that omits the `parallel` key must decode
// to nil (unobserved), never to the Go zero value 0 — a bare int would
// silently promote "key absent" into "observed 0" and falsely alarm a
// correctly-loaded backend.
type msPsRow struct {
	Identifier string `json:"identifier"`
	ModelKey   string `json:"modelKey"`
	Parallel   *int   `json:"parallel"`
}

// msParallelProbeTimeout bounds the `lms ps --json` shell-out. Deliberately its
// own short budget rather than sharing the outer 4s Health() timeout across all
// entries — a hung lms CLI must not block the whole proprioception cycle.
const msParallelProbeTimeout = 2 * time.Second

// checkParallelDrift shells `lms ps --json` and compares the observed parallel
// value for the row matching loadedID (or e.model, prefix-either-direction —
// mirrors the /api/v0/models matching above) against e.parallel. A probe
// failure (binary missing, non-zero exit, bad JSON), no matching row, or a
// matching row whose `parallel` key was omitted is NON-FATAL — skip the check
// rather than erroring, the same "unobserved, not wrong" treatment the
// engine-side provider gives a nil Parallel. The err return is non-nil ONLY on
// a genuine observed mismatch, which folds into the existing (progressing,
// err) issues aggregation in Health().
//
// The observed return distinguishes "checked and matched" from "could not
// check at all" — probeModelStateEntry uses it to attach a gap note when a
// parallel target is declared but this probe produced no observation, so a
// dead/missing lms CLI degrades the watch's coverage visibly instead of
// silently reporting clean.
func checkParallelDrift(ctx context.Context, e modelStateEntry, loadedID string) (observed bool, err error) {
	if e.lmsCLIPath == "" {
		return false, nil
	}
	psCtx, cancel := context.WithTimeout(ctx, msParallelProbeTimeout)
	defer cancel()
	out, cmdErr := exec.CommandContext(psCtx, e.lmsCLIPath, "ps", "--json").Output()
	if cmdErr != nil {
		return false, nil // lms CLI unavailable — unobserved, not an issue
	}
	var rows []msPsRow
	if json.Unmarshal(out, &rows) != nil {
		return false, nil // unparseable — unobserved
	}

	row := matchParallelRow(rows, loadedID, e.model)
	if row == nil {
		return false, nil // no matching row — unobserved
	}
	if row.Parallel == nil {
		return false, nil // `parallel` key omitted — unobserved, not a mismatch
	}
	if *row.Parallel != e.parallel {
		return true, fmt.Errorf("model %q loaded with parallel %d (want %d)", e.model, *row.Parallel, e.parallel)
	}
	return true, nil
}

// matchParallelRow selects the `lms ps --json` row to compare against,
// preferring an exact identifier match (against loadedID, then e.model) over
// a prefix match — mirrors the engine copy's mergeParallel exactly, so both
// provider copies agree on identical live state.
//
// The exact tier is two separate passes, not one OR-combined pass: pass 1
// scans for id == loadedID, pass 2 scans for id == model, and pass 1 always
// wins when both are present in the row set. loadedID is the live row's own
// identifier (resolved from /api/v0/models), so a row that IS the loaded
// instance must take priority over a same-named sibling row that merely
// happens to match e.model's declared name. A single OR-combined pass is
// order-dependent: when loadedID != e.model and some OTHER row's id equals
// e.model exactly (e.g. two models of one family are loaded and one is
// literally named the declared target), whichever of the two rows appears
// first in `lms ps --json`'s array wins — and that array order is not a
// matching signal, it reflects whatever order the CLI happens to return rows
// in. Two ordered passes make the winner id-driven instead of order-driven.
//
// `lms ps --json`'s array order is likewise not a matching signal for the
// prefix fallback below — it can and does put a duplicate instance's row
// (e.g. "target:2", suffixed by LM Studio for a second load of the same base
// model) ahead of the exact-id row ("target"). The old code took the first
// row satisfying a broad prefix-either-direction predicate, so which of
// "target" or "target:2" won depended on that incidental ordering rather
// than on which row is actually correct. Preferring an exact match first
// fixes the id regardless of array order; the prefix fallback (same "quant
// suffix / publisher prefix" cases modelIDMatch on the engine side) only
// runs when no exact match exists.
func matchParallelRow(rows []msPsRow, loadedID, model string) *msPsRow {
	rowID := func(r msPsRow) string {
		if r.Identifier != "" {
			return r.Identifier
		}
		return r.ModelKey
	}

	for i := range rows {
		id := rowID(rows[i])
		if id != "" && id == loadedID {
			return &rows[i]
		}
	}
	for i := range rows {
		id := rowID(rows[i])
		if id != "" && id == model {
			return &rows[i]
		}
	}
	for i := range rows {
		id := rowID(rows[i])
		if id == "" {
			continue
		}
		if strings.HasPrefix(id, loadedID) || strings.HasPrefix(loadedID, id) ||
			strings.HasPrefix(id, model) || strings.HasPrefix(model, id) {
			return &rows[i]
		}
	}
	return nil
}
