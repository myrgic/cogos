// cli_doctor_modelprobe.go: opt-in live probe of every configured model.
//
// Observed 2026-11-08: Anthropic refused claude-opus-5-5 ("Claude Code 2.1.74
// does not support this model; version 2.1.280 or newer is required") only at
// call time, so no static check saw it. With --live, doctor sends one
// max_tokens=1 chat completion through the LOCAL kernel for each distinct
// model actually configured for use and reports per-model OK / refused with
// the provider's error code. Default runs report UNKNOWN and name the flag.
//
// Model sources: Hermes profile model.default (only when the profile routes
// through this kernel), the kernel's own alias ids as advertised by the live
// GET /v1/models (falling back to the compiled intentAliases table, said so),
// STAGE_MODEL_* in stage.env files, and the CURRENT stage per-room preferences
// in rooms.json next to each stage.env (rooms[].prefs.model and
// prefs.model_catalog_choice; never board/work_queue history). Models that
// run outside the kernel (opencode/*, a Hermes profile on another provider)
// are not probed and are named. The set is capped (modelProbeCap, in
// source-priority order); skipped models and every source that could not be
// scanned are named in the output.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const modelProbeCap = 12

// modelProbeTimeout is a var so a test can shorten it.
var modelProbeTimeout = 45 * time.Second

type probeModel struct {
	id      string
	sources []string
}

// kernelAliasProbeIDs picks the kernel aliases to probe (resolved by the
// running kernel, not this binary's table): the live /v1/models ids that are
// aliases, plus every override-bearing alias this doctor knows. The kernel
// accepts haiku/sonnet/opus/fable without advertising them in /v1/models, so
// catalog membership alone would never exercise them.
func kernelAliasProbeIDs(catalog []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(id string) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range catalog {
		if id == "foreground" || id == "deliberation" || id == "local" || intentAliases[id].ModelOverride != "" {
			add(id)
		}
	}
	for id, r := range intentAliases {
		if r.ModelOverride != "" {
			add(id)
		}
	}
	sort.Strings(out)
	return out
}

// unprobedKernelAliases are kernel alias ids with no intentAliases override
// that have no single concrete model to probe; they are always named in the
// coverage line (the kernel accepts them without listing them in /v1/models).
var unprobedKernelAliases = []string{"claude", "codex", "kernel-agent", "ollama"}

// hermesModelKernelRouted reports whether profile p's model.default is sent
// through the kernel at kernelPort: model.provider is cogos, or model.base_url
// is a local address on the kernel's port. why names the actual route otherwise.
func hermesModelKernelRouted(p hermesProfile, kernelPort int) (bool, string) {
	prov := strings.ToLower(strings.TrimSpace(p.ModelProvider))
	if prov == "cogos" || prov == "custom:cogos" {
		return true, ""
	}
	if bu := strings.TrimSpace(p.ModelBaseURL); bu != "" {
		host, port := splitHostPort(bu)
		if n, _ := strconv.Atoi(port); isLocalHost(host) && n == kernelPort {
			return true, ""
		}
		return false, "base_url " + bu
	}
	if prov == "" {
		return false, "no model.provider"
	}
	return false, "provider " + prov
}

// configuredProbeModels returns the distinct configured models in
// source-priority order (Hermes, stage, kernel aliases). catalog is the live
// /v1/models id list (nil when not fetched).
func configuredProbeModels(root string, opts DoctorOptions, catalog []string) ([]probeModel, []string) {
	var notes []string
	h := opts.doctorHostOrDefault()
	idx := map[string]*probeModel{}
	var order []*probeModel
	add := func(id, src string) {
		id = strings.TrimSpace(id)
		if id == "" || strings.Contains(id, "${") {
			return
		}
		m, ok := idx[id]
		if !ok {
			m = &probeModel{id: id}
			idx[id] = m
			order = append(order, m)
		}
		m.sources = append(m.sources, src)
	}
	kport := kernelPortForDoctor(root, opts)
	var unrouted []string
	for _, p := range loadHermesProfiles(h.home) {
		if p.ModelDefault == "" {
			continue
		}
		if ok, why := hermesModelKernelRouted(p, kport); !ok {
			unrouted = append(unrouted, fmt.Sprintf("hermes profile %s model.default %s (%s)", p.Name, p.ModelDefault, why))
			continue
		}
		add(p.ModelDefault, "hermes profile "+p.Name+" model.default")
	}
	stageFiles := stageEnvFiles(h.home)
	if len(stageFiles) == 0 {
		notes = append(notes, "stage room model preferences (rooms.json): no stage state dir discovered (set STAGE_STATE)")
	}
	for _, f := range stageFiles {
		roomsPath := filepath.Join(filepath.Dir(f), "rooms.json")
		if rooms, err := os.ReadFile(roomsPath); err != nil {
			notes = append(notes, "stage room model preferences: "+roomsPath+" not readable")
		} else {
			ids, outside := roomModelsFromJSON(rooms)
			for _, id := range ids {
				add(id, "stage room preference in "+roomsPath)
			}
			for _, id := range outside {
				unrouted = append(unrouted, "stage room preference "+id+" (runs outside the kernel)")
			}
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		kv := parseDotenvMap(string(raw))
		var keys []string
		for k := range kv {
			if strings.HasPrefix(k, "STAGE_MODEL_") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			add(kv[k], k+" in "+f)
		}
	}
	if catalog != nil {
		// Probe the alias ids themselves: the running kernel resolves them,
		// so its own default and alias table is what gets exercised.
		for _, a := range kernelAliasProbeIDs(catalog) {
			add(a, "kernel alias "+a+" (live /v1/models)")
		}
		notes = append(notes, "kernel aliases accepted but not probed (no concrete model to exercise): "+strings.Join(unprobedKernelAliases, ", "))
	} else {
		var aliases []string
		for a := range intentAliases {
			aliases = append(aliases, a)
		}
		sort.Strings(aliases)
		for _, a := range aliases {
			if t := intentAliases[a].ModelOverride; t != "" {
				add(t, "kernel alias "+a)
			}
		}
		notes = append(notes, "the kernel's own default model and live alias table (aliases above come from this doctor binary; --live reads the running kernel's /v1/models)")
	}
	if len(unrouted) > 0 {
		sort.Strings(unrouted)
		notes = append(notes, "not kernel-routed, not probed: "+strings.Join(unrouted, ", "))
	}
	out := make([]probeModel, 0, len(order))
	for _, m := range order {
		out = append(out, *m)
	}
	return out, notes
}

// roomModelsFromJSON extracts the models a stage rooms.json CURRENTLY selects,
// in document order and without duplicates: rooms[].prefs.model and
// prefs.model_catalog_choice ("custom:cogos:" prefix stripped). Board and
// work_queue history is ignored. Short aliases (sonnet/opus/haiku, any kernel
// alias) map to their intentAliases target. Ids that run outside the kernel
// (opencode/*, opencode's own free tier) are returned apart in outside.
func roomModelsFromJSON(data []byte) (models, outside []string) {
	var doc any
	if json.Unmarshal(data, &doc) != nil {
		return nil, nil
	}
	seen := map[string]bool{}
	emit := func(v any) {
		id, ok := v.(string)
		if !ok {
			return
		}
		id = strings.TrimPrefix(strings.TrimSpace(id), "custom:cogos:")
		if t := intentAliases[id].ModelOverride; t != "" {
			id = t
		}
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		if strings.HasPrefix(id, "opencode/") {
			outside = append(outside, id)
			return
		}
		models = append(models, id)
	}
	var walk func(v any, parent string)
	walk = func(v any, parent string) {
		switch x := v.(type) {
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if k == "board" || k == "work_queue" {
					continue // task history, not configuration
				}
				if parent == "prefs" && (k == "model" || k == "model_catalog_choice") {
					emit(x[k])
				}
				walk(x[k], k)
			}
		case []any:
			for _, e := range x {
				walk(e, parent)
			}
		}
	}
	walk(doc, "")
	return models, outside
}

// kernelCatalogIDs fetches the ids the running kernel advertises at
// GET /v1/models (nil when unreachable or unparsable).
func kernelCatalogIDs(endpoint string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/models", nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := readLimited(resp.Body, 4<<20)
	if resp.StatusCode/100 != 2 {
		return nil
	}
	var l aliasCatalogEntryList
	if json.Unmarshal(body, &l) != nil || len(l.Data) == 0 {
		return nil
	}
	ids := make([]string, 0, len(l.Data))
	for _, m := range l.Data {
		ids = append(ids, m.ID)
	}
	return ids
}

func doctorModelProbes(g *DoctorGroup, root string, opts DoctorOptions) {
	const name = "model probe"
	var catalog []string
	if opts.Live && !opts.SkipNetwork {
		ep := opts.kernelEndpoint
		if ep == "" {
			ep = kernelEndpointForDoctor(root)
		}
		catalog = kernelCatalogIDs(ep)
	}
	models, notes := configuredProbeModels(root, opts, catalog)
	unscanned := ""
	if len(notes) > 0 {
		unscanned = " Not scanned: " + strings.Join(notes, "; ") + "."
	}
	if !opts.Live {
		ids := make([]string, 0, len(models))
		for _, m := range models {
			ids = append(ids, m.id)
		}
		g.add(name, StatusUnknown, fmt.Sprintf(
			"not run: provider refusals (e.g. a Claude Code version too old for a model) only show at call time. %d distinct configured model(s): %s. "+
				"Fix: cogos doctor --live (sends max_tokens=1 per model through the local kernel; spends a few tokens each)",
			len(models), strings.Join(ids, ", "))+unscanned)
		return
	}
	if opts.SkipNetwork {
		g.add(name, StatusUnknown, "skipped (--skip-network)")
		return
	}
	if len(models) == 0 {
		g.add(name, StatusUnknown, "no configured models found (Hermes profiles, stage.env, kernel aliases)")
		return
	}
	endpoint := opts.kernelEndpoint
	if endpoint == "" {
		endpoint = kernelEndpointForDoctor(root)
	}
	grantPath := opts.grantPath
	if grantPath == "" {
		p, err := nodeRootVaultPath()
		if err != nil {
			g.add(name, StatusUnknown, err.Error())
			return
		}
		grantPath = p
	}
	raw, err := os.ReadFile(grantPath)
	if err != nil || strings.TrimSpace(string(raw)) == "" {
		g.add(name, StatusUnknown, fmt.Sprintf("no node-root grant readable at %s, cannot authenticate the probe", grantPath))
		return
	}
	token := strings.TrimSpace(string(raw))

	probe, skipped := models, []probeModel(nil)
	if len(models) > modelProbeCap {
		probe, skipped = models[:modelProbeCap], models[modelProbeCap:]
	}
	for _, m := range probe {
		st, detail := probeOneModel(endpoint, token, m.id)
		g.add(name+": "+m.id, st, fmt.Sprintf("%s (configured by: %s)", detail, strings.Join(m.sources, "; ")))
	}
	if len(skipped) > 0 {
		var ids []string
		for _, m := range skipped {
			ids = append(ids, m.id)
		}
		g.add(name+": skipped", StatusUnknown, fmt.Sprintf("probe cap is %d; not probed: %s", modelProbeCap, strings.Join(ids, ", ")))
	}
	if unscanned != "" {
		g.add(name+": coverage", StatusUnknown, strings.TrimSpace(unscanned))
	}
}

// probeOneModel sends a max_tokens=1 chat completion for model through the
// kernel at endpoint and classifies the answer. The token is only ever sent
// as the Authorization header.
func probeOneModel(endpoint, token, model string) (DoctorStatus, string) {
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": 1,
		"stream":     false,
		"messages":   []map[string]string{{"role": "user", "content": "."}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), modelProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return StatusUnknown, "bad probe request: " + err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return StatusUnknown, fmt.Sprintf("no answer from %s within %s; the kernel or its upstream provider may be hung (not a refusal)", endpoint, modelProbeTimeout)
		}
		return StatusUnknown, fmt.Sprintf("kernel unreachable at %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	rb, _ := readLimited(resp.Body, 1<<16)
	if resp.StatusCode/100 == 2 {
		return StatusOK, "accepted a max_tokens=1 request"
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return StatusUnknown, "kernel rejected the doctor's node-root grant (HTTP 401); model not tested. Fix: cogos doctor external-client checks above"
	}
	return classifyProbeFailure(resp.StatusCode, rb)
}

var kernelUpstreamStatusRE = regexp.MustCompile(`status (\d{3})`)

// classifyProbeFailure maps a non-2xx kernel answer to a verdict. The kernel
// wraps every upstream failure as HTTP 500 {type: server_error, code: null}
// and carries the provider's status and body inside the message
// ("claude-oauth: status 400: {...}"), so the provider's own status and error
// type are recovered from the message. A provider 4xx is a refusal (FAIL);
// 5xx, 429 and 408 are transient and leave the model UNKNOWN.
func classifyProbeFailure(httpStatus int, body []byte) (DoctorStatus, string) {
	var e struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	msg := e.Error.Message
	status := httpStatus
	code := e.Error.Type
	if e.Error.Code != nil {
		code = fmt.Sprint(e.Error.Code)
	}
	if m := kernelUpstreamStatusRE.FindStringSubmatch(msg); m != nil {
		status, _ = strconv.Atoi(m[1])
		if i := strings.Index(msg, "{"); i >= 0 {
			var inner struct {
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal([]byte(msg[i:]), &inner) == nil && inner.Error.Message != "" {
				msg = inner.Error.Message
				if inner.Error.Type != "" {
					code = inner.Error.Type
				}
			}
		}
	}
	if len(msg) > 500 {
		msg = msg[:500] + "..."
	}
	if status >= 500 || status == http.StatusTooManyRequests || status == http.StatusRequestTimeout {
		return StatusUnknown, fmt.Sprintf("inconclusive, transient provider/kernel error: HTTP %d code=%s: %s", status, code, msg)
	}
	return StatusFail, fmt.Sprintf("refused: HTTP %d code=%s: %s", status, code, msg)
}
