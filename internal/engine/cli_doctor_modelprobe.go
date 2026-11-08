// cli_doctor_modelprobe.go: opt-in live probe of every configured model.
//
// Observed 2026-11-08: Anthropic refused claude-opus-5-5 ("Claude Code 2.1.74
// does not support this model; version 2.1.280 or newer is required") only at
// call time, so no static check saw it. With --live, doctor sends one
// max_tokens=1 chat completion through the LOCAL kernel for each distinct
// model actually configured for use and reports per-model OK / refused with
// the provider's error code. Default runs report UNKNOWN and name the flag.
//
// Model sources: Hermes profile model.default, the kernel's alias targets
// (intentAliases), STAGE_MODEL_* in stage.env files, and the stage per-room
// preferences in rooms.json next to each stage.env (prefs.model,
// prefs.model_catalog_choice, model_id "custom:cogos:<model>"). The set is
// capped (modelProbeCap, in source-priority order); skipped models and every
// source that could not be scanned are named in the output.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
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

const (
	modelProbeCap     = 12
	modelProbeTimeout = 45 * time.Second
)

type probeModel struct {
	id      string
	sources []string
}

// configuredProbeModels returns the distinct configured models in
// source-priority order (Hermes, stage, kernel aliases).
func configuredProbeModels(opts DoctorOptions) ([]probeModel, []string) {
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
	for _, p := range loadHermesProfiles(h.home) {
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
			for _, id := range roomModelsFromJSON(rooms) {
				add(id, "stage room preference in "+roomsPath)
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
	out := make([]probeModel, 0, len(order))
	for _, m := range order {
		out = append(out, *m)
	}
	notes = append(notes, "the kernel's own default model and live alias table (aliases above come from this doctor binary)")
	return out, notes
}

// roomModelsFromJSON extracts every model a stage rooms.json selects, in
// document order and without duplicates: prefs.model, prefs.model_catalog_choice
// and model_id values ("custom:cogos:" prefix stripped). Short aliases
// (sonnet/opus/haiku, any kernel alias) map to their intentAliases target.
func roomModelsFromJSON(data []byte) []string {
	var doc any
	if json.Unmarshal(data, &doc) != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	emit := func(v any) {
		id, ok := v.(string)
		if !ok {
			return
		}
		id = strings.TrimPrefix(strings.TrimSpace(id), "custom:cogos:")
		if t := intentAliases[id].ModelOverride; t != "" {
			id = t
		}
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
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
				switch {
				case k == "model" && parent == "prefs", k == "model_catalog_choice", k == "model_id":
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
	return out
}

func doctorModelProbes(g *DoctorGroup, root string, opts DoctorOptions) {
	const name = "model probe"
	models, notes := configuredProbeModels(opts)
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
