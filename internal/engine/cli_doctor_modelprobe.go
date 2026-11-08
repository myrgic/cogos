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
// (intentAliases), and STAGE_MODEL_* in stage.env files. Stage per-room
// preferences (rooms.json prefs.model) are not scanned. The set is capped
// (modelProbeCap, in source-priority order) and the skipped models are named.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
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
func configuredProbeModels(opts DoctorOptions) []probeModel {
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
	for _, f := range stageEnvFiles(h.home) {
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
	return out
}

func doctorModelProbes(g *DoctorGroup, root string, opts DoctorOptions) {
	const name = "model probe"
	models := configuredProbeModels(opts)
	if !opts.Live {
		ids := make([]string, 0, len(models))
		for _, m := range models {
			ids = append(ids, m.id)
		}
		g.add(name, StatusUnknown, fmt.Sprintf(
			"not run: provider refusals (e.g. a Claude Code version too old for a model) only show at call time. %d distinct configured model(s): %s. "+
				"Fix: cogos doctor --live (sends max_tokens=1 per model through the local kernel; spends a few tokens each)",
			len(models), strings.Join(ids, ", ")))
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
	var e struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(rb, &e)
	msg := e.Error.Message
	if len(msg) > 200 {
		msg = msg[:200] + "..."
	}
	code := fmt.Sprint(e.Error.Code)
	if e.Error.Code == nil {
		code = e.Error.Type
	}
	return StatusFail, fmt.Sprintf("refused: HTTP %d code=%s: %s", resp.StatusCode, code, msg)
}
