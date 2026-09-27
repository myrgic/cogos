package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const localLLMEndpointEnv = "COGOS_LLM_ENDPOINT"

type LocalLLMBackend string

const (
	LocalLLMBackendOllama       LocalLLMBackend = "ollama"
	LocalLLMBackendOpenAICompat LocalLLMBackend = "openai-compat"
)

type LocalLLMTarget struct {
	BaseURL string
	Backend LocalLLMBackend
	Models  []string
}

func normalizeLocalLLMEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	endpoint = strings.TrimRight(endpoint, "/")
	if strings.HasSuffix(endpoint, "/v1") {
		endpoint = strings.TrimSuffix(endpoint, "/v1")
	}
	return strings.TrimRight(endpoint, "/")
}

func resolveLocalLLMEndpoint(cfgEndpoint string) string {
	if endpoint := normalizeLocalLLMEndpoint(cfgEndpoint); endpoint != "" {
		return endpoint
	}
	if endpoint := normalizeLocalLLMEndpoint(os.Getenv(localLLMEndpointEnv)); endpoint != "" {
		return endpoint
	}
	return openaiCompatDefaultEndpoint
}

func detectLocalLLMTarget(ctx context.Context, cfgEndpoint string) (LocalLLMTarget, error) {
	baseURL := resolveLocalLLMEndpoint(cfgEndpoint)
	// Probe OpenAI-compat first (LM Studio at :1234). Ollama probe is kept as
	// a secondary fallback to preserve backward compat for nodes that still run
	// Ollama, but Ollama is no longer the default backend on this node.
	if models, err := probeOpenAICompatModels(ctx, baseURL); err == nil {
		return LocalLLMTarget{
			BaseURL: baseURL,
			Backend: LocalLLMBackendOpenAICompat,
			Models:  models,
		}, nil
	} else if models, err2 := probeOllamaModels(ctx, baseURL); err2 == nil {
		return LocalLLMTarget{
			BaseURL: baseURL,
			Backend: LocalLLMBackendOllama,
			Models:  models,
		}, nil
	} else {
		return LocalLLMTarget{}, fmt.Errorf("local llm unavailable at %s (openai-compat probe: %v; ollama probe: %v)", baseURL, err, err2)
	}
}

func probeOllamaModels(ctx context.Context, baseURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, normalizeLocalLLMEndpoint(baseURL)+"/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(tags.Models))
	for _, model := range tags.Models {
		if model.Name != "" {
			out = append(out, model.Name)
		}
	}
	return out, nil
}

func probeOpenAICompatModels(ctx context.Context, baseURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, normalizeLocalLLMEndpoint(baseURL)+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var result openaiModelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(result.Data))
	for _, model := range result.Data {
		if model.ID != "" {
			out = append(out, model.ID)
		}
	}
	return out, nil
}

// localProviderDefaultTimeoutSec is the fallback HTTP client timeout (in
// seconds) used when no provider config is available or the configured
// provider's timeout is unset/zero. Sized to cover cold-load of an 8B-class
// quantized model on Apple Silicon under memory pressure (~110s clean,
// ~150-180s under load). Operators can override per provider via
// providers(.local).yaml — see resolveLocalProviderTimeout.
const localProviderDefaultTimeoutSec = 300

func buildLocalProvider(target LocalLLMTarget, model string, timeoutSec int) Provider {
	if timeoutSec <= 0 {
		timeoutSec = localProviderDefaultTimeoutSec
	}
	cfg := ProviderConfig{
		Endpoint: target.BaseURL,
		Model:    model,
		Timeout:  timeoutSec,
	}
	switch target.Backend {
	case LocalLLMBackendOllama:
		cfg.ContextWindow = 32768
		return NewOllamaProvider("agent-local", cfg)
	default:
		cfg.MaxTokens = openaiCompatDefaultMaxToks
		return NewOpenAICompatProvider("agent-local", cfg)
	}
}

// resolveLocalProviderTimeout returns the HTTP timeout (in seconds) configured
// for the local-tier provider, honoring the providers.yaml + providers.local.yaml
// merge so operators have a single source of truth.
//
// Lookup order:
//
//   - the provider named by routing.default_local, when enabled with a timeout
//   - any enabled provider whose Endpoint resolves to a localhost URL
//   - 0 (caller falls back to localProviderDefaultTimeoutSec)
//
// Returns 0 on missing/unreadable config; callers must tolerate that.
func resolveLocalProviderTimeout(cfg *Config) int {
	if cfg == nil {
		return 0
	}
	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		return 0
	}
	// Prefer the provider the node declared as its local default.
	if name := pcfg.Routing.DefaultLocal; name != "" {
		if pc, ok := pcfg.Providers[name]; ok && pc.IsEnabled() && pc.Timeout > 0 {
			return pc.Timeout
		}
	}
	// Sorted, so a node with several localhost providers gets the same answer
	// on every boot (map order is random).
	names := make([]string, 0, len(pcfg.Providers))
	for name := range pcfg.Providers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pc := pcfg.Providers[name]
		if !pc.IsEnabled() || pc.Timeout <= 0 {
			continue
		}
		if isLocalEndpoint(pc.Endpoint) {
			return pc.Timeout
		}
	}
	return 0
}

// isLocalEndpoint reports whether an endpoint URL points at a loopback host.
// Used to find a sensible fallback timeout source when no "ollama" provider
// entry is configured.
func isLocalEndpoint(endpoint string) bool {
	endpoint = strings.ToLower(strings.TrimSpace(endpoint))
	if endpoint == "" {
		return false
	}
	for _, marker := range []string{"localhost", "127.0.0.1", "::1", "0.0.0.0"} {
		if strings.Contains(endpoint, marker) {
			return true
		}
	}
	return false
}

// normalizeModelNameForLegacy converts an LM-Studio / MLX-style model name
// (e.g. "google/gemma-4-26b-a4b") to a short tag form ("gemma4:26b") for
// cross-format name matching against the list returned by the local LLM server.
//
// Conversion rules (applied in order):
//  1. Strip a leading "vendor/" prefix (e.g. "google/gemma-4-26b-a4b" →
//     "gemma-4-26b-a4b").
//  2. Replace "gemma-4" with "gemma4".
//  3. Truncate at the size tag (e.g. "26b" or "e4b") and convert
//     "gemma4-NNb" → "gemma4:NNb".
//
// If the name does not look like an LMS-style cross-format name, the original
// string is returned unchanged so normal prefix matching still applies.
var lmsModelPattern = regexp.MustCompile(`^(?:[^/]+/)?gemma-4-([0-9]+[bB]|e[0-9]+[bB])`)

// normalizeModelNameForOllama is an alias retained for backward compat with
// callers that use the old name. Delegates to normalizeModelNameForLegacy.
func normalizeModelNameForOllama(name string) string {
	return normalizeModelNameForLegacy(name)
}

func normalizeModelNameForLegacy(name string) string {
	name = strings.TrimSpace(name)
	if !strings.Contains(name, "/") && !strings.HasPrefix(strings.ToLower(name), "gemma-4") {
		// Not an LMS-style name; return as-is so prefix matching is unaffected.
		return name
	}
	lower := strings.ToLower(name)
	m := lmsModelPattern.FindStringSubmatch(lower)
	if len(m) < 2 {
		return name
	}
	sizeTag := strings.ToLower(m[1]) // e.g. "26b", "e4b"
	return "gemma4:" + sizeTag
}

func resolvePreferredLocalModel(models []string, preferred string) string {
	preferred = strings.TrimSpace(preferred)
	if preferred != "" {
		// First pass: exact match or prefix match.
		for _, model := range models {
			if model == preferred || strings.HasPrefix(model, preferred) {
				return model
			}
		}
		// Second pass: try the short-tag equivalent of the configured name.
		// This handles LM-Studio / MLX style names like "google/gemma-4-26b-a4b"
		// that may appear in a server's model list under a shorter alias.
		normalized := normalizeModelNameForLegacy(preferred)
		if normalized != preferred {
			for _, model := range models {
				if model == normalized || strings.HasPrefix(model, normalized) {
					return model
				}
			}
		}
	}
	// No preferred match: return the first available model rather than a
	// hard-coded Ollama model name (Ollama is decommissioned).
	if len(models) > 0 {
		return models[0]
	}
	return ""
}

var largeLocalModelPattern = regexp.MustCompile(`(^|[^0-9])([0-9]{2,3})b([^0-9]|$)`)

func looksLikeLargeLocalModel(model string) bool {
	m := largeLocalModelPattern.FindStringSubmatch(strings.ToLower(model))
	if len(m) < 3 {
		return false
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return false
	}
	return n >= 26
}

func resolveDispatchLocalModel(models []string, preferred string, requested DispatchModel) (string, DispatchModel, string) {
	switch requested {
	case DispatchModel26B:
		for _, model := range models {
			if looksLikeLargeLocalModel(model) {
				return model, DispatchModel26B, ""
			}
		}
		fallback := resolvePreferredLocalModel(models, preferred)
		if fallback == "" {
			return "", DispatchModelE4B, "26b route unavailable: no local models are loaded"
		}
		return fallback, DispatchModelE4B, "26b route unavailable, using preferred local model"
	default:
		selected := resolvePreferredLocalModel(models, preferred)
		if selected == "" {
			return "", DispatchModelE4B, "no local models are loaded"
		}
		// Report a mismatch when neither the preferred name nor its
		// normalized equivalent was found, so the operator can diagnose
		// configuration drift.
		if preferred != "" && selected != preferred && selected != normalizeModelNameForLegacy(preferred) {
			return selected, DispatchModelE4B, fmt.Sprintf("configured local model %q not loaded, using %q", preferred, selected)
		}
		return selected, DispatchModelE4B, ""
	}
}
