// serve_compat.go — v2 compatibility endpoints for Phase 0 cutover.
//
// These endpoints allow v3 to replace v2 as the production kernel on port 5100.
// Consumers: OpenClaw cogos plugin, CogBus plugin, launchd service.
//
// DEPRECATED: These compatibility routes exist only for migration from v2.
// They will be removed once all clients migrate to standard endpoints.
// Standard endpoints: /v1/chat/completions, /v1/messages, /mcp, /health
//
// Endpoints:
//
//	GET  /v1/card            — kernel capability card (OpenClaw auth flow)
//	GET  /v1/models          — OpenAI-compatible model list
//	GET  /memory/search      — memory search (was missing from v2 too)
//	GET  /memory/read        — memory read (was missing from v2 too)
//	GET  /coherence/check    — coherence check
//	GET  /v1/providers       — provider list with health
//	GET  /v1/taa             — TAA context visibility stub
//
// Removed in the event-bus PR (were always stubs):
//
//	GET  /v1/events/stream   — replaced by the real broker-backed handler in serve.go
//	POST /v1/bus/{bus_id}/ack — dropped; no consumer, new SSE uses Last-Event-ID
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func (s *Server) logCompatDeprecated(r *http.Request) {
	slog.Debug("compat: deprecated endpoint hit", "path", r.URL.Path)
}

// registerCompatRoutes adds v2-compatible endpoints to the mux.
// Called from NewServer after all v3 routes are registered.
func (s *Server) registerCompatRoutes(mux *http.ServeMux) {
	// Tier A: blocking for OpenClaw plugin
	s.route(mux, "GET /v1/card", s.handleCard)
	s.route(mux, "GET /v1/models", s.handleModels)

	// Tier B: event stream + bus ack — now real, registered in serve.go
	// (handleEvents + handleEventsStream). handleBusAck deleted — no
	// consumer relied on it and the new SSE resume uses Last-Event-ID.

	// Tier C: operational stability
	s.route(mux, "GET /v1/providers", s.handleProviders)
	s.route(mux, "GET /v1/taa", s.handleTAA)
	s.route(mux, "GET /memory/search", s.handleMemorySearch)
	s.route(mux, "GET /memory/read", s.handleMemoryRead)
	s.route(mux, "GET /coherence/check", s.handleCoherenceCheck)
}

// ── Tier A: OpenClaw plugin ────────────────────────────────────────────────────

// handleCard returns the kernel capability card. Used by the OpenClaw cogos
// plugin for auth flow and model resolution.
func (s *Server) handleCard(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)
	port := s.cfg.Port
	if port == 0 {
		port = 6931
	}

	// Model limits come from the SAME cached list /v1/models serves
	// (composeModelsList), so the two endpoints are one dataset viewed twice
	// and cannot disagree — for frontier ids, for the "local" alias (whose
	// window is a LIVE probe result, not a Capabilities() field — review
	// round 4), for anything. Before this the card was a hand-maintained
	// literal — sonnet 200k, opus 1M, "Local (Ollama)" #417 decommissioned.
	models := cardModelsFrom(composeModelsList(r.Context(), s.router))
	// defaultModel is drawn from the same projection, in curated preference
	// order, so it can never name a model the card does not list (review
	// round 5: a zero-config local-only node had frontier ids filtered out of
	// models while defaultModel still said claude-sonnet-4-6). Empty string
	// when nothing is configured — honest, and a client validating
	// defaultModel ∈ models sees a consistent card either way.
	defaultModel := ""
	if len(models) > 0 {
		defaultModel, _ = models[0]["id"].(string)
	}

	card := map[string]any{
		"schemaVersion":   "1.0",
		"name":            "CogOS Kernel v3",
		"humanReadableId": "cogos/kernel-v3",
		"description":     "v3 production kernel — foveated context, TRM, attentional field",
		"url":             fmt.Sprintf("http://localhost:%d", port),
		"defaultModel":    defaultModel,
		"models":          models,
		"capabilities": map[string]bool{
			"streaming":         true,
			"taaAware":          true,
			"foveatedContext":   true,
			"memoryIntegration": true,
			"modelRouting":      s.router != nil,
			"trmScoring":        s.process.TRM() != nil,
			"attentionalField":  true,
		},
		"endpoints": map[string]string{
			"inference": "/v1/chat/completions",
			"models":    "/v1/models",
			"health":    "/health",
			"foveated":  "/v1/context/foveated",
			"attention": "/v1/attention",
			"card":      "/v1/card",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(card)
}

// compatModelPermission mirrors the OpenAI model_permission object.
type compatModelPermission struct {
	ID            string `json:"id"`
	Object        string `json:"object"`
	Created       int64  `json:"created"`
	AllowSampling bool   `json:"allow_sampling"`
	AllowLogprobs bool   `json:"allow_logprobs"`
	AllowView     bool   `json:"allow_view"`
}

// compatModel is a single entry in the /v1/models list. `tier` and
// `description` are cogos extensions ignored by standard OpenAI clients.
//
// ContextLength is `omitempty` deliberately (#518): most entries (intent
// aliases, static frontier/eclipse ids, and live ids from a provider with no
// comparable upstream field) have no known context window. Emitting 0 or a
// guessed default there would be worse than omitting the field — a client
// that sees no field can fall back to its own default; one that sees a wrong
// number cannot tell the difference from a real answer.
//
// Capabilities is `omitempty` for the identical reason, one field over
// (#640): it is populated from the SERVING provider's Capabilities().
// Capabilities (string form of each Capability, e.g. "vision", "tool_use"),
// and an entry whose provider declares an empty capability slice — or whose
// provider cannot be resolved at all (a static/alias entry not backed by a
// live probe) — omits the field entirely rather than emitting `[]` or a
// guessed list. `[]` on the wire would read as "declared and supports
// nothing", which is a different (and false) claim from "unknown".
type compatModel struct {
	ID            string                  `json:"id"`
	Object        string                  `json:"object"`
	Created       int64                   `json:"created"`
	OwnedBy       string                  `json:"owned_by"`
	Permission    []compatModelPermission `json:"permission"`
	Tier          string                  `json:"tier,omitempty"`
	Description   string                  `json:"description,omitempty"`
	ContextLength int                     `json:"context_length,omitempty"`
	Capabilities  []string                `json:"capabilities,omitempty"`
}

type compatModelsResponse struct {
	Object string        `json:"object"`
	Data   []compatModel `json:"data"`
}

// mkCompatModel builds a compatModel with the boilerplate permission block.
func mkCompatModel(id, owner, tier, description string, now int64) compatModel {
	return compatModel{
		ID: id, Object: "model", Created: now, OwnedBy: owner,
		Tier:        tier,
		Description: description,
		Permission: []compatModelPermission{{
			ID:            "modelperm-" + id,
			Object:        "model_permission",
			Created:       now,
			AllowSampling: true,
			AllowLogprobs: true,
			AllowView:     true,
		}},
	}
}

// modelsPerProviderTimeout bounds each provider's live ListModels call during
// /v1/models composition. One down or slow (or 401ing) backend must never fail
// the endpoint nor block it beyond this budget — it is skipped (graceful
// degradation).
const modelsPerProviderTimeout = 2 * time.Second

// modelsCacheTTL bounds how long a composed /v1/models list is reused before it
// is recomposed (which fires the live per-provider probes). Keeps concurrent
// callers from stampeding every backend on every request.
const modelsCacheTTL = 45 * time.Second

// modelsCacheEntry holds the last composed /v1/models list for one router and
// when it was built. Recomposition happens under the entry's lock
// (single-flight): concurrent callers on a cold/stale entry collapse into one
// composition instead of each firing the live backend probes. This mirrors the
// hold-lock-during-fetch rationale of the provider Available() TTL caches (#441).
type modelsCacheEntry struct {
	mu        sync.Mutex
	data      []compatModel
	fetchedAt time.Time
}

// modelsListCaches keys a modelsCacheEntry by the router it was composed from,
// so that distinct routers (the daemon's single live router in production; many
// independent routers in tests) never serve each other's stale menus. The outer
// mutex guards only the map; the per-router probe/compose runs under the entry's
// own lock. A nil router uses the shared zero-key entry.
//
// Entries are intentionally NEVER evicted. In production this is bounded: the
// daemon builds exactly one long-lived router, so the map holds a single entry
// for the process lifetime. The unbounded shape only matters if the kernel ever
// recreates routers at runtime (reload/reconcile) — should that land, evict the
// stale router's entry on teardown (e.g. in SimpleRouter.Close) or re-key this
// cache off the Server (one per daemon) instead of the Router. In tests each
// freshly-constructed router leaks one entry, which is negligible for a test
// process.
var (
	modelsListCachesMu sync.Mutex
	modelsListCaches   = map[Router]*modelsCacheEntry{}
)

// modelsCacheFor returns the cache entry for a router, creating it on first use.
func modelsCacheFor(router Router) *modelsCacheEntry {
	modelsListCachesMu.Lock()
	defer modelsListCachesMu.Unlock()
	e, ok := modelsListCaches[router]
	if !ok {
		e = &modelsCacheEntry{}
		modelsListCaches[router] = e
	}
	return e
}

// handleModels returns an OpenAI-compatible model list — a live view of the
// providers/models actually registered and loaded, plus the always-on
// software-defined intent aliases.
//
// Menu shape:
//  1. Intent aliases (owned_by "cogos"): foreground, deliberation (frontier),
//     local. Gated on isFrontierConfigured / isLocalConfigured — software-defined
//     names, not hardware-presence signals. These MUST match the intentAliases
//     table in resolve.go so a selected entry resolves on both gateway and
//     dispatch.
//  2. Static frontier IDs (owned_by "anthropic", tier "frontier-managed"):
//     claude-sonnet-4-6, claude-opus-4-7, claude-haiku-4-5-20251001. Retained so
//     existing clients keep working even when the live Anthropic catalog probe
//     is unavailable.
//  3. Static eclipse-26b (tier "lan-local") when a provider actually serves the
//     eclipse-26b model string (ProviderForModel) — retained for the same
//     reason, and gated on the same predicate that admits it so it is never
//     advertised-then-rejected.
//  4. LIVE-enumerated IDs: each registered provider that implements ModelLister
//     is probed (GET /v1/models) with a bounded per-provider timeout,
//     concurrently. Frontier providers emit their claude ids BARE
//     (owned_by "anthropic"); OpenAI-compat / local providers emit composite
//     "<provider>/<model>" ids (owned_by "cogos:<provider>"). Embedding models
//     are tagged (description "embedding"), never dropped. A provider whose probe
//     errors or times out is skipped — the endpoint never 500s and never blocks
//     beyond the per-provider budget. A provider that additionally implements
//     ModelContextLister (currently OpenAICompatProvider against an LM
//     Studio backend, via its native GET /api/v0/models) contributes a
//     `context_length` on each entry — the LOADED context, never the
//     checkpoint's theoretical max, and omitted entirely when unknown (#518).
//
// The composed list is deduped by final id and served from a ~45s TTL cache so
// concurrent callers don't stampede every backend.
//
// THE ADMISSION-PARITY INVARIANT: every id emitted here satisfies
// IsKnownModel(router, id) == true (resolve.go), so a client selecting any
// advertised id never gets a boundary 400.
func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)

	data := composeModelsList(r.Context(), s.router)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(compatModelsResponse{
		Object: "list",
		Data:   data,
	})
}

// composeModelsList returns the /v1/models entries, served from the TTL cache
// when fresh and recomposed under the cache lock (single-flight) when
// cold/stale. The caller's ctx bounds the live provider probes; each probe is
// additionally capped at modelsPerProviderTimeout.
//
// If the caller's ctx is cancelled/expired while composing, the live per-provider
// probes (whose contexts descend from it) return early and otherwise-healthy
// providers are skipped, yielding an incomplete list. That partial list is served
// to this caller but is NOT written to the shared cache: a caller going away says
// nothing about provider health, and caching it would serve the degraded menu to
// every unrelated caller until the TTL expires. Same guard the Available() TTL
// cache applies (#441).
func composeModelsList(ctx context.Context, router Router) []compatModel {
	entry := modelsCacheFor(router)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.data != nil && time.Since(entry.fetchedAt) < modelsCacheTTL {
		return entry.data
	}
	data := buildModelsList(ctx, router)
	if ctx.Err() != nil {
		// Caller cancelled/timed out mid-compose — do not poison the shared cache
		// with a list that may be missing healthy providers. Leave the entry cold
		// so the next caller rebuilds cleanly.
		return data
	}
	entry.data = data
	entry.fetchedAt = time.Now()
	return data
}

// buildModelsList composes the model menu: intent aliases + static frontier /
// eclipse ids (config-gated, in-memory) followed by the live-enumerated ids from
// every ModelLister provider (bounded, concurrent, graceful-skip). Deduped by
// final id, first occurrence wins so the static entries keep their curated
// tier/description when a live probe would re-emit the same id.
func buildModelsList(ctx context.Context, router Router) []compatModel {
	now := time.Now().Unix()

	// Gate the static/alias entries on real provider availability. All checks
	// are in-memory map lookups (no I/O) — same pattern as isEclipseConfigured.
	frontierConfigured := isFrontierConfigured(router)
	localConfigured := isLocalConfigured(router)
	// The static eclipse-26b id is admissible (IsKnownModel true) ONLY when a
	// provider actually serves the eclipse-26b model string. Gating its emission
	// on the same predicate keeps emit and admit in lockstep — emitting it merely
	// because a provider named "eclipse"/"lmstudio" exists (isEclipseConfigured's
	// broader name match) would advertise an id the kernel then 400s. The real
	// eclipse model is surfaced via live enumeration as a composite id regardless.
	eclipseServed := eclipseModelServed(router)

	// Live enumeration first (probe every ModelLister provider concurrently,
	// each bounded, skipping any that error/time out): the alias entries below
	// derive their context_length (#518) from the live listing of whatever the
	// alias actually routes to.
	live := liveModelEntries(ctx, router, now)

	var data []compatModel
	seen := make(map[string]bool)
	add := func(m compatModel) {
		if m.ID == "" || seen[m.ID] {
			return
		}
		seen[m.ID] = true
		data = append(data, m)
	}
	withCtx := func(m compatModel, ctxLen int) compatModel {
		if ctxLen > 0 {
			m.ContextLength = ctxLen
		}
		return m
	}
	// withCaps mirrors withCtx exactly (#640): a nil/empty slice leaves
	// Capabilities unset, which `omitempty` drops from the wire — same
	// "absent means unknown, never guessed" contract #518 established for
	// context_length.
	withCaps := func(m compatModel, caps []string) compatModel {
		if len(caps) > 0 {
			m.Capabilities = caps
		}
		return m
	}

	if frontierConfigured {
		// Intent aliases for frontier-managed tiers. Their window is whatever
		// the registered frontier provider actually declares — claude-oauth and
		// claude-code declare 1M (context beta), the API-key AnthropicProvider
		// declares 200k. Never a constant: /v1/card already derives from the
		// same Capabilities() and the two endpoints must agree (#518 review).
		//
		// Capabilities (#640) mirror this exactly: foreground/deliberation DO
		// get context_length today via frontierContextLength, so they get
		// capabilities too via the same resolved provider's Capabilities().
		fctx := frontierContextLength(router)
		fcaps := frontierCapabilities(router)
		add(withCaps(withCtx(mkCompatModel("foreground", "cogos", "frontier-managed",
			"interactive, full capability (managed Claude, Max sub)", now), fctx), fcaps))
		add(withCaps(withCtx(mkCompatModel("deliberation", "cogos", "frontier-managed",
			"heavier reasoning (Opus)", now), fctx), fcaps))
	}
	if localConfigured {
		// Intent alias for the local-sovereign tier. Its context window is
		// whatever is loaded on the provider "local" actually resolves to —
		// pulled from the live listings, omitted when genuinely unknown (#518).
		// Capabilities (#640) come from the same resolved provider's
		// Capabilities() — provider-scoped, not per-loaded-model, so no live
		// listing lookup is needed the way context_length needs one.
		add(withCaps(withCtx(mkCompatModel("local", "cogos", "local-sovereign",
			"private, no egress (E4B on this node)", now), localAliasContextLength(router, live)),
			localAliasCapabilities(router)))
	}
	if frontierConfigured {
		// Static frontier model IDs — retained so clients keep working even when
		// the live Anthropic catalog probe is unavailable. Window comes from the
		// serving frontier provider's declared capability (#518); capabilities
		// (#640) mirror it via the same frontierCapabilities lookup.
		fctx := frontierContextLength(router)
		fcaps := frontierCapabilities(router)
		add(withCaps(withCtx(mkCompatModel("claude-sonnet-4-6", "anthropic", "frontier-managed", "", now), fctx), fcaps))
		add(withCaps(withCtx(mkCompatModel("claude-opus-4-7", "anthropic", "frontier-managed", "", now), fctx), fcaps))
		add(withCaps(withCtx(mkCompatModel("claude-haiku-4-5-20251001", "anthropic", "frontier-managed", "fast, low-cost", now), fctx), fcaps))
	}
	if eclipseServed {
		// Same rule as every other entry: the window is what the serving
		// provider declares, or omitted when it declares nothing (#518 review
		// round 2 — this static entry was the last one shipping without it).
		// Capabilities (#640) follow the identical serving-provider lookup.
		add(withCaps(withCtx(mkCompatModel("eclipse-26b", "cogos", "lan-local",
			"LAN-resident 26B model (Eclipse node)", now), servingProviderContextLength(router, "eclipse-26b")),
			servingProviderCapabilities(router, "eclipse-26b")))
	}

	for _, m := range live {
		add(m)
	}

	return data
}

// cardModelsFrom projects the /v1/models list into the card's curated shape.
// The card has always advertised a short curated set (a sonnet, an opus, and
// the local alias) rather than the full catalog; it keeps that shape but every
// limit is read from the /v1/models entry for the same id — never computed
// independently. An id absent from the list is absent from the card. A window
// the list omits is omitted here too (never invented).
func cardModelsFrom(list []compatModel) []map[string]any {
	byID := make(map[string]compatModel, len(list))
	for _, m := range list {
		byID[m.ID] = m
	}
	type curated struct {
		id, name string
		out      int
	}
	want := []curated{
		{"claude-sonnet-4-6", "Claude Sonnet 4.6", 8192},
		{"claude-opus-4-7", "Claude Opus 4.7", 32000},
		{"local", "Local (LM Studio)", 4096},
	}
	out := make([]map[string]any, 0, len(want))
	for _, c := range want {
		m, ok := byID[c.id]
		if !ok {
			continue
		}
		limits := map[string]int{"output": c.out}
		if m.ContextLength > 0 {
			limits["context"] = m.ContextLength
		}
		out = append(out, map[string]any{"id": c.id, "name": c.name, "limits": limits})
	}
	return out
}

// capabilityStrings converts a provider's []Capability into the wire string
// form used by compatModel.Capabilities (#640) — "vision", "tool_use", etc.
// Returns nil (not an empty non-nil slice) for a nil/empty input, so callers
// that feed the result straight to compatModel.Capabilities get the
// omitempty "absent means unknown" behavior for free.
func capabilityStrings(caps []Capability) []string {
	if len(caps) == 0 {
		return nil
	}
	out := make([]string, len(caps))
	for i, c := range caps {
		out[i] = string(c)
	}
	return out
}

// servingProviderContextLength returns the context window declared by whichever
// provider the router would route model id to, or 0 (omitted) when nothing
// serves it. One rule for every advertised entry, static or live (#518).
func servingProviderContextLength(router Router, id string) int {
	if router == nil {
		return 0
	}
	name, ok := router.ProviderForModel(id)
	if !ok || name == "" {
		return 0
	}
	var n int
	router.RangeProviders(func(p Provider) {
		if p.Name() == name {
			n = p.Capabilities().MaxContextTokens
		}
	})
	return n
}

// servingProviderCapabilities returns the capability list declared by
// whichever provider the router would route model id to, or nil (omitted)
// when nothing serves it or that provider declares no capabilities (#640).
// Mirrors servingProviderContextLength exactly — same resolution, same
// "absent means unknown" contract, one field over.
func servingProviderCapabilities(router Router, id string) []string {
	if router == nil {
		return nil
	}
	name, ok := router.ProviderForModel(id)
	if !ok || name == "" {
		return nil
	}
	var caps []string
	router.RangeProviders(func(p Provider) {
		if p.Name() == name {
			caps = capabilityStrings(p.Capabilities().Capabilities)
		}
	})
	return caps
}

// frontierContextLength returns the context window the registered frontier
// provider declares via Capabilities().MaxContextTokens, or 0 (omitted) when
// no frontier provider is registered. Used for the static frontier entries
// and the foreground/deliberation aliases so /v1/models agrees with /v1/card,
// which derives from the same Capabilities() (#518 review finding).
func frontierContextLength(router Router) int {
	if router == nil {
		return 0
	}
	name, ok := frontierProviderName(router)
	if !ok {
		return 0
	}
	var n int
	router.RangeProviders(func(p Provider) {
		if p.Name() == name {
			n = p.Capabilities().MaxContextTokens
		}
	})
	return n
}

// frontierCapabilities returns the capability list the registered frontier
// provider declares, or nil (omitted) when no frontier provider is
// registered or it declares none (#640). Used for the static frontier
// entries and the foreground/deliberation aliases — same resolution
// frontierContextLength uses, so the two fields can never name a different
// serving provider for the same entry.
func frontierCapabilities(router Router) []string {
	if router == nil {
		return nil
	}
	name, ok := frontierProviderName(router)
	if !ok {
		return nil
	}
	var caps []string
	router.RangeProviders(func(p Provider) {
		if p.Name() == name {
			caps = capabilityStrings(p.Capabilities().Capabilities)
		}
	})
	return caps
}

// localAliasContextLength resolves the "local" intent alias to its target
// provider (same logic clients hit at request time, ResolveModelRequest) and
// returns the context window of that provider's CONFIGURED model, looked up in
// the already-probed live entries — the loaded window, never a guess (#518).
// Returns 0 (⇒ omitted on the wire) when the alias doesn't resolve, the
// provider has no configured model, or the live probe didn't report a window.
func localAliasContextLength(router Router, live []compatModel) int {
	res := ResolveModelRequest(router, "local", "")
	if res.PreferProvider == "" || router == nil {
		return 0
	}
	var model string
	router.RangeProviders(func(p Provider) {
		if p.Name() != res.PreferProvider {
			return
		}
		if ms := p.Capabilities().ModelsAvailable; len(ms) > 0 {
			model = ms[0]
		}
	})
	if model == "" {
		return 0
	}
	want := res.PreferProvider + "/" + model
	for _, m := range live {
		if m.ID == want {
			return m.ContextLength
		}
	}
	return 0
}

// localAliasCapabilities resolves the "local" intent alias to its target
// provider (same resolution localAliasContextLength uses) and returns that
// provider's declared Capabilities() (#640). Unlike context_length,
// capability support is a property of the PROVIDER, not the currently-loaded
// model, so this does not need the live-listing lookup localAliasContextLength
// performs — the provider's own Capabilities() is the complete answer.
// Returns nil (⇒ omitted on the wire) when the alias doesn't resolve or the
// provider declares no capabilities.
func localAliasCapabilities(router Router) []string {
	res := ResolveModelRequest(router, "local", "")
	if res.PreferProvider == "" || router == nil {
		return nil
	}
	var caps []string
	router.RangeProviders(func(p Provider) {
		if p.Name() == res.PreferProvider {
			caps = capabilityStrings(p.Capabilities().Capabilities)
		}
	})
	return caps
}

// liveModelEntries walks the router's providers, probing each ModelLister (or
// the richer ModelContextLister, #518) with a bounded per-provider timeout,
// concurrently, and returns the composed live entries. A provider whose probe
// errors or times out is skipped (slog.Debug): the endpoint never fails and
// never blocks beyond modelsPerProviderTimeout. Order is deterministic (by
// provider Name(), which RangeProviders guarantees), so the caller's
// first-occurrence dedupe is stable.
func liveModelEntries(ctx context.Context, router Router, now int64) []compatModel {
	if router == nil {
		return nil
	}

	// Snapshot the ModelLister/ModelContextLister providers in Name() order.
	// Every concrete provider today (OpenAICompatProvider) that implements
	// ModelContextLister also implements plain ModelLister, but the check
	// accepts either so a future provider implementing only the richer
	// interface isn't silently excluded from the menu.
	var listers []Provider
	router.RangeProviders(func(p Provider) {
		if _, ok := p.(ModelLister); ok {
			listers = append(listers, p)
			return
		}
		if _, ok := p.(ModelContextLister); ok {
			listers = append(listers, p)
		}
	})
	if len(listers) == 0 {
		return nil
	}

	// Probe concurrently; per-provider results indexed to preserve order.
	results := make([][]compatModel, len(listers))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i, p := range listers {
		wg.Add(1)
		go func(i int, p Provider) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, modelsPerProviderTimeout)
			defer cancel()
			listings, err := listModelsForProvider(pctx, p)
			if err != nil {
				slog.Debug("compat: /v1/models: provider enumeration skipped",
					"provider", p.Name(), "err", err)
				return
			}
			entries := make([]compatModel, 0, len(listings))
			frontier := isFrontierProvider(p)
			for _, listing := range listings {
				if listing.ID == "" {
					continue
				}
				entries = append(entries, modelEntryFor(p, listing, frontier, now))
			}
			mu.Lock()
			results[i] = entries
			mu.Unlock()
		}(i, p)
	}
	wg.Wait()

	var out []compatModel
	for _, entries := range results {
		out = append(out, entries...)
	}
	return out
}

// listModelsForProvider returns model listings for p, preferring the richer
// ModelContextLister (context_length included, #518) when the provider
// implements it, and falling back to the plain ModelLister (ids only,
// ContextLength left at 0/unknown) otherwise. The caller (liveModelEntries)
// has already confirmed p implements at least ModelLister before calling this.
func listModelsForProvider(ctx context.Context, p Provider) ([]ModelListing, error) {
	if cl, ok := p.(ModelContextLister); ok {
		return cl.ListModelsWithContext(ctx)
	}
	ids, err := p.(ModelLister).ListModels(ctx)
	if err != nil {
		return nil, err
	}
	listings := make([]ModelListing, 0, len(ids))
	for _, id := range ids {
		listings = append(listings, ModelListing{ID: id})
	}
	return listings, nil
}

// isFrontierProvider reports whether p is an Anthropic/Claude frontier provider,
// so its live model ids are emitted BARE (owned_by "anthropic", preserving
// existing client naming) rather than composite.
//
// The set is the explicit canonical name list ONLY — deliberately NOT a
// !IsLocal structural fallthrough. Bare claude ids are admitted by
// resolveLiveCatalog's claude-* branch, which requires a frontier provider
// (frontierProviderName, same name set). A non-local, non-claude ModelLister
// (none exists today — OpenAICompatProvider is IsLocal=true — but a future one
// could) whose ids were emitted bare would have NO admission path (not a
// composite prefix, not a claude- id, not ProviderForModel), producing an
// advertise-then-reject. Restricting to the names that have a real bare
// admission path keeps emit ⇔ admit; everything else emits composite, which
// always admits via the ProviderForName prefix guard.
func isFrontierProvider(p Provider) bool {
	switch p.Name() {
	case "claude-oauth", "anthropic", "claude-code":
		return true
	}
	return false
}

// isEmbeddingModelID reports whether a model id names an embedding model, so the
// entry is tagged (description "embedding") instead of presented as a chat model.
func isEmbeddingModelID(id string) bool {
	l := strings.ToLower(id)
	return strings.Contains(l, "text-embedding") || strings.Contains(l, "embed")
}

// modelEntryFor builds the compatModel for a live-enumerated id. Frontier
// providers emit bare claude ids (owned_by "anthropic"); OpenAI-compat / local
// providers emit composite "<provider>/<model>" ids (owned_by "cogos:<provider>")
// with a tier derived from the provider name/capabilities. Embedding ids are
// tagged rather than dropped.
//
// Bare emission is additionally guarded on the "claude-" prefix: resolveLiveCatalog
// only admits bare frontier ids that start with "claude-", so an id from a frontier
// provider that lacks the prefix (Anthropic's /v1/models returns only claude-* ids
// today, so this is defensive) falls back to composite emission — which always
// admits via the ProviderForName prefix guard — rather than becoming an
// advertise-then-reject bare id.
//
// listing.ContextLength (#518) is carried onto the entry only when > 0 — see
// compatModel's doc comment for why 0/absent must stay omitted rather than
// becoming a wire 0.
//
// Capabilities (#640) come from p.Capabilities().Capabilities — the SERVING
// provider's declared list, converted via capabilityStrings — for every live
// id regardless of branch (bare claude or composite). Unlike context_length,
// no upstream per-model field is consulted here: ModelListing carries no
// capability data (LM Studio's /api/v0/models exposes no per-model
// vision/capability flag as of this change — see provider_openai.go's
// ListModelsWithContext doc comment), so this is provider-scoped, not
// model-scoped, for every provider today. omitempty drops it when the
// provider declares nothing.
func modelEntryFor(p Provider, listing ModelListing, frontier bool, now int64) compatModel {
	id := listing.ID
	desc := ""
	if isEmbeddingModelID(id) {
		desc = "embedding"
	}
	var m compatModel
	if frontier && strings.HasPrefix(id, "claude-") {
		m = mkCompatModel(id, "anthropic", "frontier-managed", desc, now)
		if listing.ContextLength == 0 {
			// Anthropic's /v1/models exposes no per-model context field. Fall
			// back to what THIS serving provider declares — 1M for claude-oauth
			// / claude-code, 200k for the API-key provider — never a constant
			// that ignores which provider is actually behind the id (#518).
			m.ContextLength = p.Capabilities().MaxContextTokens
		}
	} else {
		name := p.Name()
		composite := name + "/" + id
		tier := "frontier-managed"
		switch {
		case strings.Contains(strings.ToLower(name), "eclipse"):
			tier = "lan-local"
		case p.Capabilities().IsLocal:
			tier = "local-sovereign"
		}
		m = mkCompatModel(composite, "cogos:"+name, tier, desc, now)
	}
	if listing.ContextLength > 0 {
		m.ContextLength = listing.ContextLength
	}
	m.Capabilities = capabilityStrings(p.Capabilities().Capabilities)
	return m
}

// eclipseModelServed reports whether some registered provider actually serves
// the eclipse-26b model string. This is exactly the condition under which
// IsKnownModel(router, "eclipse-26b") is true (via ProviderForModel), so it is
// the correct emit-gate for the static eclipse-26b menu entry: emit ⇔ admit.
// The broader name-based isEclipseConfigured is retained for callers that only
// need to know an eclipse/lmstudio provider is present, but must NOT gate the
// static id emission (that would advertise-then-reject).
func eclipseModelServed(router Router) bool {
	if router == nil {
		return false
	}
	_, ok := router.ProviderForModel("eclipse-26b")
	return ok
}

// isEclipseConfigured returns true when the router has a registered provider
// that serves the eclipse-26b LAN model. Checks by provider name ("eclipse",
// "lmstudio") and by model string ("eclipse-26b"). Fast: in-memory lookups only.
func isEclipseConfigured(router Router) bool {
	if router == nil {
		return false
	}
	for _, name := range []string{"eclipse", "lmstudio"} {
		if _, ok := router.ProviderForName(name); ok {
			return true
		}
	}
	_, ok := router.ProviderForModel("eclipse-26b")
	return ok
}

// isFrontierConfigured returns true when the router has a registered provider
// that can serve frontier (Anthropic/Claude) models. Checks by canonical
// provider names ("anthropic", "claude-code") and by representative model IDs.
// Fast: in-memory lookups only, no network I/O.
//
// Known tradeoff: this only matches the canonical provider names plus the two
// representative model IDs. A frontier provider registered under a non-standard
// name (and not serving either representative model string) would be hidden
// from the /v1/models menu. Acceptable: the default config and docs use the
// canonical names; extend the name list here if a new alias is introduced.
func isFrontierConfigured(router Router) bool {
	if router == nil {
		return false
	}
	for _, name := range []string{"claude-oauth", "anthropic", "claude-code"} {
		if _, ok := router.ProviderForName(name); ok {
			return true
		}
	}
	for _, model := range []string{"claude-sonnet-4-6", "claude-opus-4-7"} {
		if _, ok := router.ProviderForModel(model); ok {
			return true
		}
	}
	return false
}

// isLocalConfigured returns true when the router has at least one registered
// provider whose Capabilities().IsLocal is true. Uses FirstLocalProvider so
// the check requires no concrete type assertion. Fast: in-memory lookup only.
func isLocalConfigured(router Router) bool {
	if router == nil {
		return false
	}
	_, ok := router.FirstLocalProvider()
	return ok
}

// ── Tier C: Operational stability ──────────────────────────────────────────────

// handleProviders returns provider health information.
func (s *Server) handleProviders(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)
	type providerInfo struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Available bool   `json:"available"`
	}

	var providers []providerInfo
	if sr, ok := s.router.(*SimpleRouter); ok && sr != nil {
		sr.mu.RLock()
		for _, p := range sr.providers {
			providers = append(providers, providerInfo{
				Name:      p.Name(),
				Type:      p.Name(),
				Available: p.Available(r.Context()),
			})
		}
		sr.mu.RUnlock()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"providers": providers,
	})
}

// handleTAA returns a stub TAA context visibility response.
func (s *Server) handleTAA(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"version": "v3-foveated",
		"note":    "v3 uses foveated context assembly, not tiered TAA",
		"zones":   []string{"nucleus", "foveal-docs", "conversation", "reserve"},
	})
}

// handleMemorySearch searches CogDocs by query string.
//
// Delegates to SearchMemory (constellation FTS5 + bm25, grep fallback) — the
// same path the MCP memory_search tool uses. It must NOT re-implement ranking:
// a previous version scored docs with queryRelevance()*2.0 + salience, where
// relevance is capped at 1.0 and salience is unbounded (observed 4.2–4.3). The
// salience term dominated the sort, so the query string was inert — three
// unrelated queries returned byte-identical results. See myrgic/cogos#578.
//
// Salience is deliberately NOT blended in here. If attentional weighting is
// wanted it must be a tiebreaker *within* relevance-ranked results, never an
// additive term that can outrank the query.
func (s *Server) handleMemorySearch(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)
	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "missing query parameter", http.StatusBadRequest)
		return
	}

	// Request surface is deliberately unchanged: `query` only, limit fixed at
	// the 20 this endpoint always returned. SearchMemory supports limit and
	// sector, but exposing them here would widen a deprecated endpoint's
	// contract inside a bugfix — a separate change if anyone wants it.
	const limit = 20

	// A retrieval error must surface as an error, not as an empty result set:
	// the caller has to be able to tell "no matches" from "I am broken".
	out, err := SearchMemory(s.cfg.WorkspaceRoot, query, limit, "")
	if err != nil {
		http.Error(w, fmt.Sprintf("memory search failed: %v", err),
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// handleMemoryRead reads a CogDoc by path.
func (s *Server) handleMemoryRead(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing path parameter", http.StatusBadRequest)
		return
	}

	// Resolve relative to workspace .cog/mem/
	absPath := path
	if !filepath.IsAbs(path) {
		absPath = filepath.Join(s.cfg.WorkspaceRoot, ".cog", "mem", path)
	}

	// Security: ensure path is under .cog/mem/. Use pathWithin (not a bare
	// HasPrefix) so a sibling like ".cog/mem-evil" — which textually starts with
	// the mem root — does not pass the containment check.
	memRoot := filepath.Join(s.cfg.WorkspaceRoot, ".cog", "mem")
	cleanPath := filepath.Clean(absPath)
	if !pathWithin(memRoot, cleanPath) {
		http.Error(w, "path outside memory root", http.StatusForbidden)
		return
	}

	content, err := os.ReadFile(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Write(content)
}

// handleCoherenceCheck runs a quick coherence check.
func (s *Server) handleCoherenceCheck(w http.ResponseWriter, r *http.Request) {
	s.logCompatDeprecated(r)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"coherent":   true,
		"version":    "v3",
		"field_size": s.process.Field().Len(),
		"index_size": func() int {
			idx := s.process.Index()
			if idx == nil {
				return 0
			}
			return len(idx.ByURI)
		}(),
		"trm_loaded":    s.process.TRM() != nil,
		"process_state": s.process.State().String(),
	})
}
