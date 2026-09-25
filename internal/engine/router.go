// router.go — SimpleRouter + BuildRouter
//
// SimpleRouter implements the Router interface with rule-based provider selection:
//  1. Check process-state routing overrides
//  2. Try preferred provider first, then fallback chain
//  3. Filter by availability + required capabilities
//  4. Score local > cloud (sovereignty gradient)
//  5. Record every routing decision for future sentinel training
//
// BuildRouter reads .cog/config/providers.yaml and instantiates enabled providers.
// When no providers.yaml is present it probes for a reachable local backend
// (LM Studio on :1234, then Ollama on :11434) and builds a default config around
// whichever responds; if neither is up it surfaces a "no local model configured"
// placeholder rather than a dead default.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
	"gopkg.in/yaml.v3"
)

var toolCallRejectionsByProvider sync.Map // map[string]*atomic.Int64

// availState is a single provider's last observed readiness.
type availState struct {
	ready    bool
	lastSeen time.Time
}

// availSnapshot is an immutable map of provider name → readiness, swapped
// atomically by the background maintainer so Route reads it lock-free.
type availSnapshot map[string]availState

// defaultAvailTTL is the interval between background availability probes.
const defaultAvailTTL = 10 * time.Second

// SimpleRouter implements Router with rule-based provider selection.
type SimpleRouter struct {
	mu        sync.RWMutex
	providers []Provider // ordered by registration sequence
	byName    map[string]Provider

	cfg RoutingConfig

	// Provider availability maintained off the request hot path. A background
	// goroutine (Start) probes every provider concurrently on availTTL and
	// swaps an immutable snapshot; Route reads it lock-free via available().
	// When the snapshot is cold/stale (no maintainer running, just-registered
	// provider, or stalled ticker) available() falls back to a bounded inline
	// probe so short-lived callers that never call Start stay correct.
	//
	// This is the fix for the per-request blocking probe: a dead provider deep
	// in the fallback chain used to cost its full TCP timeout on every request
	// (it was probed inline in Route); now that cost is paid in the background,
	// concurrently, once per availTTL, and never on a request.
	avail    atomic.Pointer[availSnapshot]
	availTTL time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once

	// tickHooks run alongside probeAll on every availTTL tick (and once
	// synchronously from Start's initial warm-up). This is the reconcile seam
	// for discovery-based auto-registration (see maybeAutoRegisterClaudeOAuth):
	// a node's credentials can appear after boot (first `claude /login` on this
	// machine), so registration must be re-checked on a running timer, not only
	// once at BuildRouter time. Guarded by mu like the rest of the router's
	// mutable state.
	tickHooks []func()

	// Atomics for lock-free stats.
	totalRequests atomic.Int64
	escalations   atomic.Int64
	fallbacks     atomic.Int64
	byProvider    sync.Map // map[string]*atomic.Int64
}

// NewSimpleRouter creates an empty router with the given routing config.
func NewSimpleRouter(cfg RoutingConfig) *SimpleRouter {
	return &SimpleRouter{
		cfg:      cfg,
		byName:   make(map[string]Provider),
		availTTL: defaultAvailTTL,
		stopCh:   make(chan struct{}),
	}
}

// RegisterProvider adds a provider to the pool.
// Providers are kept sorted by Name() so that ProviderForModel iteration
// is deterministic when multiple providers share the same Model() string.
func (r *SimpleRouter) RegisterProvider(p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[p.Name()] = p
	r.providers = append(r.providers, p)
	sort.Slice(r.providers, func(i, j int) bool {
		return r.providers[i].Name() < r.providers[j].Name()
	})
}

// DeregisterProvider removes a provider by name.
func (r *SimpleRouter) DeregisterProvider(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byName, name)
	updated := r.providers[:0]
	for _, p := range r.providers {
		if p.Name() != name {
			updated = append(updated, p)
		}
	}
	r.providers = updated
}

// ProviderForName returns the registered provider name when `name` is an
// exact match for a provider's Name(). Used to detect provider aliases so
// callers can target a specific provider without forwarding the alias as a
// ModelOverride. Returns ("", false) when no provider matches.
func (r *SimpleRouter) ProviderForName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if p, ok := r.byName[name]; ok {
		return p.Name(), true
	}
	return "", false
}

// ProviderForModel returns the registered provider name whose Name() or
// Model() matches `model`. Name match takes precedence over model match so
// callers can target a specific provider instance even when multiple
// providers serve the same underlying model. Returns ("", false) when no
// provider matches.
func (r *SimpleRouter) ProviderForModel(model string) (string, bool) {
	if model == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if p, ok := r.byName[model]; ok {
		return p.Name(), true
	}
	for _, p := range r.providers {
		if p.Model() == model {
			return p.Name(), true
		}
	}
	return "", false
}

// FirstLocalProvider returns the name of the first registered provider whose
// Capabilities().IsLocal is true. Providers are iterated in registration
// order (alphabetically sorted by Name). Returns ("", false) when none is
// registered.
func (r *SimpleRouter) FirstLocalProvider() (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		if p.Capabilities().IsLocal {
			return p.Name(), true
		}
	}
	return "", false
}

// RangeProviders calls fn for each registered provider. Providers are visited
// in Name() order. A snapshot of the provider slice is taken under the read
// lock and the lock is released before fn runs, so fn may perform network I/O
// (e.g. a live GET /v1/models) without holding r.mu — and must not call back
// into the router (no re-entrancy).
func (r *SimpleRouter) RangeProviders(fn func(p Provider)) {
	r.mu.RLock()
	providers := make([]Provider, len(r.providers))
	copy(providers, r.providers)
	r.mu.RUnlock()
	for _, p := range providers {
		fn(p)
	}
}

// Route selects the best available provider for the request.
func (r *SimpleRouter) Route(ctx context.Context, req *CompletionRequest) (Provider, *RoutingDecision, error) {
	start := time.Now()
	r.totalRequests.Add(1)

	r.mu.RLock()
	providers := make([]Provider, len(r.providers))
	copy(providers, r.providers)
	cfg := r.cfg
	r.mu.RUnlock()

	// Provider preference: explicit > process-state > default.
	preferred := req.Metadata.PreferProvider
	if preferred == "" && req.Metadata.ProcessState != "" {
		preferred = cfg.ProcessStateRouting[req.Metadata.ProcessState]
	}
	if preferred == "" {
		preferred = cfg.Default
	}

	// Build a priority-ordered candidate list:
	// 1. preferred provider, 2. fallback chain, 3. remaining providers.
	ordered := r.buildCandidateOrder(providers, preferred, cfg.FallbackChain)

	var scores []ProviderScore
	var selected Provider
	escalated := false
	fallbackUsed := false
	fallbackFrom := ""

	for i, p := range ordered {
		caps := p.Capabilities()
		capsMet := caps.HasAllCapabilities(req.Metadata.RequiredCapabilities)
		avail := r.available(ctx, p)
		// Compat-aware fallback (inference-pipeline-robustness FIX 3): a request
		// carrying an explicit ModelOverride, when its primary is down, falls
		// through the chain still carrying that override. A bare local-model id
		// like "ornith-1.0-35b" must not be fired at a LOCAL model-serving sibling
		// (lmstudio/ollama/mlx) that hasn't loaded it — that 404s opaquely — so
		// providerCanServe skips such a candidate and lets the router reach the
		// sibling that does serve it. Frontier providers (anthropic, claude-oauth,
		// and the claude-code / codex CLIs) are model-agnostic within their family
		// and honour any override verbatim, so providerCanServe keeps them
		// eligible — this preserves degraded-mode frontier failover, e.g. a
		// "sonnet" override reaching a backup claude-oauth or the claude-code CLI
		// after the preferred frontier drops (the case the flight review flagged).
		//
		// The gate applies to FALLBACK candidates only (i > 0). The preferred
		// provider at position 0 was resolved explicitly (PreferProvider / alias
		// table already validated the override against it — e.g. "opus" → this
		// provider), so it is never skipped: that preserves all managed-frontier
		// routing where the override intentionally differs from the provider's
		// configured model. Only the involuntary carry-over into a fallback that
		// can't serve the override is filtered.
		canServe := i == 0 || providerCanServe(p, req.ModelOverride)
		// Per-provider deny policy, enforced at the true routing boundary (not
		// just the HTTP handlers that build this request): a fallback candidate
		// that operator policy denies for this model must never be selected,
		// even though providerCanServe above deliberately treats remote/
		// aggregator providers as model-agnostic for legitimate frontier
		// failover. Without this, a denied provider named in cfg.FallbackChain
		// (or reached via buildCandidateOrder's "remaining providers" tail)
		// could still serve a request whose ModelOverride/PreferProvider was
		// denied at admission for a DIFFERENT provider, once the preferred
		// provider becomes unavailable and Route falls through the chain.
		//
		// effectiveModel falls back to the provider's own configured default
		// model (p.Model()) when req.ModelOverride is empty — a bare
		// provider-name request (ResolveModelRequest's "named provider ->
		// {name, ""}" case) leaves ModelOverride empty and the provider then
		// serves ITS OWN configured model (OpenAICompatProvider.effectiveModel,
		// PiProvider.buildArgs both fall back to Model() when no override is
		// given). Checking only ModelOverride=="" would let an operator's
		// denied default model (e.g. openrouter configured with
		// `model: anthropic/claude-fable-5.1`) reach upstream on every request
		// that just names the provider, reproducing the incident this policy
		// exists to block through the one request shape the override-only
		// check can't see (cog-review round 2, resolve.go:115).
		effectiveModel := req.ModelOverride
		if effectiveModel == "" {
			effectiveModel = p.Model()
		}
		if _, denied := DeniedByPolicy(p.Name(), effectiveModel); denied {
			canServe = false
		}

		score := ProviderScore{
			Provider:        p.Name(),
			RawScore:        computeScore(p, req),
			Available:       avail,
			CapabilitiesMet: capsMet,
		}
		if !caps.IsLocal {
			score.SwapPenalty = 0.10
		}
		score.AdjustedScore = score.RawScore - score.SwapPenalty
		scores = append(scores, score)

		if avail && capsMet && canServe && selected == nil {
			selected = p
			if i > 0 {
				fallbackUsed = true
				fallbackFrom = ordered[0].Name()
			}
			if !caps.IsLocal {
				escalated = true
				r.escalations.Add(1)
			}
		}
	}

	if selected == nil {
		return nil, nil, fmt.Errorf("router: no available provider for request %s", req.Metadata.RequestID)
	}

	// Track per-provider count.
	counter, _ := r.byProvider.LoadOrStore(selected.Name(), &atomic.Int64{})
	counter.(*atomic.Int64).Add(1)
	if fallbackUsed {
		r.fallbacks.Add(1)
	}

	decision := &RoutingDecision{
		RequestID:        req.Metadata.RequestID,
		SelectedProvider: selected.Name(),
		Scores:           scores,
		Reason:           routeReason(escalated, fallbackUsed),
		Escalated:        escalated,
		FallbackUsed:     fallbackUsed,
		FallbackFrom:     fallbackFrom,
		Timestamp:        time.Now().UTC(),
		LatencyNs:        time.Since(start).Nanoseconds(),
	}

	slog.Debug("router: selected",
		"provider", selected.Name(),
		"escalated", escalated,
		"fallback", fallbackUsed,
		"latency_us", time.Since(start).Microseconds())

	return selected, decision, nil
}

// available reports whether provider p is ready, reading the maintained
// availability snapshot when it holds a fresh entry and falling back to a
// bounded inline probe otherwise. The fast path is lock-free and O(1); the
// fallback exists so callers that never start the background maintainer (and
// freshly-registered providers not yet probed) still observe real readiness.
func (r *SimpleRouter) available(ctx context.Context, p Provider) bool {
	ttl := r.availTTL
	if ttl <= 0 {
		ttl = defaultAvailTTL
	}
	if snap := r.avail.Load(); snap != nil {
		if st, ok := (*snap)[p.Name()]; ok && time.Since(st.lastSeen) <= 3*ttl {
			return st.ready // maintained readiness — no probe on the hot path
		}
	}
	// Cold or stale: probe inline, but bound it so a dead provider can't hang
	// the request beyond probeTimeout.
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return p.Available(pctx)
}

// probeTimeout bounds a single availability probe (background or inline
// fallback) so one unreachable endpoint can't stall a whole probe cycle.
const probeTimeout = 2 * time.Second

// availCacheTTL bounds how long a provider's Available() result is cached, so
// the defaultAvailTTL ticker (and the per-request /v1/providers handler) don't
// fire a live GET /v1/models at the upstream on every probe (#441). Providers
// that do a live reachability check — OpenAICompatProvider, ClaudeOAuthProvider
// — reuse this; MLXSupervisedProvider already keeps an equivalent probe cache.
const availCacheTTL = 30 * time.Second

// probeHTTPTimeout is the internal ceiling a provider's probeAvailable() puts on
// its own reachability HTTP call, independent of the provider's (much larger)
// request timeout. Available() holds a per-provider mutex across probeAvailable
// so concurrent callers collapse into one probe (#441); without an internal
// bound, a hung-but-accepting upstream would let a single /v1/providers request
// hold that mutex for the full client timeout (300s on the lmstudio providers),
// blocking the router's probeAll goroutine and Route()'s inline fallback — whose
// own probeTimeout bounds the probe, not the availMu.Lock() behind it (flight
// review, inference-pipeline-robustness FIX 1). ClaudeOAuthProvider already caps
// its probe at this ceiling; OpenAICompatProvider and OllamaProvider now match.
const probeHTTPTimeout = 10 * time.Second

// probeAll probes every registered provider concurrently, each bounded by
// probeTimeout, and atomically swaps in the resulting snapshot. A cycle costs
// roughly the slowest single probe — not the sum — and never blocks Route.
func (r *SimpleRouter) probeAll(ctx context.Context) {
	r.mu.RLock()
	ps := make([]Provider, len(r.providers))
	copy(ps, r.providers)
	r.mu.RUnlock()

	next := make(availSnapshot, len(ps))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Add(1)
		go func(p Provider) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, probeTimeout)
			defer cancel()
			ready := p.Available(pctx)
			mu.Lock()
			next[p.Name()] = availState{ready: ready, lastSeen: time.Now()}
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	r.avail.Store(&next)
}

// AddTickHook registers fn to run on every availTTL tick of the background
// availability maintainer (and once during Start's synchronous warm-up).
// Used by BuildRouter to wire discovery-based provider auto-registration
// (see maybeAutoRegisterClaudeOAuth) onto the same timer that already
// re-probes availability, so a node notices new local credentials without a
// restart. Hooks run sequentially and must be cheap/non-blocking-ish — they
// share the tick cadence with probeAll. Safe to call before or after Start.
func (r *SimpleRouter) AddTickHook(fn func()) {
	if fn == nil {
		return
	}
	r.mu.Lock()
	r.tickHooks = append(r.tickHooks, fn)
	r.mu.Unlock()
}

// runTickHooks invokes every registered tick hook. Snapshot the slice under
// the lock, then run hooks lock-free so a hook calling back into the router
// (e.g. RegisterProvider) cannot deadlock.
func (r *SimpleRouter) runTickHooks() {
	r.mu.RLock()
	hooks := make([]func(), len(r.tickHooks))
	copy(hooks, r.tickHooks)
	r.mu.RUnlock()
	for _, fn := range hooks {
		fn()
	}
}

// Start primes the availability cache synchronously, then maintains it on a
// ticker until ctx is cancelled or Close is called. Idempotent callers that
// never invoke Start keep working via available()'s inline-probe fallback.
func (r *SimpleRouter) Start(ctx context.Context) {
	if r.availTTL <= 0 {
		r.availTTL = defaultAvailTTL
	}
	r.probeAll(ctx) // warm the cache before the first request can read it
	r.runTickHooks()
	go func() {
		t := time.NewTicker(r.availTTL)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			case <-t.C:
				r.probeAll(ctx)
				r.runTickHooks()
			}
		}
	}()
}

// Close stops the background availability maintainer. Safe to call more than
// once and safe to call when Start was never invoked.
func (r *SimpleRouter) Close() {
	r.stopOnce.Do(func() {
		if r.stopCh != nil {
			close(r.stopCh)
		}
	})
}

// Stats returns current routing statistics.
func (r *SimpleRouter) Stats() RouterStats {
	stats := RouterStats{
		TotalRequests:                r.totalRequests.Load(),
		EscalationCount:              r.escalations.Load(),
		FallbackCount:                r.fallbacks.Load(),
		RequestsByProvider:           make(map[string]int64),
		ToolCallRejectionsByProvider: make(map[string]int64),
	}
	r.byProvider.Range(func(k, v any) bool {
		stats.RequestsByProvider[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	toolCallRejectionsByProvider.Range(func(k, v any) bool {
		stats.ToolCallRejectionsByProvider[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	if stats.TotalRequests > 0 {
		var local int64
		r.mu.RLock()
		for _, p := range r.providers {
			if p.Capabilities().IsLocal {
				if n, ok := stats.RequestsByProvider[p.Name()]; ok {
					local += n
				}
			}
		}
		r.mu.RUnlock()
		stats.SovereigntyRatio = float64(local) / float64(stats.TotalRequests)
	}
	return stats
}

func recordToolCallRejection(providerName string) { //nolint:unused // called from tool_loop.go (mcpserver build tag)
	if providerName == "" {
		return
	}
	counter, _ := toolCallRejectionsByProvider.LoadOrStore(providerName, &atomic.Int64{})
	counter.(*atomic.Int64).Add(1)
}

// buildCandidateOrder returns providers ordered by routing preference.
func (r *SimpleRouter) buildCandidateOrder(all []Provider, preferred string, chain []string) []Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var ordered []Provider
	seen := map[string]bool{}

	for _, name := range append([]string{preferred}, chain...) {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if p, ok := r.byName[name]; ok {
			ordered = append(ordered, p)
		}
	}
	// Append providers not in the explicit lists.
	for _, p := range all {
		if !seen[p.Name()] {
			ordered = append(ordered, p)
		}
	}
	return ordered
}

// computeScore returns a raw fitness score [0.0, 1.0] for a provider.
func computeScore(p Provider, req *CompletionRequest) float64 {
	score := 0.5
	caps := p.Capabilities()
	if caps.IsLocal {
		score += 0.4
	}
	if req.Metadata.PreferLocal && caps.IsLocal {
		score += 0.1
	}
	return score
}

// providerCanServe reports whether provider p can serve an explicit
// ModelOverride. It backs the compat-aware fallback in Route (FIX 3): a
// ModelOverride must not be carried into a FALLBACK provider that can't honour
// it. Route applies this only to candidates past the preferred one (i > 0); the
// preferred provider was resolved explicitly and is always eligible.
//
// The gate is scoped to the ACTUAL hazard: a bare model id fired at a
// LOCAL MODEL-SERVING provider (ollama, lmstudio/openai-compat, mlx) that only
// serves models it has loaded — such a provider 404s on an id it hasn't loaded,
// so a mismatched override there is an opaque failure and it must be skipped.
//
// Frontier providers are model-AGNOSTIC within their model family and honour
// req.ModelOverride verbatim (Anthropic API / claude-oauth pass the override
// straight through; the claude-code and codex CLIs pass it to `--model`). They
// each advertise only their single configured model in ModelsAvailable, yet
// happily serve any sibling id, so they must NEVER be gated by the advertised
// list — otherwise a "sonnet" request whose preferred claude-oauth is down would
// skip every backup frontier provider (a second claude-oauth, the anthropic
// provider, or the claude-code CLI) and fail with "no available provider",
// silently breaking exactly the degraded-mode frontier failover the router
// exists for. This is the class the flight review flagged in the first cut.
//
// The frontier set is identified structurally, not by name:
//   - !IsLocal  → a remote API provider (anthropic, claude-oauth): family-
//     agnostic, honours any override → eligible.
//   - AgenticHarness → a local CLI harness (claude-code, codex): also family-
//     agnostic and IsLocal, so IsLocal alone would wrongly gate it → eligible.
//
// Only a local, non-agentic model-serving provider is gated by its advertised
// models.
//
// Rules, chosen to preserve today's routing whenever no override is in play:
//   - Empty override → always true (default routing; the provider uses its own
//     configured model). This is the common case and must stay unchanged.
//   - Frontier provider (see above) → always true; it honours the override.
//   - Local model-serving provider whose Model()/ModelsAvailable includes the
//     override (exact or prefix, mirroring the /v1/models match in the provider
//     probes) → true.
//   - Local model-serving provider that declares NO specific model at all (empty
//     Model() and empty ModelsAvailable — generic endpoints) → true, so a
//     provider that never advertised a model isn't newly excluded.
//   - Otherwise (a local model-serving provider advertises specific models and
//     the override is none of them) → false: skip it so a bare local-model id
//     can't be fired at a provider that hasn't loaded it and would fail opaquely.
func providerCanServe(p Provider, modelOverride string) bool {
	if modelOverride == "" {
		return true
	}
	caps := p.Capabilities()
	// Frontier providers (remote APIs and local agentic CLIs) are model-agnostic
	// within their family and honour any override verbatim — never gate them.
	if !caps.IsLocal || caps.AgenticHarness {
		return true
	}
	declaredAny := false
	if m := p.Model(); m != "" {
		declaredAny = true
		if modelMatches(m, modelOverride) {
			return true
		}
	}
	for _, m := range caps.ModelsAvailable {
		if m == "" {
			continue
		}
		declaredAny = true
		if modelMatches(m, modelOverride) {
			return true
		}
	}
	// A local model-serving provider that advertises no model is treated as
	// model-agnostic and left eligible; one that advertises specific models but
	// not this one is skipped.
	return !declaredAny
}

// modelMatches reports whether an advertised model id serves the requested one,
// using the same exact-or-prefix rule the provider availability probes apply to
// /v1/models entries (e.g. "gemma-4-26b" advertised serves a "gemma-4" request).
func modelMatches(advertised, requested string) bool {
	return advertised == requested ||
		strings.HasPrefix(advertised, requested) ||
		strings.HasPrefix(requested, advertised)
}

func routeReason(escalated, fallback bool) string {
	switch {
	case fallback:
		return "fallback: primary provider unavailable"
	case escalated:
		return "escalated: no local provider available"
	default:
		return "local: best available provider"
	}
}

// ── BuildRouter ───────────────────────────────────────────────────────────────

// BuildRouter constructs a Router from workspace configuration.
// Reads .cog/config/providers.yaml; falls back to a default Ollama config.
func BuildRouter(cfg *Config, opts ...BuildRouterOption) (Router, error) {
	var bro buildRouterOpts
	for _, o := range opts {
		o(&bro)
	}

	pcfg, err := loadProvidersConfig(cfg)
	if err != nil {
		slog.Warn("router: providers.yaml not found, probing local backends", "err", err)
		pcfg = defaultProvidersConfig(cfg.LocalModel)
	}

	router := NewSimpleRouter(pcfg.Routing)

	for name, pc := range pcfg.Providers {
		if !pc.IsEnabled() {
			continue
		}
		p, err := makeProvider(name, pc, bro.procMgr)
		if err != nil {
			slog.Warn("router: skipping provider", "name", name, "err", err)
			continue
		}
		router.RegisterProvider(p)
		slog.Info("router: registered", "name", name, "model", pc.Model)
	}

	// Auto-discover OpenAI-compatible servers on well-known ports. Skippable so
	// tests don't pick up whatever LLM server happens to be live on the host.
	if !bro.noAutoDiscover {
		autoDiscoverOpenAICompat(router, pcfg)
	}

	// Discovery-not-declaration: auto-register a node-local claude-oauth
	// provider from the node's own Claude Code credentials if one isn't
	// already declared. Checked once here at boot, and re-checked on every
	// availability-maintainer tick below (see AddTickHook) so a credential
	// created after boot (the operator's first `claude /login` on this
	// machine) is picked up without a kernel restart. Skippable alongside
	// OpenAI-compat auto-discovery so tests stay hermetic (a devbox with a
	// logged-in `claude` CLI would otherwise non-deterministically register
	// this provider in unrelated router tests).
	if !bro.noAutoDiscover {
		maybeAutoRegisterClaudeOAuth(router, pcfg, bro.procMgr)
		router.AddTickHook(func() {
			maybeAutoRegisterClaudeOAuth(router, pcfg, bro.procMgr)
		})
	}

	// Start the background availability maintainer when a lifecycle context is
	// provided (the long-running daemon). Short-lived callers omit it and rely
	// on available()'s inline-probe fallback.
	if bro.ctx != nil {
		router.Start(bro.ctx)
	}

	return router, nil
}

type buildRouterOpts struct {
	procMgr        *ProcessManager
	ctx            context.Context
	noAutoDiscover bool
}

// BuildRouterOption configures BuildRouter.
type BuildRouterOption func(*buildRouterOpts)

// WithProcessManager provides a ProcessManager for providers that spawn subprocesses.
func WithProcessManager(pm *ProcessManager) BuildRouterOption {
	return func(o *buildRouterOpts) { o.procMgr = pm }
}

// WithoutAutoDiscovery disables the live probe of well-known OpenAI-compatible
// backends (LM Studio :1234, etc.) AND discovery-based claude-oauth
// auto-registration (see maybeAutoRegisterClaudeOAuth). Production leaves both
// on; tests pass this so router construction is hermetic and does not depend
// on whatever local LLM servers are running, or whatever Claude Code
// credentials happen to be logged in, on the host.
func WithoutAutoDiscovery() BuildRouterOption {
	return func(o *buildRouterOpts) { o.noAutoDiscover = true }
}

// WithRouterContext ties the router's background availability maintainer to
// ctx. When provided, BuildRouter starts the maintainer (warming the cache
// before returning); when cancelled, the maintainer stops. Omit it for
// short-lived callers (one-shot CLIs, tests) — they fall back to inline
// availability probing and start no goroutine.
func WithRouterContext(ctx context.Context) BuildRouterOption {
	return func(o *buildRouterOpts) { o.ctx = ctx }
}

func loadProvidersConfig(cfg *Config) (ProvidersConfig, error) {
	basePath := filepath.Join(cfg.CogDir, "config", "providers.yaml")
	data, err := os.ReadFile(basePath)
	if err != nil {
		return ProvidersConfig{}, err
	}
	var pcfg ProvidersConfig
	if err := yaml.Unmarshal(data, &pcfg); err != nil {
		return ProvidersConfig{}, fmt.Errorf("parse providers.yaml: %w", err)
	}

	// Deep-merge providers.local.yaml on top if present. The local file is
	// gitignored and holds node-specific endpoints, API key env-var names, and
	// fallback overrides. Documented since the file shipped; this is the
	// implementation that backs the documentation.
	localPath := filepath.Join(cfg.CogDir, "config", "providers.local.yaml")
	if localData, lerr := os.ReadFile(localPath); lerr == nil {
		var local ProvidersConfig
		if perr := yaml.Unmarshal(localData, &local); perr != nil {
			slog.Warn("router: providers.local.yaml parse error, ignoring overlay", "err", perr)
		} else {
			pcfg = mergeProvidersConfig(pcfg, local)
			slog.Info("router: providers.local.yaml merged",
				"providers_added", len(local.Providers),
				"path", localPath,
			)
		}
	} else if !os.IsNotExist(lerr) {
		slog.Warn("router: providers.local.yaml read error, ignoring overlay", "err", lerr)
	}

	applyLocalModelConfig(cfg, &pcfg)
	return pcfg, nil
}

// mergeProvidersConfig deep-merges overlay onto base and returns the result.
// Per-provider entries are field-level merged (overlay non-zero values win,
// zero values keep base). Routing uses the same shape. New providers in the
// overlay are added; the base map is preserved for keys not in overlay.
func mergeProvidersConfig(base, overlay ProvidersConfig) ProvidersConfig {
	if overlay.Providers != nil {
		if base.Providers == nil {
			base.Providers = make(map[string]ProviderConfig, len(overlay.Providers))
		}
		for name, oc := range overlay.Providers {
			if existing, ok := base.Providers[name]; ok {
				base.Providers[name] = mergeProviderConfig(existing, oc)
			} else {
				base.Providers[name] = oc
			}
		}
	}
	base.Routing = mergeRoutingConfig(base.Routing, overlay.Routing)
	base.ClaudeOAuth = mergeClaudeOAuthConfig(base.ClaudeOAuth, overlay.ClaudeOAuth)
	return base
}

// mergeClaudeOAuthConfig deep-merges overlay onto base. Auto is a pointer
// with nil-means-unset semantics: an overlay that omits claude_oauth.auto
// entirely (Auto == nil) must NOT clobber a base value — only an overlay
// that explicitly sets auto (true or false) overrides the base.
func mergeClaudeOAuthConfig(base, overlay ClaudeOAuthAutoConfig) ClaudeOAuthAutoConfig {
	if overlay.Auto != nil {
		base.Auto = overlay.Auto
	}
	return base
}

func mergeProviderConfig(base, overlay ProviderConfig) ProviderConfig {
	if overlay.Type != "" {
		base.Type = overlay.Type
	}
	if overlay.APIKeyEnv != "" {
		base.APIKeyEnv = overlay.APIKeyEnv
	}
	if overlay.Endpoint != "" {
		base.Endpoint = overlay.Endpoint
	}
	if overlay.Model != "" {
		base.Model = overlay.Model
	}
	if overlay.ContextWindow != 0 {
		base.ContextWindow = overlay.ContextWindow
	}
	if overlay.MaxTokens != 0 {
		base.MaxTokens = overlay.MaxTokens
	}
	if overlay.Timeout != 0 {
		base.Timeout = overlay.Timeout
	}
	if overlay.Enabled != nil {
		base.Enabled = overlay.Enabled
	}
	if len(overlay.Headers) > 0 {
		if base.Headers == nil {
			base.Headers = make(map[string]string, len(overlay.Headers))
		}
		for k, v := range overlay.Headers {
			base.Headers[k] = v
		}
	}
	if len(overlay.Options) > 0 {
		if base.Options == nil {
			base.Options = make(map[string]interface{}, len(overlay.Options))
		}
		for k, v := range overlay.Options {
			base.Options[k] = v
		}
	}
	return base
}

func mergeRoutingConfig(base, overlay RoutingConfig) RoutingConfig {
	if overlay.Default != "" {
		base.Default = overlay.Default
	}
	if overlay.LocalThreshold != 0 {
		base.LocalThreshold = overlay.LocalThreshold
	}
	// FallbackChain is intentionally replaced wholesale (not element-merged) so the overlay
	// can reorder or shrink the chain; element-wise merge would make removal impossible.
	if len(overlay.FallbackChain) > 0 {
		base.FallbackChain = overlay.FallbackChain
	}
	if overlay.MaxCostPerDayUSD != 0 {
		base.MaxCostPerDayUSD = overlay.MaxCostPerDayUSD
	}
	if len(overlay.ProcessStateRouting) > 0 {
		if base.ProcessStateRouting == nil {
			base.ProcessStateRouting = make(map[string]string, len(overlay.ProcessStateRouting))
		}
		for k, v := range overlay.ProcessStateRouting {
			base.ProcessStateRouting[k] = v
		}
	}
	return base
}

func applyLocalModelConfig(cfg *Config, pcfg *ProvidersConfig) {
	if cfg == nil || pcfg == nil || cfg.LocalModel == "" {
		return
	}
	for name, pc := range pcfg.Providers {
		providerType := pc.Type
		if providerType == "" {
			providerType = name
		}
		// Apply local_model override to any local OpenAI-compat provider
		// (lmstudio, vllm, llamacpp) or the legacy ollama type.
		switch providerType {
		case "lmstudio", "openai-compat", "vllm", "llamacpp", "ollama":
		default:
			continue
		}
		if pc.Model == "" || cfg.localModelConfigured {
			pc.Model = cfg.LocalModel
			pcfg.Providers[name] = pc
		}
	}
}

// makeProvider instantiates a Provider from a ProviderConfig.
// The provider type is inferred from the name if Type is empty.
func makeProvider(name string, pc ProviderConfig, procMgr *ProcessManager) (Provider, error) {
	t := pc.Type
	if t == "" {
		t = name
	}
	switch t {
	case "ollama":
		return NewOllamaProvider(name, pc), nil
	case "anthropic":
		return NewAnthropicProvider(name, pc), nil
	// "vllm" is a first-class config citizen: it routes through the same
	// OpenAI-compatible HTTP dispatch path as lmstudio and llama.cpp servers.
	// vLLM's /v1/chat/completions and /v1/models endpoints satisfy the
	// OpenAICompatProvider contract; no separate provider implementation is
	// required for the unsupervised case (operator runs vllm serve themselves).
	// A future "vllm-supervised" type will add launchd/systemd lifecycle
	// management, mirroring mlx-supervised. See docs/inference/vllm.md and
	// the ollama-to-vllm-migration-plan cogdoc.
	case "openai-compat", "openai", "lmstudio", "vllm", "llamacpp":
		// Opt-in: if this backend declares options.model_state.manage:true,
		// register a companion lms-model-state reconciler in the global
		// reconcile registry so the autonomic ticker can keep the declared
		// model loaded at the declared context. The dispatch provider itself is
		// unchanged — this is an orthogonal, OFF-BY-DEFAULT concern.
		maybeRegisterModelStateReconciler(name, pc)
		return NewOpenAICompatProvider(name, pc), nil
	case "claude-code":
		if procMgr == nil {
			procMgr = NewProcessManager(ProcessManagerConfig{})
		}
		return NewClaudeCodeProvider(name, pc, procMgr), nil
	case "claude-oauth":
		// OAuth-credentialed provider using the operator's managed Claude
		// subscription. Reads credentials from the macOS keychain /
		// CLAUDE_CODE_OAUTH_TOKEN env var / ~/.claude/.credentials.json.
		// No API key needed — credential lifecycle is self-managed.
		//
		// CLI fallback: on a persistent 429 (subscription burst rate-limit with
		// overage disabled — see the 429 RCA) the provider delegates to a
		// claude-code CLI provider, which reaches the same Max subscription via
		// the official client. Built here so the fallback shares the process
		// manager.
		if procMgr == nil {
			procMgr = NewProcessManager(ProcessManagerConfig{})
		}
		cliFallback := NewClaudeCodeProvider(name+"-cli-fallback", ProviderConfig{
			Type:    "claude-code",
			Model:   "sonnet", // canonical CLI alias (matches the claude-code provider)
			Timeout: pc.Timeout,
		}, procMgr)
		return NewClaudeOAuthProvider(name, pc, cliFallback), nil
	case "codex":
		return NewCodexProvider(name, pc), nil
	case "pi":
		if procMgr == nil {
			procMgr = NewProcessManager(ProcessManagerConfig{})
		}
		return NewPiProvider(name, pc, procMgr), nil
	case "stub":
		return NewStubProvider(name, "stub response"), nil
	case mlxSupervisedType:
		// mlx-supervised requires Apple Metal (macOS only). On any other OS,
		// skip the provider rather than registering a dead driver — mirrors the
		// codex GOOS guard at provider_codex.go:99.
		if runtime.GOOS != "darwin" {
			slog.Debug("router: mlx-supervised skipped on non-darwin platform",
				"name", name, "goos", runtime.GOOS)
			return nil, fmt.Errorf("mlx-supervised provider %q requires darwin (got %s)", name, runtime.GOOS)
		}
		supervisor := ServiceSupervisor(NewLaunchctlController())
		p, err := newMLXSupervisedProvider(name, pc, supervisor)
		if err != nil {
			return nil, fmt.Errorf("mlx-supervised %q: %w", name, err)
		}
		// Register in the global reconcile registry so the autonomic ticker can
		// run the full plan/apply self-heal cycle. UpsertProvider is used instead
		// of RegisterProvider to allow safe re-registration (e.g. on config reload
		// or in tests that call BuildRouter multiple times).
		reconcile.UpsertProvider(mlxSupervisedType+"/"+name, p)
		slog.Info("router: registered mlx-supervised provider in reconcile registry", "name", name)
		return p, nil
	default:
		return nil, fmt.Errorf("unknown provider type %q", t)
	}
}

// maybeRegisterModelStateReconciler registers a companion lms-model-state
// reconciler for an OpenAI-compatible backend IFF it opts in via
// options.model_state.manage:true. This is the OPT-IN, OFF-BY-DEFAULT guardrail:
// absent the block (or with manage:false) nothing is registered and the kernel
// reconciles no model state on this backend.
//
// The token is pulled from the same api_key_env the dispatch provider uses.
// UpsertProvider allows safe re-registration across BuildRouter calls.
func maybeRegisterModelStateReconciler(name string, pc ProviderConfig) {
	ms := parseModelStateOptions(pc.Options)
	if !ms.Manage {
		return
	}
	// liveState comes from LM Studio's native /api/v0/models, served only by LM
	// Studio backends. vllm/llamacpp share the "openai" dispatch case but have no
	// /api/v0 endpoint, so a model_state block copied onto one would register a
	// reconciler that can only ever probe-fail. Restrict to LM-Studio-capable
	// types (a generic "openai" that is really vllm would still degrade to
	// Suspended, but excluding the explicit vllm/llamacpp types avoids the footgun).
	switch pc.Type {
	case "lmstudio", "openai", "openai-compat":
		// may serve /api/v0/models
	default:
		slog.Debug("router: lms-model-state not applicable for provider type; skipping",
			"name", name, "type", pc.Type)
		return
	}
	token := ""
	if pc.APIKeyEnv != "" {
		token = os.Getenv(pc.APIKeyEnv)
	}
	// Workspace root is injected later via LoadConfig (reconcile daemon path);
	// pass "" here and let the provider resolve the actuator best-effort.
	msp, err := newLMSModelStateProvider(name, pc, token, "")
	if err != nil {
		slog.Warn("router: lms-model-state reconciler construction failed", "name", name, "err", err)
		return
	}
	reconcile.UpsertProvider(lmsModelStateType+"/"+name, msp)
	slog.Info("router: registered lms-model-state reconciler in reconcile registry",
		"name", name, "model", ms.Model, "context_length", ms.ContextLength)
}

// probeLocalBackend probes LM Studio (:1234) then Ollama (:11434) with a
// short timeout. Returns the first reachable (name, endpoint) pair, or
// ("", "") when neither is up. Used by defaultProvidersConfig to avoid
// registering a dead default on fresh installs that have no local LLM stack.
func probeLocalBackend() (name, endpoint string) {
	type candidate struct {
		name     string
		endpoint string
		path     string
	}
	candidates := []candidate{
		{name: "lmstudio", endpoint: "http://localhost:1234", path: "/v1/models"},
		{name: "ollama", endpoint: "http://localhost:11434", path: "/api/version"},
	}
	client := &http.Client{Timeout: 1 * time.Second}
	for _, c := range candidates {
		resp, err := client.Get(c.endpoint + c.path)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return c.name, c.endpoint
			}
		}
	}
	return "", ""
}

// defaultProvidersConfig returns a minimal provider config for use when no
// providers.yaml is present. It probes LM Studio (:1234) then Ollama (:11434)
// to pick a reachable backend. When neither is up, the config records the
// state so the router can surface a clear "no local model configured" signal
// instead of routing to a dead default.
func defaultProvidersConfig(localModel string) ProvidersConfig {
	enabled := true
	disabledFalse := false

	backendName, backendEndpoint := probeLocalBackend()

	switch backendName {
	case "lmstudio":
		// LM Studio is reachable at :1234 (OpenAI-compat).
		slog.Info("router: no providers.yaml, auto-selected LM Studio as default local backend")
		return ProvidersConfig{
			Providers: map[string]ProviderConfig{
				"lmstudio": {
					Type:     "openai-compat",
					Enabled:  &enabled,
					Endpoint: backendEndpoint,
					Model:    localModel, // empty = LM Studio uses the loaded model
					Timeout:  60,
				},
			},
			Routing: RoutingConfig{
				Default:        "lmstudio",
				LocalThreshold: 0.8,
				FallbackChain:  []string{"lmstudio"},
			},
		}

	case "ollama":
		// Ollama is reachable but is no longer the default backend. Warn and
		// register it under its own name so existing Ollama installs still work,
		// but log a clear deprecation notice. Operators should migrate to LM Studio
		// and set providers.yaml with "type: lmstudio" or "type: openai-compat".
		if localModel == "" {
			localModel = defaultOllamaModel
		}
		slog.Warn("router: Ollama detected at :11434 but Ollama is decommissioned as the default backend; " +
			"registering temporarily — migrate to LM Studio (127.0.0.1:1234) and providers.yaml")
		return ProvidersConfig{
			Providers: map[string]ProviderConfig{
				"ollama": {
					Type:     "ollama",
					Enabled:  &enabled,
					Endpoint: backendEndpoint,
					Model:    localModel,
					Timeout:  60,
				},
			},
			Routing: RoutingConfig{
				Default:        "ollama",
				LocalThreshold: 0.8,
				FallbackChain:  []string{"ollama"},
			},
		}

	default:
		// Neither LM Studio nor Ollama is reachable. Register a placeholder
		// provider that is explicitly disabled so the router can report a clear
		// "no local model configured" state instead of silently failing on the
		// first inference request.
		slog.Warn("router: no providers.yaml and no local LLM backend reachable " +
			"(tried LM Studio :1234, Ollama :11434); add providers.yaml or start LM Studio")
		return ProvidersConfig{
			Providers: map[string]ProviderConfig{
				"no-local-model": {
					Type:    "stub",
					Enabled: &disabledFalse,
				},
			},
			Routing: RoutingConfig{
				LocalThreshold: 0.8,
			},
		}
	}
}

// ── Auto-discovery ───────────────────────────────────────────────────────────

// openaiCompatProbeEndpoint defines a well-known local endpoint to auto-discover.
type openaiCompatProbeEndpoint struct {
	name     string
	endpoint string
}

// openaiCompatWellKnownEndpoints lists endpoints to probe on startup.
// Ollama (localhost:11434) was previously handled separately; it is now
// decommissioned. These are OpenAI-compatible server endpoints.
var openaiCompatWellKnownEndpoints = []openaiCompatProbeEndpoint{
	{name: "lmstudio", endpoint: "http://localhost:1234"},
}

// autoDiscoverOpenAICompat probes well-known local ports for OpenAI-compatible
// servers and registers any that respond. Skips endpoints already configured
// in providers.yaml to avoid duplicates.
func autoDiscoverOpenAICompat(router *SimpleRouter, pcfg ProvidersConfig) {
	// Build a set of already-configured endpoints to avoid duplicates.
	configuredEndpoints := map[string]bool{}
	configuredNames := map[string]bool{}
	for name, pc := range pcfg.Providers {
		if pc.Endpoint != "" {
			configuredEndpoints[strings.TrimRight(pc.Endpoint, "/")] = true
		}
		configuredNames[name] = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, probe := range openaiCompatWellKnownEndpoints {
		endpoint := strings.TrimRight(probe.endpoint, "/")
		if configuredEndpoints[endpoint] || configuredNames[probe.name] {
			continue
		}

		p := NewOpenAICompatProvider(probe.name, ProviderConfig{
			Type:     "openai-compat",
			Endpoint: endpoint,
			Timeout:  5,
		})
		if p.Available(ctx) {
			router.RegisterProvider(p)
			slog.Info("router: auto-discovered", "name", probe.name, "endpoint", endpoint)
		}
	}
}
