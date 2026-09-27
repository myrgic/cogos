# RFC-0007: Named-Provider Override for Agent Dispatch

| Field    | Value                                                                                          |
|----------|------------------------------------------------------------------------------------------------|
| Status   | Draft                                                                                          |
| Author   | @chazmaniandinkle                                                                              |
| Tracking | [#TBD](https://github.com/myrgic/cogos/issues/) (Layer 2 + 3 follow-ups)                       |
| Target   | `v0.7.0` (Layer 1, this RFC) · later releases (Layers 2 + 3)                                   |
| Relates  | [RFC-0006 vLLM provider](0006-vllm-pagedattention-provider.md), bus peering RFC (internal reference omitted) |

## Summary

`cog_dispatch_to_harness` today exposes a binary `model: "e4b" | "26b"` enum that maps to two hardcoded backends. There is no way for a caller to dispatch against a *named* provider declared in `providers.yaml` / `providers.local.yaml`, even when the substrate has the provider fully wired (endpoint, auth, model). This RFC defines the wire shape and dispatcher contract that lets a caller say "run this on `desktop`" (or any other configured provider) and have the kernel honor it.

It scopes three layers. **Layer 1** ships in this RFC's PR: thread an optional `provider` argument through the MCP tool, `DispatchRequest`, and the `HarnessDispatcher`, resolving against the existing providers registry. **Layer 2** (separate PR) lets agent identity cards declare `inference_provider:` in frontmatter so a dispatcher doesn't need to specify a provider per call. **Layer 3** (separate PR) makes the autonomic loop consult `routing.process_state_routing`, which it currently bypasses.

## Background

### Current routing path

`cog_dispatch_to_harness` accepts `model: "e4b" | "26b"`. In `HarnessDispatcher.runSlot` these map to:

- `e4b` → harness default (`h.ollamaURL` / `h.model`)
- `26b` → `d.LMStudioBaseURL` + `d.LMStudioModel`, `BackendKind: backendKindOpenAI`

`LMStudioBaseURL` is resolved once at kernel startup via `detectLocalLLMTarget(ctx, COGOS_LLM_ENDPOINT)`. There is exactly one OpenAI-compatible target per kernel process, and the caller cannot select a different one at dispatch time.

The substrate already has richer routing: `SimpleRouter.Route()` respects `CompletionRequest.Metadata.PreferProvider` and consults `ProcessStateRouting`. But the dispatch path never constructs a `CompletionRequest` and never calls the router. Two parallel routing surfaces, one of them blind.

### Why this matters now

With desktop LM Studio (192.0.2.10:1234, `google/gemma-4-26b-a4b`) wired as the `desktop` provider in `providers.local.yaml`, the substrate has the quality ceiling available — but Claude Code / MCP callers can't reach it through `cog_dispatch_to_harness`. The dispatch path can hit `desktop` only by overloading `COGOS_LLM_ENDPOINT` to point there globally, which forces *every* "26b" dispatch through the desktop and breaks isolation from the laptop's local LM Studio.

The same friction will recur for any future provider — vLLM (RFC-0006), Codex, peer-fleet members. Hardcoding two model names is the wrong primitive; a provider name is.

## Layer 1 — `provider` parameter on dispatch (this PR)

### Wire shape

`dispatchToHarnessInput` (MCP) gains one field:

```jsonc
{
  "provider": "desktop"   // optional. When set, overrides `model` routing.
                          // Must name a provider in providers.{yaml,local.yaml}.
                          // Unknown names error before the dispatch runs.
}
```

`engine.DispatchRequest` gains the matching field:

```go
type DispatchRequest struct {
    // ... existing fields ...
    Provider string  // Named-provider override; resolved by HarnessDispatcher.
}
```

### Resolution contract

The dispatcher resolves a provider name into the same four values it already feeds into `ExecuteScopedOptions`: `BackendURL`, `BackendKind`, `Model`, and (new) `APIKey`. The resolver is an interface on `HarnessDispatcher`:

```go
// ProviderResolver translates a provider name into the backend config the
// harness needs. ok=false signals an unknown name; the dispatcher surfaces
// that as invalid_input.
type ProviderResolver interface {
    ResolveDispatchProvider(name string) (ResolvedProvider, bool)
}

type ResolvedProvider struct {
    BackendURL  string  // e.g. http://192.0.2.10:1234
    BackendKind string  // "openai" | "ollama"
    Model       string  // e.g. google/gemma-4-26b-a4b
    APIKey      string  // already materialized from api_key_env
}
```

Wiring lives in `main.go`: the kernel passes a resolver backed by the live providers registry. Tests pass a stub.

### Precedence

In `runSlot`:

1. If `req.Provider != ""` and the resolver returns ok → use the resolved values; ignore `req.Model`.
2. Else fall through to the existing `Model`-based dispatch (e4b/26b).
3. Unknown `req.Provider` → `AgentControllerError{Code: "invalid_input"}` at normalize time (`Normalize()` rejects).

`DispatchResult.ModelUsed` carries the resolved model id when a provider override fired (currently a `DispatchModel` enum; we widen it to accept arbitrary model strings or add a `ProviderUsed` field — chosen below).

### Auth plumbing

`ExecuteScopedOptions` gains `APIKey string`. `chatCompletionOpenAI` sets `Authorization: Bearer <APIKey>` when non-empty. The existing "26b" path (no APIKey) keeps its current behavior, which means laptop LM Studio dispatches will continue to work as long as the laptop's server doesn't require auth; if it does, the legacy "26b" wire-up needs Layer 1 anyway to reach the laptop key (`LMS_API_KEY`). This RFC does not block on retrofitting the legacy "26b" path — that's an opportunistic follow-up.

### Reachability cache

The existing `lmStudioReachable` cache is keyed on the singleton URL. For per-request providers, the cache becomes a `map[string]reachabilityEntry` keyed by `BackendURL`. Same TTL (60s), same probe shape, just keyed.

### Out of scope for Layer 1

- Agent-card-declared provider preference (Layer 2).
- Router integration / process-state routing in the autonomic loop (Layer 3).
- Refactoring `Model` into a free-form string (deferred until Layer 2 surfaces a real need).
- Multi-provider fallback chain at dispatch time (the router already does this for `CompletionRequest`; not exposed via dispatch here).

## Layer 2 — agent-card provider preference (follow-up RFC or PR)

Identity cards (`.cog/agents/identities/*.cog.md` in the workspace) gain optional frontmatter:

```yaml
inference:
  provider: desktop          # optional; matches providers.yaml name
  model: google/gemma-4-26b-a4b   # optional; overrides provider default
```

`DispatchRequest.Identity` already flows through dispatch (today as observability metadata). On Layer 2, the controller adapter reads the identity card via the kernel's identity registry, materializes `inference.provider`/`inference.model`, and populates `DispatchRequest.Provider` when the caller didn't explicitly override.

Precedence becomes: explicit `provider` arg → agent-card declaration → `Model` enum → autodetect.

This lets a Cog identity (Workspace Guardian) declare "I run on desktop" once in its card and have every dispatch into that agent honor it, no per-call argument needed.

## Layer 3 — router-aware autonomic loop (follow-up RFC or PR)

`LocalHarnessController` builds its provider once at startup via `buildLocalProvider(target, model, timeoutSec)` and never consults the router. To realize the existing `process_state_routing` config intent ("consolidating cycles should run on desktop"), the controller needs to:

1. Read `process_state` from the running session.
2. Consult `cfg.Routing.ProcessStateRouting[state]` → provider name.
3. Resolve via the same `ProviderResolver` introduced in Layer 1.
4. Fall back to the current local provider when the named provider is unreachable.

This is a behavior change for the autonomic loop and warrants its own RFC plus a feature flag for at least one release cycle.

## Migration & compatibility

Layer 1 is additive. Existing callers that pass `model: "e4b"` or `model: "26b"` continue to work unchanged. Callers that pass neither also continue to work (default to e4b). The new `provider` field is opt-in.

`DispatchResult.ModelUsed` is currently typed `DispatchModel` (a string alias). To carry arbitrary resolved model ids, we either:
- (a) widen `ModelUsed`'s documented meaning to accept any string ("freeform when ProviderUsed is set");
- (b) add a sibling `ProviderUsed string` field and keep `ModelUsed` strictly within the enum;
- (c) emit "provider:NAME" as a synthetic `ModelUsed` value.

This PR takes option (b) — additive, doesn't reinterpret existing field semantics. `ProviderUsed` is empty when the dispatch used the legacy enum path.

## Tests (Layer 1)

- `Normalize` rejects unknown provider names (invalid_input).
- `runSlot` with a resolver hit overrides `Model` and reports `ProviderUsed`.
- `runSlot` with no resolver hit and a non-empty `Provider` returns invalid_input.
- `chatCompletionOpenAI` sets the Authorization header when APIKey is non-empty and omits it when empty.
- The reachability cache is per-URL (two distinct providers with overlapping URLs don't collide; two providers with the same URL share the probe result).

## Acceptance criteria (Layer 1)

- `cog_dispatch_to_harness({provider: "desktop", task: "..."})` routes to the configured desktop endpoint with the desktop's `LMS_DESKTOP_API_TOKEN` and the resolved model.
- Existing dispatch behavior (no `provider` field) is byte-for-byte unchanged.
- Unknown provider names fail fast at normalize.
- New unit tests pass; existing dispatch tests pass without modification.
- One live test (gated on env, like `agent_dispatch_live_test.go`) confirms an end-to-end dispatch against the desktop endpoint.

## Open questions

1. Should `Provider` be exposed via a dedicated `provider` arg, or modeled as a richer `routing: { provider, model }` object in MCP input? Layer 1 takes the flat arg; if Layer 2 wants more structure, we add it then.
2. Layer 3 may want to keep the autonomic loop's hot-path provider sticky (avoid re-resolving every cycle). Cache lifecycle is a Layer 3 design decision, not specified here.
3. Whether `ProviderUsed` should also surface in the batch-level `Notes` for visibility. Probably yes; deferred to PR review.
