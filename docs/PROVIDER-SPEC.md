# CogOS v3 Inference Provider Interface — Design Specification

> **Status:** Contract complete, implementations pending
> **Created:** 2026-03-19
> **Derives from:** v3 Design Spec, Externalized MoE, Sovereignty Stack, Apple Silicon Constellation
> **Package:** `provider` (Go)

---

## Design Rationale

The inference provider interface is the abstraction layer between the v3 kernel and the actual LLM backends that do computation. It exists because models are organs, not the organism — swappable, upgradeable processing substrates that the workspace routes through based on task requirements.

Three architectural decisions drive the design:

**1. Foveated context as the request primitive.** The `CompletionRequest` doesn't carry raw chat messages. It carries the assembled output of the attentional field: nucleus content (identity), momentum (trajectory), foveal content (current focus), and parafoveal content (background). The provider receives a pre-assembled context package. It doesn't decide what's relevant — the context engine already did that.

**2. Sovereignty gradient as routing policy.** The `Router` implements the 90/10 split: 90% of requests handled locally, 10% escalated to frontier models. This isn't a preference — it's the design target from the sovereignty stack. The router uses sigmoid scoring (per DeepSeek-V3: independent per-provider, not softmax competition) with swap penalties that naturally bias toward already-loaded local models.

**3. Process state affects routing.** The continuous process model means requests arrive in different states: active interaction needs the best available model, consolidation tasks need cheap local compute, dormant heartbeats might not need inference at all. The router reads process state from request metadata and adjusts provider selection accordingly.

---

## Interface Overview

### Provider

```go
type Provider interface {
    Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error)
    Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error)
    Name() string
    Available(ctx context.Context) bool
    Capabilities() ProviderCapabilities
    Ping(ctx context.Context) (time.Duration, error)
}
```

Every LLM backend implements this interface. The kernel never calls a model API directly — it always goes through a `Provider`. This gives us:

- **Uniform cost tracking:** Every request, regardless of backend, produces the same `TokenUsage` and `InferenceEvent` records. Feeds into the existing CostTracker.
- **Transparent fallback:** If Ollama goes down, the router tries MLX, then OpenRouter, then Anthropic. The caller doesn't know or care.
- **Test isolation:** The `StubProvider` lets us test the entire request pipeline — context assembly, routing, tool callbacks — without any real inference.

### Router

```go
type Router interface {
    Route(ctx context.Context, req *CompletionRequest) (Provider, *RoutingDecision, error)
    RegisterProvider(p Provider)
    DeregisterProvider(name string)
    Stats() RouterStats
}
```

The Router is the externalized gating network. It maps directly to the Sentinel from the MoE architecture, operating at the system level instead of the token level. Routing algorithm:

1. **Filter** by required capabilities (from `RequestMetadata.RequiredCapabilities`)
2. **Filter** by availability (`Provider.Available()`)
3. **Score** remaining providers independently via sigmoid (not softmax)
4. **Apply swap penalties** (VRAM-hot: 0.0, RAM-warm: 0.05, disk-cold: 0.20)
5. **Check local threshold:** if best local `AdjustedScore >= LocalThreshold` → route local
6. **Escalate** to cloud if no local provider meets threshold
7. **Fallback chain** if selected provider returns an error

Every routing decision is recorded as a `RoutingDecision` — this is training signal for the future learned sentinel.

---

## CompletionRequest: The Foveated Context Package

The request structure mirrors the v3 attentional field:

```
┌─────────────────────────────────────────────────┐
│ CompletionRequest                                │
│                                                   │
│  SystemPrompt ← Nucleus content (identity core)  │
│                                                   │
│  Messages[]   ← Conversation in foveal window    │
│                                                   │
│  Context[]    ← Assembled attentional field:      │
│    ├── nucleus items     (salience ≥ threshold)   │
│    ├── momentum items    (recent trajectory)      │
│    ├── foveal items      (current focus)          │
│    └── parafoveal items  (background, on demand)  │
│                                                   │
│  Tools[]      ← MCP tool definitions              │
│                                                   │
│  Metadata     ← Request ID, process state,        │
│                  salience snapshot, priority       │
└─────────────────────────────────────────────────┘
```

### Context Items and Attentional Zones

Each `ContextItem` has a salience score and an `AttentionalZone` tag:

| Zone | v2 TAA Equivalent | Budget | Description |
|------|-------------------|--------|-------------|
| `nucleus` | Tier 1 (identity, 33%) | ~20% | Content that never drops below threshold |
| `momentum` | Tier 2 (temporal, 25%) | ~25% | Recent commit stream, trajectory |
| `foveal` | Tier 3 (present, 33%) | ~35% | Current interaction focus |
| `parafoveal` | Tier 4 (semantic, 6%) | ~20% | Background knowledge on demand |

Budget percentages are guidelines, not hard limits. The context engine manages the actual token budget before assembling the request.

### Metadata for Routing

`RequestMetadata` carries information the router needs but the model doesn't see:

- **ProcessState** — active/receptive/consolidating/dormant. Consolidation tasks route to cheap providers.
- **Priority** — low/normal/high/critical. High priority skips cost optimization.
- **PreferLocal** — sovereignty flag. When true, the router raises the escalation threshold for this request.
- **MaxCostUSD** — hard ceiling per request. The router skips providers that would exceed it.
- **RequiredCapabilities** — must-haves (e.g., tool_use, streaming). Only matching providers are candidates.
- **Source** — origin label for analytics ("chat", "consolidation", "coherence-check").

---

## CompletionResponse

```go
type CompletionResponse struct {
    Content      string        // Text response
    ToolCalls    []ToolCall    // MCP tool invocations
    StopReason   string        // end_turn | max_tokens | tool_use
    Usage        TokenUsage    // Token accounting
    ProviderMeta ProviderMeta  // Who served it, how fast
}
```

The response is provider-agnostic. Whether it came from Anthropic, Ollama, or a stub, the kernel sees the same structure. The `ProviderMeta` records provenance for the ledger.

### Tool Call Loop

When `ToolCalls` is non-empty:

1. Kernel receives response with `StopReason: "tool_use"`
2. Kernel executes each tool call via MCP
3. Tool results are added as `Message{Role: "tool"}` entries
4. Kernel sends a new `CompletionRequest` with the tool results
5. Repeat until the model returns `StopReason: "end_turn"` or max iterations

The tool loop routes back through the same Provider (or through the Router if the provider changed between turns). This is how MCP tool callbacks wire into the inference layer.

---

## Concrete Providers

### AnthropicProvider

- **API:** Anthropic Messages API (`/v1/messages`)
- **Auth:** API key from environment variable
- **Capabilities:** streaming, tool_use, vision, long_context, caching, json_output
- **IsLocal:** false
- **Key mapping:** SystemPrompt → `system`, Context items → prepended to system, Tools → Anthropic tool format

The primary frontier provider. Used for active interaction when local models can't meet the confidence threshold, and for complex reasoning that exceeds local model capability.

### OpenRouterProvider

- **API:** OpenAI-compatible chat completions
- **Auth:** API key from environment variable
- **Capabilities:** varies by model selected
- **IsLocal:** false
- **Key value:** Access to Llama, Mistral, DeepSeek, etc. through one key

Useful for fleet diversity — when you want to try different frontier models without managing multiple API keys. Also serves as a secondary cloud fallback if the Anthropic API is down.

### OllamaProvider

- **API:** Ollama HTTP API (`/api/chat`)
- **Auth:** none (local)
- **Capabilities:** streaming, json_output
- **IsLocal:** true
- **Key feature:** Reports model load state for swap penalty calculation

The primary local provider on systems with discrete GPUs. Manages model downloading, quantization, and VRAM scheduling. The three-tier memory hierarchy (VRAM-hot, RAM-warm, disk-cold) maps directly to swap penalties in the routing algorithm.

### MLXProvider

- **API:** mlx-lm server HTTP API (OpenAI-compatible)
- **Auth:** none (local)
- **Capabilities:** streaming, json_output
- **IsLocal:** true
- **Key feature:** Zero swap penalty on unified memory

The primary local provider on Apple Silicon. Unified memory means multiple models can be resident simultaneously with no VRAM/RAM distinction. This is what makes the 90/10 sovereignty ratio achievable on a laptop.

### StubProvider

- **API:** in-memory, configurable responses
- **Auth:** none
- **Capabilities:** all (configurable)
- **IsLocal:** true
- **Key feature:** Error injection, latency simulation

Essential for testing. The full request pipeline — context assembly, routing, tool callbacks, ledger recording — can be exercised without any real inference. Also useful for offline development and benchmarking.

---

## Configuration

```yaml
# .cog/config/providers.yaml

providers:
  anthropic:
    api_key_env: ANTHROPIC_API_KEY
    model: claude-sonnet-5
    max_tokens: 8192
    timeout: 120

  openrouter:
    api_key_env: OPENROUTER_API_KEY
    model: anthropic/claude-sonnet-4
    headers:
      HTTP-Referer: "https://github.com/cogos"
      X-Title: "CogOS v3"

  ollama:
    endpoint: http://localhost:11434
    model: qwen2.5:9b
    timeout: 60

  mlx:
    endpoint: http://localhost:8080
    model: mlx-community/Qwen2.5-7B-Instruct-4bit
    timeout: 60

  stub:
    enabled: false  # Enable for testing

routing:
  default: anthropic
  local_threshold: 0.8
  fallback_chain: [ollama, mlx, openrouter, anthropic]
  max_cost_per_day_usd: 5.00
  process_state_routing:
    active: anthropic        # Best model for interaction
    receptive: ollama        # Small local for triage
    consolidating: mlx       # Cheap local for maintenance
    dormant: stub            # No inference needed
```

### How Configuration Maps to the Sovereignty Gradient

The `local_threshold: 0.8` means: if the best local provider scores ≥ 0.8, handle locally. Only escalate to cloud when the local model's fitness for the task drops below that threshold.

The `fallback_chain` defines degradation order: try the preferred local first, then alternate local, then cloud options. The chain ensures the system always has a path to completion.

The `process_state_routing` gives default provider preferences per process state. These are overrides the router applies before scoring — consolidation tasks don't even consider Anthropic unless the process state mapping says so.

---

## Wiring into v3

### The `/v1/chat/completions` Endpoint

The current v2 endpoint returns 501. In v3, the flow becomes:

```
HTTP request arrives
  → Parse into CompletionRequest
  → Attentional gate assembles context (context engine)
    → Query salience scores
    → Select nucleus + momentum + foveal + parafoveal items
    → Budget-allocate tokens across zones
  → Router selects provider
    → Filter by capabilities
    → Score with sigmoid + swap penalties
    → Apply sovereignty threshold
  → Provider.Complete() or Provider.Stream()
  → Record InferenceEvent to ledger
  → Return CompletionResponse as HTTP response
```

### Process Loop Integration

The continuous process's phases use different provider tiers:

| Phase | Inference Need | Provider Preference |
|-------|---------------|-------------------|
| Active interaction | Full reasoning | Frontier or large local |
| Receptive triage | Intent classification | Small local (nano fleet) |
| Consolidation | Memory reorganization | Cheap local |
| Dormant heartbeat | None or minimal | Stub or skip |

The consolidation phase specifically benefits from cheap local inference — it can afford to reorganize memory, run coherence checks, and adjust weights using a small model that costs nothing per token.

### MCP Tool Callbacks

When a model returns tool calls:

1. The kernel receives `CompletionResponse.ToolCalls`
2. Each tool call is executed via the MCP tool server
3. Results are packaged as `Message{Role: "tool"}`
4. A new `CompletionRequest` is sent through the same provider (or re-routed)
5. The tool loop continues until the model stops calling tools

The Provider interface handles this transparently — it doesn't know about MCP. The kernel manages the loop.

### Ledger Events

Every inference request produces an `InferenceEvent` that feeds into:

1. **CostTracker** — per-request cost logging (existing v2 system, extended)
2. **Ledger** — hash-chained event log (existing v2 system)
3. **Router training signal** — routing decisions + outcomes for future sentinel training
4. **Sovereignty dashboard** — local vs. cloud ratio tracking

---

## Future: Speculative Parallel Drafting

The `SpeculativeDrafter` interface is included for Phase 2+. The pattern:

1. Router scores top 2-3 candidate providers
2. All candidates generate a short prefix (~30 tokens) in parallel
3. A stream evaluator (nano-model, CPU) scores the partial outputs
4. Winner continues, others abort
5. Added latency: ~200-500ms; better routing accuracy

On unified memory this is near-free — all candidates are loaded. On discrete GPU it's constrained by VRAM, so the sentinel narrows candidates to those that fit simultaneously.

The drafting loop produces training signal for the sentinel router. Over time, the sentinel's pre-hoc routing gets accurate enough that speculation becomes confirmation.

---

## Implementation Order

1. **Provider interface + StubProvider** — get the contracts compiling, tests passing
2. **AnthropicProvider** — the current default, gets v3 serving requests immediately
3. **Router (rule-based)** — hardcoded scoring based on process state + capabilities
4. **OllamaProvider** — first local inference path
5. **MLXProvider** — Apple Silicon optimized local path
6. **OpenRouterProvider** — multi-model cloud access
7. **Learned sentinel** — replace rule-based router with trained classifier
8. **Speculative drafter** — parallel candidate evaluation

This order follows the v3 developmental trajectory: Stage 1 (frontier dependency) → Stage 4 (sentinel goes local) → Stage 8 (thinker goes local).

---

*The provider interface is where the sovereignty gradient meets the code. Every routing decision is a vote for where cognition happens — on your hardware, under your control, or on someone else's server, under their terms. The interface is designed so that the default answer is always local, and cloud is the exception that requires justification.*
