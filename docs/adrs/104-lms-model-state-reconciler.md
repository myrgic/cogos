# ADR-104: `lms-model-state` — A Declarative Reconciler for LM Studio Model/Context State

| Field   | Value |
|---------|-------|
| Status  | Proposed — code landed OFF-BY-DEFAULT; operator opts in per backend |
| Author  | @chazmaniandinkle |
| Created | 2026-07-07 |
| Layer   | Kernel (engine provider) + daemon health stub |
| Refs    | [ADR-100](100-substrate-library-extraction.md) (reconcile aliases / substrate library), [ADR-095](095-daemon-reconcile-loop-driver.md) (ReconcileDaemon driver), the `mlx-supervised` precedent (`internal/engine/provider_mlx_supervised.go`), ADR-045 (layered `providers.yaml` / `providers.local.yaml` config) |

---

## Context

CogOS dispatches inference to LM Studio backends through the OpenAI-compatible
provider (`type: lmstudio` / `openai`). That provider reconciles *dispatch* — is
the endpoint reachable, does it speak `/v1/chat/completions`. It says nothing
about **which model is loaded, at what context length**. Today that dimension is
managed imperatively: a `com.cogos.lmstudio-baseline` launchd job on the local
node loads a baseline model at boot, and the operator hand-loads models on a
remote LAN backend through the LM Studio desktop app.

The only declarative inference reconciler that exists is `mlx-supervised`
(ADR precedent), and it reconciles a *different* dimension: process-up via a
launchd plist. It does not manage model/context state, and its actuator
(launchctl) cannot reach a remote LM Studio instance.

The gap: there is no way to declare "backend `lmstudio-remote` should have
`example-35b` loaded at context 262144" and have the kernel keep it that way,
the way `mlx-supervised` keeps a process up. This ADR specifies that reconciler.

### Why LM Studio's native REST surface, not `/v1/models`

The OpenAI-compat `/v1/models` endpoint lists model *ids* only. LM Studio also
serves `GET /api/v0/models`, which exposes per-model `state`
(`loaded` / `not-loaded` / `loading`), `loaded_context_length`, and
`max_context_length`. The reconciled dimension lives in those fields, so the
reconciler probes `/api/v0/models`, not `/v1/models`.

A verified sharp edge: not-loaded models **omit** `loaded_context_length`
entirely (it decodes to JSON null / Go nil), while a loaded row carries the real
value. The decoder uses a `*int` pointer so a missing field never shadows a real
one, and row-matching prefers a `state=="loaded"` row over a not-loaded
duplicate of the same id.

### `parallel` drift is watched too, but observed differently (issue #555)

`GET /api/v0/models` does **not** expose `parallel` — confirmed live against
a local `:1234` instance, it is simply absent from the response. The value
*is* exposed by the local `lms` CLI's `lms ps --json` (confirmed live:
`{"identifier":"example-35b",...,"parallel":1}`), which is a **local-only**
command — `lms ps --help` shows no `--host` flag, the same LM-Link-gated
local/remote asymmetry §3 already documents for the actuator's `lms load` fast
path. So `parallel` drift is only observable on local backends; on a remote
backend a declared `parallel` target cannot be checked through
this mechanism at all — the reconciler says so explicitly in its `Health()`
message rather than silently reporting Healthy on context alone. The same gap
note fires when a *local* backend's probe itself fails to observe anything
(missing/renamed CLI, timeout, no matching `lms ps` row) — an unwatched
`parallel` dimension must never present as clean coverage either.

`ComputePlan` emits a dedicated `<name>/parallel` action for a parallel-only
mismatch (context otherwise fine) — remediated the same way as `<name>/context`
(unload+reload; LM Studio has no live resize for either dimension), and
**local-only**: the `@lmstudio/sdk` load config used for the remote/websocket
actuator genuinely has no per-load parallelism knob (§3's `lms-actuator` does
not accept or forward it), but the **local** `lms load` CLI fast-path does —
confirmed live via `lms load --help` (`--parallel <count>`). On a remote
backend the dimension remains alarm-only, since `parallelMismatch` can never
fire there in the first place (`Parallel` is only ever populated by the local
`lms ps --json` probe). Because the local fast-path is also what executes the
`<name>/context` action's unload+reload, that action now threads the declared
`parallel` target into every `lms load` it issues, context-triggered reloads
included — otherwise a context remediation would drop parallelism back to LM
Studio's app default and manufacture a fresh `parallel` mismatch behind the
context fix.

`<name>/parallel` re-emission is dampened: `ComputePlan` will not issue a
second `<name>/parallel` action for the same target against an *unchanged*
observed parallel value — it fires once, and only fires again once
`FetchLive`'s next probe reports a *different* observed value (converged,
still wrong but freshly so, or moved by other means), or once the target row
converges (loaded at the declared parallelism), at which point `ComputePlan`
clears the record outright so a later re-drift to the same observed value
fires a fresh action rather than being shadowed by the pre-convergence
record. This exists because a non-converging observed value (LM Studio
clamping or rounding the request, or the reload simply not taking effect)
would otherwise unload and reload the model on every autonomic tick forever.

The round trip `lms load --parallel N` → `lms ps --json` reporting exactly
`N` back was verified live on a local backend 2026-08-14 against `example-35b`:
`--parallel 2` landed as `"parallel": 2`, restoring `--parallel 1` landed as
`"parallel": 1`, context length held at 262144 across both reloads, and warm
reloads took roughly 12s. The dampening gate is kept regardless, as
defense-in-depth for other backends and LM Studio versions where the round
trip may not hold.

The dampening state is process-local and non-persistent — it lives on the
provider instance, not in `reconcile.State`, and a fresh provider is
constructed on every router rebuild (including a kernel restart), so the gate
resets to unarmed. Worst case that costs one extra unload+reload after a
rebuild, not a thrash loop.

## Decision

Introduce a new reconcile type, `lms-model-state`, that manages the
loaded-model/context-length state of a single LM Studio backend against a
declared target. It follows the `mlx-supervised` dual-shape precedent exactly,
swapping two things:

| Dimension | `mlx-supervised` | `lms-model-state` |
|-----------|------------------|-------------------|
| Reconciled state | process-up (launchd plist loaded) | model-loaded-at-target-context |
| Live probe | `launchctl list` + `/v1/models` | `GET /api/v0/models` |
| Actuator | launchctl (local plist) | Node `@lmstudio/sdk` ws:// bridge (or local `lms` CLI) |

### §1 — The engine provider (`internal/engine/provider_lms_model_state.go`)

`LMSModelStateProvider` implements `reconcile.Reconcilable` (seven methods) plus
`reconcile.Tokenable`. It is **not** an `engine.Provider` — dispatch stays with
the existing `lmstudio` provider; this is an orthogonal concern on the same
backend.

- **`FetchLive`** — read-only `GET {baseURL}/api/v0/models` with a Bearer token
  (~4s timeout); caches the decoded rows under a `RWMutex`. This is the only
  network I/O in the read path.
- **`ComputePlan`** — diffs the declared target against the live rows and emits
  `Update`-typed actions keyed by a `Name` suffix: `<name>/load` (target not
  loaded), `<name>/context` (loaded at the wrong context — LM Studio has no live
  resize, so this is an unload+reload), and `<name>/unload` (`jit_evict` only,
  when a non-target model crowds the card). An empty plan means the target is
  loaded at the target context.
- **`ApplyPlan`** — the only mutating method. Invokes the actuator via
  `exec.CommandContext`, with the token injected through the environment
  (`LMS_ACTUATOR_TOKEN`), never argv.
- **`Health`** — O(1) from the cached rows, no I/O. The autonomic ticker calls
  it every self-heal tick, so it must never block.

### §2 — Health three-axis mapping (Argo Sync × Health × Operation)

| Live condition | Sync | Health | Operation |
|----------------|------|--------|-----------|
| `model_state.manage` not set | Unknown | **Suspended** | Idle |
| SDK actuator not installed (and no local CLI fallback) | Unknown | **Suspended** | Idle |
| Backend unreachable / off-LAN | Unknown | **Suspended** | Idle |
| target `state=="loading"` | Unknown | Progressing | Waiting |
| target absent / not-loaded | OutOfSync | Missing | Idle |
| loaded at wrong context | OutOfSync | Degraded | Idle |
| loaded, `parallel≠target` (local only) | OutOfSync | Degraded | Idle |
| loaded at target context | Synced | Healthy | Idle |

Context is checked before `parallel`, so if both are simultaneously wrong only
the context mismatch is reported that cycle — a known, accepted simplification
(see issue #555), not a full multi-dimensional drift report. A `parallel`
target declared against a **remote** backend never triggers the Degraded row
above (it cannot be observed there); instead the Healthy/Degraded message gets
an appended note that the parallel target isn't observable via `lms ps`, so the
gap stays visible instead of presenting as full coverage.

The deliberate call: an **unreachable** backend maps to **Suspended, not
Degraded**. A box that is simply off or off-LAN is not something the self-heal
driver should try to "fix" — self-heal fires on Degraded/Missing/OutOfSync, and
we do not want the ticker attempting loads against a machine that is unreachable.

### §3 — The actuator (`scripts/lms-actuator/`)

A Node CLI, `lms-actuator.mjs`, using `@lmstudio/sdk`. It constructs
`new LMStudioClient({ baseUrl: "ws://host:port", clientPasskey: <token> })` and
calls `client.llm.load(model, { config: { contextLength }, ttl })` /
`client.llm.unload(model)`. It prints a single JSON result line for `ApplyPlan`
to parse. A `list` verb (read-only `listLoaded`) and a `--dry-run` flag exist
for connection self-tests that never mutate.

Why an external Node process rather than a Go ws client: LM Studio's websocket
protocol is a bespoke authenticated channel/RPC scheme with SuperJSON-style
per-endpoint serialization. The vendor SDK owns that protocol; re-implementing it
in Go would be fragile. The actuator is the same "shell-out to the tool that owns
the surface" pattern `mlx-supervised` uses with launchctl.

**Remote vs local:** on a localhost backend the provider may fast-path through
`~/.lmstudio/bin/lms load … --context-length …`. On a remote backend (another
host) it always uses the SDK actuator — the `lms` CLI cannot reach a remote
instance (LM Link gated).

### §4 — Registration (opt-in, off-by-default)

`router.go` `makeProvider` calls `maybeRegisterModelStateReconciler` in the
`openai/lmstudio` branch. It constructs and `reconcile.UpsertProvider`s the
reconciler **only** when the backend declares `options.model_state.manage: true`,
pulling the token from the same `api_key_env` the dispatch provider uses. Absent
the block, nothing is registered.

The daemon-side health stub (`internal/providers/daemon/lms_model_state.go`,
wired via `internal/providers/all`) reports `Suspended` when no backend opts in —
mirroring `mlx_inference.go`.

## Guardrails (why this ships safe)

1. **Non-destructive read path.** `FetchLive` / `Health` / `ComputePlan` are
   read-only. Only `ApplyPlan` mutates, and only via the external actuator. The
   actuator's connection + read-only path was verified against a **mock**
   websocket server (ws upgrade + auth-frame accepted + `listLoaded` dispatched);
   a real load against a live backend was deliberately **not** run. That
   path is "built + connection-verified + mock-tested" and still needs operator
   live-verification before it is trusted.
2. **Opt-in, disabled by default.** No `model_state` block ⇒ Suspended, empty
   plan, nothing registered. A kernel restart reconciles no model state until the
   operator adds `manage: true` to a backend in their live `providers.local.yaml`.
   This ADR ships the code and a **commented** example only (see
   `docs/inference/lms-model-state.md`); it does not enable the reconciler on any
   node.

## Two-writer hazard (documented, not changed here)

The imperative `com.cogos.lmstudio-baseline` launchd job on the local node loads a
baseline model at boot. If a `lms-model-state` reconciler with a *different*
target were enabled on the same backend, the two writers would race (launchd
loads model A at boot; the reconciler unloads it and loads model B; a relaunch of
the job reverts it). Resolution when the reconciler is trusted: scope the launchd
job to boot-only (or retire it) so the reconciler is the single writer. **This
ADR does not touch that job** — it only documents the hazard.

## Head-of-line blocking in the self-heal loop (accepted tradeoff)

`autonomic_ticker.healDegradedProviders` runs each provider's `ApplyPlan`
**synchronously and sequentially** within one tick. `lms-model-state`'s
`ApplyPlan` can block for up to `lmsApplyTimeout` (180s) because
`@lmstudio/sdk` `load()` does not return until the model is fully resident, and a
262144-context load on a 24 GB card is not instant. A slow load therefore stalls
the *entire* self-heal pass — including `mlx-supervised` process-restart healing
for every provider iterated after it — for the duration of that load.

This is a **latency** concern, not a correctness bug, and it is bounded:

- The blocking-load-vs-recheck race is handled in `ComputePlan` itself, not only
  at the ticker. Once a load is dispatched LM Studio reports `state == "loading"`
  with a nil `loaded_context_length`; `ComputePlan` returns an **empty plan** for
  any `state == "loading"` target (and `contextMismatch` treats unknown context as
  no-mismatch), so no unload+reload is ever emitted mid-load. This guard is
  caller-independent, which matters because there are two callers with different
  gating: the autonomic ticker's `healDegradedProviders` gates on `Health()` (which
  returns `Progressing` for a loading target, so `needsHeal` is false), **but the
  always-on `ReconcileDaemon` calls `ComputePlan`/`ApplyPlan` directly with no
  `Health()` gate** — so the guard had to live in `ComputePlan` or a multi-minute
  load (e.g. 262144) would be restarted every daemon poll and never finish.
- The stall only occurs while a managed backend is actually mid-load, which is
  rare (opt-in, and only during genuine drift-correction), and is naturally
  bounded by `lmsApplyTimeout`.

Accepted as-is for the initial ship. If it becomes a problem in practice, the
options are (a) run the `lms-model-state` apply on its own worker goroutine with
the provider marked in-flight, so one slow load does not freeze self-healing for
the other reconcilers, or (b) lower `lmsApplyTimeout` and let the `Progressing`
state carry the wait across ticks. Both are deferred until the reconciler is
operator-trusted and enabled on a live node.

## Consequences

- The kernel gains a declarative handle on LM Studio model/context state, closing
  the gap between `mlx-supervised` (process-up) and the imperative baseline job.
- A new build-time dependency: `scripts/lms-actuator/node_modules/@lmstudio/sdk`
  (pinned in `package.json`; `npm install` run in that dir). The Go side shells
  out; there is no Go-module dependency.
- Verified target values on the author's backends: a 35B MoE loads and serves
  at **`context_length: 262144`** on a 24 GB card and on the local node alike
  (an earlier 65536 "ceiling" note was refuted — do NOT hardcode 65536).

## Testing

- Engine unit tests: fake `/api/v0/models` server including the null
  `loaded_context_length` case; `ComputePlan` load/context/unload/synced/opt-out;
  `Health` three-axis mapping (synced / degraded-wrong-context / progressing /
  missing / suspended-no-config / suspended-unreachable / suspended-no-actuator);
  `ApplyPlan` against a **fake** actuator asserting argv + `LMS_ACTUATOR_TOKEN`
  env and that the token never leaks into argv (no real load).
- Daemon stub: opt-in parser + Suspended-when-none.
- `go build ./...` and `go test -race ./internal/engine/... ./internal/providers/...`
  green.
