# LM Studio Model-State Reconciler (`lms-model-state`)

The `lms-model-state` reconciler keeps an LM Studio backend loaded with the
**model you declare, at the context length you declare** — the declarative
equivalent of hand-loading a model in the LM Studio desktop app or running the
imperative `com.cogos.lmstudio-baseline` launchd job.

It is **orthogonal to dispatch**. Dispatch (chat completions) still flows through
the existing `lmstudio` / `openai` provider on the same backend. This reconciler
adds a second concern on top: model/context state. See
[ADR-104](../adrs/104-lms-model-state-reconciler.md) for the design rationale.

> **This feature is OFF BY DEFAULT.** It activates only when a backend declares
> `options.model_state.manage: true`. Absent that, nothing is registered and the
> kernel reconciles no model state.

## How it works

| Method | What it does |
|--------|--------------|
| `FetchLive` | read-only `GET {endpoint}/api/v0/models` (LM Studio's native REST surface — exposes per-model `state` + `loaded_context_length`); on a **local** backend also shells `lms ps --json` and merges its `parallel` field — `/api/v0/models` does not expose `parallel` at all |
| `ComputePlan` | diffs declared target vs live → `load` / `context` (unload+reload; LM Studio has no live resize) / `parallel` (unload+reload at the declared parallelism; **local only** — a nil observed `Parallel` on a remote backend means this case never fires there) / `unload` (jit_evict) actions |
| `ApplyPlan` | shells the Node `@lmstudio/sdk` actuator (`scripts/lms-actuator/`) over `ws://`; token via `LMS_ACTUATOR_TOKEN` env, never argv |
| `Health` | O(1) from cached rows: Synced/Healthy when loaded at target; Degraded on wrong context **or** wrong `parallel` (local only, until the next self-heal cycle applies the `parallel` action); Missing when absent; Progressing while loading; **Suspended** when unmanaged, unreachable, or the actuator is not installed |

An **unreachable** backend reports **Suspended, not Degraded** — the autonomic
ticker does not try to self-heal a box that is simply off or off-LAN.

### `parallel` drift — local-only observability, local-only remediation

`parallel` is watched the same way `context_length` is, with one asymmetry: it
can only be **observed** (and, as of this reconciler's local fast-path, only
**remediated**) on a local backend. `lms ps --json` (the source for the
observed value) is a local-only CLI — `lms ps --help` shows no `--host` flag —
so a remote backend can never be checked against a declared
`parallel` target through this mechanism; `Health()`'s message says so
explicitly rather than silently reporting full coverage. Likewise, if the local
probe itself produces no observation (lms CLI missing/renamed, the probe times
out or errors, or no `lms ps` row matches the loaded model), `Health()` appends
the same kind of gap note rather than presenting a dead watch as clean coverage.

On a local backend, `ComputePlan` emits a dedicated `parallel` action
(unload+reload via the same "set-context" actuator verb `context` uses,
carrying forward the previously-correct `context_length` so the reload does
not fall back to LM Studio's default context) whenever the target is loaded at
the wrong parallelism and context is otherwise fine — mirroring how `context`
drift is remediated. This is possible because the local `lms load` CLI
fast-path *does* have a `--parallel <count>` flag (confirmed live via
`lms load --help`), unlike the `@lmstudio/sdk` load config used for remote
backends, which genuinely has no per-load parallelism knob — so on a remote
backend `parallel` remains alarm-only, never actuated. The local fast-path also
threads the declared `parallel` target into *every* `lms load` it issues,
including a context-triggered reload, so remediating a `context_length`
mismatch on a local backend never silently resets parallelism to the app
default and manufactures a fresh `parallel` mismatch behind it.

## Prerequisites

Install the actuator's SDK once:

```bash
cd scripts/lms-actuator
npm install
```

Verify the actuator can open a connection and do a read-only op **without loading
anything** (against a mock, or the `list` verb against a reachable backend):

```bash
# read-only: list loaded models (no mutation)
LMS_ACTUATOR_TOKEN=$LMSTUDIO_REMOTE_API_KEY \
  node scripts/lms-actuator/lms-actuator.mjs list --host 192.0.2.10 --port 1234

# dry-run: resolve + print the plan, issue no load/unload
LMS_ACTUATOR_TOKEN=$LMSTUDIO_REMOTE_API_KEY \
  node scripts/lms-actuator/lms-actuator.mjs load --host 192.0.2.10 --port 1234 \
       --model example-35b --context-length 262144 --dry-run
```

## Config (commented example — do NOT enable blindly)

Add this to `providers.local.yaml` and set `manage: true` **only** after the
actuator is installed and you have live-verified it against the target backend.

```yaml
providers:
  lmstudio-remote:
    type: openai                    # OpenAI-compatible dispatch (LM Studio REST)
    enabled: true
    endpoint: "http://192.0.2.10:1234"
    api_key_env: LMSTUDIO_REMOTE_API_KEY    # Bearer token; also used by the reconciler
    model: "example-35b"
    timeout: 300
    context_window: 262144
    options:
      model_state:
        manage: true                # the opt-in switch; false/absent ⇒ Suspended
        model: "example-35b"
        context_length: 262144      # VERIFIED loaded + serving on the 24GB card
        parallel: 1
        keep_warm: true
        jit_evict: false            # true ⇒ unload a non-target model crowding the card
```

**Context length: use `262144`, not `65536`.** An earlier 65536 "ceiling" note
was refuted: a 35B MoE loads and serves at 262144 on a 24 GB card.

### Local vs remote

- **Remote backend (another host):** always uses the Node SDK actuator.
  The `lms` CLI cannot reach a remote instance (LM Link gated).
- **Localhost backend:** may fast-path through
  `~/.lmstudio/bin/lms load … --context-length …` when the CLI is present.

## Two-writer hazard

If the node also runs a `com.cogos.lmstudio-baseline` launchd job that loads a
baseline model at boot, and you enable this reconciler with a **different** target on the
same backend, the two writers race. Before trusting the reconciler, scope that
launchd job to boot-only (or retire it) so the reconciler is the single writer.
Do not enable both with divergent targets.

## Guardrails

1. The read path (`FetchLive`/`Health`/`ComputePlan`) is non-destructive. Only
   `ApplyPlan` mutates, and only via the external actuator.
2. Opt-in, off by default. No `model_state` block ⇒ Suspended, empty plan,
   nothing registered.

## Out of scope: remote-backend `parallel` observability

Extending `parallel` drift detection to remote backends would require
the Node SDK actuator's `list` verb (`client.llm.listLoaded()`) to expose a
parallel-equivalent field over the websocket bridge. That is unverified — the
SDK is not installed in a bare checkout (`scripts/lms-actuator/node_modules` is
absent until `npm install` is run), so `listLoaded()`'s returned shape could not
be inspected to confirm it. Deferred; `scripts/lms-actuator/lms-actuator.mjs` is
untouched by this change.
