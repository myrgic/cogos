---
type: rfc-draft
title: "Harness Catalog: dispatch-to-any-installed-harness on a node"
status: draft
scope: "Design only — no implementation in this document"
operator_directive_verbatim: >
  "dispatch-to-harness should be able to dispatch to any harness currently
  installed and available on the host node (Hermes by default)"
relates:
  - "RFC-0007 (dispatch provider override)"
  - "internal/engine/agent_dispatch.go (DispatchRequest contract)"
  - "internal/engine/local_agent_harness.go (DispatchToHarness boundary)"
  - "pkg/substrate/identity/rbac_bindings.go (HarnessRegistry naming collision, see §2.3)"
---

# RFC Draft: Harness Catalog

---

## 1. Summary

Today `cog_dispatch_to_harness` always dispatches into exactly one harness:
`LocalHarnessController`, the in-process agent loop identified by
`AgentID = "primary"`. This is a controller-selection problem disguised as a
provider-selection problem — `DispatchRequest.Provider` already lets a caller
pick which *inference backend* serves a dispatch (RFC-0007), but there is no
equivalent axis for picking which *harness process* executes it. A harness is
not a provider: it is the thing that holds tool scope, session lifecycle, and
an execution loop around a provider, not the provider itself.

This RFC proposes:

1. A **`HarnessController` interface**, extracted at the existing
   `DispatchToHarness` method boundary, so the kernel can hold more than one
   concrete implementation.
2. A **catalog of installed harnesses** on the host node — declared
   resources, not a runtime registry — discovered by declaration file plus
   an availability probe.
3. A **`harness` parameter** on `dispatchToHarnessInput` / `DispatchRequest`,
   defaulting per node config (Hermes when installed and available, `primary`
   otherwise), fulfilling the operator directive verbatim above.
4. A first concrete second controller, **`HermesHarnessController`**,
   adapting the Hermes non-interactive CLI contract to the
   `HarnessController` interface.

This document is design-only. No code changes ship with it. Implementation
is a separate kernel PR (see §7, Out of Scope) once this RFC's direction is
accepted.

---

## 2. Motivation

### 2.1 The operator directive names a gap the code doesn't have a slot for

The requirement statement is verbatim: *dispatch-to-harness should be able to
dispatch to any harness currently installed and available on the host node
(Hermes by default)*. Read literally, this asks for two things the current
contract cannot express:

- **Enumeration** — "any harness ... installed and available" implies there
  is a knowable set, checkable at dispatch time, not a single hardcoded
  target.
- **A default that isn't `primary`** — "Hermes by default" means the
  zero-config behavior of `cog_dispatch_to_harness` should change on a node
  where Hermes is present, without every caller having to say so.

`DispatchRequest.AgentID` (`agent_dispatch.go:60-61`) looks like it could be
this axis, but it isn't: `LocalHarnessController.DispatchToHarness`
(`local_agent_harness.go:1339-1341`) rejects any `AgentID` that doesn't match
its own `c.agentID` — it's an identity check against a singleton, not a
lookup key into a set of controllers. There is exactly one controller behind
the whole surface today.

### 2.2 Hermes discovery already happened; it just isn't wired

`plan-hermes-kernel-loop-wiring.cog.md` (steps 1–3, 2026-08-12 discovery)
already established the shape of a Hermes adapter: a subprocess contract
(`HERMES_EPHEMERAL_SYSTEM_PROMPT=... hermes chat -q '...' -m <model>
--provider lmstudio -Q`, plus isolation flags or a curated profile), a
missing wall-clock timeout that the caller must supply, and usage read-back
from `~/.hermes/state.db`. None of this touches the kernel today — "Hermes
appears nowhere in the kernel source" per that plan's discovery notes. This
RFC is the design step (plan step 1) that has to land before the adapter
(plan step 2) has an interface to implement against.

### 2.3 The name `HarnessRegistry` is already taken

`pkg/substrate/identity/rbac_bindings.go:486-545` defines `HarnessRegistry`:
an in-memory, per-session, ephemeral store of `HarnessBindingCRD` values
(`sessionID/type → binding`), used by the RBAC provider on both the CLI and
daemon binaries to attach/resolve/detach identity-scoped harness bindings.
It has nothing to do with installed-harness discovery — it is a session
binding cache, not a catalog of executable harnesses. A corpus-wide search
(`grep -rn "HarnessRegistry" .`) confirms this is the only definition and
every reference is the RBAC one.

Reusing the name would collide two unrelated concepts under one identifier —
"the catalog of what's installed" vs. "the map of who's bound to what for
this session" — which is exactly the kind of identity confusion the
search-before-rename discipline exists to catch. This RFC's catalog type is
named **`HarnessCatalog`** (§4.1) and the collision is called out explicitly
so a future reader who greps for `HarnessRegistry` and finds the RBAC type
does not conclude this RFC was never implemented.

---

## 3. Status Quo

### 3.1 One controller, one dispatch path

```
cog_dispatch_to_harness (MCP tool, mcp_server.go:988-1006)
  → dispatchToHarnessInput{AgentID, Task, Scope, Tools, Model, Provider, ...}
  → engine.DispatchRequest (agent_dispatch.go:56-177)
  → LocalHarnessController.DispatchToHarness (local_agent_harness.go:1335)
      - rejects any AgentID != c.agentID
      - resolves an inference PROVIDER (4-path precedence, §local_agent_harness.go:1351-1375)
      - runs the harness's own Execute loop
```

`AgentID` selects *which agent identity this one controller is willing to
answer as* (a guard, effectively always "primary" in practice), not *which
controller handles the request*. `Provider` (RFC-0007) selects the inference
backend within that one controller. Both axes are inside a single harness
process. There is no axis for "which harness process."

### 3.2 The shared inference lock

The metabolic cycle's 1-minute `runTicker`/`runCycle` and ad-hoc dispatches
already serialize on one inference lock (`ollamaMu`,
`local_agent_harness.go:373-378`) when they target the same local inference
channel. This is load-bearing: the kernel is already the arbiter of its own
inference concurrency for the single controller it has today. Any second
controller that can also reach the local LM Studio channel (Hermes, via
`--provider lmstudio`) must serialize through the *same* lock, or harness
runs will stampede the metabolic cycle — see §4.4.

### 3.3 Async dispatch already decouples caller from execution time

`Async=true` (`agent_dispatch.go` batch path, `dispatchJobReceipt`) returns a
job handle immediately, pollable via `cog_poll_dispatch` /
`GET /v1/dispatch-jobs/{id}`. This matters for Hermes specifically: a cold
Hermes invocation measured 26.65s (warm, cache-aligned prefix: ~3s) versus
7.48s for the bare local-harness path (plan §7, envelope check). A caller
that cannot tolerate that latency synchronously already has the async path
available without any change proposed here.

---

## 4. Proposal

### 4.1 `HarnessController` interface, extracted at the existing boundary

```go
// HarnessController is the interface LocalHarnessController's
// DispatchToHarness method already implements implicitly. Extracting it
// formalizes the boundary so the kernel can hold more than one concrete
// controller and route a dispatch to the one the caller (or node default)
// names.
type HarnessController interface {
    DispatchToHarness(ctx context.Context, req DispatchRequest) (*DispatchBatchResult, error)
}
```

No change to `DispatchRequest` or `DispatchBatchResult` shape is required for
the interface extraction itself — `LocalHarnessController.DispatchToHarness`
already has this exact signature. This is a pure extraction, not a
behavioral change, and is low-risk to land first, independent of the catalog
and the Hermes adapter.

### 4.2 `HarnessCatalog`: installed harnesses as declared resources

Following the RFC-060 declared-resource shape (a harness is a member of a
declared set, not a thing discovered by probing the whole filesystem for
every possible binary), each catalog entry carries:

```go
type HarnessCatalogEntry struct {
    ID                string // stable identifier, e.g. "primary", "hermes"
    Kind              string // "local-inproc" | "subprocess-cli" | ...
    InvocationContract string // pointer to the adapter's documented contract
    LivenessProbe     func(ctx context.Context) error
    EnvelopePointer   string // cog:// or file path to the harness's envelope/config doc
    Default           bool   // node-config default flag (§4.3)
}

type HarnessCatalog struct {
    entries map[string]HarnessCatalogEntry
}
```

**Discovery = declaration file + availability probe**, not filesystem
scanning:

- A **declaration file** (`harnesses.yaml`, node-config-tier, alongside
  `providers.yaml`/`providers.local.yaml`) lists what the operator has told
  the node about — id, kind, invocation contract pointer, envelope pointer.
  Nothing is auto-discovered by walking `$PATH`; an undeclared harness is not
  in the catalog regardless of whether the binary happens to exist on disk.
  This mirrors the providers-config precedent (RFC-0007 Layer 1) rather than
  inventing a new discovery mode.
- An **availability probe**, run at catalog-build time (and re-checked on
  liveness query), determines whether a declared entry is actually usable
  right now. For the Hermes entry: `hermes` binary resolvable on `$PATH` AND
  `~/.hermes/` present. A declared-but-unavailable entry stays in the catalog
  (visible via `cog_list_harnesses`, a read-only companion tool this RFC also
  proposes) but is not eligible as a dispatch target or as the resolved
  default.

`primary` (today's `LocalHarnessController`) is always present in the
catalog, always available, `Default: true` only when nothing else qualifies
— it is the floor, not a declared entry a node config could omit.

### 4.3 The `harness` dispatch parameter and its default rule

`dispatchToHarnessInput` gains one field:

```go
Harness string `json:"harness,omitempty" jsonschema:"Which installed harness catalog entry handles this dispatch. Empty uses the node's configured default (see harness_default in kernel.yaml): hermes when installed and available, primary otherwise. Unknown or unavailable harness names error before any slot runs."`
```

Default resolution order, evaluated once per dispatch batch (not per slot —
a batch dispatches into one harness):

1. Caller-supplied `Harness` name, explicit. Must resolve to a catalog entry
   that is both declared and currently available, or the dispatch fails
   `invalid_input` before any slot runs (same fail-fast discipline as unknown
   `Provider` names, §local_agent_harness.go:1394-1404 — never silently
   fall through to a different harness because that would mask a config
   typo the same way an unknown provider name would).
2. `harness_default` in `kernel.yaml`, if the operator pinned one explicitly.
3. Node-config auto rule: **hermes when installed and available**, else
   **primary**. This is the literal fulfillment of "Hermes by default" — the
   default is a property of the node's installed set, not a hardcoded
   string, so a node without Hermes installed keeps today's behavior
   byte-identical.

This preserves every existing caller: no `Harness` field set, Hermes not
installed → resolves to `primary`, unchanged from today.

### 4.4 The Hermes adapter contract

`HermesHarnessController` implements `HarnessController` by adapting the
non-interactive Hermes CLI contract discovered in the wiring plan:

- **Subprocess exec**, not an in-process call — Hermes runs as
  `HERMES_EPHEMERAL_SYSTEM_PROMPT='<system prompt>' hermes chat -q '<task>'
  -m <model> --provider lmstudio -Q` (plus isolation flags / a curated
  profile per plan step 3). The task and system-prompt fields of
  `DispatchRequest` map onto the `-q` argument and the ephemeral env var
  respectively.
- **Kernel-supplied wall-clock deadline.** Hermes has no timeout of its own
  (plan discovery, verbatim: "No wall-clock timeout exists in Hermes — the
  caller must supply one"). The adapter wraps the subprocess in a
  `context.WithTimeout` derived from `DispatchRequest.TimeoutSeconds` (same
  240s default / cap-enforced ceiling as the local path,
  `agent_dispatch.go:103-118`) and kills the process on deadline exceeded —
  the same discipline `LocalHarnessController` already applies internally,
  extended to a process the kernel does not control the internals of.
- **Fresh session per dispatch.** No session chaining across dispatches —
  the wiring plan's measured design law is explicit: "chained = eliminated."
  Each dispatch is a new Hermes invocation with no prior turn history.
- **Usage read-back from `state.db`.** Hermes records `input_tokens` /
  `output_tokens` per session in `~/.hermes/state.db`'s `sessions` table.
  The adapter reads the row for the session it just ran (correlated by the
  session id Hermes assigns, surfaced in its own output) and populates
  `DispatchResult`'s usage-shaped fields the same way the local path
  populates `ServedModel`/`ProviderUsed` today — post-hoc read, not a value
  the subprocess call itself returns synchronously.
- **Routes through the shared local-inference serialization.** When the
  Hermes invocation's `--provider lmstudio` targets the same local LM
  Studio channel the metabolic cycle uses, the adapter acquires the existing
  `ollamaMu` inference lock (§3.2) before starting the subprocess and
  releases it when the subprocess exits. This is not optional: without it, a
  Hermes dispatch and a metabolic-cycle tick can both hit LM Studio
  concurrently, which is the exact failure class the lock exists to prevent
  for the local controller today. A Hermes dispatch routed at a *non-local*
  provider (a future profile with a remote backend) would not need the lock
  — the requirement is scoped to shared-channel contention, not to Hermes
  categorically.

### 4.5 `cog_list_harnesses` (read-only companion tool)

A small addition alongside the `harness` dispatch parameter: a read-only MCP
tool that returns the catalog's current state (id, kind, default flag,
available bool, last-probe timestamp). This is the discoverability
counterpart to `cog_list_agents` / `cog_get_agent_state` for the harness
axis, and lets a caller check "is Hermes actually available right now" before
committing to a synchronous dispatch that would otherwise fail fast anyway.
No write surface — attaching/detaching harnesses to the catalog is a
node-config change (`harnesses.yaml`), not a runtime MCP mutation.

---

## 5. Composition with Existing Primitives

### 5.1 RFC-0007 (dispatch provider override)

Provider selection and harness selection are orthogonal axes that compose:
`harness=hermes, provider=lmstudio-local` means "run the Hermes CLI
adapter, and inside it Hermes talks to the lmstudio-local provider."
`harness` (this RFC) picks the controller; `provider` (RFC-0007) picks what
that controller's own inference call resolves to. Neither field implies a
value for the other; `LocalHarnessController`'s 4-path provider resolution
(§3.1) is unchanged and untouched by this RFC.

### 5.2 Async dispatch and `TimeoutCapSeconds`

Both existing mechanisms apply unchanged to Hermes dispatches: `Async=true`
returns a job handle immediately (relevant given Hermes's higher cold-start
latency, §3.3), and `TimeoutCapSeconds` is re-stamped by the controller from
node config regardless of what the transport carried (the same anti-override
discipline `local_agent_harness.go:1342-1346` already applies) — a remote
caller cannot loosen the Hermes adapter's kill-deadline past the executing
node's own cap.

### 5.3 Identity (Wave 6b) and `HarnessBindingCRD`

`DispatchIdentity` (OIDC-shaped, `agent_dispatch.go:43-54`) is observability
metadata only today for every controller, including the proposed Hermes one
— consistent with the existing note that full CRD-based identity binding
waits for the Wave 6b migration (§6, Non-Goals). The RBAC `HarnessRegistry`
(§2.3) and this RFC's `HarnessCatalog` remain deliberately separate concepts
post-Wave-6b too: a session's `HarnessBindingCRD` may eventually need to
reference *which catalog entry* it's bound to, but that reference is a
future addition to the RBAC type, not a merge of the two.

---

## 6. Non-Goals

- **No new daemon.** Every harness in the catalog runs as either an
  in-process controller (`primary`) or a subprocess the kernel spawns and
  supervises for the duration of one dispatch (`hermes`). Nothing in this
  RFC stands up a long-running sidecar process, a new listener, or a new
  port.
- **No harness marketplace.** The catalog is a declared, operator-authored
  list (`harnesses.yaml`) on one node, not a discovery/publish mechanism for
  third-party harness plugins, not a registry other nodes pull from, and not
  versioned or distributed independently of node config. Cross-node harness
  availability (a peer node's catalog) is out of scope; `TargetNode`-routed
  dispatches (existing BEP cluster mechanism) continue to run against
  whatever the *remote* node's own default resolves to, unchanged.
- **Identity enforcement waits for Wave 6b.** As today, `DispatchIdentity`
  claims are trace metadata, not an authorization check, for every
  controller — the Hermes adapter does not get identity enforcement the
  local path doesn't already have. When Wave 6b lands, both controllers pick
  it up together; this RFC does not special-case Hermes ahead of that
  migration.
- **No change to `HarnessRegistry` (RBAC).** The existing RBAC type at
  `rbac_bindings.go:486-545` is untouched by this proposal; §2.3 documents
  the naming collision so it isn't rediscovered as a surprise later, not to
  propose renaming or merging it.

---

## 7. Out of Scope (Implementation)

This RFC is design-only. Follow-on work, sequenced per the wiring plan:

1. Kernel PR: `HarnessController` interface extraction (§4.1) — mechanical,
   independently mergeable, no behavior change.
2. Kernel PR: `HarnessCatalog` + `harnesses.yaml` config loading +
   `cog_list_harnesses` tool (§4.2, §4.5).
3. Kernel PR: `HermesHarnessController` (§4.4) + `harness` dispatch
   parameter and default-resolution wiring (§4.3).
4. Security precondition for any of this reaching the plugin-triggered path
   (Stop-hook auto-dispatch): the kernel HTTP surface's open-CSRF gap
   (board item 75) — tracked separately, gates the *plugin* wiring, not this
   catalog design.

Each PR goes through the normal cogos-dev review gate; no self-merge.

---

## 8. Open Questions

- Should `harnesses.yaml` live under the same config precedence tier as
  `providers.yaml`/`providers.local.yaml` (workspace override over node
  default), or is a harness catalog node-only (no per-workspace override,
  since "installed on this host" is inherently a node property, not a
  workspace one)? Leaning node-only; providers answer "what backend," this
  answers "what's physically on this machine."
- Does `cog_list_harnesses` belong in the `observe` scope
  (`dispatchToHarnessInput.Scope` enumeration, `mcp_server.go:991`) by
  default, or does it need its own scope entry? Leaning: add to `observe`,
  it's a read-only kernel-health-adjacent surface like the rest of that
  scope.
- Liveness-probe cadence: probe on every `cog_list_harnesses` call
  (cheap for Hermes: binary-on-PATH + directory-exists, no subprocess spawn
  needed) vs. cached with a TTL. Leaning: no cache needed given how cheap the
  Hermes probe is; revisit if a future harness's probe is expensive.

---

## Appendix: Key File References

| Reference | Path |
|---|---|
| `DispatchToHarness` boundary | `internal/engine/local_agent_harness.go:1335` |
| `DispatchRequest` contract | `internal/engine/agent_dispatch.go:56-177` |
| `dispatchToHarnessInput` wire shape | `internal/engine/mcp_server.go:988-1006` |
| Shared inference lock (`ollamaMu`) | `internal/engine/local_agent_harness.go:373-378` |
| Provider 4-path resolution | `internal/engine/local_agent_harness.go:1351-1375` |
| Existing `HarnessRegistry` (RBAC, distinct concept) | `pkg/substrate/identity/rbac_bindings.go:486-545` |
| Discovery source | `~/workspaces/cog/.cog/mem/procedural/plan-hermes-kernel-loop-wiring.cog.md` |
