# internal/acp/testdata — golden frame corpus

## Status: captured 2026-08-28, live against claude 2.1.250

Operator ran `claude /login` to clear a dead-refresh-token OAuth condition
that had every `claude --print` invocation failing earlier the same day
(unrelated to this spike — a standing issue, diagnosed via the
`cc-oauth-forensics` skill). All four captures below and both live
cancellation-resumability tests in `cancellation_live_test.go` ran for real
after that fix. No fixtures were fabricated during the blocked window.

## Captures

All four ran from this worktree's root with a fresh `--session-id`
(uuidgen) and `--allowedTools "Read"`, driving the same tool-forcing prompt:

```sh
SID=$(uuidgen | tr 'A-Z' 'a-z')
printf '%s\n' '{"type":"user","message":{"role":"user","content":[{"type":"text","text":"Read the first line of go.mod using the Read tool and tell me what Go module this repo is."}]}}' \
  | claude --print --verbose --input-format stream-json --output-format stream-json \
      --allowedTools "Read" --session-id "$SID" \
      > internal/acp/testdata/<file>.ndjson
```

| File | Flags added | Session ID | Lines |
|---|---|---|---|
| `golden_tool_turn_baseline.ndjson` | none | `3bf3ff20-8504-4616-a747-bd1511afced4` | 18 |
| `golden_tool_turn_partial.ndjson` | `--include-partial-messages` | `421efb5a-ab05-4fff-becc-01910014e0e2` | 42 |
| `golden_tool_turn_hookevents.ndjson` | `--include-hook-events` | `0e3ae6a0-3f28-4fed-8425-9f07702126c9` | 23 |
| `golden_tool_turn_partial_hookevents.ndjson` | both | `8c04a8d9-5c47-487f-91c9-ce18ddc660fc` | 48 |

All four exited 0 with empty stderr, and all four genuinely exercised a
tool call (the assistant emitted a `tool_use` for `Read`, the tool result
came back as a `user` frame with a `tool_use_result`/`tool_result` payload,
and the final answer correctly named `github.com/myrgic/cogos`).

## Frame census (`go test -run TestFrameCensus -v ./internal/acp/...`)

| type/subtype | baseline | hookevents | partial | partial+hookevents |
|---|---|---|---|---|
| system/hook_started | 3 | 7 | 3 | 7 |
| system/hook_response | 3 | 7 | 3 | 7 |
| system/init | 1 | 1 | 1 | 1 |
| system/thinking_tokens | 2 | 0 | 2 | 0 |
| system/status | 0 | 0 | 0 | 2 |
| system/task_summary | 2 | 2 | 2 | 2 |
| system/post_turn_summary | 1 | 1 | 1 | 1 |
| assistant | 3 | 2 | 3 | 3 |
| user | 1 | 1 | 1 | 1 |
| stream_event | 0 | 0 | 22 | 22 |
| rate_limit_event | 1 | 1 | 1 | 1 |
| result/success | 1 | 1 | 1 | 1 |

`rate_limit_event` is not in `acp.EventType` — every row above falls
through `ParseLine` to `Event.Unknown` for that frame today (harmless: the
translator just needs to add it, not a parse failure).

### What each flag actually gates

- **`--include-partial-messages` gates `stream_event` cleanly and
  completely.** 0 in both captures without the flag, 22 in both captures
  with it. This is a clean, deterministic signal — the strongest one in
  the whole census.
- **`--include-hook-events` does NOT gate hook-frame *existence* — it gates
  *which lifecycle points* get reported.** Without the flag, only
  `SessionStart:startup` hooks appear (3 `hook_started` + 3 `hook_response`
  — examplenode's 3 configured user-level hooks: corpus-resonance,
  loopback-resolver, memory-janitor). With the flag, the same 3 SessionStart
  events appear PLUS `UserPromptSubmit` (2), `PreToolUse:Read` (1), and
  `Stop` (1) — 7 and 7. So this corrects the L1 brief's framing (the ADR-093
  §10 text implicitly read as "the flag gates hook-frame presence"): session
  lifecycle hooks are visible unconditionally; per-turn/per-tool hook
  lifecycle only surfaces with `--include-hook-events`.
- **`thinking_tokens`/`status` counts are noise, not flag-driven** — each
  capture is an independent, non-deterministic model invocation, and these
  two subtypes are incidental progress indicators (`thinking_tokens`:
  running token-estimate counter; `status`: `"requesting"` ping) that
  varied across the 4 independent runs without a clean correlation to
  either flag. Not treated as a finding.
- `assistant` frame count (2 vs 3) is likewise just how many content blocks
  the model happened to split a given turn into (e.g. a `thinking` block
  landing in its own frame sometimes) — not flag-driven.

### The three L1 verdicts

1. **Does `rate_limit_event` exist?** YES — present in all 4 captures,
   exactly once each, unconditionally (not gated by either flag). Confirms
   ADR-093 §10's May observation still holds on 2.1.250.
2. **Does SIGINT leave a usable/resumable session?** YES, conditionally —
   see "SIGINT resumability" below. The short version: yes, reliably, once
   the turn has actually started generating; unreliable if the interrupt
   lands during the ~2-5s subprocess-startup/hook window.
3. **What actually gates `stream_event`?** `--include-partial-messages`,
   cleanly and exclusively. `--include-hook-events` has no effect on it.

## Cancellation — SIGINT and stdin-close resumability (live, 2026-08-28)

Both `cancellation_live_test.go` tests ran for real against live claude
(`TestCancel_SIGINT_ResumabilityAfter_LiveClaude`,
`TestCancel_StdinClose_ResumabilityAfter_LiveClaude`). Flow: spawn a fresh
pinned session, send a prompt engineered to force a long generation ("write
a very long, detailed 1500-word essay..."), wait, cancel, drain trailing
frames, `Wait()`, then spawn a second subprocess with
`SpawnOpts{SessionID: pinned, ResumeExisting: true}` and check whether a
trivial "reply with exactly: RESUMED" follow-up actually comes back
containing RESUMED.

### SIGINT — timing-sensitive, not simply yes/no

First pass used a 2s pre-cancel delay (mirroring "cancel mid-turn" loosely)
and was **flaky**: 1 of 4 trials resumed cleanly, 3 of 4 did not. Digging
in with manual captures (`kill -INT` at controlled delays) explained why:

- At 2s, the subprocess is often still inside `SessionStart:startup` hook
  execution — the capture that failed to resume terminated after only 6
  frames (the 3 hook_started/hook_response pairs), **before `system.init`
  was even emitted**. No properly-initialized session to resume from.
- At 5-6s, the subprocess is reliably past `system.init` and into real
  generation (`thinking_tokens` frames observed). SIGINT there produces a
  clean `result` frame (`subtype: "error_during_execution", is_error:
  true`), the process exits without a crash, and `--resume <id>` afterward
  replies coherently — 4/4 clean runs once the delay was bumped to 5s (2
  via `go test`, 2 manual, one taken to a full raw resume transcript
  showing `"result":"RESUMED"`, `"stop_reason":"end_turn"`).

**Verdict: SIGINT delivered once the turn is genuinely in flight (past
`system.init`) leaves a resumable session, reliably (4/4).** SIGINT
delivered during the subprocess's startup/hook window is a race whose
outcome (mostly no usable session, 1/4 usable in this sample) depends on
exactly where the interrupt lands relative to session-file
initialization — this is a real hazard for a production `Cancel()` caller
that fires immediately after `Send()` without knowing whether `system.init`
has arrived yet. `cancellation_live_test.go` now waits 5s before cancelling
to get a deterministic test; a production caller should gate `Cancel` on
having seen `system.init` first, not on wall-clock time.

### stdin-close — does not abort in-flight work at all

`TestCancel_StdinClose_ResumabilityAfter_LiveClaude` took ~75s to
complete — because stdin-close does **not** interrupt the current turn; it
only prevents future turns. The in-flight 1500-word essay ran to natural
completion (`result subtype: "success", is_error: false`) and *then* the
subprocess exited on EOF. Resumability was trivially true (2/2) because
nothing was actually aborted — this is a different semantic contract than
SIGINT, not a weaker version of the same one: **stdin-close is "stop
sending new input," not "abort the current turn."** A caller wanting fast
turn abandonment should use `CancelSIGINT`; `CancelStdinClose` is for
graceful "no more turns after this one" shutdown.

## Contract drift vs. the May §10 baseline

- **Frame catalogue is wider still.** May's list (`system.{init,
  hook_started, hook_response}`, `stream_event`, `assistant`, `result`,
  `rate_limit_event`) is missing `system.thinking_tokens`,
  `system.task_summary`, `system.post_turn_summary`, `system.status`, and
  the `user` frame (tool-result carrier — already flagged in the L1
  research brief §4.1 as needing a `streamjson.go` fix, now empirically
  confirmed present and shaped as `{tool_use_id, type: "tool_result",
  content}`). None of these are exotic — they showed up on ordinary runs,
  not edge cases.
  - `rate_limit_info` payload itself grew too: `unifiedWindows` with
    per-window `{five_hour, seven_day}` utilization, `overageStatus`,
    `overageDisabledReason` — richer than a May-era bare quota ping would
    suggest, though the May text didn't record the payload shape to diff
    against directly.
- **`--include-hook-events`'s actual effect was previously unrecorded** —
  the May findings didn't test it explicitly against a real tool-using
  turn; this capture is the first evidence that it's additive
  (more lifecycle points) rather than a plain gate.
- **Cancellation is no longer purely unvalidated** (May explicitly flagged
  it as not-yet-tested) — now empirically characterized, including the
  startup-window race, which is new information nobody had in May.
- Everything else in §10 (multi-turn continuity, required
  `--verbose` flag, `--session-id` create-only semantics) still holds —
  `TestSpike_MultiTurnOverResume` and `TestSpike_OnePromptOneResponse` both
  pass cleanly against 2.1.250.

## Fixture privacy note (flagged by review, 2026-08-28)

The four `golden_tool_turn_*.ndjson` fixtures above were recorded verbatim
from a live capture on the capturing operator's own machine and
intentionally contain that machine's local paths and username where they
appear in tool-call/tool-result payloads (e.g. `Read` targeting this
worktree's `go.mod` by absolute path). There are no credentials in any
fixture — the session IDs listed in the Captures table are one-shot
per-capture UUIDs, not reusable secrets. Recorded verbatim is deliberate
per golden-corpus discipline (the fixtures are supposed to be a faithful,
unedited record of what `claude --print --output-format stream-json`
actually emitted, not a synthesized approximation). This has been flagged
by review; retained as-is pending operator ruling on whether the local
paths/username should be redacted.

## Sanitization (2026-08-31, supersedes the 2026-08-28 redaction note)

**These fixtures are synthesized, not merely redacted, and only their frame
shape is real.**

The 2026-08-28 note claimed a username/machine-name substitution left
"remaining runtime values ... non-identifying." **That claim was false.** After
it landed, all four fixtures still carried the operator's entire private status
board inside a `SessionStart` hook payload (project state, ~148 task ids, two
real personal names), the full `system.init` MCP/plugin inventory naming which
third-party accounts were connected, local plugin-cache paths, and an IPC
socket path — in a public repository. A substring denylist cannot be verified,
so it was trusted instead of checked.

`sanitize_fixture.py` replaces that approach:

- **`system.init` is rebuilt from an allowlist.** Only protocol-relevant scalars
  survive (`type`, `subtype`, `session_id`, `model`, `claude_code_version`,
  `permissionMode`, capability flags…). Inventory fields are replaced with
  shape-preserving synthetic values; `messaging_socket_path` is dropped. A new
  upstream field is excluded *by default* — the failure mode is a missing field,
  never an unnoticed leak.
- **`system.hook_response` payloads are synthesized**, never edited: the real
  `additionalContext` is discarded wholesale and replaced with
  `<status_board redacted-for-fixture />`, preserving only `hookEventName`.
- **A post-condition is asserted mechanically.** `--verify` scans for 11
  forbidden patterns and exits non-zero on any hit. CI runs it on every PR
  (`ACP fixture hygiene gate`).

**What is still real, and load-bearing:** frame `type`, `subtype`, ordering,
count, and JSON validity. Every L1 finding is reproducible from these fixtures
— `rate_limit_event` present, `stream_event` 0→22 under
`--include-partial-messages`, hook frames 3→7 under `--include-hook-events`.
Payload interiors are read by no test, which is what makes aggressive synthesis
safe.

**Regenerating after a fresh capture:**

```sh
python3 internal/acp/testdata/sanitize_fixture.py --in-place
python3 internal/acp/testdata/sanitize_fixture.py --verify   # must exit 0
go test ./internal/acp/ -run TestFrameCensus -v               # shape unchanged
```

The sanitizer refuses to write if frame type/subtype/order/count would change,
so privacy can never be bought by silently degrading the corpus.

**Historical note:** unsanitized blobs existed on this branch in commits
`a1d919e`, `aa09c58`, `aa34a09`, `bb2e4d2`, `f9feb7d` before the history
rewrite. Treat anything recorded from those SHAs as disclosed.
