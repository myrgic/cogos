# internal/acp/testdata — golden frame corpus

## Status: CAPTURED 2026-08-28 (L1 lane closed)

`claude` 2.1.250 on Darkstar. An earlier attempt this same day was blocked by a
dead-refresh-token OAuth condition (`Failed to authenticate: OAuth session
expired and could not be refreshed`, diagnosed via `cc-oauth-forensics`); the
operator ran `claude /login` and all four captures plus both live cancellation
tests then ran for real. Nothing below is fabricated — every table is a census
of a committed `.ndjson` file, and every verdict is a logged test result.

## Captures

Run from the worktree root so `go.mod` is a realistic `Read` target, fresh
`--session-id` per capture, raw stdout redirected untouched:

```sh
SID=$(uuidgen | tr 'A-Z' 'a-z')
printf '%s\n' '{"type":"user","message":{"role":"user","content":[{"type":"text","text":"Read the first line of go.mod using the Read tool and tell me what Go module this repo is."}]}}' \
  | claude --print --verbose --input-format stream-json --output-format stream-json \
      --allowedTools "Read" --session-id "$SID" \
      > internal/acp/testdata/golden_tool_turn_baseline.ndjson
```

| File | Extra flags | Session ID |
|---|---|---|
| `golden_tool_turn_baseline.ndjson` | none | `3bf3ff20-8504-4616-a747-bd1511afced4` |
| `golden_tool_turn_partial.ndjson` | `--include-partial-messages` | `421efb5a-ab05-4fff-becc-01910014e0e2` |
| `golden_tool_turn_hookevents.ndjson` | `--include-hook-events` | `0e3ae6a0-3f28-4fed-8425-9f07702126c9` |
| `golden_tool_turn_partial_hookevents.ndjson` | both | `8c04a8d9-5c47-487f-91c9-ce18ddc660fc` |

All four `.stderr` files are empty (0 bytes). All four turns are
`result/success`, `is_error=false`, with a real `Read` tool call and a real
tool result. `TestFrameCensus` (`census_test.go`) reads these and prints the
census below; it is the drift detector for future `claude` upgrades.

## Frame census

| frame | baseline | +partial | +hookevents | both |
|---|---|---|---|---|
| `system/init` | 1 | 1 | 1 | 1 |
| `system/hook_started` | 3 | 3 | 7 | 7 |
| `system/hook_response` | 3 | 3 | 7 | 7 |
| `system/status` | 0 | 2 | 0 | 2 |
| `system/thinking_tokens` | 2 | 2 | 0 | 0 |
| `system/task_summary` | 2 | 2 | 2 | 2 |
| `system/post_turn_summary` | 1 | 1 | 1 | 1 |
| `assistant` | 3 | 3 | 2 | 3 |
| `user` (tool result) | 1 | 1 | 1 | 1 |
| `stream_event` | 0 | 22 | 0 | 22 |
| `rate_limit_event` | 1 | 1 | 1 | 1 |
| `result/success` | 1 | 1 | 1 | 1 |

`stream_event` breakdown under `--include-partial-messages` (baseline run):
`message_start` ×2, `content_block_start` (`text` / `thinking` / `tool_use`),
`content_block_delta` × {`text_delta`, `thinking_delta`, `signature_delta`,
`input_json_delta`}, `content_block_stop` ×3, `message_delta` ×2,
`message_stop` ×2.

## Verdicts — what this kills

**R2 resolved: `rate_limit_event` EXISTS.** Present exactly once in all four
captures, and it is **not** in the `acp.EventType` set, so it currently falls
through `ParseLine` to `Unknown`. Shape:

```json
{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","resetsAt":...,
 "rateLimitType":"five_hour","overageStatus":"rejected",
 "overageDisabledReason":"out_of_credits","isUsingOverage":false,
 "unifiedWindows":{"five_hour":{"utilization":0.07,"resetsAt":...},
                   "seven_day":{"utilization":0.26,"resetsAt":...}}},
 "uuid":"…","session_id":"…"}
```

This is genuinely useful telemetry (subscription-lane utilization against two
windows) and belongs in an ACP `usage_update` or an out-of-band status, not in
`Unknown`.

**`--include-partial-messages` DOES gate `stream_event` entirely.** 0 frames
without it, 22 with. The whole streaming path in the §4.1 translator mapping is
conditional on this flag — it must be passed unconditionally by
ManagedSession, not assumed on by default.

**`--include-hook-events` is ADDITIVE, not a gate.** `hook_started` /
`hook_response` appear without it (3 pairs, from Darkstar's own user-level
hooks) and the flag raises that to 7 pairs. The L1 brief's assumption that the
flag gates their existence is wrong. Note also that turning hook events on
*suppressed* `system/thinking_tokens` in these runs (2 → 0) — an interaction
worth re-checking, not yet explained.

**Four undocumented `system` subtypes** none of which appear in ADR-093 or any
published changelog: `status` (`{"status":"requesting"}`, partial-only),
`thinking_tokens` (`estimated_tokens`, `estimated_tokens_delta`),
`task_summary` (`{"detail":"Reading go.mod"}` — a human-readable progress
line), and `post_turn_summary` (`summarizes_uuid`, `status_category:
"review_ready"`, `status_detail`, `needs_action`). `task_summary` in particular
is a ready-made ACP tool-call `title`, and `post_turn_summary` has no ACP slot
at all. The translator's `Unknown` fallback must stay total: this vendor ships
new `system` subtypes without documenting them.

**The `user` frame carries the tool result, richly.** Confirms the §4.1 fix:
`message.content[].tool_use_id` + `tool_result`, plus a sibling
`tool_use_result` object with typed file metadata (`filePath`, `content`,
`numLines`, `startLine`, `totalLines`). That sibling is what makes real ACP
diffs/locations possible. `EventUser` is declared at `streamjson.go:17` but
absent from the `Event` union and `ParseLine`'s switch, so today all of this
lands in `Unknown`. First code fix, unchanged.

## Cancellation fixtures

No static fixtures — the observable is live process behavior. See
`cancellation_test.go` (fake process, deterministic, always runs) and
`cancellation_live_test.go` (real `claude`).

**R4 resolved. Both cancellation paths leave a RESUMABLE session.**

| Path | Frames after cancel | Terminal `result` | Resumable? |
|---|---|---|---|
| `CancelSIGINT` | 11 | `subtype=error_during_execution`, `is_error=true` | **yes** — `--resume` replied `RESUMED` |
| `CancelStdinClose` | 14 | `subtype=success`, `is_error=false` | **yes** — `--resume` replied `RESUMED` |

Consequences for the ACP server: `session/cancel` is implementable against
either path, and the ACP contract ("agent may still emit trailing
`session/update`s, then answers the original `session/prompt` with
`stopReason: "cancelled"`") maps cleanly — 11–14 trailing frames arrive after
the signal and must be drained, not dropped. **Neither engine result subtype
means "cancelled"**, so the translator must track cancellation state itself
and override the mapped `stopReason`; taking `is_error=true` at face value
would surface a user cancel as a refusal.

**Timing hazard, empirically load-bearing.** A 2s delay before SIGINT was
flaky: 3 of 4 trials landed the signal during subprocess startup /
`SessionStart` hook execution, before `system/init` flushed, producing a
truncated ~6-frame capture and a session that looked unresumable. 5s reliably
lands after init and inside real generation. A production `Cancel()` must
refuse (or queue) a cancel that arrives before `system/init` has been seen —
there is no session to cancel yet, and killing there loses it.
