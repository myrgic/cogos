# internal/acp/testdata — golden frame corpus

## Status: capture BLOCKED as of 2026-08-28

`claude --version` on Darkstar is 2.1.250 (current — no upgrade needed for
this spike). Every invocation of `claude --print` (plain or stream-json)
fails immediately with:

    Failed to authenticate: OAuth session expired and could not be refreshed

Diagnosed via the `cc-oauth-forensics` skill as a dead-refresh-token
condition (keychain `Claude Code-credentials` has `accessToken.expiresAt: 0`
— needs refresh — but the refresh itself fails), confirmed as a standing,
already-known issue rather than something triggered by this spike's own
`claude` calls. Fix requires an interactive `claude /login` (operator
action, out of scope for an automated lane). The golden_*.ndjson fixtures
below do not exist yet; `census_test.go`'s `TestFrameCensus` and
`cancellation_live_test.go`'s two `*_LiveClaude` tests are skipped
pending capture.

**No fixtures have been fabricated to stand in for real captures.** The
tables below are the planned filenames and the exact invocation each one
will use, to be run verbatim once `claude /login` succeeds.

## Planned captures

All captures should be run from this repo's root (`internal/acp/../..`,
i.e. the `cogos` worktree root) so `go.mod` is a realistic Read target, with
a fresh `--session-id` per capture (uuidgen), and should redirect raw
stdout — untouched — straight to the target file:

```sh
SID=$(uuidgen | tr 'A-Z' 'a-z')
printf '%s\n' '{"type":"user","message":{"role":"user","content":[{"type":"text","text":"Read the first line of go.mod using the Read tool and tell me what Go module this repo is."}]}}' \
  | claude --print --verbose --input-format stream-json --output-format stream-json \
      --allowedTools "Read" --session-id "$SID" \
      > internal/acp/testdata/golden_tool_turn_baseline.ndjson
```

| File | Invocation (flags added to the baseline above) | Purpose |
|---|---|---|
| `golden_tool_turn_baseline.ndjson` | none (baseline shown above) | default frame catalogue for a tool-using turn |
| `golden_tool_turn_partial.ndjson` | add `--include-partial-messages` | frames that only appear when partial-message streaming is on (expected: `stream_event` content_block_delta/input_json_delta chatter) |
| `golden_tool_turn_hookevents.ndjson` | add `--include-hook-events` | full hook lifecycle frames, vs. the `hook_started`/`hook_response` frames that (per an initial manual probe on 2026-08-28, without this flag) already appeared unconditionally on this machine due to Darkstar's own configured user-level hooks — worth re-confirming with the flag once live captures resume: does `--include-hook-events` add MORE hook subtypes, or is it orthogonal to what already surfaces? |
| `golden_tool_turn_partial_hookevents.ndjson` | both flags together | union / interaction check |

Each capture's exact session ID and any stderr should be noted in this file
as an appendix once run, so re-capture is fully reproducible.

## Cancellation fixtures

No separate fixture files — the cancellation tests drive `claude` live and
log frames inline (see `cancellation_live_test.go`), since the interesting
observable is the live process's exit behavior and post-cancel resumability
check, not a static NDJSON artifact.

## Manual probe already run (2026-08-28, pre-OAuth-death, informational only)

Before the OAuth session died mid-investigation, one baseline-shaped
invocation (no --include-partial-messages / --include-hook-events) DID
complete far enough to observe the frame shape below, though it ultimately
also hit the same auth failure inside the turn (its `result.result` is the
same "Failed to authenticate..." text — so this is NOT a valid golden
fixture and was not saved as one). It still cheaply confirms the
system-frame ordering and field shape ahead of a real capture:

```
system/hook_started   x3
system/hook_response  x3
system/init            (fields: cwd, session_id, tools, mcp_servers, model,
                         permissionMode, slash_commands,
                         terminal_slash_commands, apiKeySource,
                         claude_code_version, output_style, agents, skills,
                         plugins, capabilities, analytics_disabled,
                         product_feedback_disabled, uuid, memory_paths,
                         messaging_socket_path, fast_mode_state,
                         fast_mode_disabled_reason)
assistant               (one frame, synthetic auth-error message)
result/success          (is_error=true — the auth failure surfaces as a
                         "successful" turn subtype carrying an error payload,
                         not as its own error subtype — worth re-checking
                         against a real successful turn once unblocked)
```

Notably: `system/hook_started` and `system/hook_response` appeared WITHOUT
passing `--include-hook-events` — contradicting the assumption in the L1
research brief that this flag gates their presence. Either the flag's
effect is additive (more hook detail, not gating existence) or Darkstar's
locally-configured hooks (`corpus-resonance`, `loopback-resolver`,
`memory-janitor` — see `reference_autonomic_hook_layer_map.md`) always
surface here regardless of the flag on this specific machine/config. This
needs re-confirming once live captures resume by diffing baseline vs.
+hookevents captures.

Also notable: **no `rate_limit_event` frame appeared** in this probe or in
either of the pre-existing `spike_test.go` / `multiturn_test.go` runs (both
of which also failed on the same OAuth error) — but none of these are
successful turns, so this is not yet evidence either way; ADR-093 §10
reported observing one during a real one-prompt test in May. Re-check once
real turns complete.
