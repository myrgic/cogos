#!/bin/bash
# fakeclaude.sh — minimal stand-in for `claude --print --verbose
# --input-format stream-json --output-format stream-json` used ONLY to
# exercise Subprocess.Cancel's OS-level mechanics (SIGINT delivery,
# stdin-close draining) without needing a real, authenticated `claude`
# binary. All argv is ignored.
#
# It does NOT model claude's actual protocol semantics or session-resume
# behavior — that is exactly the question this fake cannot answer. See
# cancellation_test.go's *_LiveClaude tests (skipped pending the Darkstar
# OAuth fix — see testdata/README.md) for the real question: does claude
# itself leave a resumable session after SIGINT / stdin-close?

trap 'echo "{\"type\":\"result\",\"subtype\":\"cancelled\",\"is_error\":true,\"result\":\"sigint\"}"; exit 130' INT

echo '{"type":"system","subtype":"init","session_id":"fake-session"}'

while IFS= read -r _line; do
  echo '{"type":"stream_event","event":{"type":"content_block_delta"}}'
  sleep 1
  echo '{"type":"result","subtype":"success","is_error":false,"result":"ok"}'
done

# stdin closed (EOF) -- loop above exited naturally, not via signal.
echo '{"type":"result","subtype":"success","is_error":false,"result":"stdin-closed"}'
exit 0
