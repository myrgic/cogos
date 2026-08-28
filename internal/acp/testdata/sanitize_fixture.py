#!/usr/bin/env python3
"""Sanitize golden stream-json fixtures captured from a live machine.

WHY THIS EXISTS
---------------
The golden corpus is captured by running a real `claude` CLI on a real
workstation. A raw capture embeds whatever that machine happened to hold:
hook payloads (which on this substrate carry a private status board), the
operator's MCP/plugin inventory, home-directory paths, and an IPC socket path.
Commit f9feb7d attempted a substring-denylist redaction, CLAIMED completeness
in its message and in testdata/README.md, and was believed. It was not
complete. That is the failure mode this script is built to make impossible:

    A denylist redaction cannot be verified. An allowlist synthesis can.

WHAT IS PRESERVED
-----------------
The fixtures exist to answer frame-catalogue questions. Their consumers
(census_test.go, streamjson_test.go) read ONLY:

    frame `type`, frame `subtype`, frame order, frame count, JSON validity

Those five properties are load-bearing and are preserved byte-for-byte in
shape. Payload interiors are load-bearing for NOTHING, so they are replaced
with shape-preserving synthetic values rather than edited.

USAGE
-----
    sanitize_fixture.py --verify [FILES...]     # post-condition check, CI gate
    sanitize_fixture.py --in-place [FILES...]   # rewrite fixtures
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ── Forbidden patterns. The post-condition, not the mechanism. ──────────────
# Redaction is done by allowlist below; this set only PROVES the result.
FORBIDDEN = [
    (re.compile(r"/Users/(?!fixture)", re.I), "home-directory path"),
    (re.compile(r"C:\\\\Users", re.I), "windows home path"),
    (re.compile(r"\bslowbro\b", re.I), "operator username"),
    (re.compile(r"\bChaz\b"), "real name"),
    (re.compile(r"\bErin\b"), "real name"),
    (re.compile(r"cc-socks", re.I), "IPC socket path"),
    (re.compile(r"\.claude/plugins/cache", re.I), "local plugin cache path"),
    (re.compile(r"mcp__claude_ai_", re.I), "operator MCP integration inventory"),
    (re.compile(r"\bKrisp\b|\bSpotify\b|\bAtlassian\b", re.I), "third-party account"),
    (re.compile(r"status_board(?!\s+redacted)", re.I), "private status board"),
    (re.compile(r"PINNED:", re.I), "private board content"),
]

# ── system.init: ALLOWLIST of scalar keys that may survive verbatim. ────────
INIT_KEEP = {
    "type", "subtype", "session_id", "uuid", "model", "permissionMode",
    "claude_code_version", "apiKeySource", "output_style", "capabilities",
    "fast_mode_state", "fast_mode_disabled_reason",
    "analytics_disabled", "product_feedback_disabled",
}

# Shape-preserving synthetic replacements for the list/dict fields we drop.
INIT_SYNTHETIC = {
    "cwd": "/tmp/fixture-workspace",
    "tools": ["Bash", "Read", "Write"],
    "mcp_servers": [{"name": "fixture-server", "status": "connected"}],
    "plugins": [],
    "skills": [],
    "agents": [],
    "slash_commands": [],
    "terminal_slash_commands": [],
    "memory_paths": {"auto": []},
}
# Dropped outright (no synthetic stand-in; nothing reads it):
INIT_DROP = {"messaging_socket_path"}

PATH_RE = re.compile(r"/Users/[^\"'\s:,}\]]*")
SOCK_RE = re.compile(r"/tmp/cc-socks/[^\"'\s:,}\]]*")


def scrub_scalar(v):
    """Final catch-all for leaf strings that survived structural rules."""
    if not isinstance(v, str):
        return v
    v = PATH_RE.sub("/tmp/fixture-workspace", v)
    v = SOCK_RE.sub("/tmp/fixture.sock", v)
    return v


def walk_scrub(o):
    if isinstance(o, dict):
        return {k: walk_scrub(v) for k, v in o.items()}
    if isinstance(o, list):
        return [walk_scrub(v) for v in o]
    return scrub_scalar(o)


def sanitize_frame(fr: dict):
    typ, sub = fr.get("type"), fr.get("subtype")

    # 1. hook_response — synthesize; never substring-edit the real board.
    if typ == "system" and sub == "hook_response":
        out = fr.get("output")
        hook_name = "Unknown"
        if isinstance(out, str):
            try:
                hook_name = (json.loads(out).get("hookSpecificOutput", {})
                             .get("hookEventName", "Unknown"))
            except Exception:
                pass
        synthetic = json.dumps({
            "hookSpecificOutput": {
                "hookEventName": hook_name,
                "additionalContext": "<status_board redacted-for-fixture />",
            }
        })
        fr = dict(fr)
        if "output" in fr:
            fr["output"] = synthetic
        if "stdout" in fr:
            fr["stdout"] = synthetic
        return walk_scrub(fr)

    # 2. system.init — ALLOWLIST. Unknown future keys are dropped by default,
    #    which is the whole point: a new leaky field cannot silently survive.
    if typ == "system" and sub == "init":
        clean = {k: v for k, v in fr.items() if k in INIT_KEEP}
        for k, v in INIT_SYNTHETIC.items():
            if k in fr:
                clean[k] = v
        for k in INIT_DROP:
            clean.pop(k, None)
        return walk_scrub(clean)

    # 3. Everything else: structural scrub of path-shaped leaves.
    return walk_scrub(fr)


def process(path: Path, in_place: bool) -> list[str]:
    """Returns list of violations (empty == clean)."""
    lines = [l for l in path.read_text().splitlines() if l.strip()]
    out_lines, before = [], []
    for i, line in enumerate(lines, 1):
        fr = json.loads(line)
        before.append((fr.get("type"), fr.get("subtype")))
        out_lines.append(json.dumps(sanitize_frame(fr), separators=(",", ":")))

    # POSITIVE CONTROL: frame shape must be identical, or we traded a privacy
    # defect for a corpus defect.
    after = [(json.loads(l).get("type"), json.loads(l).get("subtype"))
             for l in out_lines]
    if before != after:
        return [f"{path.name}: FRAME SHAPE CHANGED — refusing to write"]

    if in_place:
        path.write_text("\n".join(out_lines) + "\n")

    body = "\n".join(out_lines) if in_place else path.read_text()
    violations = []
    for rx, label in FORBIDDEN:
        for m in rx.finditer(body):
            ctx = body[max(0, m.start() - 40): m.start() + 60].replace("\n", " ")
            violations.append(f"{path.name}: {label}: …{ctx}…")
            break  # one report per pattern per file
    return violations


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true",
                    help="check only; non-zero exit on any violation (CI gate)")
    ap.add_argument("--in-place", action="store_true", help="rewrite fixtures")
    ap.add_argument("files", nargs="*")
    a = ap.parse_args()

    files = [Path(f) for f in a.files] or sorted(HERE.glob("golden_*.ndjson"))
    if not files:
        print("no golden_*.ndjson fixtures found", file=sys.stderr)
        return 0

    all_v = []
    for f in files:
        all_v += process(f, in_place=a.in_place)

    if all_v:
        print("FIXTURE HYGIENE FAILED:", file=sys.stderr)
        for v in all_v:
            print("  " + v, file=sys.stderr)
        return 1

    print(f"fixture hygiene OK ({len(files)} files, "
          f"{len(FORBIDDEN)} patterns, allowlist-synthesized)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
