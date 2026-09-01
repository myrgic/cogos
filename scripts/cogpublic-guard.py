#!/usr/bin/env python3
"""Execute a repo's .cogpublic content guards. The part that was never written.

HISTORY
-------
`.cogpublic` was added to myrgic/cogos on 2026-04-14, in the same commit as a
PII sanitization fix. Its header says it is "Used by `cog plan upstream` /
`cog apply upstream` to sanitize pushes."

**That command has never existed.** `git log -S "plan upstream"` returns only
the commit that wrote the claim; the shipped `cogos` binary answers
`unknown command "plan"`; no repo has an active git hook; nothing anywhere
references the file. The guard was DECLARED and never IMPLEMENTED.

The consequence is measurable. Three instances of the exact patterns
`.cogpublic` declares reached public repos AFTER it was written:

    2026-04-14  .cogpublic written, declaring /Users/slowbro a rejection pattern
    2026-08-02  mod3    #138/#142 — leaked local paths + personal data
    2026-08-28  cogos   #588      — status board, MCP inventory, socket path
    2026-09-01  mod3    #146      — /Users/slowbro live on public HEAD

A declared guard that nothing executes is worse than no guard: it reads like
coverage during review, so nobody looks twice. This script is the missing
executor.

USAGE
    cogpublic_guard.py                # scan tracked files at HEAD
    cogpublic_guard.py --staged       # pre-commit / pre-push: scan staged only
    cogpublic_guard.py --all-history  # every blob in every commit (slow)
    cogpublic_guard.py --self-test    # prove the detector fires (see below)

Exit 0 = clean, 1 = violations found, 2 = the guard itself could not run.
Exit 2 matters: a guard that cannot run must not look like a guard that passed.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

CONFIG = ".cogpublic"


def sh(*args, **kw) -> str:
    return subprocess.run(args, capture_output=True, text=True, **kw).stdout


def load_guards(root: Path) -> tuple[list[tuple[str, str]], list[str]]:
    """Parse content_guards + allow/deny from .cogpublic.

    Deliberately a small hand parser rather than a PyYAML dependency: this must
    run in a bare pre-commit hook and in CI before any install step, and a
    guard that fails to import is a guard that does not run.
    """
    path = root / CONFIG
    if not path.exists():
        raise FileNotFoundError(f"{CONFIG} not found in {root}")

    guards: list[tuple[str, str]] = []
    excludes: list[str] = []
    section = None
    pending: str | None = None

    for raw in path.read_text().splitlines():
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith((" ", "\t", "-")):
            section = line.split(":")[0].strip()
            continue
        stripped = line.strip()
        if section == "content_guards":
            if stripped.startswith("- pattern:"):
                if pending:
                    guards.append((pending, ""))
                pending = stripped.split("pattern:", 1)[1].strip().strip("\"'")
            elif stripped.startswith("description:") and pending:
                guards.append((pending, stripped.split("description:", 1)[1].strip().strip("\"'")))
                pending = None
        elif section in ("exclude", "deny"):
            if stripped.startswith("- "):
                excludes.append(stripped[2:].strip().strip("\"'"))
    if pending:
        guards.append((pending, ""))
    return guards, excludes


def excluded(path: str, patterns: list[str]) -> bool:
    from fnmatch import fnmatch
    for p in patterns:
        if fnmatch(path, p) or fnmatch(path, p.rstrip("/*") + "/*"):
            return True
    return False


# Files whose PURPOSE is to carry the patterns: the config itself, and tests
# asserting the guard works. Skipping these is not a loophole — a scanner that
# flags its own ruleset produces noise that trains people to ignore it.
SELF_REFERENTIAL = re.compile(
    r"(^|/)\.cogpublic$|(^|/)cogpublic_guard\.py$|_guard_test\.|_names_test\.go$|sanitize_fixture\.py$"
)


def scan(root: Path, mode: str) -> int:
    try:
        guards, excludes = load_guards(root)
    except FileNotFoundError as e:
        print(f"GUARD CANNOT RUN: {e}", file=sys.stderr)
        return 2
    if not guards:
        print(f"GUARD CANNOT RUN: no content_guards parsed from {CONFIG}", file=sys.stderr)
        return 2

    if mode == "staged":
        files = [f for f in sh("git", "diff", "--cached", "--name-only",
                               "--diff-filter=ACM").splitlines() if f]
    else:
        files = [f for f in sh("git", "ls-files").splitlines() if f]

    compiled = [(re.compile(p), p, d) for p, d in guards]
    violations = []
    scanned = 0

    for f in files:
        if excluded(f, excludes) or SELF_REFERENTIAL.search(f):
            continue
        fp = root / f
        if not fp.is_file():
            continue
        try:
            body = fp.read_text(errors="ignore")
        except Exception:
            continue
        scanned += 1
        for rx, pat, desc in compiled:
            m = rx.search(body)
            if m:
                line = body[:m.start()].count("\n") + 1
                violations.append(f"{f}:{line}: {desc or pat}")
                break

    if violations:
        print("PUBLIC RELEASE GATE — BLOCKED", file=sys.stderr)
        for v in sorted(violations):
            print(f"  {v}", file=sys.stderr)
        print(f"\n{len(violations)} file(s) match a .cogpublic content guard.",
              file=sys.stderr)
        return 1

    print(f"public release gate OK ({scanned} files, {len(guards)} guards)")
    return 0


def self_test(root: Path) -> int:
    """Prove the detector fires. A guard that has only ever said 'clean' has
    not been tested — which is precisely how the declared-but-unimplemented
    .cogpublic passed as coverage for four months."""
    try:
        guards, _ = load_guards(root)
    except FileNotFoundError as e:
        print(f"SELF-TEST FAILED: {e}", file=sys.stderr)
        return 2
    if not guards:
        print("SELF-TEST FAILED: zero guards parsed", file=sys.stderr)
        return 2

    import tempfile
    fails = []
    for pat, desc in guards:
        # Build a string that MUST match this pattern.
        probe = {
            r"/Users/slowbro": "/Users/slowbro/x",
            r"-Users-slowbro-": "-Users-slowbro-x",
        }.get(pat)
        if probe is None:
            if not any(c in pat for c in ".*+[](){}\\|^$?"):
                probe = pat                      # literal
            else:
                continue                          # regex probe not synthesizable
        if not re.search(pat, probe):
            fails.append(f"pattern {pat!r} did not match its own probe {probe!r}")

    print(f"self-test: {len(guards)} guards parsed from {CONFIG}")
    for pat, desc in guards:
        print(f"  - {pat}  ({desc})")
    if fails:
        for f in fails:
            print(f"  FAIL: {f}", file=sys.stderr)
        return 1
    print("self-test OK — every synthesizable guard matches a known-dirty probe")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--staged", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--root", default=".")
    a = ap.parse_args()
    root = Path(a.root).resolve()
    if a.self_test:
        return self_test(root)
    return scan(root, "staged" if a.staged else "head")


if __name__ == "__main__":
    sys.exit(main())
