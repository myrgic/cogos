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


def parse_value(raw: str) -> str:
    """Extract a scalar YAML value, quote-aware.

    DEFECT FIXED (review of #19): the previous implementation was
    `raw.strip().strip("\\"'")`, which silently compiled an inline comment into
    the regex. `pattern: "/Users/slowbro"  # operator home` became the pattern
    `/Users/slowbro"  # operator home`, matching nothing — the scan reported
    clean AND the self-test passed (probe==pattern matches tautologically).
    A guard silently degraded to matching nothing, on the exact pattern family
    that caused the incident this tool exists to prevent.

    Quoted values are read to their closing quote and everything after is
    discarded. Unquoted values are truncated at ` #`. A `#` inside quotes is
    preserved, since it is legal inside a regex character class.
    """
    s = raw.strip()
    if s and s[0] in "\"'":
        q = s[0]
        end = s.find(q, 1)
        if end == -1:
            raise ValueError(f"unterminated quote in value: {raw!r}")
        return s[1:end]
    # Unquoted: a comment must be preceded by whitespace to count as one.
    m = re.search(r"\s#", s)
    return (s[:m.start()] if m else s).strip()


def load_guards(root: Path) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Parse content_guards + exclude from .cogpublic.

    Deliberately a small hand parser rather than a PyYAML dependency: this must
    run in a bare pre-commit hook and in CI before any install step, and a
    guard that fails to import is a guard that does not run.

    Returns (guards, excludes) where each guard is (pattern, description, probe).
    `probe` is an optional known-dirty string the self-test asserts against.
    """
    path = root / CONFIG
    if not path.exists():
        raise FileNotFoundError(f"{CONFIG} not found in {root}")

    guards: list[tuple[str, str, str]] = []
    excludes: list[str] = []
    section = None
    pending: dict | None = None

    def flush():
        nonlocal pending
        if pending:
            guards.append((pending["pattern"], pending.get("description", ""),
                           pending.get("probe", "")))
            pending = None

    for raw in path.read_text().splitlines():
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith((" ", "\t", "-")):
            flush()
            section = line.split(":")[0].strip()
            continue
        stripped = line.strip()
        if section == "content_guards":
            if stripped.startswith("- pattern:"):
                flush()
                pending = {"pattern": parse_value(stripped.split("pattern:", 1)[1])}
            elif pending is not None:
                for key in ("description", "probe"):
                    if stripped.startswith(f"{key}:"):
                        pending[key] = parse_value(stripped.split(f"{key}:", 1)[1])
        elif section in ("exclude", "deny"):
            if stripped.startswith("- "):
                excludes.append(parse_value(stripped[2:]))
    flush()
    return guards, excludes


def excluded(path: str, patterns: list[str]) -> bool:
    from fnmatch import fnmatch
    for p in patterns:
        if fnmatch(path, p) or fnmatch(path, p.rstrip("/*") + "/*"):
            return True
    return False


# The config itself is skipped: it necessarily contains every pattern, and a
# scanner that flags its own ruleset produces noise that trains people to
# ignore it.
#
# DEFECT FIXED (review of #19): this was previously a FILENAME regex that also
# excluded `sanitize_fixture.py`, `*_names_test.go`, and `*_guard_test.*`.
# Filenames are contributor-controllable, so a real leak was bypassable by
# naming the file `sanitize_fixture.py` — reproduced: exit 0 with the leak
# present, while identical content in `ordinary.py` exited 1. Exclusion by
# filename is not a security property. Only the config path is skipped now;
# anything else that legitimately carries a pattern must be listed explicitly
# in that repo's `exclude:`, which is a reviewable declaration rather than an
# invisible convention.
SELF_REFERENTIAL = re.compile(r"(^|/)\.cogpublic$")


def scan(root: Path, mode: str) -> int:
    try:
        guards, excludes = load_guards(root)
    except (FileNotFoundError, ValueError) as e:
        print(f"GUARD CANNOT RUN: {e}", file=sys.stderr)
        return 2
    if not guards:
        print(f"GUARD CANNOT RUN: no content_guards parsed from {CONFIG}", file=sys.stderr)
        return 2

    if mode == "staged":
        files = [f for f in sh("git", "diff", "--cached", "--name-only",
                               "--diff-filter=ACM").splitlines() if f]
    elif mode == "tree":
        # Every file on disk, git or not. For BUILD ARTIFACTS: a generated
        # deploy tree is `git init` + `git add .` with nothing committed, so
        # `git ls-files` returns empty and a HEAD scan would examine zero
        # files. Found by the positive control in
        # internal/providers/site/gate_artifact_test.go — the clean-artifact
        # case failed, which is exactly what a positive control is for.
        files = [
            str(p.relative_to(root))
            for p in root.rglob("*")
            if p.is_file() and ".git/" not in str(p.relative_to(root))
        ]
    else:
        files = [f for f in sh("git", "ls-files").splitlines() if f]

    compiled = [(re.compile(p), p, d) for p, d, _ in guards]
    violations = []
    scanned = 0

    for f in files:
        if excluded(f, excludes) or SELF_REFERENTIAL.search(f):
            continue
        fp = root / f
        if not fp.is_file():
            continue
        try:
            content = fp.read_text(errors="ignore")
        except Exception:
            continue
        scanned += 1
        # Report EVERY guard a file trips, not just the first. Breaking on the
        # first match hid co-located findings: a file with both a home path and
        # a token showed one line, so fixing it looked sufficient.
        for rx, pat, desc in compiled:
            m = rx.search(content)
            if m:
                line = content[:m.start()].count("\n") + 1
                violations.append(f"{f}:{line}: {desc or pat}")

    if violations:
        print("PUBLIC RELEASE GATE — BLOCKED", file=sys.stderr)
        for v in sorted(violations):
            print(f"  {v}", file=sys.stderr)
        print(f"\n{len(violations)} finding(s) match a .cogpublic content guard.",
              file=sys.stderr)
        return 1

    # A scan that examined nothing is not a pass. In `staged` mode zero files is
    # legitimate (nothing staged touches tracked content); at HEAD it means the
    # excludes swallowed the repo or git listed nothing, and reporting "OK" for
    # that is the silent-green failure this tool exists to prevent.
    if scanned == 0 and mode != "staged":  # tree/head: nothing scanned == cannot run
        print("GUARD CANNOT RUN: scanned 0 files at HEAD — check `exclude:` "
              "patterns and that this is a git worktree", file=sys.stderr)
        return 2

    print(f"public release gate OK ({scanned} files, {len(guards)} guards)")
    return 0


def self_test(root: Path) -> int:
    """Prove the detector fires. A guard that has only ever said 'clean' has
    not been tested — which is precisely how the declared-but-unimplemented
    .cogpublic passed as coverage for four months.

    DEFECT FIXED (review of #19): this previously printed "every synthesizable
    guard matches a known-dirty probe" while SILENTLY SKIPPING every pattern
    containing a regex metacharacter — i.e. all the token guards
    (`sk-…`, `ghp_…`, `xoxb-…`). It could report OK having tested nothing, and
    the reader could not tell. Untested guards are now reported explicitly and
    a repo may supply its own `probe:` to convert one into a tested guard.
    """
    try:
        guards, _ = load_guards(root)
    except (FileNotFoundError, ValueError) as e:
        print(f"SELF-TEST FAILED: {e}", file=sys.stderr)
        return 2
    if not guards:
        print("SELF-TEST FAILED: zero guards parsed", file=sys.stderr)
        return 2

    # Built-in probes for the org baseline patterns. A repo can override or
    # extend by adding `probe:` beside any guard in its own .cogpublic.
    BUILTIN = {
        r"sk-[a-zA-Z0-9]{20,}": "sk-" + "a" * 24,
        r"ghp_[a-zA-Z0-9]{20,}": "ghp_" + "b" * 24,
        r"xoxb-[a-zA-Z0-9]+": "xoxb-abc123",
        r"@gmail\.com|@yahoo\.com|@hotmail\.com": "someone@gmail.com",
    }

    fails, tested, untested = [], [], []
    print(f"self-test: {len(guards)} guards parsed from {CONFIG}")

    for pat, desc, declared_probe in guards:
        probe = declared_probe or BUILTIN.get(pat)
        if probe is None and not any(c in pat for c in ".*+[](){}\\|^$?"):
            probe = pat + "X"          # literal pattern: any superstring matches
        if probe is None:
            untested.append((pat, desc))
            print(f"  - {pat}  ({desc})  [UNTESTED — add `probe:` to test it]")
            continue
        if re.search(pat, probe):
            tested.append(pat)
            print(f"  - {pat}  ({desc})  [tested]")
        else:
            fails.append(f"pattern {pat!r} did not match its probe {probe!r}")
            print(f"  - {pat}  ({desc})  [FAILED]")

    if fails:
        for f in fails:
            print(f"  FAIL: {f}", file=sys.stderr)
        return 1

    print(f"self-test OK — {len(tested)} guard(s) verified against a "
          f"known-dirty probe, {len(untested)} untested")
    if untested:
        print("  untested guards are NOT proven to fire; add a `probe:` line "
              "beside each to close the gap")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--staged", action="store_true")
    ap.add_argument("--tree", action="store_true",
                    help="scan every file on disk, not just git-tracked ones "
                         "(for build artifacts with nothing committed yet)")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--root", default=".")
    a = ap.parse_args()
    root = Path(a.root).resolve()
    if a.self_test:
        return self_test(root)
    mode = "staged" if a.staged else ("tree" if a.tree else "head")
    return scan(root, mode)


if __name__ == "__main__":
    sys.exit(main())
