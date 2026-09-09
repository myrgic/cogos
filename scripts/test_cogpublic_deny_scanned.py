#!/usr/bin/env python3
"""Negative controls for two deny-list bugs found across two rounds of
blocking review of this PR's diff.

CASE 1 -- exclude/deny conflation (round 1): load_guards() folded `deny:`
into the same `excludes` list used to skip content scanning
(`elif section in ('exclude', 'deny')`). `.cogpublic` lists `.cog/**` under
`deny:`, and this repo already tracks files under `.cog/**`. Under the
buggy parser, a secret planted in a tracked `.cog/**` file would be
silently exempted from the leak scan -- the exact "guard reads like
coverage but doesn't fire" failure this PR exists to fix, reproduced
inside the fix itself.

CASE 2 -- extension-glob deny is root-only (round 2, found in the fix for
case 1): `denied()` compiled a slash-free deny glob like `*.gguf` with the
same `glob_to_regex()` used for `exclude:`, which treats `*` as matching
within a single path segment only. `pkg/foo/testdata/weights.gguf` was
therefore silently NOT denied -- a nested binary model-weight file, the
realistic case this PR's own description says the deny-by-path feature
exists to catch, scanned clean and would have published. `denied()` now
matches a slash-free wildcard glob against the path's BASENAME at any
depth (see `deny_glob_to_regex()` in cogpublic-guard.py).

Each case builds a throwaway git repo (not this one), runs
`cogpublic-guard.py --root <tmp>` (HEAD mode, git-tracked files), and
asserts the scan finds the planted file (exit 1, violation reported). If
either bug is present, the scan reports OK (exit 0) because the file was
silently excluded or the deny glob never matched -- proving the bug is
real and the fix closes it.

Run directly: python3 scripts/test_cogpublic_deny_scanned.py
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
GUARD = HERE / "cogpublic-guard.py"

# Obviously fake, never a real credential.
FAKE_SECRET = "sk-" + "FAKEFAKEFAKEFAKEFAKE"

COGPUBLIC = f"""version: 1
allow:
  - "*.md"
deny:
  - .cog/**
content_guards:
  - pattern: 'sk-[a-zA-Z0-9]{{20,}}'
    description: "OpenAI API key pattern (test fixture, fake)"
exclude:
  - scripts/cogpublic-guard.py
"""


COGPUBLIC_GGUF = """version: 1
allow:
  - "*.md"
deny:
  - "*.gguf"
content_guards:
  - pattern: 'sk-[a-zA-Z0-9]{20,}'
    description: "OpenAI API key pattern (test fixture, fake, unused here)"
exclude:
  - scripts/cogpublic-guard.py
"""

# Nested on purpose: a root-level "weights.gguf" would have passed even
# under the round-2 bug (root-only match happened to cover the root case).
# The realistic failure -- and the one the PR description names -- is a
# model weight sitting a few directories deep.
NESTED_GGUF_PATH = "pkg/foo/testdata/weights.gguf"


def build_repo(tmp: Path) -> None:
    (tmp / ".cogpublic").write_text(COGPUBLIC)
    cog_dir = tmp / ".cog"
    cog_dir.mkdir()
    # Planted, obviously-fake secret-shaped content under a deny-listed path.
    (cog_dir / "probe.txt").write_text(f"leaked token: {FAKE_SECRET}\n")

    def run(*args):
        subprocess.run(args, cwd=tmp, check=True, capture_output=True, text=True)

    run("git", "init", "-q")
    run("git", "config", "user.email", "test@example.invalid")
    run("git", "config", "user.name", "test")
    run("git", "add", "-A")
    run("git", "commit", "-q", "-m", "seed")


def build_repo_nested_gguf(tmp: Path) -> None:
    (tmp / ".cogpublic").write_text(COGPUBLIC_GGUF)
    target = tmp / NESTED_GGUF_PATH
    target.parent.mkdir(parents=True)
    # Clean content on purpose -- this must be caught by the deny-by-path
    # check alone, with zero content_guards involvement, exactly as a real
    # binary model-weight file would be (a .gguf's bytes match none of the
    # content_guards regexes).
    target.write_text("clean content, matches no content_guards pattern\n")

    def run(*args):
        subprocess.run(args, cwd=tmp, check=True, capture_output=True, text=True)

    run("git", "init", "-q")
    run("git", "config", "user.email", "test@example.invalid")
    run("git", "config", "user.name", "test")
    run("git", "add", "-A")
    run("git", "commit", "-q", "-m", "seed")


def run_guard(tmp: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(GUARD), "--root", str(tmp)],
        capture_output=True, text=True,
    )


def case_deny_exclude_conflation() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="cogpublic-deny-test-"))
    try:
        build_repo(tmp)
        proc = run_guard(tmp)
        print("--- case 1 (deny/exclude conflation): guard stdout ---")
        print(proc.stdout)
        print("--- guard stderr ---")
        print(proc.stderr)
        print(f"--- exit code: {proc.returncode} ---")

        found = "probe.txt" in proc.stdout + proc.stderr and proc.returncode == 1
        if found:
            print("PASS: deny-listed .cog/** path WAS scanned and the "
                  "planted secret was caught.")
            return 0
        print("FAIL: deny-listed .cog/** path was NOT flagged -- the "
              "content scan silently skipped a deny-listed path.",
              file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def case_nested_extension_glob() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="cogpublic-deny-gguf-test-"))
    try:
        build_repo_nested_gguf(tmp)
        proc = run_guard(tmp)
        print("--- case 2 (nested *.gguf deny): guard stdout ---")
        print(proc.stdout)
        print("--- guard stderr ---")
        print(proc.stderr)
        print(f"--- exit code: {proc.returncode} ---")

        blocked = (NESTED_GGUF_PATH in proc.stdout + proc.stderr
                   and proc.returncode == 1)
        if blocked:
            print(f"PASS: nested deny-listed path {NESTED_GGUF_PATH!r} "
                  "(matching *.gguf) WAS blocked by path alone, with "
                  "clean content and no content_guards hit.")
            return 0
        print(f"FAIL: nested path {NESTED_GGUF_PATH!r} matching deny glob "
              "'*.gguf' was NOT flagged -- the extension-only deny glob "
              "only matches at the repo root, not at depth.",
              file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    rc1 = case_deny_exclude_conflation()
    rc2 = case_nested_extension_glob()
    return 1 if (rc1 or rc2) else 0


if __name__ == "__main__":
    sys.exit(main())
