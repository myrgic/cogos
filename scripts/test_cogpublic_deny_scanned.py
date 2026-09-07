#!/usr/bin/env python3
"""Negative control for the exclude/deny conflation bug (blocking review of
this PR's original diff).

The bug: load_guards() folded `deny:` into the same `excludes` list used to
skip content scanning (`elif section in ('exclude', 'deny')`). `.cogpublic`
lists `.cog/**` under `deny:`, and this repo already tracks files under
`.cog/**`. Under the buggy parser, a secret planted in a tracked `.cog/**`
file would be silently exempted from the leak scan -- the exact
"guard reads like coverage but doesn't fire" failure this PR exists to fix,
reproduced inside the fix itself.

This test builds a throwaway git repo (not this one) with:
  - a .cogpublic declaring `deny: [".cog/**"]` and a content_guard pattern
  - a tracked file at .cog/probe.txt containing an obviously fake
    secret-shaped string that matches the guard pattern

It then runs `cogpublic-guard.py --root <tmp>` (HEAD mode, git-tracked files)
and asserts the scan finds the planted secret (exit 1, violation reported).
If the deny/exclude conflation bug is present, the scan reports OK (exit 0)
because .cog/** was silently excluded -- proving the bug is real and the
fix closes it.

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


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="cogpublic-deny-test-"))
    try:
        build_repo(tmp)
        proc = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(tmp)],
            capture_output=True, text=True,
        )
        print("--- guard stdout ---")
        print(proc.stdout)
        print("--- guard stderr ---")
        print(proc.stderr)
        print(f"--- exit code: {proc.returncode} ---")

        found = "probe.txt" in proc.stdout + proc.stderr and proc.returncode == 1
        if found:
            print("PASS: deny-listed .cog/** path WAS scanned and the "
                  "planted secret was caught.")
            return 0
        else:
            print("FAIL: deny-listed .cog/** path was NOT flagged -- the "
                  "content scan silently skipped a deny-listed path.",
                  file=sys.stderr)
            return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
