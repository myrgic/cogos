#!/usr/bin/env python3
"""Syntax-check every `run:` block in every workflow.

A workflow can be perfectly valid YAML and still contain shell that cannot
parse. That is exactly how ledger L24's fix shipped broken: an edit to
pr-review.yml left an orphaned `fi`, `yaml.safe_load` was happy, and three
required checks failed in CI with

    /home/runner/work/_temp/<id>.sh: line 84: syntax error near unexpected token `fi'

The local check I ran validated the YAML and never the embedded bash, so it
could not have caught it. This closes that gap: it extracts each `run:` block
and runs `bash -n` over it.

Limitation, stated honestly: `bash -n` parses, it does not execute, so this
catches structural breakage (unbalanced if/fi/quotes/heredocs) and nothing
about runtime behaviour. GitHub expression interpolation `${{ ... }}` is left
as-is; it is substituted before the shell ever sees it, and it does not affect
parse structure in practice.

Exit 0 when every block parses, 1 otherwise. No arguments.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile

try:
    import yaml
except ImportError:
    print("PyYAML required: uv run --with pyyaml python3 scripts/ci/check-workflow-shell.py")
    raise SystemExit(2)

REPO = pathlib.Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"


def shell_blocks(doc: dict):
    """Yield (job_name, step_index, step_name, script) for every run: block."""
    for job_name, job in (doc.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        for idx, step in enumerate(job.get("steps") or []):
            if not isinstance(step, dict):
                continue
            script = step.get("run")
            if isinstance(script, str) and script.strip():
                yield job_name, idx, step.get("name", "<unnamed>"), script


def main() -> int:
    failures = 0
    checked = 0

    for path in sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml")):
        try:
            doc = yaml.safe_load(path.read_text())
        except yaml.YAMLError as exc:
            print(f"FAIL {path.name}: invalid YAML: {exc}")
            failures += 1
            continue
        if not isinstance(doc, dict):
            continue

        for job_name, idx, step_name, script in shell_blocks(doc):
            checked += 1
            tmp = tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False)
            try:
                tmp.write(script)
                tmp.close()
                proc = subprocess.run(
                    ["bash", "-n", tmp.name], capture_output=True, text=True
                )
                if proc.returncode != 0:
                    failures += 1
                    detail = proc.stderr.strip().replace(tmp.name, "<step>")
                    print(f"FAIL {path.name} :: job '{job_name}' step {idx} ({step_name})")
                    for line in detail.splitlines():
                        print(f"       {line}")
            finally:
                os.unlink(tmp.name)

    if failures:
        print(f"\n{failures} shell block(s) failed to parse across {checked} checked.")
        return 1

    # Positive report: this ran and found nothing, as distinct from not running.
    print(f"OK: {checked} workflow shell block(s) parse cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
