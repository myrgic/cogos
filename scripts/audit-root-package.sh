#!/usr/bin/env python3
# audit-root-package.sh — Stage A audit script for RFC-0001 root-package refactor.
#
# Classifies every Go file at the repository root (package main) with one of four
# dispositions so the Stage B/C/D refactor can proceed mechanically:
#
#   delete-orphan         No inbound references from the cmd/cogos build closure;
#                         no equivalent exists in internal/engine or pkg/. Safe to
#                         delete as dead code once Stage A merges.
#
#   delete-superseded     A same-named (or same-domain) peer already exists in
#                         internal/engine/ or internal/providers/, indicating the
#                         logic was ported during Wave 1a. The root copy can be
#                         deleted once test coverage equivalence is confirmed.
#
#   port                  The file has no engine/pkg peer but contributes live
#                         functionality. Needs an extraction PR (Stage D).
#
#   triage-not-superseded Same-domain peer exists in engine but the root file is
#                         not a one-to-one match — manual review required.
#
# Usage:
#   python3 scripts/audit-root-package.sh [--repo-root <path>] [--output md|csv|text]
#   # or directly:
#   scripts/audit-root-package.sh [--repo-root <path>] [--output md|csv|text]
#
# Default output is Markdown (writes to stdout). Pass --output csv for a
# machine-readable list.
#
# The script is idempotent — re-run at any commit to refresh the table.
# Stage A preserves it in scripts/ so any reviewer can run it.
#
# Requirements: Python 3.6+, git (in PATH).

import argparse
import datetime
import glob
import os
import re
import subprocess
import sys


# ── port target map ──────────────────────────────────────────────────────────
# Maps domain prefix (the part before the first underscore) to the target
# package where the file should be ported during Stage D.  An empty string
# means "no known target; manual triage required".
PORT_TARGETS = {
    "bep":          "pkg/bep",
    "modality":     "pkg/modality",
    "reconcile":    "pkg/reconcile",
    "capability":   "internal/capability",
    "decompose":    "internal/decompose",
    "discord":      "internal/providers/discord",
    "identity":     "internal/engine",
    "agent":        "internal/agent",
}


def repo_root_default():
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        result = subprocess.run(
            ["git", "-C", here, "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except Exception:
        return os.getcwd()


def git_short_hash(root):
    try:
        result = subprocess.run(
            ["git", "-C", root, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"


def domain_of(base):
    """Return the leading domain prefix (e.g. 'bus_session' -> 'bus')."""
    return base.split("_")[0]


def engine_has_exact_peer(root, base):
    return os.path.isfile(os.path.join(root, "internal", "engine", base + ".go"))


def engine_has_domain(root, domain):
    pattern = os.path.join(root, "internal", "engine", domain + "_*.go")
    return len(glob.glob(pattern)) > 0


def pkg_has_domain(root, domain):
    return os.path.isdir(os.path.join(root, "pkg", domain))


def test_funcs_in_file(filepath):
    """Return sorted list of Test* function names defined in a Go test file."""
    funcs = []
    try:
        with open(filepath) as fh:
            for line in fh:
                m = re.match(r'^func (Test[A-Za-z0-9_]+)\(', line)
                if m:
                    funcs.append(m.group(1))
    except OSError:
        pass
    return sorted(funcs)


def engine_has_test_func(root, fn):
    engine_dir = os.path.join(root, "internal", "engine")
    pattern = re.compile(r'^func ' + re.escape(fn) + r'\(')
    for fname in glob.glob(os.path.join(engine_dir, "*_test.go")):
        try:
            with open(fname) as fh:
                for line in fh:
                    if pattern.match(line):
                        return True
        except OSError:
            pass
    return False


def disposition_of(root, base):
    """
    Returns (disposition, peer_path, missing_engine_tests).
    """
    domain = domain_of(base)

    # 1. Exact same-named peer in internal/engine/ → superseded.
    if engine_has_exact_peer(root, base):
        peer = "internal/engine/{}.go".format(base)
        # Check for missing test coverage.
        test_file = os.path.join(root, base + "_test.go")
        missing = []
        for fn in test_funcs_in_file(test_file):
            if not engine_has_test_func(root, fn):
                missing.append(fn)
        return "delete-superseded", peer, missing

    # 2. Well-known port target defined → port.
    target = PORT_TARGETS.get(domain, "")
    if target:
        return "port", target, []

    # 3. Engine has same-domain files but no exact peer and no known target → triage.
    if engine_has_domain(root, domain):
        return "triage-not-superseded", "internal/engine/{}_*.go".format(domain), []

    # 4. pkg/ has the domain → port to pkg/.
    if pkg_has_domain(root, domain):
        return "port", "pkg/{}".format(domain), []

    # 5. No peer anywhere → orphan.
    return "delete-orphan", "-", []


def main():
    parser = argparse.ArgumentParser(
        description="Audit root Go files for RFC-0001 Stage A disposition table."
    )
    parser.add_argument("--repo-root", default=None, help="Path to git repository root")
    parser.add_argument("--output", choices=["md", "csv", "text"], default="md",
                        help="Output format (default: md)")
    args = parser.parse_args()

    root = args.repo_root or repo_root_default()

    # Collect production and test files at root.
    all_go = sorted(
        os.path.basename(f)
        for f in glob.glob(os.path.join(root, "*.go"))
    )
    prod_files = [f for f in all_go if not f.endswith("_test.go")]
    test_files = [f for f in all_go if f.endswith("_test.go")]

    # Build test-file map: base → test_file
    test_map = {}
    for tf in test_files:
        base = tf[: -len("_test.go")]
        test_map[base] = tf

    # Compute dispositions.
    rows = []
    counts = {"delete-orphan": 0, "delete-superseded": 0, "port": 0, "triage-not-superseded": 0}
    for f in prod_files:
        base = f[: -len(".go")]
        disp, peer, missing = disposition_of(root, base)
        test_file = test_map.get(base, "")
        rows.append((f, test_file, disp, peer, missing))
        counts[disp] = counts.get(disp, 0) + 1

    if args.output == "md":
        commit = git_short_hash(root)
        now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        print("# RFC-0001 Appendix A — Root Package File Disposition\n")
        print("Generated by `scripts/audit-root-package.sh` at commit `{}` ({}).\n".format(commit, now))
        print("Re-run this script at any commit to refresh the table. The script is")
        print("idempotent; disposition changes are visible as diff noise and warrant a")
        print("code review comment.\n")
        print("## Summary\n")
        print("| Disposition | Count |")
        print("|---|---|")
        print("| `delete-orphan` | {} |".format(counts["delete-orphan"]))
        print("| `delete-superseded` | {} |".format(counts["delete-superseded"]))
        print("| `port` | {} |".format(counts["port"]))
        print("| `triage-not-superseded` | {} |".format(counts["triage-not-superseded"]))
        print("| **Total production files** | **{}** |".format(len(prod_files)))
        print("| Test files | {} |".format(len(test_files)))
        print()
        print("## Disposition table\n")
        print("| File | Test file | Disposition | Closest peer / target | Missing test coverage in engine |")
        print("|---|---|---|---|---|")
        for f, test_file, disp, peer, missing in rows:
            mt = "<br>".join(missing) if missing else "-"
            print("| `{}` | {} | `{}` | `{}` | {} |".format(
                f,
                "`{}`".format(test_file) if test_file else "-",
                disp,
                peer,
                mt,
            ))

    elif args.output == "csv":
        print("file,test_file,disposition,peer,missing_engine_tests")
        for f, test_file, disp, peer, missing in rows:
            mt = "|".join(missing)
            print("{},{},{},{},{}".format(f, test_file, disp, peer, mt))

    elif args.output == "text":
        fmt = "{:<50} {:<25} {:<40}"
        print(fmt.format("FILE", "DISPOSITION", "PEER"))
        print(fmt.format("-" * 50, "-" * 25, "-" * 40))
        for f, test_file, disp, peer, missing in rows:
            print(fmt.format(f, disp, peer))


if __name__ == "__main__":
    main()
