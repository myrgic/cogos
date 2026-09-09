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

DENY-BY-PATH (review of this PR, blocking): `.cogpublic`'s `deny:` list names
paths that must never leave the constellation (`.cog/**`, `*.db`, `*.gguf`,
...), but until this fix `load_guards()` parsed it and every caller discarded
it (`_denies`) -- only the 7 `content_guards` regexes gated a file, so a
deny-listed blob whose bytes happened to match none of them (a binary
`.safetensors`/`.gguf`, a `.cog/state.db` sqlite file) scanned clean and
published. Every scan mode (`head`/`staged`/`tree`) now also checks each
file's PATH against the deny globs, independent of and evaluated BEFORE the
exclude check, so a path that is both excluded and denied is still denied
(exclude only ever narrows the CONTENT scan; it must never narrow deny). A
deny hit blocks on the path alone, regardless of content, and names the glob
that matched.

DENY GLOB SEMANTICS (round 2 of the same review -- the round-1 fix above
shipped `denied()` reusing `glob_to_regex()` verbatim, which made the six
extension-only entries -- `*.db`/`*.sqlite`/`*.pt`/`*.onnx`/`*.safetensors`/
`*.gguf` -- compile to a single-segment, ROOT-ONLY match: a stray
`pkg/foo/testdata/weights.gguf` was silently NOT denied, and the self-test
never caught it because it only ever probed `denies[0]`, a directory glob
that already worked). A `deny:` glob is now one of exactly three classes,
each documented at its matching function (`denied()`, `instantiate_deny_pattern()`):

  1. No `/`, has a wildcard (e.g. `*.gguf`, `*.db`)
     -> matches the file's BASENAME, at ANY depth. This is the class the
     round-1 fix missed: an extension deny must catch the file wherever it
     lands, not only at the repo root, because content scanning cannot see
     inside a binary blob to catch it a second way.
  2. Contains a `/` (e.g. `.cog/run/**`, `autoresearch-foveated/eval-details.json`)
     -> matches from the REPO ROOT with the existing segment-aware rules
     unchanged: `**` crosses path segments, `*` does not. Same as `exclude:`.
  3. No `/`, no wildcard (a bare literal, e.g. `vendor`)
     -> denies its own whole subtree from the repo root (unchanged from
     before this fix; `glob_to_regex()`'s literal-implies-subtree suffix
     already gave this correctly).

`exclude:` semantics (`glob_to_regex()`, `excluded()`) are UNCHANGED by this
fix -- only `denied()` gained the extra basename-at-any-depth branch, via a
deny-specific compile step (`deny_glob_to_regex()`), because `exclude:`'s
existing single-segment-only meaning for a slash-free wildcard glob is
correct for excludes and must not be touched.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
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


def load_guards(root: Path) -> tuple[list[tuple[str, str, str]], list[str], list[str]]:
    """Parse content_guards + exclude + deny from .cogpublic.

    Deliberately a small hand parser rather than a PyYAML dependency: this must
    run in a bare pre-commit hook and in CI before any install step, and a
    guard that fails to import is a guard that does not run.

    Returns (guards, excludes, denies) where each guard is
    (pattern, description, probe). `probe` is an optional known-dirty string
    the self-test asserts against.

    DEFECT FIXED (blocking review of this PR): `exclude:` and `deny:` were
    folded into the same `excludes` list (`elif section in ('exclude', 'deny')`).
    `exclude:` means "not part of the leak scan" (e.g. the guard script itself,
    which legitimately contains probe strings). `deny:` means the opposite:
    "must never leave the constellation" -- .cog/**, model weights, DBs. Those
    are the paths MOST likely to carry a real leak, not the least, and this
    repo already tracks files under .cog/**. Folding deny into excludes
    silently exempted every deny-listed path from the content scan this PR
    exists to add -- reproducing, inside the fix, the exact "guard reads like
    coverage but doesn't fire" failure the PR was written to eliminate. `deny`
    is now returned separately and MUST NOT be used to skip content scanning;
    see `excluded()` and `scan()`, which only ever consult `excludes`.
    """
    path = root / CONFIG
    if not path.exists():
        raise FileNotFoundError(f"{CONFIG} not found in {root}")

    guards: list[tuple[str, str, str]] = []
    excludes: list[str] = []
    denies: list[str] = []
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
        elif section == "exclude":
            if stripped.startswith("- "):
                excludes.append(parse_value(stripped[2:]))
        elif section == "deny":
            # Deny-listed paths must NEVER leak. They are intentionally kept
            # OUT of `excludes` -- see the DEFECT FIXED note above -- so they
            # remain fully subject to the content-guard scan. `denies` is
            # ALSO consulted directly by `scan()` as an independent
            # path-based check (see `denied()`): a deny-listed path is a
            # violation regardless of content and regardless of `exclude:`
            # (blocking review of this PR -- deny was parsed but nothing
            # enforced it beyond content_guards, so a deny-listed binary
            # blob matching none of the 7 regexes scanned clean).
            if stripped.startswith("- "):
                denies.append(parse_value(stripped[2:]))
    flush()
    return guards, excludes, denies


def glob_to_regex(pat: str) -> re.Pattern:
    """Translate a glob to a regex where `*` does NOT cross a path separator.

    DEFECT FIXED (review of #19, MEDIUM-3): `excluded()` used fnmatch, whose
    `*` matches `/` too. `exclude: "docs/*"` therefore swallowed
    `docs/deep/secret.md` — and because other files still scanned, the count
    stayed nonzero and the zero-files exit-2 check never fired. Silent partial
    coverage: the dangerous middle case, worse than excluding everything,
    because nothing looks wrong.

    `*` matches within one segment, `**` spans segments, `?` is one non-slash
    character. A trailing `/` or bare directory name implies its whole subtree.
    """
    out, i = [], 0
    while i < len(pat):
        ch = pat[i]
        if ch == "*":
            if pat[i:i + 2] == "**":
                out.append(".*")
                i += 2
                if pat[i:i + 1] == "/":
                    i += 1
                continue
            out.append("[^/]*")
        elif ch == "?":
            out.append("[^/]")
        else:
            out.append(re.escape(ch))
        i += 1
    body = "".join(out)
    # Only a BARE directory name implies its subtree ("vendor" -> vendor/**).
    # A pattern containing a wildcard means exactly what it says: "docs/*" is
    # one level, NOT docs/deep/secret.md. An earlier attempt at this fix
    # appended the subtree suffix unconditionally and silently re-created the
    # very bug it was fixing — caught by re-running the reviewer's own
    # reproduction in a clean room instead of trusting the patch.
    if pat.endswith("/"):
        body += ".*"
    elif not any(c in pat for c in "*?"):
        body += "(?:/.*)?"
    return re.compile(f"^{body}$")


def excluded(path: str, patterns: list[str]) -> bool:
    for p in patterns:
        if not p:
            continue
        if glob_to_regex(p).match(path):
            return True
    return False


def deny_glob_to_regex(pat: str) -> re.Pattern:
    """Deny-specific compile step -- see the DENY GLOB SEMANTICS note at the
    top of this file for the three classes. Only class 1 (no `/`, has a
    wildcard) diverges from `glob_to_regex()`; classes 2 and 3 are that
    function unchanged, so `exclude:` semantics are untouched by this.

    DEFECT FIXED (round 2 of the blocking review of this PR): the round-1
    fix had `denied()` call `glob_to_regex()` verbatim, same as `excluded()`.
    For a slash-free wildcard pattern like `*.gguf`, `glob_to_regex()`
    correctly treats `*` as single-segment (that is right for `exclude:`,
    e.g. `docs/*`) -- but for a `deny:` extension glob there IS no other
    segment to specify; the pattern names an extension, not a location, and
    the file can land at any depth. Compiling it the `exclude:` way anchors
    the match to the repo root, so `pkg/foo/testdata/weights.gguf` -- the
    realistic case for a stray model weight -- was silently never denied.
    """
    if "/" not in pat and any(c in pat for c in "*?"):
        out = []
        for ch in pat:
            if ch == "*":
                out.append("[^/]*")
            elif ch == "?":
                out.append("[^/]")
            else:
                out.append(re.escape(ch))
        return re.compile(f"^{''.join(out)}$")
    return glob_to_regex(pat)


def denied(path: str, patterns: list[str]) -> str | None:
    """Return the first `deny:` glob that matches `path`, or None.

    Deny means the path must never publish, period. Callers must check this
    INDEPENDENTLY of, and BEFORE, excluded() -- a path that is both excluded
    and denied is still denied. Folding deny checks behind an exclude check
    would reproduce, for path-based enforcement, the exact "declared but
    silenced" bug the deny/exclude conflation in load_guards() already
    caused once for content scanning.

    Uses `deny_glob_to_regex()`, NOT `glob_to_regex()` directly (that was
    round 1's mistake -- see `deny_glob_to_regex()`'s docstring): a
    slash-free wildcard glob (`*.gguf`) is matched against the path's
    BASENAME so it fires at any depth, not just at the repo root; every
    other deny glob shape matches the full path exactly as `exclude:` does.
    """
    for p in patterns:
        if not p:
            continue
        target = path.rsplit("/", 1)[-1] if ("/" not in p and any(c in p for c in "*?")) else path
        if deny_glob_to_regex(p).match(target):
            return p
    return None


def decode_scannable(raw: bytes) -> str:
    """Decode bytes into every text form a guard might need to match.

    Returns all plausible decodings joined, not a single "best guess". Picking
    one encoding is what made the first version of this function wrong twice.

    DEFECT FIXED (review round 3): content was read `read_text(errors="ignore")`,
    i.e. UTF-8 only. UTF-16 stores ASCII as alternating NUL bytes, so
    `/Users/slowbro` decoded to garbage and every guard missed it.

    DEFECT FIXED (round 4, found by running the review's own attack list after
    the reviewer died mid-run): the first fix tried UTF-16 variants in order and
    took the first that did not raise. UTF-16-LE *never* raises on
    little-endian-ish bytes, so a UTF-16-**BE** file without a BOM decoded to
    CJK mojibake and scanned clean — and UTF-32 (whose BOM starts with the
    UTF-16 BOM bytes) was misdecoded the same way. Both reproduced: leak on
    disk, exit 0.

    A decoder that must GUESS will guess wrong. So decode under every candidate
    and search the union: a false extra decoding costs at worst a spurious
    finding a human resolves, while a missed one publishes a leak.
    """
    parts = [raw.decode("utf-8", errors="replace")]
    head = raw[:4096]
    # Wide encodings only matter when NULs are actually present; skip the work
    # (and the mojibake) for ordinary text.
    if head.count(0):
        for enc in ("utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"):
            try:
                parts.append(raw.decode(enc, errors="replace"))
            except (UnicodeDecodeError, LookupError, ValueError):
                continue
    return "\n".join(parts)


def read_blob(mode: str, path: str, root: Path) -> str | None:
    """Return the content the gate must judge, for this mode.

    DEFECT FIXED (review of #19, HIGH-2): `--staged` listed staged FILENAMES
    but read WORKTREE content. Staging a leak and then scrubbing the working
    copy produced exit 0 while the dirty blob committed anyway — the exact
    bypass a pre-commit gate exists to stop. Staged mode now reads the staged
    blob via `git show :path`, which is what actually gets committed.
    """
    if mode == "staged":
        proc = subprocess.run(["git", "-C", str(root), "show", f":{path}"],
                              capture_output=True)
        if proc.returncode != 0:
            return None
        return decode_scannable(proc.stdout)
    fp = root / path
    if not fp.is_file():
        return None
    try:
        return decode_scannable(fp.read_bytes())
    except Exception:
        return None


# Only the ROOT config is skipped: it necessarily contains every pattern, and a
# scanner that flags its own ruleset produces noise that trains people to
# ignore it.
#
# DEFECT FIXED (review round 3): this was `(^|/)\.cogpublic$`, whose `(^|/)`
# matched a `.cogpublic` at ANY depth. A leak parked in `sub/.cogpublic` scanned
# clean at exit 0 — a filename bypass, the same class as the `sanitize_fixture.py`
# rule removed in round 1, reintroduced by an over-permissive anchor. Only the
# repo-root path is exempt now; a nested `.cogpublic` is just a file and is
# scanned like one.
#
# DEFECT FIXED (review of #19): this was previously a FILENAME regex that also
# excluded `sanitize_fixture.py`, `*_names_test.go`, and `*_guard_test.*`.
# Filenames are contributor-controllable, so a real leak was bypassable by
# naming the file that way — reproduced: exit 0 with the leak present, while
# identical content in `ordinary.py` exited 1. Exclusion by filename is not a
# security property. Anything else that legitimately carries a pattern must be
# listed explicitly in that repo's `exclude:`, which is a reviewable
# declaration rather than an invisible convention.
SELF_REFERENTIAL = re.compile(r"^\.cogpublic$")

# Built-in probes for the org baseline patterns, shared by --self-test's
# per-pattern check and the symlink-target probe below. A repo can override
# or extend by adding `probe:` beside any guard in its own .cogpublic.
BUILTIN_PROBES = {
    r"sk-[a-zA-Z0-9]{20,}": "sk-" + "a" * 24,
    r"ghp_[a-zA-Z0-9]{20,}": "ghp_" + "b" * 24,
    r"xoxb-[a-zA-Z0-9]+": "xoxb-abc123",
    r"@gmail\.com|@yahoo\.com|@hotmail\.com": "someone@gmail.com",
}


def instantiate_deny_pattern(pat: str) -> str:
    """Build a concrete relative path guaranteed to match a `deny:` glob, for
    the self-test's own probe (see `_self_test_deny_path_probe`).

    Handles the three classes documented at DENY GLOB SEMANTICS (top of
    file) / `denied()`, and deliberately NESTS the probe wherever the class
    claims to match at depth -- a root-level probe would pass even under
    the round-1 bug this self-test exists to catch a second time:

      - "x/**"        -> "x/sub/probe.txt"      -- ** must cross MORE THAN
                          one segment, not just the one level a lazier probe
                          ("x/probe.txt") would also satisfy under a buggy
                          single-segment "**".
      - "*.ext"        -> "nested/dir/probe.ext" -- slash-free wildcard glob:
                          basename match at ANY depth. This is exactly the
                          class the round-1 fix left broken (root-only
                          match) while the self-test kept reporting OK.
      - bare literal   -> the literal path itself, unchanged -- it denies
                          its own subtree from the repo root, not a
                          basename anywhere, so nesting it would test the
                          wrong thing (a path that literal was never meant
                          to match).

    A generic fallback covers any other single/double-star combination a
    repo might add.
    """
    if pat.endswith("/**"):
        return pat[:-3] + "/sub/probe.txt"
    if pat.endswith("/*"):
        return pat[:-2] + "/probe.txt"
    if "/" not in pat and any(c in pat for c in "*?"):
        return "nested/dir/" + pat.replace("*", "probe").replace("?", "p")
    if "*" in pat or "?" in pat:
        return pat.replace("**", "probe").replace("*", "probe").replace("?", "p")
    return pat  # bare literal path already matches itself (own subtree)


def scan(root: Path, mode: str) -> int:
    try:
        guards, excludes, denies = load_guards(root)
    except (FileNotFoundError, ValueError) as e:
        print(f"GUARD CANNOT RUN: {e}", file=sys.stderr)
        return 2
    if not guards:
        print(f"GUARD CANNOT RUN: no content_guards parsed from {CONFIG}", file=sys.stderr)
        return 2

    # DEFECT FIXED (found while verifying the #19 review fixes): `sh()` ran git
    # in the PROCESS's cwd, ignoring --root entirely. Scanning another repo via
    # --root therefore listed THIS repo's files — the guard reported a confident
    # verdict about the wrong tree. It surfaced as mod3 reporting clean while a
    # known leak sat at tests/test_claude_session_id_binding.py:462.
    # Silent wrong-target is the worst class this tool has: it looks like a pass.
    symlinks: list[tuple[str, str]] = []  # (relpath, target-text); --tree only
    if mode == "staged":
        files = [f for f in sh("git", "-C", str(root), "diff", "--cached",
                               "--name-only", "--diff-filter=ACM").splitlines() if f]
    elif mode == "tree":
        # Every file on disk, git or not. For BUILD ARTIFACTS: a generated
        # deploy tree is `git init` + `git add .` with nothing committed, so
        # `git ls-files` returns empty and a HEAD scan would examine zero
        # files. Found by the positive control in
        # internal/providers/site/gate_artifact_test.go — the clean-artifact
        # case failed, which is exactly what a positive control is for.
        #
        # `.git/` and `.release-gate/` are skipped structurally, by path
        # component rather than substring. Unlike `git ls-files`, a filesystem
        # walk SEES the reusable workflow's own checkout, and the guard script
        # in it legitimately contains a probe string for every pattern — so a
        # --tree scan in CI would block on the scanner itself. Verified
        # 2026-09-01: it reported `.release-gate/scripts/cogpublic-guard.py:18`
        # and exited 1 against an otherwise clean tree.
        #
        # DEFECT FIXED (found alongside the Dockerfile/COGOS_REPO_ROOT review):
        # this loop used to `continue` on `p.is_symlink()` before any content
        # guard ran at all — a symlink whose TARGET path embeds a leak (a
        # build-cache alias pointing at /Users/<name>/..., say) published
        # clean with the gate reporting success. `--tree` is the last check
        # before a build artifact is force-pushed to a public repo, so a
        # silently-skipped file class there is exactly the failure mode this
        # tool exists to close. Symlinks are not resolved and read (the target
        # may not exist, or may point outside the tree); instead the link's
        # own target text is scanned as content, same as any other string.
        skip_dirs = {".git", ".release-gate"}
        files = []
        for p in root.rglob("*"):
            if p.is_symlink():
                rel = p.relative_to(root)
                if skip_dirs & set(rel.parts):
                    continue
                try:
                    target = os.readlink(str(p))
                except OSError:
                    continue
                symlinks.append((str(rel), target))
                continue
            if not p.is_file():
                continue
            rel = p.relative_to(root)
            if skip_dirs & set(rel.parts):
                continue
            files.append(str(rel))
    else:
        files = [f for f in sh("git", "-C", str(root), "ls-files").splitlines() if f]

    # Compile up front: an invalid pattern must be a loud "cannot run", not an
    # uncaught traceback mid-scan. (Review of #19, MEDIUM-4.)
    compiled = []
    for pat, desc, _ in guards:
        try:
            compiled.append((re.compile(pat), pat, desc))
        except re.error as e:
            print(f"GUARD CANNOT RUN: invalid regex {pat!r} in {CONFIG}: {e}",
                  file=sys.stderr)
            return 2
    violations = []
    scanned = 0

    for f in files:
        # DENY-BY-PATH (blocking review of this PR): checked independently of,
        # and BEFORE, the exclude/self-referential skip below. A deny-listed
        # path is a violation on the path alone, regardless of its content and
        # regardless of `exclude:` -- exclude only narrows the content scan
        # that follows; it must never narrow this. See `denied()`.
        deny_hit = denied(f, denies)
        if deny_hit is not None:
            violations.append(
                f"{f}: DENIED — path matches deny glob {deny_hit!r} "
                "(blocked regardless of content; exclude: does not override deny)")
        if excluded(f, excludes) or SELF_REFERENTIAL.search(f):
            continue
        content = read_blob(mode, f, root)
        if content is None:
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

    # Symlink targets (--tree only; see the DEFECT FIXED note above) are text,
    # not files with lines, but they go through the exact same exclude and
    # pattern checks as any other scanned content — an unlisted symlink is not
    # a free pass.
    for rel, target in symlinks:
        # Same deny-by-path check as the regular-file loop above, and for the
        # same reason: a symlink's own name can match a deny glob (a
        # `*.gguf` alias into a cache dir, say) independent of whatever its
        # target text contains.
        deny_hit = denied(rel, denies)
        if deny_hit is not None:
            violations.append(
                f"{rel}: DENIED — path matches deny glob {deny_hit!r} "
                "(symlink; blocked regardless of content; exclude: does not override deny)")
        if excluded(rel, excludes) or SELF_REFERENTIAL.search(rel):
            continue
        scanned += 1
        for rx, pat, desc in compiled:
            if rx.search(target):
                violations.append(f"{rel} -> {target!r}: {desc or pat} (symlink target)")

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
        guards, _excludes, denies = load_guards(root)
    except (FileNotFoundError, ValueError) as e:
        print(f"SELF-TEST FAILED: {e}", file=sys.stderr)
        return 2
    if not guards:
        print("SELF-TEST FAILED: zero guards parsed", file=sys.stderr)
        return 2

    fails, tested, untested = [], [], []
    print(f"self-test: {len(guards)} guards parsed from {CONFIG}")

    for pat, desc, declared_probe in guards:
        probe = declared_probe or BUILTIN_PROBES.get(pat)
        if probe is None and not any(c in pat for c in ".*+[](){}\\|^$?"):
            probe = pat + "X"          # literal pattern: any superstring matches
        try:
            re.compile(pat)
        except re.error as e:
            print(f"  - {pat}  ({desc})  [INVALID REGEX]")
            print(f"SELF-TEST FAILED: invalid regex {pat!r}: {e}", file=sys.stderr)
            return 2
        if probe is None:
            untested.append((pat, desc))
            print(f"  - {pat}  ({desc})  [UNTESTED — add `probe:` to test it]")
            continue
        if re.search(pat, probe):
            tested.append(pat)
            # A DECLARED probe is supplied by the same person who wrote the
            # pattern, so it proves only that the two are consistent — an
            # alternation like `broken|harmless` with `probe: harmless` passes
            # while the real branch is broken (review of #19, MEDIUM-5). Label
            # the provenance so "N tested" cannot overstate what was proven.
            origin = "declared probe" if declared_probe else "builtin probe"
            print(f"  - {pat}  ({desc})  [tested: {origin}]")
        else:
            fails.append(f"pattern {pat!r} did not match its probe {probe!r}")
            print(f"  - {pat}  ({desc})  [FAILED]")

    # Prove the --tree symlink fix actually fires, rather than trusting the
    # code path un-exercised — the exact "declared but never proven" failure
    # this whole tool exists to close, now for symlink targets specifically.
    sym_ok, sym_detail = _self_test_symlink_probe(root, guards)
    if sym_ok is None:
        print(f"  - symlink target scan (--tree)  [UNTESTED — {sym_detail}]")
    elif sym_ok:
        tested.append("__symlink_target__")
        print(f"  - symlink target scan (--tree)  "
              f"[tested: caught {sym_detail!r} via a symlink's target text]")
    else:
        fails.append(sym_detail)
        print(f"  - symlink target scan (--tree)  [FAILED: {sym_detail}]")

    # Prove deny-by-path fires on clean content, for EVERY deny glob class
    # declared -- not just denies[0] -- and does NOT over-match a nested
    # clean control file. denies[0]-only was the round-2 finding from the
    # blocking review of this PR: it happened to be a directory glob that
    # already worked, so the self-test reported OK while the slash-free
    # extension-glob branch (*.gguf et al.) was still root-only and broken.
    deny_ok, deny_detail = _self_test_deny_path_probe(root, denies)
    if deny_ok is None:
        print(f"  - deny-by-path scan (--tree)  [UNTESTED — {deny_detail}]")
    elif deny_ok:
        tested.append("__deny_path__")
        print(f"  - deny-by-path scan (--tree)  [tested: {deny_detail}]")
    else:
        fails.append(deny_detail)
        print(f"  - deny-by-path scan (--tree)  [FAILED: {deny_detail}]")

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


def _self_test_symlink_probe(root: Path, guards) -> tuple[bool | None, str]:
    """Prove that a symlink whose TARGET text embeds a guarded pattern is
    caught by `--tree` scanning rather than silently skipped.

    Builds a throwaway directory containing only this repo's `.cogpublic` (so
    `scan()` has a real ruleset to load) and one symlink whose target string
    is a known-dirty probe for the first testable guard. Runs an actual
    `scan(..., "tree")` against it and requires exit 1 (BLOCKED) — a guard
    that cannot demonstrate this is not proven to have the fix at all.

    Returns (True, probe) on success, (False, reason) on a real failure, or
    (None, reason) when no guard in this repo's .cogpublic is testable (no
    declared or builtin probe and no literal pattern) — that is a gap in the
    ruleset's own probes, not a failure of this check.
    """
    probe = None
    for pat, _desc, declared_probe in guards:
        candidate = declared_probe or BUILTIN_PROBES.get(pat)
        if candidate is None and not any(c in pat for c in ".*+[](){}\\|^$?"):
            candidate = pat + "X"
        if candidate:
            probe = candidate
            break
    if probe is None:
        return None, "no testable content_guard available to build a symlink probe"

    tmp = Path(tempfile.mkdtemp(prefix="cogpublic-guard-selftest-"))
    try:
        shutil.copy(root / CONFIG, tmp / CONFIG)
        os.symlink(probe, str(tmp / "leaked-alias"))
        rc = scan(tmp, "tree")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    if rc != 1:
        return False, (f"expected exit 1 (BLOCKED) scanning a symlink targeting "
                        f"{probe!r}, got exit {rc}")
    return True, probe


def _self_test_deny_path_probe(root: Path, denies: list[str]) -> tuple[bool | None, str]:
    """Prove that EVERY `deny:` glob CLASS declared in this repo's
    `.cogpublic` is blocked under --tree by path alone, even when content
    matches zero content_guards patterns.

    Round 1 (blocking review of this PR): `deny:` was parsed and every
    caller discarded it -- only the 7 content_guards regexes gated a file,
    so a deny-listed binary blob whose bytes matched none of them scanned
    clean and published.

    Round 2 (blocking review of the round-1 fix): this self-test probed
    ONLY `denies[0]` -- a directory glob (`.cog/run/**`) that already
    worked correctly -- so it never exercised the slash-free extension-glob
    branch (`*.db`/`*.sqlite`/`*.pt`/`*.onnx`/`*.safetensors`/`*.gguf`),
    which was ALSO still broken (root-only match via `glob_to_regex()`,
    never matched a nested path) despite the self-test printing OK. This
    version probes every single `deny:` entry, not just the first.

    One throwaway directory per `deny:` entry, each holding only this
    repo's `.cogpublic` plus ONE file at the path `instantiate_deny_pattern`
    builds for that entry's class (nested wherever the class claims to
    match at depth -- see its docstring) with clean, innocuous content --
    must exit 1 (BLOCKED) on the path alone. Plus one more throwaway
    directory with a NESTED clean control file under an undenied extension
    -- must exit 0, proving the fix does not over-match every nested file.

    Returns (True, summary) on success, (False, reason) on a real
    failure, or (None, reason) when this repo's .cogpublic declares no
    `deny:` patterns at all -- a gap in the ruleset, not a failure here.
    """
    if not denies:
        return None, "no deny: patterns declared in this repo's .cogpublic"

    clean_content = "clean content, matches no content_guards pattern\n"

    def run_case(rel_path: str) -> int:
        tmp = Path(tempfile.mkdtemp(prefix="cogpublic-guard-selftest-deny-"))
        try:
            shutil.copy(root / CONFIG, tmp / CONFIG)
            target = tmp / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(clean_content)
            return scan(tmp, "tree")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    probed = []
    for pat in denies:
        probe_rel = instantiate_deny_pattern(pat)
        rc_denied = run_case(probe_rel)
        if rc_denied != 1:
            return False, (f"expected exit 1 (BLOCKED) scanning {probe_rel!r} "
                            f"(matches deny glob {pat!r}) with clean content, "
                            f"got exit {rc_denied}")
        probed.append((pat, probe_rel))

    rc_clean = run_case("nested/dir/clean-control.undenied")
    if rc_clean != 0:
        return False, ("expected exit 0 scanning a nested clean control file "
                        f"under an undenied extension, got exit {rc_clean} "
                        "(deny check is over-matching)")

    return True, (f"blocked all {len(probed)} deny glob(s) by path alone with "
                   f"clean content ({probed!r}); a nested clean control file "
                   "still passed")


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
