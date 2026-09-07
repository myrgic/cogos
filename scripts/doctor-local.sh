#!/usr/bin/env bash
# doctor-local.sh — run the SAME doctor automation CI runs, locally.
#
# Parity is the point: .github/workflows/doctor.yml and this script must agree
# on the binary (built -tags fts5), the workspace shape (fresh `cogos init`),
# and the posture (advisory by default; --lint reported, not enforced). If they
# drift, the local run stops predicting the CI run and becomes decoration.
#
# Two modes, mirroring doctor's own two postures:
#
#   ./scripts/doctor-local.sh              advisory. Exit 0 unless a check FAILs.
#   ./scripts/doctor-local.sh --here       run against THIS workspace instead of
#                                          a scratch one (what CI cannot see).
#   ./scripts/doctor-local.sh --gate warn  enforce a threshold; exit 1 if any
#                                          finding is at/above it.
#
# The default (scratch workspace) answers "would CI be happy with this commit?"
# The --here mode answers "is MY machine healthy?" — a different question with
# a different answer, because host-derived findings (binary sprawl in
# ~/.cog/bin, dead paths in ~/.claude/settings.json) exist locally and not on a
# runner. Measured 2026-09-06: a clean scratch workspace reports 7 OK / 7 WARN /
# 0 FAIL / 5 UNKNOWN, while this dev machine reports more WARNs from host state.
#
# The 5 UNKNOWNs on a fresh workspace are correct, not defects: no
# constellation.db exists until something indexes it, so index/store checks
# honestly report "could not check". Never gate on UNKNOWN — doctor's contract
# is "UNKNOWN never reported as OK", and punishing that would teach checks to
# lie.
# END-OF-HELP

# no-pipefail: this script's entire job is CAPTURING non-zero exits, not dying
# on them. `cogos doctor` exits 1 on FAIL, `doctor --lint` exits 1 by design at
# a threshold, and both are recorded and reported rather than propagated. Under
# `set -e` the first advisory finding would abort the run before the verdict
# table is printed, which would defeat the point of an advisory reporter.
# `-o pipefail` IS set, and every pipeline that matters reads ${PIPESTATUS[0]}
# explicitly (see ADVISORY_EXIT below) rather than trusting `$?` after a pipe.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${TMPDIR:-/tmp}/cogos-doctor-local"
MODE="scratch"
GATE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --here)  MODE="here"; shift ;;
    --gate)  GATE="${2:-warn}"; shift 2 ;;
    -h|--help)
      # Print only the user-facing header — everything above the
      # END-OF-HELP marker. Previously `sed -n '2,34p'`, a hardcoded line
      # range that silently swallowed the implementation-rationale block
      # below it into --help output (cog-review note on 5ee1468) and would
      # drift again on any edit to the header. The marker cannot drift.
      sed -n '2,/^# END-OF-HELP$/p' "${BASH_SOURCE[0]}" \
        | grep -v '^# END-OF-HELP$' \
        | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# -- build ---------------------------------------------------------------
# -tags fts5 is not optional. A bare `go build` links go-sqlite3 WITHOUT the
# FTS5 module; doctor's index checks then measure a degraded binary and its
# negative-control (sentinel query) check silently degrades to grep. Same tag
# the Makefile, release.yml, and the CI doctor job use.
echo "== building cogos (-tags fts5) =="
# Declare the tag as well as passing it: a binary built with -tags fts5 but
# without this -X reports build_tags.mismatch=true even though fts5 works.
BUILD_TAGS="fts5"
if ! (cd "$REPO_ROOT" && go build -tags "$BUILD_TAGS" \
        -ldflags="-X github.com/myrgic/cogos/internal/engine.BuildTags=${BUILD_TAGS}" \
        -o "$BIN" ./cmd/cogos); then
  echo "doctor-local: build FAILED — cannot run doctor against an unbuilt tree" >&2
  exit 2
fi

# Assert the tag actually took. A binary that merely *declares* fts5 is a real
# incident this pairing exists to prevent: symbol presence is not module
# availability, so check the count, not the mere presence. (Recorded outside
# this repo, in the operator's cog workspace friction ledger
# 2026-09-06-friction-ledger-memory-search-fts5 L-01 "Live kernel built without
# FTS5" — cited as provenance, not as a resolvable in-repo reference. The
# in-repo statement of the same CGO/FTS5 hazard is in .github/workflows/
# release.yml.)
syms=$(strings "$BIN" 2>/dev/null | grep -c fts5 || true)
if [ "${syms:-0}" -lt 5 ]; then
  echo "doctor-local: WARNING — only ${syms} fts5 symbols in the built binary." >&2
  echo "  A correctly tagged build shows ~25; a tagless one shows ~1." >&2
  echo "  doctor's index checks may be measuring a degraded binary." >&2
fi

# -- workspace -----------------------------------------------------------
if [ "$MODE" = "here" ]; then
  WS="${COGOS_WORKSPACE:-$HOME/workspaces/cog}"
  echo "== doctor against LIVE workspace: $WS =="
  echo "   (host-derived findings expected; CI cannot see these)"
else
  WS=$(mktemp -d)
  trap 'rm -rf "$WS"' EXIT
  echo "== doctor against a fresh scratch workspace =="
  echo "   (this is what CI sees: $WS)"
  "$BIN" init --workspace "$WS" >/dev/null 2>&1 || {
    echo "doctor-local: cogos init failed" >&2; exit 2; }
fi

# -- run -----------------------------------------------------------------
OUT="${TMPDIR:-/tmp}/doctor-local-$$.txt"
( cd "$WS" && "$BIN" doctor ) | tee "$OUT"
ADVISORY_EXIT=${PIPESTATUS[0]}

echo
echo "== verdict counts =="
for v in OK WARN FAIL UNKNOWN; do
  n=$(grep -cE "^[[:space:]]*\[$v\]" "$OUT" || true)
  printf '  %-8s %s\n' "$v" "${n:-0}"
done
echo "  advisory exit: $ADVISORY_EXIT  (0 unless a check FAILs)"

# -- lint posture --------------------------------------------------------
echo
echo "== --lint posture (what a gate WOULD do) =="
for sev in warn fail; do
  ( cd "$WS" && "$BIN" doctor --lint --severity-min "$sev" ) >/dev/null 2>&1
  echo "  --severity-min $sev -> exit $?"
done

if [ -n "$GATE" ]; then
  echo
  echo "== ENFORCING --gate $GATE =="
  ( cd "$WS" && "$BIN" doctor --lint --severity-min "$GATE" ) >/dev/null 2>&1
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "doctor-local: findings at/above '$GATE' — see the report above." >&2
    exit 1
  fi
  echo "doctor-local: clean at threshold '$GATE'."
fi

exit "$ADVISORY_EXIT"
