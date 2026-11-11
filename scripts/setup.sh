#!/usr/bin/env bash
# setup.sh — End-user install for CogOS
#
# Downloads the latest release binary and installs cogos + cog.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/myrgic/cogos/main/scripts/setup.sh | sh
#
# Steps: detect platform, download release, install cogos + cog, set PATH,
# install Claude Code if missing (official native installer), verify.
#
# Or clone and run locally:
#   ./scripts/setup.sh

set -euo pipefail

REPO="myrgic/cogos"
INSTALL_DIR="$HOME/.cogos/bin"

# ── Colors ────────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${BOLD}$*${NC}"; }
ok()    { echo -e "  ${GREEN}✓${NC} $*"; }
warn()  { echo -e "  ${YELLOW}!${NC} $*"; }
fail()  { echo -e "  ${RED}✗${NC} $*"; exit 1; }

# ── Detect platform ──────────────────────────────────────────────────────────

detect_platform() {
    local os arch

    case "$(uname -s)" in
        Darwin) os="darwin" ;;
        Linux)  os="linux" ;;
        *)      fail "Unsupported OS: $(uname -s)" ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64) arch="amd64" ;;
        arm64|aarch64) arch="arm64" ;;
        *)             fail "Unsupported architecture: $(uname -m)" ;;
    esac

    echo "${os}-${arch}"
}

# ── Find latest release ──────────────────────────────────────────────────────

get_latest_version() {
    if command -v gh &>/dev/null; then
        gh api "repos/$REPO/releases/latest" --jq '.tag_name' 2>/dev/null && return
    fi

    curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null \
        | grep '"tag_name"' \
        | head -1 \
        | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/'
}

# ── Main ──────────────────────────────────────────────────────────────────────

info "CogOS Installer"
echo ""

PLATFORM=$(detect_platform)
ok "Platform: $PLATFORM"

info "Finding latest release..."
VERSION=$(get_latest_version)

if [ -z "$VERSION" ]; then
    fail "Could not determine latest version. Check https://github.com/$REPO/releases"
fi

ok "Version: $VERSION"
echo ""

# Download binary.
BINARY_NAME="cogos-${PLATFORM}"
DOWNLOAD_URL="https://github.com/$REPO/releases/download/$VERSION/$BINARY_NAME"

info "Downloading $BINARY_NAME..."
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# ── Load the shared running-daemon guard ─────────────────────────────────────
# This script supports two invocations (see the header): a local checkout
# (`./scripts/setup.sh`) and `curl -fsSL .../setup.sh | sh`, which has no
# local checkout to source a sibling file from. Prefer the local copy when
# one exists; otherwise fetch the SAME guard from the release ref this
# script is itself installing, so the check travels with the version being
# installed. See scripts/lib/refuse-if-running.sh for the guard itself and
# why it is sourced here rather than hand-copied (again -- this script used
# to carry its own inline copy; see RETRO-486.md for why that kept
# diverging from its siblings).
#
# $0 rather than BASH_SOURCE: this script is documented to run under `sh`
# via a pipe, where BASH_SOURCE is unset/unsupported; checking whether $0
# resolves to a real file on disk works under both sh and bash and
# correctly reports "no local checkout" when piped (in that mode $0 is not
# a path to this file).
SCRIPT_DIR=""
if [ -f "$0" ] && [ "$(basename "$0")" = "setup.sh" ]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

if [ -n "$SCRIPT_DIR" ] && [ -f "$SCRIPT_DIR/lib/refuse-if-running.sh" ]; then
    # shellcheck source=lib/refuse-if-running.sh
    . "$SCRIPT_DIR/lib/refuse-if-running.sh"
elif curl -fsSL -o "$TMPDIR/refuse-if-running.sh" \
        "https://raw.githubusercontent.com/$REPO/$VERSION/scripts/lib/refuse-if-running.sh" 2>/dev/null; then
    # shellcheck source=lib/refuse-if-running.sh
    . "$TMPDIR/refuse-if-running.sh"
else
    # Could not obtain the guard by either path. Per the guard's own
    # fail-closed contract, "could not verify" is not "safe" -- refuse
    # rather than install blind unless the caller explicitly overrides.
    refuse_if_running() {
        if [ "${ALLOW_RUNNING_INSTALL:-}" = "1" ]; then
            return 0
        fi
        warn "Could not load the running-daemon safety check (network issue,"
        warn "or $VERSION predates it). Refusing rather than installing blind."
        warn "Re-run with ALLOW_RUNNING_INSTALL=1 to override, only if you"
        warn "know nothing is running."
        return 1
    }
fi

if ! curl -fsSL -o "$TMPDIR/cogos" "$DOWNLOAD_URL"; then
    fail "Download failed. Check that $VERSION has a release for $PLATFORM"
fi
chmod +x "$TMPDIR/cogos"
ok "Downloaded"

# Download cog CLI wrapper.
COG_URL="https://raw.githubusercontent.com/$REPO/$VERSION/scripts/cog"
if curl -fsSL -o "$TMPDIR/cog" "$COG_URL" 2>/dev/null; then
    chmod +x "$TMPDIR/cog"
    ok "Downloaded cog CLI"
else
    warn "Could not download cog CLI wrapper (non-fatal)"
fi

echo ""

# Install.
info "Installing to $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"

# Refuse to clobber a running kernel before writing anything.
refuse_if_running "$INSTALL_DIR/cogos" || exit 1

mv "$TMPDIR/cogos" "$INSTALL_DIR/cogos"
ok "cogos → $INSTALL_DIR/cogos"

if [ -f "$TMPDIR/cog" ]; then
    mv "$TMPDIR/cog" "$INSTALL_DIR/cog"
    ok "cog   → $INSTALL_DIR/cog"
fi

echo ""

# PATH setup.
SHELL_NAME="$(basename "$SHELL")"

if echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    ok "$INSTALL_DIR is already in PATH"
else
    PROFILE=""
    case "$SHELL_NAME" in
        zsh)  PROFILE="$HOME/.zshrc" ;;
        bash)
            [ -f "$HOME/.bash_profile" ] && PROFILE="$HOME/.bash_profile" || PROFILE="$HOME/.bashrc"
            ;;
        *)    PROFILE="$HOME/.profile" ;;
    esac

    if [ -n "$PROFILE" ] && ! grep -qF '.cogos/bin' "$PROFILE" 2>/dev/null; then
        echo "" >> "$PROFILE"
        echo "# CogOS" >> "$PROFILE"
        echo 'export PATH="$HOME/.cogos/bin:$PATH"' >> "$PROFILE"
        ok "Added to $PROFILE"
        warn "Run 'source $PROFILE' or open a new terminal"
    fi

    export PATH="$INSTALL_DIR:$PATH"
fi

echo ""

# Claude Code (declared prerequisite): same shared step setup-dev.sh uses.
# Loaded from the local checkout, else from the release ref being installed.
if [ -n "$SCRIPT_DIR" ] && [ -f "$SCRIPT_DIR/lib/ensure-claude-code.sh" ]; then
    # shellcheck source=lib/ensure-claude-code.sh
    . "$SCRIPT_DIR/lib/ensure-claude-code.sh"
elif curl -fsSL -o "$TMPDIR/ensure-claude-code.sh" \
        "https://raw.githubusercontent.com/$REPO/$VERSION/scripts/lib/ensure-claude-code.sh" 2>/dev/null; then
    # shellcheck source=lib/ensure-claude-code.sh
    . "$TMPDIR/ensure-claude-code.sh"
else
    ensure_claude_code() {
        command -v claude >/dev/null 2>&1 && return 0
        warn "Claude Code not found; install: curl -fsSL https://claude.ai/install.sh | bash"
    }
fi
info "Ensuring Claude Code (claude binary)..."
ensure_claude_code

echo ""

# Verify.
info "Verifying..."
VERSION_OUT=$("$INSTALL_DIR/cogos" version 2>&1)
ok "$VERSION_OUT"

echo ""
info "Installation complete!"
echo ""
echo "  Quick start:"
echo ""
echo "    cogos init --workspace ~/my-project"
echo "    cogos serve --workspace ~/my-project"
echo "    curl http://localhost:6931/health"
echo ""
