#!/usr/bin/env bash
# setup-dev.sh — Developer setup for CogOS
#
# Run this after cloning the repo to set up your local dev environment:
#   git clone https://github.com/myrgic/cogos.git
#   cd cogos
#   ./scripts/setup-dev.sh
#
# What it does:
#   1. Checks prerequisites (Go, Docker/Colima, git)
#   2. Builds cogos from source
#   3. Installs cogos binary to ~/.cog/bin/cogos
#   4. Installs cog CLI wrapper to ~/.cog/bin/cog
#   5. Adds ~/.cog/bin to PATH (shell profile)
#   6. Verifies the install

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="$HOME/.cog/bin"
SHELL_NAME="$(basename "$SHELL")"

# Shared running-daemon guard -- see scripts/lib/refuse-if-running.sh for
# why this is sourced rather than hand-copied (again).
# shellcheck source=lib/refuse-if-running.sh
. "$REPO_DIR/scripts/lib/refuse-if-running.sh"

# ── Colors ────────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${BOLD}$*${NC}"; }
ok()    { echo -e "  ${GREEN}✓${NC} $*"; }
warn()  { echo -e "  ${YELLOW}!${NC} $*"; }
fail()  { echo -e "  ${RED}✗${NC} $*"; }

# ── Prerequisites ─────────────────────────────────────────────────────────────

info "Checking prerequisites..."

MISSING=0

if command -v go &>/dev/null; then
    GO_VERSION=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | head -1)
    ok "Go ($GO_VERSION)"
else
    fail "Go not found — install from https://go.dev/dl/"
    MISSING=1
fi

if command -v git &>/dev/null; then
    ok "git"
else
    fail "git not found"
    MISSING=1
fi

if command -v docker &>/dev/null; then
    ok "Docker"
elif command -v nerdctl &>/dev/null; then
    ok "nerdctl (Docker alternative)"
elif command -v colima &>/dev/null; then
    ok "Colima (container runtime)"
else
    warn "No container runtime found (Docker, nerdctl, or Colima)"
    warn "Container-based deployment and e2e tests won't work"
    warn "Install Docker Desktop or run: brew install colima && colima start"
fi

if [ "$MISSING" -gt 0 ]; then
    echo ""
    fail "Missing required tools. Install them and re-run this script."
    exit 1
fi

echo ""

# ── Build ─────────────────────────────────────────────────────────────────────

info "Building cogos from source..."

cd "$REPO_DIR"

# The dev- prefix is load-bearing, not cosmetic -- see the Makefile's VERSION
# comment for the full rationale. Short version: a bare `git describe` string
# is valid semver whose suffix parses as a prerelease, and prereleases sort
# BEFORE their base tag, so a bare describe string reads as OLDER than the
# release it descends from and self-update's GATE D/F would not treat it as
# a dev build. Prefixing with dev- makes normVersion() return "" so GATE D
# fires and self-update stays inert.
GIT_DESCRIBE=$(git describe --tags --always --dirty 2>/dev/null || true)
if [ -n "$GIT_DESCRIBE" ]; then
    VERSION="dev-${GIT_DESCRIBE}"
else
    VERSION="dev"
fi
BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS="-s -w -X github.com/myrgic/cogos/internal/engine.Version=${VERSION} -X github.com/myrgic/cogos/internal/engine.BuildTime=${BUILD_TIME}"

go build -tags fts5 -ldflags="$LDFLAGS" -o cogos ./cmd/cogos

ok "Built cogos ($VERSION)"
echo ""

# ── Install ───────────────────────────────────────────────────────────────────

info "Installing to $INSTALL_DIR..."

mkdir -p "$INSTALL_DIR"

# Refuse to clobber a running kernel before writing anything.
refuse_if_running "$INSTALL_DIR/cogos" || exit 1

# Install cogos binary.
cp cogos "$INSTALL_DIR/cogos"
chmod +x "$INSTALL_DIR/cogos"
ok "cogos → $INSTALL_DIR/cogos"

# Install cog CLI wrapper.
cp scripts/cog "$INSTALL_DIR/cog"
chmod +x "$INSTALL_DIR/cog"
ok "cog   → $INSTALL_DIR/cog"

# Clean up build artifact from repo dir.
rm -f cogos

echo ""

# ── PATH ──────────────────────────────────────────────────────────────────────

if echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    ok "$INSTALL_DIR is already in PATH"
else
    info "Adding $INSTALL_DIR to PATH..."

    PROFILE=""
    case "$SHELL_NAME" in
        zsh)  PROFILE="$HOME/.zshrc" ;;
        bash)
            if [ -f "$HOME/.bash_profile" ]; then
                PROFILE="$HOME/.bash_profile"
            else
                PROFILE="$HOME/.bashrc"
            fi
            ;;
        *)    PROFILE="$HOME/.profile" ;;
    esac

    PATH_LINE='export PATH="$HOME/.cog/bin:$PATH"'

    if [ -n "$PROFILE" ] && ! grep -qF '.cog/bin' "$PROFILE" 2>/dev/null; then
        echo "" >> "$PROFILE"
        echo "# CogOS" >> "$PROFILE"
        echo "$PATH_LINE" >> "$PROFILE"
        ok "Added to $PROFILE"
        warn "Run 'source $PROFILE' or open a new terminal for PATH to take effect"
    elif [ -n "$PROFILE" ]; then
        ok "Already in $PROFILE"
    fi

    # Also export for this session.
    export PATH="$INSTALL_DIR:$PATH"
fi

echo ""

# ── Verify ────────────────────────────────────────────────────────────────────

info "Verifying installation..."

if "$INSTALL_DIR/cogos" version &>/dev/null; then
    VERSION_OUT=$("$INSTALL_DIR/cogos" version 2>&1)
    ok "cogos: $VERSION_OUT"
else
    fail "cogos binary not working"
    exit 1
fi

if [ -x "$INSTALL_DIR/cog" ]; then
    ok "cog CLI installed"
else
    fail "cog CLI not found"
fi

echo ""

# ── Summary ───────────────────────────────────────────────────────────────────

info "Setup complete!"
echo ""
echo "  Next steps:"
echo ""
echo "    # Initialize a workspace"
echo "    cogos init --workspace ~/my-project"
echo ""
echo "    # Start the daemon"
echo "    cogos serve --workspace ~/my-project"
echo ""
echo "    # Or use the cog CLI"
echo "    cd ~/my-project"
echo "    cog health"
echo ""
echo "  Dev commands:"
echo ""
echo "    make build        # rebuild"
echo "    make test         # unit tests"
echo "    make e2e-local    # end-to-end test"
echo "    make e2e          # e2e in container"
echo ""
