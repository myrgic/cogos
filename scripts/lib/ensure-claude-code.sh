#!/usr/bin/env bash
# ensure-claude-code.sh — shared installer step: Claude Code is a declared
# prerequisite of a CogOS node.
#
# Why: the kernel's claude-oauth provider reads the installed Claude Code
# version (internal/engine/provider_claudeoauth.go, detectClaudeCodeVersion)
# and falls back to a pinned constant when no `claude` binary is on PATH.
# Anthropic refuses newer models to stale client versions ("Claude Code X does
# not support this model; version Y or newer is required"), so a node without
# `claude` silently loses those models. `cog doctor` reports the same gap.
#
# Sourced (not executed) by scripts/setup.sh and scripts/setup-dev.sh. The
# caller supplies info/ok/warn helpers. Idempotent: skips when a working
# `claude` already exists. Uses Anthropic's official native installer
# (https://claude.ai/install.sh -> ~/.local/bin/claude). Windows: run
# `irm https://claude.ai/install.ps1 | iex` in PowerShell.
#
# Failure is a warning, not an abort: a node can run without Claude Code, it
# just cannot use the claude-oauth provider's newer models.
#
# no-pipefail: sourced library — 'set -e'/'pipefail' here would mutate the
# calling shell's option state; the callers already set their own.

ensure_claude_code() {
    local v
    if command -v claude >/dev/null 2>&1 && v="$(claude --version 2>/dev/null)" && [ -n "$v" ]; then
        ok "Claude Code already installed ($v) -- skipping"
        return 0
    fi
    if ! command -v curl >/dev/null 2>&1; then
        warn "curl not found; install Claude Code manually: https://claude.ai/install.sh"
        return 0
    fi
    info "Installing Claude Code (official native installer)..."
    if curl -fsSL https://claude.ai/install.sh | bash; then
        # Native installer places the binary in ~/.local/bin.
        if ! command -v claude >/dev/null 2>&1 && [ -x "$HOME/.local/bin/claude" ]; then
            export PATH="$HOME/.local/bin:$PATH"
            warn "Add \$HOME/.local/bin to the PATH of the shell/service that runs cogos"
        fi
        if command -v claude >/dev/null 2>&1; then
            ok "Claude Code installed ($(claude --version 2>/dev/null || echo unknown))"
        else
            warn "Installer ran but 'claude' is not on PATH; see 'cog doctor'"
        fi
    else
        warn "Claude Code install failed; run 'curl -fsSL https://claude.ai/install.sh | bash' manually"
        warn "Without it the claude-oauth provider cannot use newer models ('cog doctor' will flag this)"
    fi
    return 0
}
