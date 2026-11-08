package engine

import (
	"fmt"
	"os/exec"
)

// doctorClaudeCode reports whether the Claude Code CLI is installed.
//
// Claude Code is a declared prerequisite of a node: the claude-oauth provider
// stamps requests with the installed `claude` version (detectClaudeCodeVersion)
// and falls back to claudeCodeVersionFallback when none is found. Anthropic
// refuses newer models to stale client versions (observed 2026-11-08: "Claude
// Code 2.1.74 does not support this model; version 2.1.280 or newer is
// required"), so a missing binary silently narrows the usable model set.
//
// WARN, not FAIL: a node can run without it (other providers still work).
// Detection reuses detectClaudeCodeVersion; LookPath only disambiguates
// "missing" from "present but reported the fallback value".
func doctorClaudeCode(g *DoctorGroup) {
	const name = "claude code"
	path, err := exec.LookPath("claude")
	if err != nil {
		g.add(name, StatusWarn, fmt.Sprintf(
			"`claude` not on PATH; claude-oauth falls back to client version %s and newer models are refused. "+
				"Fix: curl -fsSL https://claude.ai/install.sh | bash (ensure ~/.local/bin is on the kernel's PATH). "+
				"Detail: scripts/lib/ensure-claude-code.sh, provider_claudeoauth.go",
			claudeCodeVersionFallback))
		return
	}
	v := detectClaudeCodeVersion()
	g.add(name, StatusOK, fmt.Sprintf("%s (%s)", v, path))
}
