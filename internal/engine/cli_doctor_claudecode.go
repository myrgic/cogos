package engine

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// doctorClaudeCode reports whether the Claude Code CLI is installed AND
// reachable by the kernel service.
//
// Claude Code is a declared prerequisite of a node: the claude-oauth provider
// stamps requests with the installed `claude` version (detectClaudeCodeVersion)
// and falls back to claudeCodeVersionFallback when none is found on the
// KERNEL's PATH. Anthropic refuses newer models to stale client versions
// (observed 2026-11-08: "Claude Code 2.1.74 does not support this model;
// version 2.1.280 or newer is required"), so a binary the kernel cannot see
// silently narrows the usable model set.
//
// The kernel is a service with its own PATH, so the binary is resolved against
// that PATH when discoverable (discoverKernelPATH) and the report says which
// PATH was used. WARN, not FAIL: a node can run without it.
func doctorClaudeCode(g *DoctorGroup, root string, opts DoctorOptions) {
	const name = "claude code"
	kp := discoverKernelPATH(root, opts)
	callerPATH := os.Getenv("PATH")
	find := func(pathEnv string) (string, bool) {
		for _, bin := range []string{"claude", "claude-code"} { // detectClaudeCodeVersion's order
			if m := lookPathIn(bin, pathEnv); len(m) > 0 {
				return m[0], true
			}
		}
		return "", false
	}
	callerHit, callerOK := find(callerPATH)

	if !kp.Found {
		if !callerOK {
			g.add(name, StatusWarn, fmt.Sprintf(
				"`claude` not found on this shell's PATH, and the kernel's own PATH is not discoverable (%s); claude-oauth falls back to client version %s and newer models are refused. "+
					"Fix: curl -fsSL https://claude.ai/install.sh | bash, then put ~/.local/bin on the kernel service's PATH. "+
					"Detail: scripts/lib/ensure-claude-code.sh, provider_claudeoauth.go",
				kp.Source, claudeCodeVersionFallback))
			return
		}
		g.add(name, StatusUnknown, fmt.Sprintf(
			"%s on this shell's PATH, but the kernel's own PATH is not discoverable (%s), so whether the kernel finds it is unverified. "+
				"Run doctor as the kernel's user while the kernel is up, or check the unit/plist PATH contains %s",
			callerHit, kp.Source, filepath.Dir(callerHit)))
		return
	}

	kernelHit, kernelOK := find(kp.PATH)
	if !kernelOK {
		extra := ""
		if callerOK {
			extra = fmt.Sprintf(" This shell finds it at %s, so the shell is NOT representative of the service.", callerHit)
		}
		fixDir := "~/.local/bin"
		if callerOK {
			fixDir = filepath.Dir(callerHit)
		}
		g.add(name, StatusWarn, fmt.Sprintf(
			"the kernel cannot find `claude` on its PATH (from %s); claude-oauth will use claudeCodeVersionFallback %s and newer models are refused.%s "+
				"Fix: add %s to PATH in the kernel's unit Environment= / launchd EnvironmentVariables, then restart the kernel. Detail: provider_claudeoauth.go detectClaudeCodeVersion",
			kp.Source, claudeCodeVersionFallback, extra, fixDir))
		return
	}
	vctx, vcancel := context.WithTimeout(context.Background(), cliHelpTimeout)
	defer vcancel()
	out, err := exec.CommandContext(vctx, kernelHit, "--version").Output()
	v, ok := parseClaudeCodeVersion(string(out))
	if err != nil || !ok {
		g.add(name, StatusWarn, fmt.Sprintf(
			"%s is on the kernel's PATH (from %s) but `--version` gave no usable version; claude-oauth will use claudeCodeVersionFallback %s",
			kernelHit, kp.Source, claudeCodeVersionFallback))
		return
	}
	detail := fmt.Sprintf("%s (%s); resolved against the kernel's PATH from %s", v, kernelHit, kp.Source)
	if v == claudeCodeVersionFallback {
		g.add(name, StatusWarn, detail+fmt.Sprintf("; this equals claudeCodeVersionFallback %s, the version Anthropic refuses newer models for. Fix: claude update, then restart the kernel", claudeCodeVersionFallback))
		return
	}
	if !kp.Start.IsZero() {
		if bt := claudeBinaryModTime(kernelHit); bt.After(kp.Start) {
			g.add(name, StatusWarn, detail+fmt.Sprintf(
				"; the binary changed at %s, after the kernel process started at %s. The kernel detects the client version once, at its first claude-oauth request, and keeps it until restart; if that request came before the change it is still sending the earlier version (claudeCodeVersionFallback %s if claude was missing then). "+
					"Fix: restart the kernel if a model is refused for its Claude Code version (doctor --live probes this). Detail: provider_claudeoauth.go getClaudeCodeVersion (sync.Once)",
				bt.Format(time.RFC3339), kp.Start.Format(time.RFC3339), claudeCodeVersionFallback))
			return
		}
	}
	if strings.TrimSpace(callerHit) != "" && callerHit != kernelHit {
		detail += fmt.Sprintf("; note this shell resolves a different binary (%s)", callerHit)
	}
	g.add(name, StatusOK, detail)
}

// claudeBinaryModTime is the newest mtime among the path itself and the file a
// symlink resolves to: `claude update` repoints the link and writes a new
// versioned target.
func claudeBinaryModTime(path string) time.Time {
	var t time.Time
	if st, err := os.Lstat(path); err == nil {
		t = st.ModTime()
	}
	if st, err := os.Stat(path); err == nil && st.ModTime().After(t) {
		t = st.ModTime()
	}
	return t
}
