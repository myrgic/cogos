package engine

import (
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// doctorSyncthingConflict reports a standalone Syncthing process on this node.
//
// The kernel speaks BEP natively (pkg/substrate/bep; bep_engine.go) and
// derives its node identity from its own BEP device cert. A standalone
// Syncthing is a second BEP speaker with a second device identity, usually
// contending for the same :22000 listen port and, when it was installed
// before the kernel's BEP landed, an operator-level source of confusion
// ("BEP != Syncthing", Chaz 2026-10-03). Found live on both constellation
// nodes that day: darkstar (homebrew launchd service, zero folders) and
// eclipse (Windows service), both killed by hand. This check keeps them dead.
//
// Severity is WARN, never FAIL: Syncthing is not wrong in itself, and a node
// with BEP disabled may run it legitimately. The detail says what to do.
func doctorSyncthingConflict(g *DoctorGroup, bepEnabled bool, bepListen string) {
	pids := syncthingProcesses()
	if len(pids) == 0 {
		g.add("syncthing conflict", StatusOK, "no standalone syncthing process (kernel BEP is the only BEP speaker)")
		return
	}
	detail := fmt.Sprintf("standalone syncthing running (pid %s). The kernel speaks BEP itself; a second BEP speaker is a second device identity and may contend for %s.",
		strings.Join(pids, ","), orDefault(bepListen, ":22000"))
	if bepEnabled {
		detail += " Stop and disable it: macOS `brew services stop syncthing` (+ remove ~/Library/LaunchAgents/homebrew.mxcl.syncthing.plist); Windows `Stop-Service syncthing; Set-Service syncthing -StartupType Disabled`."
	} else {
		detail += " Kernel BEP is disabled on this node, so this may be intentional; if the node is meant to join the constellation, remove syncthing before enabling BEP."
	}
	// Second signal: who actually owns the listen port. If it isn't us, say so.
	if owner := listenerHint(orDefault(bepListen, ":22000")); owner != "" {
		detail += "\n" + owner
	}
	g.add("syncthing conflict", StatusWarn, detail)
}

func orDefault(s, d string) string {
	if strings.TrimSpace(s) == "" {
		return d
	}
	return s
}

// syncthingProcesses returns the pids of processes whose image name is
// syncthing, on the host's native process lister. Empty on any error.
// A package-level var so tests can inject a fixed list.
var syncthingProcesses = findSyncthingProcesses

func findSyncthingProcesses() []string {
	var out []byte
	var err error
	switch runtime.GOOS {
	case "windows":
		out, err = exec.Command("tasklist", "/FI", "IMAGENAME eq syncthing.exe", "/FO", "CSV", "/NH").Output()
		if err != nil {
			return nil
		}
		var pids []string
		for _, line := range strings.Split(string(out), "\n") {
			f := strings.Split(strings.TrimSpace(line), "\",\"")
			if len(f) >= 2 && strings.HasPrefix(strings.ToLower(f[0]), "\"syncthing") {
				pids = append(pids, strings.Trim(f[1], "\""))
			}
		}
		return pids
	default:
		out, err = exec.Command("pgrep", "-x", "syncthing").Output()
		if err != nil {
			return nil // pgrep exits 1 when nothing matches
		}
		return strings.Fields(string(out))
	}
}

// listenerHint probes the BEP listen port and reports whether something is
// bound. It cannot name the owner portably, so it only adds a line when the
// port is open and the kernel's own engine is not the one we're running in
// (the doctor runs out-of-process from the daemon).
func listenerHint(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 400*time.Millisecond)
	if err != nil {
		return fmt.Sprintf("%s is not accepting connections; if the kernel's BEP is enabled it should be listening here.", addr)
	}
	_ = c.Close()
	return fmt.Sprintf("%s is bound; confirm the owner is cogos (`lsof -nP -iTCP:%s -sTCP:LISTEN` / `Get-NetTCPConnection -LocalPort %s`).", addr, port, port)
}
