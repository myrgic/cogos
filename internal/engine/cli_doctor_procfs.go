// cli_doctor_procfs.go: read-only /proc introspection shared by the
// cross-node doctor checks (kernel runtime PATH, stale inherited grants,
// supervision cgroups).
//
// Every function takes the proc root as a parameter (production: "/proc") so
// tests can build a fake tree in a temp dir. Nothing here is Linux-only at
// compile time; on a host without /proc every function simply finds nothing.
package engine

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// doctorHost bundles the host facts the cross-node checks consult, so tests
// can inject a fake /proc and a fake OS without build tags.
type doctorHost struct {
	procRoot string // "/proc" in production
	goos     string // runtime.GOOS in production
	home     string // user home directory
	uid      int    // current user id (-1 when unknown)
}

func defaultDoctorHost() doctorHost {
	home, _ := os.UserHomeDir()
	return doctorHost{procRoot: "/proc", goos: runtime.GOOS, home: home, uid: os.Getuid()}
}

// doctorHostOrDefault returns opts.host when a test set it, else the real host.
func (o DoctorOptions) doctorHostOrDefault() doctorHost {
	if o.host != nil {
		return *o.host
	}
	return defaultDoctorHost()
}

// procListenerPIDs returns the pids holding a LISTEN socket on port, found by
// matching the socket inodes in <proc>/net/tcp{,6} to <proc>/<pid>/fd links.
// Processes whose fd directory is unreadable (other users) are skipped.
func procListenerPIDs(procRoot string, port int) []int {
	inodes := map[string]bool{}
	for _, f := range []string{"tcp", "tcp6"} {
		data, err := os.ReadFile(filepath.Join(procRoot, "net", f))
		if err != nil {
			continue
		}
		for i, line := range strings.Split(string(data), "\n") {
			if i == 0 {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" { // 0A = TCP_LISTEN
				continue
			}
			idx := strings.LastIndex(fields[1], ":")
			if idx < 0 {
				continue
			}
			p, err := strconv.ParseInt(fields[1][idx+1:], 16, 32)
			if err != nil || int(p) != port {
				continue
			}
			inodes[fields[9]] = true
		}
	}
	if len(inodes) == 0 {
		return nil
	}
	var pids []int
	for _, pid := range procPIDs(procRoot) {
		entries, err := os.ReadDir(filepath.Join(procRoot, strconv.Itoa(pid), "fd"))
		if err != nil {
			continue
		}
		for _, e := range entries {
			link, err := os.Readlink(filepath.Join(procRoot, strconv.Itoa(pid), "fd", e.Name()))
			if err != nil || !strings.HasPrefix(link, "socket:[") {
				continue
			}
			if inodes[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids
}

// procPIDs lists numeric directory names under procRoot, ascending.
func procPIDs(procRoot string) []int {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil
	}
	var out []int
	for _, e := range entries {
		if n, err := strconv.Atoi(e.Name()); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

// procEnviron parses <proc>/<pid>/environ (NUL-separated KEY=VALUE).
func procEnviron(procRoot string, pid int) (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "environ"))
	if err != nil {
		return nil, err
	}
	env := map[string]string{}
	for _, kv := range strings.Split(string(data), "\x00") {
		if i := strings.Index(kv, "="); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}
	return env, nil
}

// procComm returns the short command name of pid ("" when unreadable).
func procComm(procRoot string, pid int) string {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "comm"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// procCmdline returns pid's argv joined by spaces ("" when unreadable).
func procCmdline(procRoot string, pid int) string {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(string(b), "\x00", " "))
}

// procUID returns the real uid of pid from <proc>/<pid>/status ("Uid:" line).
func procUID(procRoot string, pid int) (int, bool) {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Uid:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				if n, err := strconv.Atoi(f[1]); err == nil {
					return n, true
				}
			}
		}
	}
	return 0, false
}

// procCgroupUnit returns the innermost systemd *.service unit in pid's
// cgroup path ("" for scopes, user@N.service managers, or no systemd cgroup).
func procCgroupUnit(procRoot string, pid int) string {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] != "0" && parts[1] != "name=systemd" {
			continue
		}
		comps := strings.Split(parts[2], "/")
		for i := len(comps) - 1; i >= 0; i-- {
			c := comps[i]
			if strings.HasSuffix(c, ".service") && !strings.HasPrefix(c, "user@") {
				return c
			}
		}
	}
	return ""
}

// grantFingerprint is the only form a grant/key value may take in doctor
// output: the first 10 hex chars of its sha256.
func grantFingerprint(token string) string {
	h := sha256Hex(token)
	if len(h) > 10 {
		return h[:10]
	}
	return h
}
