// cli_doctor_kernelpath.go: discover the PATH the kernel SERVICE actually runs
// with, so doctor resolves service-relevant binaries (claude, codex, pi)
// against it instead of the caller's shell PATH.
//
// Observed 2026-11-08 on a Linux node: doctor (run from a login shell) said
// "claude not found" while the kernel found it, and the reverse is equally
// possible when the unit's PATH is narrower than the shell's. Discovery order:
//
//	Linux : /proc/<pid>/environ of the process listening on the kernel port,
//	        else Environment= of that pid's systemd unit file.
//	macOS : EnvironmentVariables.PATH of the kernel's launchd plist (label from
//	        the node manifest, default com.cogos.kernel); no PATH key means
//	        launchd's default /usr/bin:/bin:/usr/sbin:/sbin.
//
// When nothing is discoverable the result says so and callers fall back to the
// caller's PATH while stating that they did.
package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const launchdDefaultPATH = "/usr/bin:/bin:/usr/sbin:/sbin"

type kernelPathInfo struct {
	PATH   string // "" when not discovered
	Source string // human pointer to where PATH came from, or why it was not found
	Found  bool
}

// kernelPortForDoctor returns the kernel's TCP port as doctor would probe it.
func kernelPortForDoctor(root string, opts DoctorOptions) int {
	ep := opts.kernelEndpoint
	if ep == "" {
		ep = kernelEndpointForDoctor(root)
	}
	_, p := splitHostPort(ep)
	n, _ := strconv.Atoi(p)
	return n
}

func discoverKernelPATH(root string, opts DoctorOptions) kernelPathInfo {
	h := opts.doctorHostOrDefault()
	switch h.goos {
	case "linux":
		return discoverKernelPATHLinux(root, opts, h)
	case "darwin":
		return discoverKernelPATHDarwin(root, h)
	}
	return kernelPathInfo{Source: fmt.Sprintf("kernel PATH discovery not implemented for %s", h.goos)}
}

func discoverKernelPATHLinux(root string, opts DoctorOptions, h doctorHost) kernelPathInfo {
	port := kernelPortForDoctor(root, opts)
	if port == 0 {
		return kernelPathInfo{Source: "kernel port unknown"}
	}
	pids := procListenerPIDs(h.procRoot, port)
	if len(pids) == 0 {
		return kernelPathInfo{Source: fmt.Sprintf("no process visible listening on :%d (kernel not running, or its fds are unreadable by this user)", port)}
	}
	pid := pids[0]
	env, err := procEnviron(h.procRoot, pid)
	if err == nil {
		if p, ok := env["PATH"]; ok {
			return kernelPathInfo{PATH: p, Found: true, Source: fmt.Sprintf("/proc/%d/environ (kernel pid %d on :%d)", pid, pid, port)}
		}
		return kernelPathInfo{Source: fmt.Sprintf("/proc/%d/environ has no PATH", pid)}
	}
	// environ unreadable (different user): fall back to the unit file.
	unit := procCgroupUnit(h.procRoot, pid)
	if unit != "" {
		if p, file := systemdUnitPATH(h.home, unit); file != "" {
			return kernelPathInfo{PATH: p, Found: true, Source: fmt.Sprintf("Environment= in %s (unit %s)", file, unit)}
		}
	}
	return kernelPathInfo{Source: fmt.Sprintf("/proc/%d/environ unreadable (%v) and no PATH in unit file", pid, err)}
}

var systemdEnvPathRE = regexp.MustCompile(`(?:^|\s)"?PATH=([^"\s]+)"?`)

// systemdUnitPATH scans the usual unit directories for unit and returns the
// PATH from its Environment= lines (last assignment wins) plus the file path.
func systemdUnitPATH(home, unit string) (path, file string) {
	dirs := []string{
		filepath.Join(home, ".config", "systemd", "user"),
		"/etc/systemd/system", "/usr/lib/systemd/system", "/lib/systemd/system",
	}
	for _, d := range dirs {
		f := filepath.Join(d, unit)
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "Environment=") {
				continue
			}
			if m := systemdEnvPathRE.FindStringSubmatch(strings.TrimPrefix(line, "Environment=")); m != nil {
				path = m[1]
			}
		}
		if path != "" {
			return path, f
		}
	}
	return "", ""
}

var plistPathRE = regexp.MustCompile(`(?s)<key>EnvironmentVariables</key>\s*<dict>(.*?)</dict>`)
var plistPathValRE = regexp.MustCompile(`(?s)<key>PATH</key>\s*<string>(.*?)</string>`)

func discoverKernelPATHDarwin(root string, h doctorHost) kernelPathInfo {
	label := "com.cogos.kernel"
	if m, err := LoadManifest(DefaultManifestPath(root)); err == nil {
		if k, ok := m.Services["kernel"]; ok && k.Launchd != "" {
			label = k.Launchd
		}
	}
	for _, f := range []string{
		filepath.Join(h.home, "Library", "LaunchAgents", label+".plist"),
		filepath.Join("/Library", "LaunchDaemons", label+".plist"),
	} {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if strings.HasPrefix(string(data), "bplist") {
			if out, cerr := exec.Command("plutil", "-convert", "xml1", "-o", "-", f).Output(); cerr == nil {
				data = out
			}
		}
		if m := plistPathRE.FindSubmatch(data); m != nil {
			if pm := plistPathValRE.FindSubmatch(m[1]); pm != nil {
				return kernelPathInfo{PATH: strings.TrimSpace(string(pm[1])), Found: true, Source: fmt.Sprintf("EnvironmentVariables.PATH in %s", f)}
			}
		}
		return kernelPathInfo{PATH: launchdDefaultPATH, Found: true,
			Source: fmt.Sprintf("launchd default PATH (no EnvironmentVariables.PATH in %s)", f)}
	}
	return kernelPathInfo{Source: fmt.Sprintf("no launchd plist for label %s under ~/Library/LaunchAgents or /Library/LaunchDaemons", label)}
}

// lookPathIn resolves name against an explicit PATH string, returning every
// executable match in order.
func lookPathIn(name, pathEnv string) []string {
	var out []string
	for _, dir := range filepath.SplitList(pathEnv) {
		if dir == "" {
			continue
		}
		cand := filepath.Join(dir, name)
		info, err := os.Stat(cand)
		if err != nil || info.IsDir() || info.Mode()&0111 == 0 {
			continue
		}
		out = append(out, cand)
	}
	return out
}
