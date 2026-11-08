//go:build !windows

package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFakeClaude(t *testing.T, dir, version string) string {
	t.Helper()
	p := filepath.Join(dir, "claude")
	writeFileMode(t, p, "#!/bin/sh\necho '"+version+" (Claude Code)'\n", 0o755)
	return p
}

// kernel service PATH lacks claude, caller's PATH has it: the shell lied.
func TestDoctorClaudeCode_KernelPathMissesWhatShellHas(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	shellBin := t.TempDir()
	writeFakeClaude(t, shellBin, "9.8.7")
	t.Setenv("PATH", shellBin)

	fp := newFakeProc(t)
	fp.add(500, 1000, "cogos", map[string]string{"PATH": "/usr/bin:/bin"}, "/system.slice/cogos-kernel.service", 6931)
	opts := DoctorOptions{host: fp.host(home), kernelEndpoint: "http://127.0.0.1:6931"}

	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusWarn {
		t.Fatalf("want one WARN, got %s", allDetail(g))
	}
	d := g.Checks[0].Detail
	for _, want := range []string{"kernel cannot find", "/proc/500/environ", claudeCodeVersionFallback, "NOT representative", shellBin} {
		if !strings.Contains(d, want) {
			t.Errorf("detail missing %q:\n%s", want, d)
		}
	}
}

// kernel PATH has claude though the shell does not: must be OK, with the PATH named.
func TestDoctorClaudeCode_KernelPathFindsWhatShellLacks(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir())
	svcBin := t.TempDir()
	writeFakeClaude(t, svcBin, "9.8.7")

	fp := newFakeProc(t)
	fp.add(500, 1000, "cogos", map[string]string{"PATH": svcBin}, "/system.slice/cogos-kernel.service", 6931)
	opts := DoctorOptions{host: fp.host(home), kernelEndpoint: "http://127.0.0.1:6931"}

	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if g.Checks[0].Status != StatusOK || !strings.Contains(g.Checks[0].Detail, "9.8.7") || !strings.Contains(g.Checks[0].Detail, "/proc/500/environ") {
		t.Fatalf("want OK naming version and PATH source, got %s", allDetail(g))
	}
}

func TestDoctorClaudeCode_KernelPathUndiscoverable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	shellBin := t.TempDir()
	writeFakeClaude(t, shellBin, "9.8.7")
	t.Setenv("PATH", shellBin)
	fp := newFakeProc(t) // no listener
	opts := DoctorOptions{host: fp.host(home), kernelEndpoint: "http://127.0.0.1:6931"}

	g := &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if g.Checks[0].Status != StatusUnknown || !strings.Contains(g.Checks[0].Detail, "not discoverable") {
		t.Fatalf("want UNKNOWN, got %s", allDetail(g))
	}

	t.Setenv("PATH", t.TempDir())
	g = &DoctorGroup{Name: "t"}
	doctorClaudeCode(g, t.TempDir(), opts)
	if g.Checks[0].Status != StatusWarn || !strings.Contains(g.Checks[0].Detail, claudeCodeVersionFallback) {
		t.Fatalf("want WARN, got %s", allDetail(g))
	}
}

func TestDiscoverKernelPATH_SystemdUnitFallbackWhenEnvironUnreadable(t *testing.T) {
	home := t.TempDir()
	fp := newFakeProc(t)
	fp.add(500, 1000, "cogos", nil, "/system.slice/cogos-kernel.service", 6931)
	if err := os.Remove(filepath.Join(fp.root, "500", "environ")); err != nil {
		t.Fatal(err)
	}
	writeFileMode(t, filepath.Join(home, ".config", "systemd", "user", "cogos-kernel.service"),
		"[Service]\nEnvironment=\"HOME=/x\" \"PATH=/opt/a:/opt/b\"\n", 0o644)
	kp := discoverKernelPATH(t.TempDir(), DoctorOptions{host: fp.host(home), kernelEndpoint: "http://127.0.0.1:6931"})
	if !kp.Found || kp.PATH != "/opt/a:/opt/b" || !strings.Contains(kp.Source, "cogos-kernel.service") {
		t.Fatalf("got %+v", kp)
	}
}

func TestDiscoverKernelPATH_DarwinPlist(t *testing.T) {
	home := t.TempDir()
	ws := t.TempDir()
	writeFileMode(t, filepath.Join(home, "Library", "LaunchAgents", "com.cogos.kernel.plist"), `<?xml version="1.0"?>
<plist><dict><key>Label</key><string>com.cogos.kernel</string>
<key>EnvironmentVariables</key><dict><key>PATH</key><string>/opt/homebrew/bin:/usr/bin</string></dict></dict></plist>`, 0o644)
	h := &doctorHost{procRoot: t.TempDir(), goos: "darwin", home: home, uid: 501}
	kp := discoverKernelPATH(ws, DoctorOptions{host: h})
	if !kp.Found || kp.PATH != "/opt/homebrew/bin:/usr/bin" {
		t.Fatalf("got %+v", kp)
	}

	// No EnvironmentVariables: launchd's default PATH, said out loud.
	writeFileMode(t, filepath.Join(home, "Library", "LaunchAgents", "com.cogos.kernel.plist"),
		`<plist><dict><key>Label</key><string>com.cogos.kernel</string></dict></plist>`, 0o644)
	kp = discoverKernelPATH(ws, DoctorOptions{host: h})
	if !kp.Found || kp.PATH != launchdDefaultPATH || !strings.Contains(kp.Source, "launchd default") {
		t.Fatalf("got %+v", kp)
	}
}

func TestCheckArgvContractsOnPath_KernelPathMissingIsWarn(t *testing.T) {
	shellBin := t.TempDir()
	writeFileMode(t, filepath.Join(shellBin, "fakecli"), "#!/bin/sh\necho '--flag'\n", 0o755)
	t.Setenv("PATH", shellBin)
	t.Setenv("HOME", t.TempDir())
	g := &DoctorGroup{Name: "t"}
	checkArgvContractsOnPath(g, []cliArgvContract{{provider: "p", bin: "fakecli", flags: []string{"--flag"}}},
		kernelPathInfo{PATH: "/usr/bin", Found: true, Source: "/proc/9/environ"})
	if g.Checks[0].Status != StatusWarn || !strings.Contains(g.Checks[0].Detail, "not on the kernel's PATH") {
		t.Fatalf("got %s", allDetail(g))
	}

	g = &DoctorGroup{Name: "t"}
	checkArgvContractsOnPath(g, []cliArgvContract{{provider: "p", bin: "fakecli", flags: []string{"--flag"}}},
		kernelPathInfo{PATH: shellBin, Found: true, Source: "/proc/9/environ"})
	if g.Checks[0].Status != StatusOK || !strings.Contains(g.Checks[0].Detail, "resolved on the kernel's PATH") {
		t.Fatalf("got %s", allDetail(g))
	}
}
