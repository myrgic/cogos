//go:build !windows

package engine

import (
	"path/filepath"
	"strings"
	"testing"
)

const supervisionManifest = `apiVersion: cog.os/v1
kind: NodeManifest
services:
  kernel: {port: 6931, command: k, health: /health, restart: always}
  stage: {port: 8080, command: s, health: /health, restart: always}
  mod3: {port: 7860, command: m, health: /health, restart: always}
`

func TestSupervision_StageInsideKernelUnitWarns(t *testing.T) {
	root := t.TempDir()
	writeFileMode(t, filepath.Join(root, ".cog", "config", "node", "manifest.yaml"), supervisionManifest, 0o644)
	fp := newFakeProc(t)
	fp.add(100, 1000, "cogos", nil, "/system.slice/cogos-kernel.service", 6931)
	fp.add(101, 1000, "python", nil, "/system.slice/cogos-kernel.service", 8080)
	fp.add(102, 1000, "mod3", nil, "/system.slice/cogos-mod3.service", 7860)
	g := &DoctorGroup{Name: "t"}
	doctorServiceSupervision(g, root, DoctorOptions{host: fp.host(t.TempDir())})
	c := groupCheck(g, "service supervision: cogos-kernel.service")
	if c == nil || c.Status != StatusWarn {
		t.Fatalf("got %s", allDetail(g))
	}
	for _, want := range []string{"kernel (pid 100)", "stage (pid 101)", "its own unit"} {
		if !strings.Contains(c.Detail, want) {
			t.Errorf("missing %q: %s", want, c.Detail)
		}
	}
	if strings.Contains(c.Detail, "mod3") {
		t.Errorf("mod3 has its own unit: %s", c.Detail)
	}
}

func TestSupervision_SeparateUnitsOKAndNonLinuxUnknown(t *testing.T) {
	root := t.TempDir()
	writeFileMode(t, filepath.Join(root, ".cog", "config", "node", "manifest.yaml"), supervisionManifest, 0o644)
	fp := newFakeProc(t)
	fp.add(100, 1000, "cogos", nil, "/system.slice/cogos-kernel.service", 6931)
	fp.add(101, 1000, "python", nil, "/user.slice/user-1000.slice/user@1000.service/app.slice/cogos-stage.service", 8080)
	g := &DoctorGroup{Name: "t"}
	doctorServiceSupervision(g, root, DoctorOptions{host: fp.host(t.TempDir())})
	if g.Checks[0].Status != StatusOK {
		t.Fatalf("got %s", allDetail(g))
	}
	g = &DoctorGroup{Name: "t"}
	doctorServiceSupervision(g, root, DoctorOptions{host: &doctorHost{procRoot: fp.root, goos: "darwin"}})
	if g.Checks[0].Status != StatusUnknown {
		t.Fatalf("got %s", allDetail(g))
	}
}
