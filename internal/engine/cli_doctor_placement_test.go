//go:build !windows

package engine

import (
	"path/filepath"
	"strings"
	"testing"
)

func placementFixture(t *testing.T) (root, home string) {
	t.Helper()
	root, home = t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("STAGE_STATE", "")
	c := filepath.Join(root, ".cog")
	writeFileMode(t, filepath.Join(c, "config", "placement.yaml"), "always_on: [server]\n", 0o644)
	writeFileMode(t, filepath.Join(c, "conf", "registry", "machines", "laptop.yaml"), "hostname: Mac.localdomain\naddress: 192.0.2.10\n", 0o644)
	writeFileMode(t, filepath.Join(c, "conf", "registry", "machines", "server.yaml"), "hostname: server\naddress: 192.0.2.20\n", 0o644)
	writeFileMode(t, filepath.Join(c, "config", "node", "nodes", "laptop.node.yaml"), "metadata:\n  id: laptop\nbinds:\n  machine: laptop\n", 0o644)
	writeFileMode(t, filepath.Join(c, "config", "node", "nodes", "server.node.yaml"), "metadata:\n  id: server\nbinds:\n  machine: server\n", 0o644)
	return root, home
}

func TestPlacement_FlagsLaptopDependencies(t *testing.T) {
	root, home := placementFixture(t)
	writeFileMode(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-laptop:
    type: lmstudio
    enabled: true
    endpoint: http://192.0.2.10:1234
    model: m
  lmstudio-server:
    type: lmstudio
    enabled: true
    endpoint: http://192.0.2.20:1234
    model: m
  local-only:
    type: lmstudio
    enabled: true
    endpoint: http://127.0.0.1:1234
    model: m
`, 0o644)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "lanes", "runs", "stage", "stage.env"),
		"STAGE_TTS=192.0.2.10:7860\nSTAGE_UPSTREAM=laptop.lan:7871\nSTAGE_KERNEL=http://192.0.2.20:6931\nSTAGE_VOICE=eng_uk_m_davids\nSTAGE_FORUM=1556038535495290950\n", 0o644)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml"),
		"providers:\n  remote:\n    base_url: http://Mac.localdomain:8080/v1\n", 0o644)

	g := &DoctorGroup{Name: "t"}
	doctorPlacementDependencies(g, root, DoctorOptions{host: newFakeProc(t).host(home)})
	d := allDetail(g)
	for _, want := range []string{"providers.lmstudio-laptop.endpoint", "STAGE_TTS", "STAGE_UPSTREAM", "providers.remote.base_url", `node "laptop"`, "always_on"} {
		if !strings.Contains(d, want) {
			t.Errorf("missing %q in:\n%s", want, d)
		}
	}
	for _, bad := range []string{"lmstudio-server", "local-only", "STAGE_KERNEL", "STAGE_VOICE", "STAGE_FORUM"} {
		if strings.Contains(d, bad) {
			t.Errorf("false positive %q in:\n%s", bad, d)
		}
	}
	for _, c := range g.Checks {
		if c.Status != StatusWarn {
			t.Errorf("want only WARNs, got %s %s", c.Status, c.Name)
		}
	}
}

func TestPlacement_CleanAndUnknown(t *testing.T) {
	root, home := placementFixture(t)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "lanes", "runs", "stage", "stage.env"), "STAGE_TTS=192.0.2.20:7860\n", 0o644)
	g := &DoctorGroup{Name: "t"}
	doctorPlacementDependencies(g, root, DoctorOptions{host: newFakeProc(t).host(home)})
	if c := g.Checks[0]; c.Status != StatusOK || !strings.Contains(c.Detail, "1 non-loopback") {
		t.Fatalf("got %s", allDetail(g))
	}
	g = &DoctorGroup{Name: "t"}
	doctorPlacementDependencies(g, t.TempDir(), DoctorOptions{host: newFakeProc(t).host(home)})
	if g.Checks[0].Status != StatusUnknown {
		t.Fatalf("got %s", allDetail(g))
	}
}

func TestPlacement_FlagsHermesModelBaseURL(t *testing.T) {
	root, home := placementFixture(t)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml"),
		"model:\n  default: m\n  provider: custom\n  base_url: http://192.0.2.10:1234/v1\n", 0o644)
	g := &DoctorGroup{Name: "t"}
	doctorPlacementDependencies(g, root, DoctorOptions{host: newFakeProc(t).host(home)})
	d := allDetail(g)
	if !strings.Contains(d, "model.base_url") || !strings.Contains(d, `node "laptop"`) {
		t.Fatalf("model.base_url not flagged:\n%s", d)
	}
}
