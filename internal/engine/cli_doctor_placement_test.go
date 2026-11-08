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
	c := filepath.Join(root, ".cog")
	writeFileMode(t, filepath.Join(c, "config", "placement.yaml"), "always_on: [eclipse]\n", 0o644)
	writeFileMode(t, filepath.Join(c, "conf", "registry", "machines", "darkstar.yaml"), "hostname: Mac.localdomain\naddress: 192.168.10.140\n", 0o644)
	writeFileMode(t, filepath.Join(c, "conf", "registry", "machines", "eclipse.yaml"), "hostname: eclipse\naddress: 192.168.10.191\n", 0o644)
	writeFileMode(t, filepath.Join(c, "config", "node", "nodes", "darkstar.node.yaml"), "metadata:\n  id: darkstar\nbinds:\n  machine: darkstar\n", 0o644)
	writeFileMode(t, filepath.Join(c, "config", "node", "nodes", "eclipse.node.yaml"), "metadata:\n  id: eclipse\nbinds:\n  machine: eclipse\n", 0o644)
	return root, home
}

func TestPlacement_FlagsLaptopDependencies(t *testing.T) {
	root, home := placementFixture(t)
	writeFileMode(t, filepath.Join(root, ".cog", "config", "providers.yaml"), `providers:
  lmstudio-darkstar:
    type: lmstudio
    enabled: true
    endpoint: http://192.168.10.140:1234
    model: m
  lmstudio-eclipse:
    type: lmstudio
    enabled: true
    endpoint: http://192.168.10.191:1234
    model: m
  local-only:
    type: lmstudio
    enabled: true
    endpoint: http://127.0.0.1:1234
    model: m
`, 0o644)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "lanes", "runs", "stage", "stage.env"),
		"STAGE_TTS=192.168.10.140:7860\nSTAGE_UPSTREAM=darkstar.lan:7871\nSTAGE_KERNEL=http://192.168.10.191:6931\nSTAGE_VOICE=eng_uk_m_davids\nSTAGE_FORUM=1556038535495290950\n", 0o644)
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "config.yaml"),
		"providers:\n  remote:\n    base_url: http://Mac.localdomain:8080/v1\n", 0o644)

	g := &DoctorGroup{Name: "t"}
	doctorPlacementDependencies(g, root, DoctorOptions{host: newFakeProc(t).host(home)})
	d := allDetail(g)
	for _, want := range []string{"providers.lmstudio-darkstar.endpoint", "STAGE_TTS", "STAGE_UPSTREAM", "providers.remote.base_url", `node "darkstar"`, "always_on"} {
		if !strings.Contains(d, want) {
			t.Errorf("missing %q in:\n%s", want, d)
		}
	}
	for _, bad := range []string{"lmstudio-eclipse", "local-only", "STAGE_KERNEL", "STAGE_VOICE", "STAGE_FORUM"} {
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
	writeFileMode(t, filepath.Join(home, ".hermes", "profiles", "cog", "lanes", "runs", "stage", "stage.env"), "STAGE_TTS=192.168.10.191:7860\n", 0o644)
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
