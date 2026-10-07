package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/bep"
)

// The process probe is host-dependent, so the unit tests exercise the
// decision logic through a seam: they call the reporting path with a fixed
// pid list via the package-level hook below.

func TestDoctorSyncthingConflict_NoneIsOK(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() ([]string, error) { return nil, nil }
	defer func() { syncthingProcesses = restore }()

	g := &DoctorGroup{Name: "t"}
	doctorSyncthingConflict(g, true, ":22000")
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusOK {
		t.Fatalf("want one OK check, got %+v", g.Checks)
	}
}

func TestDoctorSyncthingConflict_RunningWithBEPIsWarnWithRemedy(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() ([]string, error) { return []string{"1112"}, nil }
	defer func() { syncthingProcesses = restore }()

	g := &DoctorGroup{Name: "t"}
	doctorSyncthingConflict(g, true, ":22000")
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusWarn {
		t.Fatalf("want one WARN check, got %+v", g.Checks)
	}
	d := g.Checks[0].Detail
	for _, want := range []string{"pid 1112", ":22000", "brew services stop syncthing", "Set-Service syncthing"} {
		if !strings.Contains(d, want) {
			t.Errorf("detail missing %q:\n%s", want, d)
		}
	}
}

func TestDoctorSyncthingConflict_RunningWithoutBEPIsWarnMaybeIntentional(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() ([]string, error) { return []string{"7"}, nil }
	defer func() { syncthingProcesses = restore }()

	g := &DoctorGroup{Name: "t"}
	doctorSyncthingConflict(g, false, "")
	if g.Checks[0].Status != StatusWarn {
		t.Fatalf("want WARN, got %s", g.Checks[0].Status)
	}
	if !strings.Contains(g.Checks[0].Detail, "may be intentional") {
		t.Errorf("want the BEP-disabled caveat, got:\n%s", g.Checks[0].Detail)
	}
	// Empty listen addr (cluster.yaml unreadable or BEP off) falls back to the
	// kernel's own BEP default, derived from the constant (6932), not
	// Syncthing's 22000. A sentinel-free check: it must name the constant's
	// value and must not name 22000 as the kernel's address.
	want := fmt.Sprintf(":%d", bep.DefaultListenPort)
	if !strings.Contains(g.Checks[0].Detail, want) {
		t.Errorf("empty listen addr should default to %s, got:\n%s", want, g.Checks[0].Detail)
	}
	if strings.Contains(g.Checks[0].Detail, ":22000") {
		t.Errorf("fallback must not name Syncthing's :22000 as the kernel's BEP address:\n%s", g.Checks[0].Detail)
	}
}

// Review #663 round 2: a process lister that could not run (no pgrep, no
// tasklist, denied) observed nothing; that must be UNKNOWN, never OK.
func TestDoctorSyncthingConflict_ListerFailureIsUnknownNotOK(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() ([]string, error) { return nil, errors.New("pgrep: executable file not found in $PATH") }
	defer func() { syncthingProcesses = restore }()

	g := &DoctorGroup{Name: "t"}
	doctorSyncthingConflict(g, true, "")
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusUnknown {
		t.Fatalf("lister failure must be UNKNOWN, got %+v", g.Checks)
	}
	if !strings.Contains(g.Checks[0].Detail, "pgrep") {
		t.Errorf("detail should carry the cause: %q", g.Checks[0].Detail)
	}
}

// The real Unix lister must tell "pgrep ran and matched nothing" (exit 1: OK)
// from "pgrep could not run" (error): same class as the injected-failure test,
// through the real function with a PATH that has no pgrep.
func TestFindSyncthingProcesses_MissingListerIsAnError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exercises the pgrep branch")
	}
	t.Setenv("PATH", t.TempDir()) // no pgrep here
	if _, err := findSyncthingProcesses(); err == nil {
		t.Fatal("a missing pgrep must be an error, not an empty (clean) result")
	}
}

// Review #663 round 2: a corrupt cluster.yaml must surface as UNKNOWN, not be
// silently read as "BEP disabled" (which would also tell the operator that a
// running syncthing "may be intentional").
func TestDoctorBEPAndSyncthing_CorruptClusterConfigIsUnknown(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() ([]string, error) { return []string{"42"}, nil }
	defer func() { syncthingProcesses = restore }()

	root := t.TempDir()
	cfgDir := filepath.Join(root, ".cog", "config")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "cluster.yaml"), []byte("enabled: [unterminated\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	g := &DoctorGroup{Name: "t"}
	doctorBEPAndSyncthing(g, root)
	var cfgCheck *DoctorCheck
	for i := range g.Checks {
		if g.Checks[i].Name == "bep cluster config" {
			cfgCheck = &g.Checks[i]
		}
	}
	if cfgCheck == nil || cfgCheck.Status != StatusUnknown {
		t.Fatalf("corrupt cluster.yaml must add an UNKNOWN 'bep cluster config' check, got %+v", g.Checks)
	}

	// and a healthy / absent config must not add one
	g2 := &DoctorGroup{Name: "t"}
	doctorBEPAndSyncthing(g2, t.TempDir())
	for _, c := range g2.Checks {
		if c.Name == "bep cluster config" {
			t.Fatalf("absent cluster.yaml (BEP simply off) must not be UNKNOWN: %+v", c)
		}
	}
}
