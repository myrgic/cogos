package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/bep"
)

// The process probe is host-dependent, so the unit tests exercise the
// decision logic through a seam: they call the reporting path with a fixed
// pid list via the package-level hook below.

func TestDoctorSyncthingConflict_NoneIsOK(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() []string { return nil }
	defer func() { syncthingProcesses = restore }()

	g := &DoctorGroup{Name: "t"}
	doctorSyncthingConflict(g, true, ":22000")
	if len(g.Checks) != 1 || g.Checks[0].Status != StatusOK {
		t.Fatalf("want one OK check, got %+v", g.Checks)
	}
}

func TestDoctorSyncthingConflict_RunningWithBEPIsWarnWithRemedy(t *testing.T) {
	restore := syncthingProcesses
	syncthingProcesses = func() []string { return []string{"1112"} }
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
	syncthingProcesses = func() []string { return []string{"7"} }
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
