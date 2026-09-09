package site

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGateArtifactBlocksBuildTimeLeak is the regression for the deploy-target
// exposure found 2026-09-01.
//
// The five myrgic.* repos are public Pages deploy targets, force-pushed
// wholesale by Deploy(). They were exempted from CI governance as
// "machine-managed" — but Deploy() runs `bash build.sh` on the OPERATOR's
// machine and publishes whatever it produces. A build script that embeds
// $HOME, `pwd`, or tool output injects machine-local data that never existed
// in the `sites` repo, so a gate on the source cannot see it, and CI on the
// target cannot stop it (Pages serves `main` the instant the push lands).
//
// This test proves the gate catches content that appears only in the built
// artifact.
func TestGateArtifactBlocksBuildTimeLeak(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", repoRoot)

	artifact := t.TempDir()
	// Simulate a build script that interpolated the operator's home directory
	// into generated output — the exact mod3 #146 shape, in a file no reviewer
	// reads as configuration.
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"),
		[]byte(`<!-- built from /Users/slowbro/workspaces/myrgic/sites -->`), 0o644))

	err := gateArtifact(context.Background(), artifact)
	if err == nil {
		t.Fatal("gate ALLOWED an artifact containing the operator home path; " +
			"a build-time leak would publish to a public Pages repo")
	}
	if !strings.Contains(err.Error(), "BLOCKED") {
		t.Fatalf("expected a BLOCKED verdict, got: %v", err)
	}

	// The staged policy file must not survive into the published tree.
	if _, statErr := os.Stat(filepath.Join(artifact, ".cogpublic")); statErr == nil {
		t.Error("gate left .cogpublic in the artifact; it would be published")
	}
}

// TestGateArtifactAllowsCleanArtifact is the positive control. A gate that has
// only ever blocked is as untrustworthy as one that has only ever passed — it
// could be refusing everything.
func TestGateArtifactAllowsCleanArtifact(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", repoRoot)

	artifact := t.TempDir()
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"),
		[]byte("<!doctype html><title>myrgic</title>"), 0o644))
	must(t, os.WriteFile(filepath.Join(artifact, "CNAME"),
		[]byte("myrgic.com"), 0o644))

	if err := gateArtifact(context.Background(), artifact); err != nil {
		t.Fatalf("gate BLOCKED a clean artifact: %v", err)
	}
}

// TestGateArtifactExplainsMissingPython3 covers the shipped-image regression
// this finding fixes: guard and policy are both present and readable, but the
// running process has no python3 on PATH (the pre-fix runtime image's exact
// shape). The interpreter never starts, so CombinedOutput's `out` is empty —
// folding that into the generic BLOCKED message would print an empty,
// misleading "content violation" instead of naming the real gap.
func TestGateArtifactExplainsMissingPython3(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", repoRoot)
	t.Setenv("PATH", "") // python3 cannot be found regardless of what's installed

	artifact := t.TempDir()
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"), []byte("clean"), 0o644))

	err := gateArtifact(context.Background(), artifact)
	if err == nil {
		t.Fatal("gate PASSED with no python3 reachable; it must fail closed")
	}
	if !strings.Contains(err.Error(), "release gate unavailable") ||
		!strings.Contains(err.Error(), "python3") {
		t.Fatalf("error must name python3 as the missing piece, got: %v", err)
	}
}

// TestGateArtifactFailsClosed: every inability to run must abort the deploy.
// A check that cannot run must never be mistaken for a check that passed —
// the whole lesson of the .cogpublic that declared guards nothing executed.
func TestGateArtifactFailsClosed(t *testing.T) {
	t.Setenv("COGOS_REPO_ROOT", t.TempDir()) // no guard, no policy

	artifact := t.TempDir()
	must(t, os.WriteFile(filepath.Join(artifact, "index.html"), []byte("clean"), 0o644))

	if err := gateArtifact(context.Background(), artifact); err == nil {
		t.Fatal("gate PASSED with no guard installed; it must fail closed")
	}
}

// TestRepoRootForGuardPrefersEnv proves COGOS_REPO_ROOT wins even when the
// working directory would resolve to something else via the walk-up.
func TestRepoRootForGuardPrefersEnv(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", repoRoot)

	got, err := repoRootForGuard()
	if err != nil {
		t.Fatalf("repoRootForGuard: %v", err)
	}
	if got != repoRoot {
		t.Fatalf("got %q, want %q", got, repoRoot)
	}
}

// TestRepoRootForGuardWalksUpWhenEnvUnset is the local-checkout case: no
// COGOS_REPO_ROOT set, but the working directory sits inside a tree with a
// .cogpublic above it.
func TestRepoRootForGuardWalksUpWhenEnvUnset(t *testing.T) {
	repoRoot := findRepoRoot(t)
	t.Setenv("COGOS_REPO_ROOT", "")
	t.Chdir(filepath.Join(repoRoot, "internal", "providers", "site"))

	got, err := repoRootForGuard()
	if err != nil {
		t.Fatalf("repoRootForGuard: %v", err)
	}
	if got != repoRoot {
		t.Fatalf("got %q, want %q", got, repoRoot)
	}
}

// TestRepoRootForGuardFallsBackToCompiledInDefault covers the container shape
// this finding fixes: cwd is a mounted workspace with no .cogpublic anywhere
// above it (so the walk-up finds nothing), and COGOS_REPO_ROOT is unset —
// exactly what happens if it were ever stripped at `docker run` time. The
// compiled-in default must still resolve when it actually has the guard's
// policy installed.
func TestRepoRootForGuardFallsBackToCompiledInDefault(t *testing.T) {
	t.Setenv("COGOS_REPO_ROOT", "")

	fakeImageRoot := t.TempDir()
	must(t, os.WriteFile(filepath.Join(fakeImageRoot, ".cogpublic"), []byte("version: 1\n"), 0o644))
	orig := compiledInGuardRoot
	compiledInGuardRoot = fakeImageRoot
	t.Cleanup(func() { compiledInGuardRoot = orig })

	// cwd with nothing above it: a workspace mount, not a checkout.
	t.Chdir(t.TempDir())

	got, err := repoRootForGuard()
	if err != nil {
		t.Fatalf("repoRootForGuard: %v", err)
	}
	if got != fakeImageRoot {
		t.Fatalf("got %q, want the compiled-in default %q", got, fakeImageRoot)
	}
}

// TestRepoRootForGuardFailsClosedWhenNothingResolves proves the final error
// path is explicit about what to do (set COGOS_REPO_ROOT) rather than a bare
// "not found" — a container that hit this branch has no other way to know.
func TestRepoRootForGuardFailsClosedWhenNothingResolves(t *testing.T) {
	t.Setenv("COGOS_REPO_ROOT", "")

	orig := compiledInGuardRoot
	compiledInGuardRoot = filepath.Join(t.TempDir(), "does-not-exist")
	t.Cleanup(func() { compiledInGuardRoot = orig })

	t.Chdir(t.TempDir())

	_, err := repoRootForGuard()
	if err == nil {
		t.Fatal("repoRootForGuard resolved with no .cogpublic reachable anywhere; it must fail closed")
	}
	if !strings.Contains(err.Error(), "release gate unavailable") ||
		!strings.Contains(err.Error(), "COGOS_REPO_ROOT") {
		t.Fatalf("error must name the gap and the fix (COGOS_REPO_ROOT), got: %v", err)
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, ".cogpublic")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("no .cogpublic above the test working directory")
		}
		dir = parent
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
