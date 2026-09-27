// compact_panic_test.go — regression test for the compaction single-flight
// slot leak found alongside the 2026-08-27 silent-stall investigation.
//
// THE BUG THIS PINS
// -----------------
// claimCompactSlot sets r.compacting = true, and recordCompactResult is the
// ONLY code path that clears it. The compaction pass runs in a goroutine, so
// a panic anywhere inside it skipped recordCompactResult entirely and left
// the single-flight slot claimed for the life of the process.
//
// Every subsequent maybeCompact then returned early at the `if r.compacting`
// guard — with no error, no log line, and nothing in Health() to show for it.
// Compaction would silently never run again, raw files would grow past their
// budget, and the 5m/1h tiers would stop being written, which is exactly the
// on-disk shape observed on a live node (raw current, 5m frozen days earlier).
//
// Same defect class as the Health() blindness in health_staleness_test.go: a
// failure whose only symptom is the absence of activity.
package vitalsretention

import (
	"strings"
	"testing"
	"time"
)

// TestCompactPanicReleasesSlot: after a compaction pass panics, the recorder
// must be able to claim the slot again. Before the fix this deadlocked the
// single-flight guard permanently.
func TestCompactPanicReleasesSlot(t *testing.T) {
	SetWorkspaceRoot(t.TempDir())
	t.Cleanup(func() { SetWorkspaceRoot("") })

	r := &Recorder{}

	panicked := make(chan struct{})
	restore := SetCompactHookForTest(func(_ *Recorder, _, _ string, _ Config) error {
		close(panicked)
		panic("simulated compaction panic (e.g. nil map, bad index)")
	})
	t.Cleanup(restore)

	base, err := r.baseDir()
	if err != nil {
		t.Fatalf("baseDir: %v", err)
	}

	r.maybeCompact(base, "test-node")

	select {
	case <-panicked:
	case <-time.After(2 * time.Second):
		t.Fatal("compaction hook was never invoked")
	}

	r.compactWG.Wait()

	r.mu.Lock()
	stillCompacting := r.compacting
	gotErr := r.lastCompactErr
	r.mu.Unlock()

	if stillCompacting {
		t.Fatal("r.compacting is STILL true after the compaction goroutine " +
			"panicked — the single-flight slot leaked. Every future " +
			"maybeCompact will return early at the guard and compaction " +
			"will never run again for the life of this process, silently.")
	}
	if gotErr == nil {
		t.Error("a panicking compaction recorded no error; Health() would " +
			"report this recorder as fine while compaction is dead")
	} else if !strings.Contains(gotErr.Error(), "panic") {
		t.Errorf("lastCompactErr = %q; want it to name the panic so the cause "+
			"is recoverable from Health() alone", gotErr)
	}
}

// TestCompactPanicDoesNotKillProcess: the handler contract is "never blocks,
// never panics" because it runs inside the autonomic ticker's dispatch. An
// unrecovered panic in this goroutine would take the whole kernel down.
func TestCompactPanicDoesNotKillProcess(t *testing.T) {
	SetWorkspaceRoot(t.TempDir())
	t.Cleanup(func() { SetWorkspaceRoot("") })

	r := &Recorder{}
	restore := SetCompactHookForTest(func(_ *Recorder, _, _ string, _ Config) error {
		panic("boom")
	})
	t.Cleanup(restore)

	base, err := r.baseDir()
	if err != nil {
		t.Fatalf("baseDir: %v", err)
	}

	r.maybeCompact(base, "test-node")
	r.compactWG.Wait() // reaching this line at all is the assertion

	// And the recorder must still be usable afterwards.
	r.mu.Lock()
	r.lastCompactAt = time.Time{} // make a fresh claim due
	r.mu.Unlock()

	ran := make(chan struct{})
	restore2 := SetCompactHookForTest(func(_ *Recorder, _, _ string, _ Config) error {
		close(ran)
		return nil
	})
	t.Cleanup(restore2)

	r.maybeCompact(base, "test-node")
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("compaction never ran again after an earlier pass panicked — " +
			"the recorder was permanently wedged")
	}
	r.compactWG.Wait()
}
