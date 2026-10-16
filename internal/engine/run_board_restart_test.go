package engine

import (
	"testing"
)

// W1 restart and boot tests for the kernel run board. These drive only the
// exported skeleton API. Against the current stubs they fail with
// ErrRunBoardNotImplemented (or an empty fold), which is the expected red.

func TestRunBoardRestartRebuildsEqualFold(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(resetLedgerCacheForTest)
	resetLedgerCacheForTest()

	st := NewRunBoardStore(root)
	a, err := st.Open(RunOpen{
		Title:    "restart fixture a",
		Owner:    "cog",
		Executor: RunExecutor{Kind: RunExecutorManual},
		Origin:   RunOrigin{By: "chaz"},
	}, "chaz")
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	b, err := st.Open(RunOpen{
		Title:    "restart fixture b",
		Owner:    "work",
		Executor: RunExecutor{Kind: RunExecutorManual},
		Origin:   RunOrigin{By: "chaz"},
	}, "chaz")
	if err != nil {
		t.Fatalf("open b: %v", err)
	}
	if _, err := st.End(a.ID, RunEnd{Outcome: RunOutcomeOK, ObservedBy: "kernel"}, "chaz"); err != nil {
		t.Fatalf("end a: %v", err)
	}
	if _, err := st.Archive(a.ID, "chaz"); err != nil {
		t.Fatalf("archive a: %v", err)
	}
	if _, err := st.Delivered(b.ID, RunDelivered{To: "front", At: "2026-10-16T00:00:00Z"}, "chaz"); err != nil {
		t.Fatalf("delivered b: %v", err)
	}
	want := st.FoldHash()
	if want == "" {
		t.Fatal("live fold hash is empty after writes")
	}

	resetLedgerCacheForTest()
	re, err := RebuildRunBoardStoreFromLedger(root)
	if err != nil {
		t.Fatalf("rebuild from ledger: %v", err)
	}
	if got := re.FoldHash(); got != want {
		t.Fatalf("rebuilt fold hash = %q, want equal to live %q", got, want)
	}
	if run, ok := re.Get(a.ID); !ok || !run.Archived || run.End == nil || run.End.Outcome != RunOutcomeOK {
		t.Fatalf("rebuilt run a = %+v ok=%v, want archived with run.end ok", run, ok)
	}
}

func TestRunBoardKernelBootMarksOwnedRunsLost(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(resetLedgerCacheForTest)
	resetLedgerCacheForTest()

	// A manual run the kernel owns, with no live driver (no Live record, no
	// executor ref). The kernel dies before anyone calls run.end.
	st := NewRunBoardStore(root)
	r, err := st.Open(RunOpen{
		Title:    "orphan at boot",
		Owner:    "cog",
		Executor: RunExecutor{Kind: RunExecutorManual},
		Origin:   RunOrigin{By: "chaz"},
	}, "chaz")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Simulated kernel restart: rebuild the board from the ledger.
	resetLedgerCacheForTest()
	re, err := RebuildRunBoardStoreFromLedger(root)
	if err != nil {
		t.Fatalf("boot rebuild: %v", err)
	}
	got, ok := re.Get(r.ID)
	if !ok {
		t.Fatalf("run %s missing after boot", r.ID)
	}
	// W0 invariant I4: a kernel-owned run with no live driver closes as lost,
	// observed by the kernel.
	if got.End == nil {
		t.Fatalf("run %s has no run.end after boot; want kernel-written lost", r.ID)
	}
	if got.End.Outcome != RunOutcomeLost || got.End.ObservedBy != "kernel" {
		t.Fatalf("boot end = outcome %q observed_by %q; want lost/kernel",
			got.End.Outcome, got.End.ObservedBy)
	}
	if got.State != RunStateDone {
		t.Fatalf("boot state = %q; want %q", got.State, RunStateDone)
	}
}
