package engine

import (
	"testing"
)

// W1 lifecycle tests for the kernel run board. These pin the store rules the
// body slice must satisfy: one run.end per run (a second end is refused, the
// HTTP 409), archive only after run.end (I2), and delivered only after
// run.end. Against the skeleton every store method returns
// ErrRunBoardNotImplemented, so these fail at Open with that error.

func runBoardTestOpen(t *testing.T, st *RunBoardStore, title string) *Run {
	t.Helper()
	run, err := st.Open(RunOpen{
		Title:    title,
		Owner:    "cog",
		Executor: RunExecutor{Kind: RunExecutorManual},
		Origin:   RunOrigin{By: "node-a"},
	}, "node-a")
	if err != nil {
		t.Fatalf("Open(%q): %v", title, err)
	}
	if run == nil {
		t.Fatalf("Open(%q): nil run", title)
	}
	return run
}

func runBoardTestEnd(outcome string) RunEnd {
	return RunEnd{Outcome: outcome, ObservedBy: "kernel"}
}

func TestRunBoardSecondEndIsRefused(t *testing.T) {
	st := NewRunBoardStore("")
	run := runBoardTestOpen(t, st, "second end")

	if _, err := st.End(run.ID, runBoardTestEnd(RunOutcomeOK), "node-a"); err != nil {
		t.Fatalf("first End: %v", err)
	}
	if _, err := st.End(run.ID, runBoardTestEnd(RunOutcomeFail), "node-a"); err == nil {
		t.Fatalf("second End: want refusal (HTTP 409), got nil error")
	}
	got, ok := st.Get(run.ID)
	if !ok || got.End == nil {
		t.Fatalf("Get after first End: want run with End set, got ok=%v end=%v", ok, got != nil && got.End != nil)
	}
	if got.End.Outcome != RunOutcomeOK {
		t.Fatalf("second End overwrote outcome: got %q, want %q", got.End.Outcome, RunOutcomeOK)
	}
}

func TestRunBoardArchiveBeforeEndIsRefused(t *testing.T) {
	st := NewRunBoardStore("")
	run := runBoardTestOpen(t, st, "archive early")

	if _, err := st.Archive(run.ID, "node-a"); err == nil {
		t.Fatalf("Archive before run.end: want refusal (HTTP 409, I2), got nil error")
	}
	got, ok := st.Get(run.ID)
	if !ok {
		t.Fatalf("Get: run disappeared")
	}
	if got.Archived {
		t.Fatalf("run archived without a prior run.end")
	}
}

func TestRunBoardDeliveredBeforeEndIsRefused(t *testing.T) {
	st := NewRunBoardStore("")
	run := runBoardTestOpen(t, st, "delivered early")

	d := RunDelivered{To: "front-session", At: "2026-10-16T12:00:00Z"}
	if _, err := st.Delivered(run.ID, d, "node-a"); err == nil {
		t.Fatalf("Delivered before run.end: want refusal, got nil error")
	}
	got, ok := st.Get(run.ID)
	if !ok {
		t.Fatalf("Get: run disappeared")
	}
	if got.Delivered != nil {
		t.Fatalf("run.delivered recorded before run.end")
	}
}

func TestRunBoardExactlyOneRunEndPerRun(t *testing.T) {
	st := NewRunBoardStore("")
	a := runBoardTestOpen(t, st, "run a")
	b := runBoardTestOpen(t, st, "run b")

	if _, err := st.End(a.ID, runBoardTestEnd(RunOutcomeOK), "node-a"); err != nil {
		t.Fatalf("End(a): %v", err)
	}
	// A refused second end must not append a second run.end entry.
	_, _ = st.End(a.ID, runBoardTestEnd(RunOutcomeCrash), "node-a")
	if _, err := st.End(b.ID, runBoardTestEnd(RunOutcomeLost), "node-a"); err != nil {
		t.Fatalf("End(b): %v", err)
	}

	counts := map[string]int{}
	for _, ev := range st.EventsSince(0) {
		if ev.Kind == "run.end" {
			counts[ev.Run]++
		}
	}
	for _, id := range []string{a.ID, b.ID} {
		if counts[id] != 1 {
			t.Fatalf("run %s: want exactly one run.end entry, got %d", id, counts[id])
		}
	}
}
