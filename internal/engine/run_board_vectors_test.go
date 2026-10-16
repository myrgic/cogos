// run_board_vectors_test.go - table tests for the kernel run board (W1).
//
// The vectors come from testdata/run-vectors.json, vendored from
// apps/stage/tests/fixtures/run-vectors.json. The Python side already passes
// them against apps/stage/workers.py and journal.py. These tests drive the
// Go RunBoardStore through the same states and outcomes, using only the
// exported W1 API. Against the W1 skeleton every store call returns
// ErrRunBoardNotImplemented, so the tests fail at the first store call.
package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type runVectorsFile struct {
	Version int `json:"version"`
	States  []struct {
		ID     string `json:"id"`
		Expect struct {
			State  string `json:"state"`
			Reason string `json:"reason"`
		} `json:"expect"`
	} `json:"states"`
	Outcomes []struct {
		ID     string   `json:"id"`
		Expect []string `json:"expect"`
	} `json:"outcomes"`
}

// runVectorStateMap maps the Python item_state names in the fixture to the
// Go run states. "planned" is the Go open state; "done" and "reported" are
// shared names.
var runVectorStateMap = map[string]string{
	"planned":  RunStateOpen,
	"reported": RunStateReported,
	"done":     RunStateDone,
}

// runVectorOutcomeMap maps the fixture outcome names (decide_outcome) to the
// Go run.end outcomes.
var runVectorOutcomeMap = map[string]string{
	"ok":     RunOutcomeOK,
	"fail":   RunOutcomeFail,
	"crash":  RunOutcomeCrash,
	"killed": RunOutcomeKilled,
	"lost":   RunOutcomeLost,
}

func loadRunVectors(t *testing.T) runVectorsFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "run-vectors.json"))
	if err != nil {
		t.Fatalf("read run vectors: %v", err)
	}
	var v runVectorsFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse run vectors: %v", err)
	}
	if v.Version != 1 {
		t.Fatalf("run vectors version = %d, want 1", v.Version)
	}
	return v
}

func runVectorState(t *testing.T, id string) (string, string) {
	t.Helper()
	for _, s := range loadRunVectors(t).States {
		if s.ID == id {
			return s.Expect.State, s.Expect.Reason
		}
	}
	t.Fatalf("state vector %s not in fixture", id)
	return "", ""
}

func runVectorOutcome(t *testing.T, id string) []string {
	t.Helper()
	for _, o := range loadRunVectors(t).Outcomes {
		if o.ID == id {
			return o.Expect
		}
	}
	t.Fatalf("outcome vector %s not in fixture", id)
	return nil
}

func vecRunOpen(t *testing.T, st *RunBoardStore) *Run {
	t.Helper()
	run, err := st.Open(RunOpen{
		Title:    "run-board vector",
		Owner:    "cog",
		Executor: RunExecutor{Kind: RunExecutorManual},
		Origin:   RunOrigin{By: "test"},
	}, "test")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if run == nil {
		t.Fatal("Open returned nil run")
	}
	return run
}

// TestRunBoardStateVectors drives the store through the fixture states that
// the W1 API can express: S1 planned, S5 reported, S7 done, S8 done before end.
// S2, S3, S4 and S6 need executor resolution, a live-set or processed writes,
// or a clock, none of which is in the W1 API. They are covered by the Python
// side and the W2/W4 slices.
func TestRunBoardStateVectors(t *testing.T) {
	t.Run("S1-planned", func(t *testing.T) {
		want, _ := runVectorState(t, "S1-planned")
		st := NewRunBoardStore("")
		run := vecRunOpen(t, st)
		if got := run.State; got != runVectorStateMap[want] {
			t.Fatalf("state = %q, want %q", got, runVectorStateMap[want])
		}
	})

	t.Run("S5-reported", func(t *testing.T) {
		want, _ := runVectorState(t, "S5-reported")
		st := NewRunBoardStore("")
		run := vecRunOpen(t, st)
		if _, err := st.End(run.ID, RunEnd{Outcome: RunOutcomeOK, ObservedBy: "kernel"}, "test"); err != nil {
			t.Fatalf("End: %v", err)
		}
		got, err := st.Delivered(run.ID, RunDelivered{To: "front", At: "2026-10-16T00:00:01Z"}, "test")
		if err != nil {
			t.Fatalf("Delivered: %v", err)
		}
		if got.State != runVectorStateMap[want] {
			t.Fatalf("state = %q, want %q", got.State, runVectorStateMap[want])
		}
	})

	t.Run("S7-done", func(t *testing.T) {
		want, _ := runVectorState(t, "S7-done")
		st := NewRunBoardStore("")
		run := vecRunOpen(t, st)
		if _, err := st.End(run.ID, RunEnd{Outcome: RunOutcomeOK, ObservedBy: "kernel"}, "test"); err != nil {
			t.Fatalf("End: %v", err)
		}
		if _, err := st.Delivered(run.ID, RunDelivered{To: "front", At: "2026-10-16T00:00:01Z"}, "test"); err != nil {
			t.Fatalf("Delivered: %v", err)
		}
		got, err := st.Archive(run.ID, "test")
		if err != nil {
			t.Fatalf("Archive: %v", err)
		}
		if got.State != runVectorStateMap[want] {
			t.Fatalf("state = %q, want %q", got.State, runVectorStateMap[want])
		}
	})

	// I2 and S8: archive before run.end is refused, and the run is not done.
	t.Run("S8-done-before-end-is-not-done", func(t *testing.T) {
		_, _ = runVectorState(t, "S8-done-before-end-is-not-done")
		st := NewRunBoardStore("")
		run := vecRunOpen(t, st)
		got, err := st.Archive(run.ID, "test")
		if err == nil {
			t.Fatalf("Archive before run.end accepted, state = %q; want refusal (I2)", got.State)
		}
		if cur, _ := st.Get(run.ID); cur != nil && cur.State == RunStateDone {
			t.Fatalf("run is done before run.end (S8)")
		}
	})
}

// TestRunBoardOutcomeVectors checks that run.end stores each fixture outcome
// verbatim. The decide_outcome mapping from snap and meta to outcome is
// Python-side; the Go store takes the outcome from the caller.
func TestRunBoardOutcomeVectors(t *testing.T) {
	v := loadRunVectors(t)
	for _, o := range v.Outcomes {
		o := o
		t.Run(o.ID, func(t *testing.T) {
			if len(o.Expect) != 2 {
				t.Fatalf("fixture expect malformed: %v", o.Expect)
			}
			want, ok := runVectorOutcomeMap[o.Expect[0]]
			if !ok {
				t.Fatalf("unknown fixture outcome %q", o.Expect[0])
			}
			st := NewRunBoardStore("")
			run := vecRunOpen(t, st)
			got, err := st.End(run.ID, RunEnd{Outcome: want, Reason: o.Expect[1], ObservedBy: "kernel"}, "test")
			if err != nil {
				t.Fatalf("End(%s): %v", want, err)
			}
			if got.End == nil || got.End.Outcome != want {
				t.Fatalf("run.end outcome = %+v, want %q", got.End, want)
			}
			if got.End.Reason != o.Expect[1] {
				t.Fatalf("run.end reason = %q, want %q", got.End.Reason, o.Expect[1])
			}
		})
	}
}

// TestRunBoardI1SecondEndRefused: a run item has at most one run.end. The
// second end is refused and the first outcome is kept.
func TestRunBoardI1SecondEndRefused(t *testing.T) {
	st := NewRunBoardStore("")
	run := vecRunOpen(t, st)
	if _, err := st.End(run.ID, RunEnd{Outcome: RunOutcomeOK, ObservedBy: "kernel"}, "test"); err != nil {
		t.Fatalf("first End: %v", err)
	}
	_, err := st.End(run.ID, RunEnd{Outcome: RunOutcomeFail, ObservedBy: "kernel"}, "test")
	if err == nil {
		t.Fatal("second End accepted; want refusal (I1)")
	}
	cur, ok := st.Get(run.ID)
	if !ok || cur.End == nil || cur.End.Outcome != RunOutcomeOK {
		t.Fatalf("first outcome not preserved after refused second end: %+v", cur)
	}
}

// TestRunBoardRestartFold: a store rebuilt from the ledger folds to the same
// state as the live store (FoldHash equal).
func TestRunBoardRestartFold(t *testing.T) {
	dir := t.TempDir()
	st := NewRunBoardStore(dir)
	run := vecRunOpen(t, st)
	if _, err := st.End(run.ID, RunEnd{Outcome: RunOutcomeOK, ObservedBy: "kernel"}, "test"); err != nil {
		t.Fatalf("End: %v", err)
	}
	rebuilt, err := RebuildRunBoardStoreFromLedger(dir)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if st.FoldHash() != rebuilt.FoldHash() {
		t.Fatalf("fold hash after restart = %q, want %q", rebuilt.FoldHash(), st.FoldHash())
	}
}
