// run_steer_test.go - POST /v1/runs/{id}/steer: text sent into a hosted
// run's running turn (HermesACPSession.Redirect), ledgered as run.steer with
// its outcome folded back as run.steer.result.
//
// Real HTTP surface, real executor, fake ACP agent (startRunAPI,
// serve_runs_exec_test.go). Not t.Parallel, for the same registry reason.

package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// waitSteerOutcome blocks until the run's steer with this seq has a recorded
// outcome and returns it.
func waitSteerOutcome(t *testing.T, st *RunBoardStore, id string, seq uint64) RunSteer {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok := st.Get(id); ok {
			for _, s := range r.Steers {
				if s.Seq == seq && s.Confirmed != nil {
					return s
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("steer %d on run %s never got an outcome", seq, id)
	return RunSteer{}
}

func steerEntries(st *RunBoardStore, id string) (steers, results int) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	for _, e := range st.entries {
		if e.itemID() != id {
			continue
		}
		switch e.logicalKind() {
		case runEntrySteer:
			steers++
		case runEntrySteerResult:
			results++
		}
	}
	return
}

// TestRunAPI_SteerFoldsIntoRunningTurn: a steer on a hosted run with a turn
// in flight is 202 with the run's turn id; the agent folds it in (the "redir"
// fake turn ends carrying the steer text), the run ends ok with no second
// turn, and the board shows one confirmed steer with the text's sha256.
func TestRunAPI_SteerFoldsIntoRunningTurn(t *testing.T) {
	h := startRunAPI(t)
	id := h.open(t, "redir work for node-b")
	waitRunTurn(t, h, id)

	const text = "[card update since your last turn] title: do Y instead"
	code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token,
		`{"text":"`+text+`","actor":"stage:346369"}`)
	if code != http.StatusAccepted {
		t.Fatalf("POST steer = %d (%v); want 202", code, out)
	}
	if out["forwarded"] != true || out["run_id"] != id || out["turn_id"] == "" {
		t.Fatalf("steer response = %v; want forwarded true, run_id, turn_id", out)
	}
	seq := uint64(out["steer_seq"].(float64))

	end := waitRunEnd(t, h.board, id)
	if end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok: the steer must not end or kill the run", end.Outcome, end.Reason)
	}
	if end.Report == nil {
		t.Fatal("run.end has no report")
	}
	s := waitSteerOutcome(t, h.board, id, seq)
	if !*s.Confirmed || s.Error != "" {
		t.Fatalf("steer outcome = confirmed %v error %q; want a confirmed fold-in", *s.Confirmed, s.Error)
	}
	sum := sha256.Sum256([]byte(text))
	if s.TextSHA256 != hex.EncodeToString(sum[:]) || s.Bytes != len(text) {
		t.Fatalf("steer ledger = sha %s bytes %d; want the text's", s.TextSHA256, s.Bytes)
	}
	if s.Actor != "chaz" || s.Label != "stage:346369" || s.TurnID != out["turn_id"] {
		t.Fatalf("steer = actor %q label %q turn %q; want chaz / stage:346369 / %v", s.Actor, s.Label, s.TurnID, out["turn_id"])
	}

	// The agent saw the text inside the run's one turn: the fake echoes it
	// as "continued:<text>" before ending that turn.
	r := getRun(t, h.board, id)
	sess := h.session(*r.Executor.Ref)
	starts, folded := 0, false
	for _, ev := range sess.Events().Subscribe(0, 512).Replay {
		if ev.Kind == MSEventTurnStart {
			starts++
		}
		if ev.Kind == MSEventSessionUpdate && strings.Contains(string(ev.Data), "continued:"+text) {
			folded = true
		}
	}
	if starts != 1 || !folded {
		t.Fatalf("turn_starts = %d, folded = %v; want 1 turn carrying the steer", starts, folded)
	}
	if n, _ := steerEntries(h.board, id); n != 1 {
		t.Fatalf("run.steer entries = %d, want 1", n)
	}
}

// TestRunAPI_SteerLedgerSurvivesRebuild: the steer and its outcome are
// ledgered entries, so a board rebuilt from the ledger shows the same steers.
func TestRunAPI_SteerLedgerSurvivesRebuild(t *testing.T) {
	h := startRunAPI(t)
	if h.board.workspaceRoot == "" {
		t.Fatal("harness board is in-memory; this test needs a ledger")
	}
	id := h.open(t, "redir work")
	waitRunTurn(t, h, id)
	code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token, `{"text":"change of plan"}`)
	if code != http.StatusAccepted {
		t.Fatalf("POST steer = %d (%v)", code, out)
	}
	waitRunEnd(t, h.board, id)
	want := waitSteerOutcome(t, h.board, id, uint64(out["steer_seq"].(float64)))

	rebuilt, err := RebuildRunBoardStoreFromLedger(h.board.workspaceRoot)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := rebuilt.Get(id)
	if !ok || len(r.Steers) != 1 {
		t.Fatalf("rebuilt run steers = %v; want the one steer", r)
	}
	got, _ := json.Marshal(r.Steers[0])
	exp, _ := json.Marshal(want)
	if string(got) != string(exp) {
		t.Fatalf("rebuilt steer = %s; want %s", got, exp)
	}
}

// TestRunAPI_SteerAfterEndIs409: an ended run is no longer hosted; a steer is
// refused and nothing is ledgered.
func TestRunAPI_SteerAfterEndIs409(t *testing.T) {
	h := startRunAPI(t)
	id := h.open(t, "hello from node-a")
	waitRunEnd(t, h.board, id)
	if code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token, `{"text":"too late"}`); code != http.StatusConflict {
		t.Fatalf("steer of an ended run = %d (%v); want 409", code, out)
	}
	if n, _ := steerEntries(h.board, id); n != 0 {
		t.Fatalf("run.steer entries = %d after a refused steer, want 0", n)
	}
}

// TestRunAPI_SteerNotHostedIs409: a manual run is on the board but not hosted
// by the executor: 409, nothing ledgered. An unknown run is 404.
func TestRunAPI_SteerNotHostedIs409(t *testing.T) {
	h := startRunAPI(t)
	code, run := runsPost(t, h.ts.URL+"/v1/runs", h.token,
		`{"title":"by hand","owner":"cog","executor":{"kind":"manual"},"origin":{"by":"node-a"}}`)
	if code != http.StatusCreated {
		t.Fatalf("open manual = %d (%v)", code, run)
	}
	id := run["id"].(string)
	if code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token, `{"text":"hi"}`); code != http.StatusConflict {
		t.Fatalf("steer of a manual run = %d (%v); want 409", code, out)
	}
	if n, _ := steerEntries(h.board, id); n != 0 {
		t.Fatalf("run.steer entries = %d, want 0", n)
	}
	if code, _ := runsPost(t, h.ts.URL+"/v1/runs/nope/steer", h.token, `{"text":"hi"}`); code != http.StatusNotFound {
		t.Fatalf("steer of an unknown run = %d; want 404", code)
	}
}

// TestRunAPI_SteerBadTextIs400: empty and over-8KiB text are refused before
// anything reaches the agent or the ledger.
func TestRunAPI_SteerBadTextIs400(t *testing.T) {
	h := startRunAPI(t)
	id := h.open(t, "slow work")
	waitRunTurn(t, h, id)
	big, _ := json.Marshal(map[string]string{"text": strings.Repeat("x", maxRunSteerBytes+1)})
	for _, body := range []string{`{"text":""}`, `{"text":"   "}`, string(big)} {
		if code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token, body); code != http.StatusBadRequest {
			t.Errorf("steer body %.40q = %d (%v); want 400", body, code, out)
		}
	}
	if n, _ := steerEntries(h.board, id); n != 0 {
		t.Fatalf("run.steer entries = %d, want 0", n)
	}
	ok, _ := json.Marshal(map[string]string{"text": strings.Repeat("x", maxRunSteerBytes)})
	if code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token, string(ok)); code != http.StatusAccepted {
		t.Fatalf("steer at exactly the cap = %d (%v); want 202", code, out)
	}
	runsPost(t, h.ts.URL+"/v1/runs/"+id+"/cancel", h.token, `{"reason":"done"}`)
}

// TestRunAPI_SteerNoRunsWriteIs403: steer needs the same grant as cancel.
func TestRunAPI_SteerNoRunsWriteIs403(t *testing.T) {
	h := startRunAPI(t)
	id := h.open(t, "slow work")
	waitRunTurn(t, h, id)
	readOnly := mintActorScopedTestGrant(t, h.srv, "reader", "chaz", ScopeManagedSessions)
	if code, _ := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", readOnly, `{"text":"hi"}`); code != http.StatusForbidden {
		t.Fatalf("steer without runs:write = %d; want 403", code)
	}
	if n, _ := steerEntries(h.board, id); n != 0 {
		t.Fatalf("run.steer entries = %d, want 0", n)
	}
	runsPost(t, h.ts.URL+"/v1/runs/"+id+"/cancel", h.token, `{"reason":"done"}`)
}

// TestRunAPI_SteerRunAsOwnTurnDoesNotEndTheRun: an agent that does not fold
// the steer in answers it as a short turn of its own (the "sleep" fake turn
// does not redirect). Redirect then writes a turn_end under the forward's id;
// that must not read as the run's turn ending. The run ends only when its own
// turn does, and the steer is recorded unconfirmed.
func TestRunAPI_SteerRunAsOwnTurnDoesNotEndTheRun(t *testing.T) {
	h := startRunAPI(t)
	id := h.open(t, "sleep1500 long work")
	waitRunTurn(t, h, id)
	code, out := runsPost(t, h.ts.URL+"/v1/runs/"+id+"/steer", h.token, `{"text":"not folded"}`)
	if code != http.StatusAccepted {
		t.Fatalf("POST steer = %d (%v); want 202", code, out)
	}
	s := waitSteerOutcome(t, h.board, id, uint64(out["steer_seq"].(float64)))
	if *s.Confirmed {
		t.Fatalf("steer confirmed by an agent that ran it as its own turn: %+v", s)
	}
	if r := getRun(t, h.board, id); r.End != nil {
		t.Fatalf("run ended %s (%q) on the steer's own turn_end; its turn is still running", r.End.Outcome, r.End.Reason)
	}
	if end := waitRunEnd(t, h.board, id); end.Outcome != RunOutcomeOK {
		t.Fatalf("run.end outcome = %q (%q); want ok", end.Outcome, end.Reason)
	}
}
