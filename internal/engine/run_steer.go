// run_steer.go - POST /v1/runs/{id}/steer: send text into a hosted run's
// running turn, so a caller can change a run's brief without cancelling and
// restarting it.
//
// A run has exactly one turn (run_exec_hermes.go), so a steer never starts a
// turn: it is HermesACPSession.Redirect, the same mid-turn line the managed
// session prompt route uses with if_busy "forward". Hermes folds the text into
// the running turn (it arrives on the agent's next tool result).
//
// Each steer is an input to the run and is ledgered (run.steer: actor, at,
// sha256 and size of the text, the turn it joined). The agent's answer arrives
// asynchronously on the session's ring as prompt_forwarded; the executor folds
// it back as run.steer.result, so GET /v1/runs/{id} shows every steer with
// confirmed true (folded in), false (refused or run as a turn of its own,
// with the error) or absent (no outcome observed yet).
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// maxRunSteerBytes caps one steer's text; maxRunSteerLabelBytes the caller's
// display label recorded with it.
const (
	maxRunSteerBytes      = 8 << 10
	maxRunSteerLabelBytes = 128
)

// runSteerObserveFor bounds how long the executor watches the session's ring
// for a steer's outcome. Hermes answers a fold-in at once; this only bounds a
// goroutine whose agent never answers.
var runSteerObserveFor = 2 * time.Minute

// Ledger sub-kinds (item.append) for steers.
const (
	runEntrySteer       = "run.steer"
	runEntrySteerResult = "run.steer.result"
)

// RunSteer is one steer as GET /v1/runs/{id} shows it. Seq is the run.steer
// entry's ledger seq (the steer's id). Confirmed is nil until the outcome is
// observed.
type RunSteer struct {
	Seq        uint64 `json:"seq"`
	At         string `json:"at"`
	Actor      string `json:"actor"`
	Label      string `json:"label,omitempty"` // the caller's display actor (body "actor")
	TurnID     string `json:"turn_id,omitempty"`
	TextSHA256 string `json:"text_sha256"`
	Bytes      int    `json:"bytes"`
	Confirmed  *bool  `json:"confirmed,omitempty"`
	Error      string `json:"error,omitempty"`
}

// runSteerResult is the body of a run.steer.result entry.
type runSteerResult struct {
	SteerSeq  uint64 `json:"steer_seq"`
	Confirmed bool   `json:"confirmed"`
	Error     string `json:"error,omitempty"`
}

// RunSteerRequest is the POST /v1/runs/{id}/steer body. Actor is a display
// label recorded with the grant's own actor; it never replaces it.
type RunSteerRequest struct {
	Text  string `json:"text"`
	Actor string `json:"actor,omitempty"`
}

func cloneRunSteers(in []RunSteer) []RunSteer {
	if in == nil {
		return nil
	}
	out := make([]RunSteer, len(in))
	for i, s := range in {
		if s.Confirmed != nil {
			c := *s.Confirmed
			s.Confirmed = &c
		}
		out[i] = s
	}
	return out
}

// runActorName is the inverse of runActor, for display.
func runActorName(a map[string]any) string {
	if n, ok := a["node"].(string); ok {
		return "node:" + n
	}
	s, _ := a["person"].(string)
	return s
}

// foldRunSteer folds a run.steer or run.steer.result entry (applyLocked).
func foldRunSteer(r *Run, e *runEntry) {
	switch e.logicalKind() {
	case runEntrySteer:
		var s RunSteer
		bodyAs(e.Body, &s)
		s.Seq, s.At, s.Actor = e.Seq, entryTime(e), runActorName(e.Actor)
		s.Confirmed, s.Error = nil, ""
		r.Steers = append(r.Steers, s)
	case runEntrySteerResult:
		var res runSteerResult
		bodyAs(e.Body, &res)
		for i := range r.Steers {
			if r.Steers[i].Seq == res.SteerSeq && r.Steers[i].Confirmed == nil {
				c := res.Confirmed
				r.Steers[i].Confirmed, r.Steers[i].Error = &c, res.Error
				return
			}
		}
	}
}

// appendSteer ledgers a steer (write-ahead, before the text is sent). Refused
// once the run has its run.end: there is no turn left to steer.
func (st *RunBoardStore) appendSteer(id, turnID, text, actor, label string) (uint64, error) {
	sum := sha256.Sum256([]byte(text))
	body := map[string]any{"item": id, "kind": runEntrySteer, "turn_id": turnID,
		"text_sha256": hex.EncodeToString(sum[:]), "bytes": len(text)}
	if label != "" {
		if len(label) > maxRunSteerLabelBytes {
			label = strings.ToValidUTF8(label[:maxRunSteerLabelBytes], "")
		}
		body["label"] = label
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	r, err := st.lookupLocked(id)
	if err != nil {
		return 0, err
	}
	if r.End != nil {
		return 0, fmt.Errorf("%w: run %s has ended; no turn to steer", ErrRunConflict, id)
	}
	e, err := st.commitLocked(r.Room, "item.append", body, actor)
	if err != nil {
		return 0, err
	}
	return e.Seq, nil
}

// appendSteerResult records a steer's observed outcome. It may land after
// run.end: it is an observation about an input the run already took.
func (st *RunBoardStore) appendSteerResult(id string, res runSteerResult) error {
	body := structBody(res)
	body["item"] = id
	body["kind"] = runEntrySteerResult
	st.mu.Lock()
	defer st.mu.Unlock()
	r, err := st.lookupLocked(id)
	if err != nil {
		return err
	}
	_, err = st.commitLocked(r.Room, "item.append", body, runExecActor)
	return err
}

// RunSteered is what Steer returns: the turn the text was sent into and the
// steer's ledger seq.
type RunSteered struct {
	RunID     string `json:"run_id"`
	TurnID    string `json:"turn_id"`
	SteerSeq  uint64 `json:"steer_seq"`
	Forwarded bool   `json:"forwarded"`
}

// Steer sends text into the run's in-flight turn. ErrRunInvalid: empty or
// over maxRunSteerBytes. ErrRunConflict: the run is not hosted by this kernel,
// has no turn in flight (not yet started, or already ending), or another steer
// is still being delivered. The outcome is recorded later (run.steer.result).
func (e *HermesRunExecutor) Steer(runID, text, actor, label string) (RunSteered, error) {
	if strings.TrimSpace(text) == "" {
		return RunSteered{}, fmt.Errorf("%w: text is required", ErrRunInvalid)
	}
	if len(text) > maxRunSteerBytes {
		return RunSteered{}, fmt.Errorf("%w: text over %d bytes", ErrRunInvalid, maxRunSteerBytes)
	}
	e.mu.Lock()
	a := e.active[runID]
	var runTurn string
	cancelled := false
	if a != nil {
		runTurn, cancelled = a.turnID, a.cancelled
	}
	e.mu.Unlock()
	if a == nil {
		return RunSteered{}, fmt.Errorf("%w: run %s is not hosted by this kernel", ErrRunConflict, runID)
	}
	if cancelled {
		return RunSteered{}, fmt.Errorf("%w: run %s is being cancelled", ErrRunConflict, runID)
	}
	sess := a.sess
	turnID := sess.Info().TurnInFlight
	if runTurn == "" || turnID != runTurn {
		// Not started yet (brief not sent), or the run's turn already ended:
		// a run has one turn, and a steer never opens another.
		return RunSteered{}, fmt.Errorf("%w: run %s has no turn in flight", ErrRunConflict, runID)
	}

	seq, err := e.board.appendSteer(runID, turnID, text, actor, label)
	if err != nil {
		return RunSteered{}, err
	}

	content, _ := json.Marshal(text) // a JSON string: Redirect wraps it as one text block
	var blocks json.RawMessage
	blocks, _ = json.Marshal([]map[string]any{{"type": "text", "text": text}})
	sub := sess.Events().Subscribe(sess.Events().LastSeq(), runEventBuffer)
	got, err := sess.Redirect(content)
	if err != nil {
		sess.Events().Unsubscribe(sub)
		why := err.Error()
		switch {
		case errors.Is(err, ErrNoTurnInFlight):
			why = "run has no turn in flight"
		case errors.Is(err, ErrTurnInFlight):
			why = "another steer is still being delivered"
		case errors.Is(err, ErrSessionNotLive):
			why = "run session is not live"
		}
		_ = e.board.appendSteerResult(runID, runSteerResult{SteerSeq: seq, Confirmed: false, Error: why})
		return RunSteered{}, fmt.Errorf("%w: run %s: %s", ErrRunConflict, runID, why)
	}
	go e.observeSteer(runID, seq, sess, sub, blocks)
	return RunSteered{RunID: runID, TurnID: got, SteerSeq: seq, Forwarded: true}, nil
}

// observeSteer watches the ring for this steer's outcome and ledgers it:
// prompt_forwarded carrying the same prompt (confirmed or not, per the
// session), or a turn_start carrying it (the agent ran it as a turn of its own:
// not a fold-in). Redirect allows one forward at a time per session, and the
// prompt is matched too, so an earlier forward's event is not taken for this one.
func (e *HermesRunExecutor) observeSteer(runID string, seq uint64, sess *HermesACPSession, sub *RingSubscription, blocks json.RawMessage) {
	defer sess.Events().Unsubscribe(sub)
	res := runSteerResult{SteerSeq: seq, Error: "no outcome observed"}
	check := func(ev ManagedSessionEvent) bool {
		var d struct {
			Prompt    json.RawMessage `json:"prompt"`
			Confirmed bool            `json:"confirmed"`
			Error     string          `json:"error"`
		}
		if ev.Kind != MSEventPromptForwarded && ev.Kind != MSEventTurnStart {
			return false
		}
		if json.Unmarshal(ev.Data, &d) != nil || !sameBlocks(d.Prompt, blocks) {
			return false
		}
		if ev.Kind == MSEventTurnStart {
			res.Confirmed, res.Error = false, "agent ran the steer as a turn of its own"
			return true
		}
		res.Confirmed, res.Error = d.Confirmed, d.Error
		if !d.Confirmed && d.Error == "" {
			res.Error = "agent did not confirm the fold-in"
		}
		return true
	}
	done := false
	for _, ev := range sub.Replay {
		if done = check(ev); done {
			break
		}
	}
	timeout := time.After(runSteerObserveFor)
	for !done {
		select {
		case ev, ok := <-sub.Live:
			if !ok { // ring closed or subscriber dropped: read what was retained, once
				evs, _ := sess.Events().Since(sub.LastSeq)
				for _, ev := range evs {
					if done = check(ev); done {
						break
					}
				}
				done = true
				continue
			}
			done = check(ev)
		case <-timeout:
			done = true
		}
	}
	if err := e.board.appendSteerResult(runID, res); err != nil {
		// The steer itself is ledgered; only its observed outcome is lost.
		slog.Warn("run exec hermes: could not record steer outcome", "run", runID, "steer", seq, "error", err)
	}
}

// sameBlocks compares two prompt block arrays by value.
func sameBlocks(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return string(xb) == string(yb)
}

// handleRunSteer serves POST /v1/runs/{id}/steer: 202 {run_id, turn_id,
// steer_seq, forwarded: true} once the text is sent into the run's running
// turn. 404 no such run; 409 not hosted here, ended, no turn in flight, or a
// steer still being delivered; 400 empty or over 8 KiB. Same grant as cancel
// (runs:write). The outcome shows on GET /v1/runs/{id} as steers[].confirmed.
func (s *Server) handleRunSteer(w http.ResponseWriter, r *http.Request) {
	st := s.runBoardOr503(w)
	if st == nil {
		return
	}
	var in RunSteerRequest
	if !decodeRunBody(w, r, &in) {
		return
	}
	id := r.PathValue("id")
	if _, ok := st.Get(id); !ok {
		writeJSONError(w, http.StatusNotFound, "not_found", "no such run")
		return
	}
	exec, _ := s.runExecutorFor()
	if exec == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "run executor not initialised")
		return
	}
	out, err := exec.Steer(id, in.Text, runActorFromRequest(r), in.Actor)
	if err != nil {
		writeRunStoreError(w, err)
		return
	}
	writeRunJSON(w, http.StatusAccepted, out)
}
