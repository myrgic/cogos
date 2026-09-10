// sessions_test.go — unit tests for the kernel-native session / handoff
// hybrid (cog://mem/semantic/surveys/2026-04-21-consolidation/
// agent-P-session-management-evaluation §"Tests to add").
//
// 26 tests total (15 original + 11 added post-codex-review):
//
//  1. TestSessionRegister_ValidID                     — happy path
//  2. TestSessionRegister_InvalidFormat               — regex rejects malformed IDs
//  3. TestSessionRegister_ReRegistration              — idempotent UPDATE semantics
//  4. TestHeartbeat_UnknownSession                    — 404
//  5. TestEnd_UnknownSession                          — 404
//  6. TestEnd_AlreadyEnded                            — 409
//  7. TestPresence_ActiveWindow                       — stale heartbeats marked inactive
//  8. TestHandoffOffer_MissingTaskFields              — 400 validation
//  9. TestHandoffClaim_Atomicity                      — concurrent claims, first wins
// 10. TestHandoffClaim_TTLExpired                     — expired offer rejected with 409
// 11. TestHandoffClaim_PhantomOffer                   — 404
// 12. TestHandoffComplete_WithoutClaim                — 409
// 13. TestReplayOnStartup                             — registry rebuilds from bus
// 14. TestClaimRejectedEventEmitted                   — amendment #4 observability
// 15. TestMCP_HandoffRoundTrip                        — MCP end-to-end
//
// Added post PR#43 codex review:
//
// 16. TestRegistryUnchangedOnBusAppendFailure         — critical #1: append-first
// 17. TestHeartbeatOnEndedSessionDoesNotMutate        — critical #2: no-mutation 409
// 18. TestReplay_EmptyBuses                           — empty replay is safe
// 19. TestReplay_UnknownEventType                     — unknown types skipped
// 20. TestReplay_OutOfOrderSeq                        — sort-by-seq determinism
// 21. TestReplay_DuplicateSeqWithConflictingPayload   — first-write-wins policy
// 22. TestReplay_CompleteWithoutClaim                 — orphaned complete tolerated
// 23. TestSessionRegister_TwoComponentIDRejected      — spec-required 3-tuple
// 24. TestHandoffOffer_NegativeTTLRejected            — 400 on ttl_seconds:-1
// 25. TestHandoffOffer_CallerSuppliedIDRejected       — kernel always mints
// 26. TestRoutes_PresenceDoesNotShadowContext         — /presence vs /{id}/context

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ─── HTTP fixtures ───────────────────────────────────────────────────────────

// newSessionsTestServer reuses the generic events fixture and returns the
// Server struct alongside an httptest base URL so tests can poke both the
// public HTTP surface and the in-memory registries directly.
func newSessionsTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	root := t.TempDir()
	cfg := &Config{WorkspaceRoot: root, CogDir: root + "/.cog", Port: 0, WriteRouteGrantAuthDisabled: true}
	nucleus := &Nucleus{Name: "test"}
	proc := NewProcess(cfg, nucleus)
	srv := NewServer(cfg, nucleus, proc)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		_ = proc.Broker().Close()
	})
	return srv, ts
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, into any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decode body: %v", err)
	}
}

// ─── 1. TestSessionRegister_ValidID ──────────────────────────────────────────

func TestSessionRegister_ValidID(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	body := map[string]any{
		"session_id": "alpha-beta-gamma",
		"workspace":  "demo",
		"role":       "author",
	}
	resp := postJSON(t, ts.URL+"/v1/sessions/register", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out sessionWriteResponse
	decodeJSON(t, resp, &out)
	if !out.OK || out.SessionID != "alpha-beta-gamma" {
		t.Errorf("bad response: %+v", out)
	}
	if out.Seq != 1 {
		t.Errorf("seq = %d, want 1", out.Seq)
	}
	if got := srv.sessionRegistry.Len(); got != 1 {
		t.Errorf("registry len = %d, want 1", got)
	}
}

// ─── 2. TestSessionRegister_InvalidFormat ────────────────────────────────────

func TestSessionRegister_InvalidFormat(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	cases := []string{
		"",            // empty
		"single",      // only one component
		"BAD-ID-HERE", // uppercase
		"has spaces-x-y",
		"trailing-dash-",
	}
	for _, id := range cases {
		body := map[string]any{"session_id": id, "workspace": "w", "role": "r"}
		resp := postJSON(t, ts.URL+"/v1/sessions/register", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%q: status = %d, want 400", id, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

// ─── 3. TestSessionRegister_ReRegistration ───────────────────────────────────

func TestSessionRegister_ReRegistration(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	body := map[string]any{
		"session_id": "same-id-twice",
		"workspace":  "demo",
		"role":       "author",
		"task":       "first",
	}
	r1 := postJSON(t, ts.URL+"/v1/sessions/register", body)
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first register = %d", r1.StatusCode)
	}

	// Second call updates the row — should not be rejected.
	body["task"] = "second"
	r2 := postJSON(t, ts.URL+"/v1/sessions/register", body)
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("re-register = %d, want 200 (update)", r2.StatusCode)
	}
	var resp2 sessionWriteResponse
	decodeJSON(t, r2, &resp2)
	if resp2.Created {
		t.Errorf("second register reported created=true")
	}
	if resp2.Session == nil || resp2.Session.Task != "second" {
		t.Errorf("task not updated: %+v", resp2.Session)
	}
	if got := srv.sessionRegistry.Len(); got != 1 {
		t.Errorf("len = %d, want 1 after update", got)
	}
	// Both events should be on the bus.
	events, _ := srv.busSessions.ReadEvents(BusSessions)
	if len(events) < 2 {
		t.Errorf("bus has %d events, want >=2", len(events))
	}
}

// ─── 4. TestHeartbeat_UnknownSession ─────────────────────────────────────────

func TestHeartbeat_UnknownSession(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	resp := postJSON(t, ts.URL+"/v1/sessions/ghost-session-id/heartbeat",
		map[string]any{"status": "live"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// ─── 5. TestEnd_UnknownSession ───────────────────────────────────────────────

func TestEnd_UnknownSession(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	resp := postJSON(t, ts.URL+"/v1/sessions/ghost-session-id/end",
		map[string]any{"reason": "test"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// ─── 6. TestEnd_AlreadyEnded ─────────────────────────────────────────────────

func TestEnd_AlreadyEnded(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	body := map[string]any{"session_id": "once-and-done", "workspace": "w", "role": "r"}
	postJSON(t, ts.URL+"/v1/sessions/register", body).Body.Close()

	endURL := ts.URL + "/v1/sessions/once-and-done/end"
	r1 := postJSON(t, endURL, map[string]any{"reason": "first"})
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first end = %d", r1.StatusCode)
	}
	r2 := postJSON(t, endURL, map[string]any{"reason": "second"})
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusConflict {
		t.Errorf("second end = %d, want 409", r2.StatusCode)
	}
}

// ─── 7. TestPresence_ActiveWindow ────────────────────────────────────────────

func TestPresence_ActiveWindow(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	// Register two sessions. Then manually backdate one's LastSeen.
	postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "fresh-session-here", "workspace": "w", "role": "r",
	}).Body.Close()
	postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "stale-session-here", "workspace": "w", "role": "r",
	}).Body.Close()

	// Rewind the stale session's LastSeen well past the default window.
	srv.sessionRegistry.mu.Lock()
	srv.sessionRegistry.rows["stale-session-here"].LastSeen =
		time.Now().UTC().Add(-2 * time.Hour)
	srv.sessionRegistry.mu.Unlock()

	resp, err := http.Get(ts.URL + "/v1/sessions/presence?active_within_seconds=300")
	if err != nil {
		t.Fatalf("GET presence: %v", err)
	}
	defer resp.Body.Close()

	var body struct {
		Sessions []struct {
			SessionID string `json:"session_id"`
			Active    bool   `json:"active"`
		} `json:"sessions"`
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != 2 {
		t.Fatalf("count = %d, want 2", body.Count)
	}
	for _, s := range body.Sessions {
		switch s.SessionID {
		case "fresh-session-here":
			if !s.Active {
				t.Errorf("%s: active=false, want true", s.SessionID)
			}
		case "stale-session-here":
			if s.Active {
				t.Errorf("%s: active=true, want false", s.SessionID)
			}
		}
	}
}

// ─── 8. TestHandoffOffer_MissingTaskFields ───────────────────────────────────

func TestHandoffOffer_MissingTaskFields(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	// No task at all.
	r1 := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "src-session-abc",
		"bootstrap_prompt": "x",
	})
	r1.Body.Close()
	if r1.StatusCode != http.StatusBadRequest {
		t.Errorf("no-task: status = %d, want 400", r1.StatusCode)
	}

	// task without next_steps.
	r2 := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "src-session-abc",
		"bootstrap_prompt": "x",
		"task":             map[string]any{"title": "T", "goal": "G"},
	})
	r2.Body.Close()
	if r2.StatusCode != http.StatusBadRequest {
		t.Errorf("no next_steps: status = %d, want 400", r2.StatusCode)
	}

	// Empty next_steps list.
	r3 := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "src-session-abc",
		"bootstrap_prompt": "x",
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{}},
	})
	r3.Body.Close()
	if r3.StatusCode != http.StatusBadRequest {
		t.Errorf("empty next_steps: status = %d, want 400", r3.StatusCode)
	}

	// Happy path.
	r4 := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "src-session-abc",
		"bootstrap_prompt": "x",
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{"step1"}},
	})
	defer r4.Body.Close()
	if r4.StatusCode != http.StatusOK {
		t.Errorf("valid offer: status = %d, want 200", r4.StatusCode)
	}
}

// ─── 9. TestHandoffClaim_Atomicity ───────────────────────────────────────────

func TestHandoffClaim_Atomicity(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	// Post an offer first.
	offerResp := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "offering-session-x",
		"bootstrap_prompt": "bp",
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{"s1"}},
	})
	var offer handoffWriteResponse
	decodeJSON(t, offerResp, &offer)
	if !offer.OK {
		t.Fatalf("offer failed: %+v", offer)
	}
	claimURL := ts.URL + "/v1/handoffs/" + offer.HandoffID + "/claim"

	// Spin up 8 concurrent claims from 8 different sessions.
	const N = 8
	var wg sync.WaitGroup
	results := make([]int, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sess := []byte("claimant-session-0")
			sess[len(sess)-1] = byte('0' + idx)
			resp := postJSON(t, claimURL, map[string]any{"claiming_session": string(sess)})
			resp.Body.Close()
			results[idx] = resp.StatusCode
		}(i)
	}
	wg.Wait()

	wins := 0
	losses := 0
	for _, code := range results {
		switch code {
		case http.StatusOK:
			wins++
		case http.StatusConflict:
			losses++
		default:
			t.Errorf("unexpected status %d", code)
		}
	}
	if wins != 1 {
		t.Errorf("wins = %d, want 1", wins)
	}
	if losses != N-1 {
		t.Errorf("losses = %d, want %d", losses, N-1)
	}

	// Confirm the claim_rejected events landed for each loser.
	events, _ := srv.busSessions.ReadEvents(BusHandoffs)
	rejCount := 0
	for _, e := range events {
		if e.Type == EvtHandoffClaimRejected {
			rejCount++
		}
	}
	if rejCount != N-1 {
		t.Errorf("claim_rejected events = %d, want %d", rejCount, N-1)
	}
}

// ─── 10. TestHandoffClaim_TTLExpired ─────────────────────────────────────────

func TestHandoffClaim_TTLExpired(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	// Offer with 1s TTL, then backdate CreatedAt + ExpiresAt to force expiry.
	offerResp := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "offering-session-y",
		"bootstrap_prompt": "bp",
		"ttl_seconds":      1,
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{"s"}},
	})
	var offer handoffWriteResponse
	decodeJSON(t, offerResp, &offer)

	// Age the row.
	srv.handoffRegistry.mu.Lock()
	srv.handoffRegistry.rows[offer.HandoffID].CreatedAt =
		time.Now().Add(-1 * time.Hour)
	srv.handoffRegistry.rows[offer.HandoffID].ExpiresAt =
		time.Now().Add(-59 * time.Minute)
	srv.handoffRegistry.mu.Unlock()

	claimResp := postJSON(t, ts.URL+"/v1/handoffs/"+offer.HandoffID+"/claim",
		map[string]any{"claiming_session": "would-be-claimant"})
	defer claimResp.Body.Close()
	if claimResp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", claimResp.StatusCode)
	}
	body, _ := jsonReadAll(claimResp)
	if !strings.Contains(body, "ttl_expired") {
		t.Errorf("expected ttl_expired in body: %q", body)
	}
}

// ─── 11. TestHandoffClaim_PhantomOffer ───────────────────────────────────────

func TestHandoffClaim_PhantomOffer(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	resp := postJSON(t, ts.URL+"/v1/handoffs/ho-0-deadbeef/claim",
		map[string]any{"claiming_session": "phantom-claimant-abc"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	// claim_rejected event should still have been emitted for observability.
	events, _ := srv.busSessions.ReadEvents(BusHandoffs)
	found := false
	for _, e := range events {
		if e.Type == EvtHandoffClaimRejected {
			if r, _ := e.Payload["reason"].(string); r == string(ClaimRejectedOfferNotFound) {
				found = true
				break
			}
		}
	}
	if !found {
		t.Errorf("no claim_rejected/offer_not_found event on bus")
	}
}

// ─── 12. TestHandoffComplete_WithoutClaim ────────────────────────────────────

func TestHandoffComplete_WithoutClaim(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	offerResp := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "offering-session-z",
		"bootstrap_prompt": "bp",
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{"s"}},
	})
	var offer handoffWriteResponse
	decodeJSON(t, offerResp, &offer)

	resp := postJSON(t, ts.URL+"/v1/handoffs/"+offer.HandoffID+"/complete",
		map[string]any{
			"completing_session": "premature-completer-xyz",
			"outcome":            "done",
		})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}
}

// ─── 13. TestReplayOnStartup ─────────────────────────────────────────────────

func TestReplayOnStartup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cfg := &Config{WorkspaceRoot: root, CogDir: root + "/.cog", Port: 0, WriteRouteGrantAuthDisabled: true}
	nucleus := &Nucleus{Name: "test"}
	proc := NewProcess(cfg, nucleus)
	srv1 := NewServer(cfg, nucleus, proc)
	ts1 := httptest.NewServer(srv1.Handler())
	t.Cleanup(func() { ts1.Close() })

	// Register two sessions, end one, leave the other live.
	postJSON(t, ts1.URL+"/v1/sessions/register", map[string]any{
		"session_id": "replay-live-session", "workspace": "w", "role": "r",
	}).Body.Close()
	postJSON(t, ts1.URL+"/v1/sessions/register", map[string]any{
		"session_id": "replay-ended-session", "workspace": "w", "role": "r",
	}).Body.Close()
	postJSON(t, ts1.URL+"/v1/sessions/replay-ended-session/end",
		map[string]any{"reason": "test-end"}).Body.Close()

	// And a handoff.
	offerResp := postJSON(t, ts1.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "replay-live-session",
		"bootstrap_prompt": "bp",
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{"s"}},
	})
	var offer handoffWriteResponse
	decodeJSON(t, offerResp, &offer)

	ts1.Close()

	// Boot a fresh Server rooted at the same workspace — replay should
	// reconstruct the same state from the bus.
	srv2 := NewServer(cfg, nucleus, proc)
	if got := srv2.sessionRegistry.Len(); got != 2 {
		t.Errorf("replay session count = %d, want 2", got)
	}
	s1, ok := srv2.sessionRegistry.Get("replay-live-session")
	if !ok || s1.Ended {
		t.Errorf("live session not replayed correctly: ok=%v ended=%v", ok, s1 != nil && s1.Ended)
	}
	s2, ok := srv2.sessionRegistry.Get("replay-ended-session")
	var endReason string
	if s2 != nil {
		endReason = s2.EndReason
	}
	if !ok || s2 == nil || !s2.Ended || endReason != "test-end" {
		t.Errorf("ended session not replayed: ok=%v ended=%v reason=%q",
			ok, s2 != nil && s2.Ended, endReason)
	}
	if got := srv2.handoffRegistry.Len(); got != 1 {
		t.Errorf("replay handoff count = %d, want 1", got)
	}
	h, ok := srv2.handoffRegistry.Get(offer.HandoffID)
	var hState string
	if h != nil {
		hState = h.State
	}
	if !ok || h == nil || hState != HandoffStateOpen {
		t.Errorf("handoff not replayed as open: ok=%v state=%q", ok, hState)
	}
}

// ─── 14. TestClaimRejectedEventEmitted (amendment #4) ────────────────────────

func TestClaimRejectedEventEmitted(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	// Post an offer; claim it once; then try to claim again. The second
	// attempt must 409 AND write a handoff.claim_rejected event with the
	// conflicting_session field populated.
	offerResp := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "first-offerer-here",
		"bootstrap_prompt": "bp",
		"task":             map[string]any{"title": "T", "goal": "G", "next_steps": []any{"s"}},
	})
	var offer handoffWriteResponse
	decodeJSON(t, offerResp, &offer)

	// First claim wins.
	r1 := postJSON(t, ts.URL+"/v1/handoffs/"+offer.HandoffID+"/claim",
		map[string]any{"claiming_session": "first-winner-abc"})
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first claim: %d", r1.StatusCode)
	}

	// Second attempt loses.
	r2 := postJSON(t, ts.URL+"/v1/handoffs/"+offer.HandoffID+"/claim",
		map[string]any{"claiming_session": "second-loser-xyz"})
	r2.Body.Close()
	if r2.StatusCode != http.StatusConflict {
		t.Fatalf("second claim: %d, want 409", r2.StatusCode)
	}

	events, _ := srv.busSessions.ReadEvents(BusHandoffs)
	var rej *BusBlock
	for i := range events {
		if events[i].Type == EvtHandoffClaimRejected {
			rej = &events[i]
			break
		}
	}
	if rej == nil {
		t.Fatal("no claim_rejected event emitted")
	}
	if r, _ := rej.Payload["reason"].(string); r != string(ClaimRejectedAlreadyClaimed) {
		t.Errorf("reason = %q, want %q", r, ClaimRejectedAlreadyClaimed)
	}
	if cs, _ := rej.Payload["conflicting_session"].(string); cs != "first-winner-abc" {
		t.Errorf("conflicting_session = %q, want first-winner-abc", cs)
	}
	if as, _ := rej.Payload["attempting_session"].(string); as != "second-loser-xyz" {
		t.Errorf("attempting_session = %q, want second-loser-xyz", as)
	}
}

// ─── 15. TestMCP_HandoffRoundTrip — integration test (Phase 2) ─────────────

// TestMCP_HandoffRoundTrip exercises the 8 cog_* MCP tools end-to-end:
// register two sessions → one offers a handoff → the other claims it →
// claim emits the offer payload back → complete cycles the state to
// HandoffStateCompleted. Round trips the actual bus through the same
// registries the HTTP surface uses.
func TestMCP_HandoffRoundTrip(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cfg := &Config{WorkspaceRoot: root, CogDir: root + "/.cog", Port: 0, WriteRouteGrantAuthDisabled: true}
	proc := NewProcess(cfg, &Nucleus{Name: "test"})
	mcpSrv := NewMCPServer(cfg, &Nucleus{Name: "test"}, proc)
	bus := NewBusSessionManager(root)
	sessions := NewSessionRegistry()
	handoffs := NewHandoffRegistry()
	mcpSrv.SetSessionsBackend(bus, sessions, handoffs)

	ctx := mcpTestCtx(t)

	// 1. Register two sessions via the MCP tool.
	reg1Result, _, err := mcpSrv.toolRegisterSession(ctx, nil, registerSessionInput{
		SessionID: "mcp-src-session", Workspace: "w", Role: "r",
	})
	if err != nil || reg1Result == nil {
		t.Fatalf("register src: %v", err)
	}
	reg2Result, _, err := mcpSrv.toolRegisterSession(ctx, nil, registerSessionInput{
		SessionID: "mcp-dst-session", Workspace: "w", Role: "r",
	})
	if err != nil || reg2Result == nil {
		t.Fatalf("register dst: %v", err)
	}

	// 2. src offers a handoff.
	offerResult, _, err := mcpSrv.toolOfferHandoff(ctx, nil, offerHandoffInput{
		FromSession:     "mcp-src-session",
		BootstrapPrompt: "bp",
		Task: map[string]interface{}{
			"title": "T", "goal": "G", "next_steps": []interface{}{"step1"},
		},
	})
	if err != nil || offerResult == nil {
		t.Fatalf("offer: %v", err)
	}
	var offerResp struct {
		OK        bool   `json:"ok"`
		HandoffID string `json:"handoff_id"`
	}
	decodeMCPJSON(t, offerResult, &offerResp)
	if !offerResp.OK || offerResp.HandoffID == "" {
		t.Fatalf("offer payload: %+v", offerResp)
	}

	// 3. dst claims it.
	claimResult, _, err := mcpSrv.toolClaimHandoff(ctx, nil, claimHandoffInput{
		HandoffID: offerResp.HandoffID, ClaimingSession: "mcp-dst-session",
	})
	if err != nil || claimResult == nil {
		t.Fatalf("claim: %v", err)
	}
	var claimResp struct {
		OK        bool                   `json:"ok"`
		HandoffID string                 `json:"handoff_id"`
		Offer     map[string]interface{} `json:"offer"`
	}
	decodeMCPJSON(t, claimResult, &claimResp)
	if !claimResp.OK || claimResp.HandoffID != offerResp.HandoffID {
		t.Fatalf("claim resp: %+v", claimResp)
	}
	if claimResp.Offer == nil || claimResp.Offer["bootstrap_prompt"] != "bp" {
		t.Errorf("claim did not surface bootstrap_prompt: %+v", claimResp.Offer)
	}

	// 4. dst completes the handoff.
	compResult, _, err := mcpSrv.toolCompleteHandoff(ctx, nil, completeHandoffInput{
		HandoffID: offerResp.HandoffID, CompletingSession: "mcp-dst-session",
		Outcome: "success", Notes: "roundtrip done",
	})
	if err != nil || compResult == nil {
		t.Fatalf("complete: %v", err)
	}
	var compResp struct {
		OK bool `json:"ok"`
	}
	decodeMCPJSON(t, compResult, &compResp)
	if !compResp.OK {
		t.Errorf("complete not ok: %+v", compResp)
	}

	// Verify bus has the full chain: 1 offer + 1 claim + 1 complete.
	events, _ := bus.ReadEvents(BusHandoffs)
	var types []string
	for _, e := range events {
		types = append(types, e.Type)
	}
	wantSeq := []string{EvtHandoffOffer, EvtHandoffClaim, EvtHandoffComplete}
	if len(events) != 3 {
		t.Fatalf("bus chain length = %d, want 3: %v", len(events), types)
	}
	for i, e := range events {
		if e.Type != wantSeq[i] {
			t.Errorf("event[%d].type = %q, want %q", i, e.Type, wantSeq[i])
		}
	}

	// In-memory state: handoff should be in completed state.
	h, ok := handoffs.Get(offerResp.HandoffID)
	if !ok || h.State != HandoffStateCompleted {
		var state string
		if h != nil {
			state = h.State
		}
		t.Errorf("final handoff state = %q (ok=%v), want completed", state, ok)
	}
}

// mcpTestCtx returns a background context with t.Deadline if set.
func mcpTestCtx(t *testing.T) context.Context {
	ctx := t.Context()
	return ctx
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func jsonReadAll(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	b, err := readAll(resp.Body)
	return string(b), err
}

func readAll(r interface {
	Read(p []byte) (int, error)
}) ([]byte, error) {
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 512)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			if err.Error() == "EOF" {
				return buf, nil
			}
			return buf, err
		}
	}
}

// ─── 16. TestRegistryUnchangedOnBusAppendFailure ─────────────────────────────
//
// Critical #1 from the codex review: the bus is ground truth, so a failed
// AppendEvent must leave the derived in-memory registry untouched. We verify
// this at the registry level for every mutating path: register, heartbeat,
// end, offer, claim, complete. Each calls the Apply* method with an appendFn
// that always errors; we then assert the registry state is identical to what
// it was before the call.
func TestRegistryUnchangedOnBusAppendFailure(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	appendErr := errors.New("simulated bus append failure")
	// failFn matches ApplyRegister's updated signature: func(time.Time) error.
	failFn := func(_ time.Time) error { return appendErr }
	// failFnSimple matches ApplyHeartbeat/ApplyEnd/ApplyOffer/ApplyClaim/ApplyComplete: func() error.
	failFnSimple := func() error { return appendErr }

	t.Run("register on empty registry stays empty", func(t *testing.T) {
		reg := NewSessionRegistry()
		state := SessionState{
			SessionID: "bus-fail-register", Workspace: "w", Role: "r",
			RegisteredAt: now, LastSeen: now,
		}
		stored, created, err := reg.ApplyRegister(state, time.Minute, now, failFn)
		if err == nil {
			t.Fatal("ApplyRegister returned nil err despite failing appendFn")
		}
		if stored != nil || created {
			t.Errorf("ApplyRegister leaked state on error: stored=%v created=%v", stored, created)
		}
		if reg.Len() != 0 {
			t.Errorf("registry has %d rows after failed register, want 0", reg.Len())
		}
	})

	t.Run("register update on existing row preserves prior state", func(t *testing.T) {
		reg := NewSessionRegistry()
		state := SessionState{
			SessionID: "bus-fail-reupdate", Workspace: "w", Role: "r", Task: "first",
			RegisteredAt: now, LastSeen: now,
		}
		if _, _, err := reg.ApplyRegister(state, time.Minute, now, nil); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		before, _ := reg.Get("bus-fail-reupdate")

		state.Task = "second"
		_, _, err := reg.ApplyRegister(state, time.Minute, now, failFn)
		if err == nil {
			t.Fatal("expected error from failFn")
		}
		after, _ := reg.Get("bus-fail-reupdate")
		if after.Task != before.Task {
			t.Errorf("task mutated on failed re-register: before=%q after=%q", before.Task, after.Task)
		}
	})

	t.Run("heartbeat does not bump LastSeen on append failure", func(t *testing.T) {
		reg := NewSessionRegistry()
		state := SessionState{
			SessionID: "bus-fail-heartbeat", Workspace: "w", Role: "r",
			RegisteredAt: now, LastSeen: now,
		}
		if _, _, err := reg.ApplyRegister(state, time.Minute, now, nil); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		before, _ := reg.Get("bus-fail-heartbeat")
		later := now.Add(30 * time.Second)
		_, ok, err := reg.ApplyHeartbeat("bus-fail-heartbeat", 0.42, "busy", "task", later, failFnSimple)
		if !ok {
			t.Fatal("heartbeat saw unregistered session")
		}
		if err == nil {
			t.Fatal("expected err from failFn")
		}
		after, _ := reg.Get("bus-fail-heartbeat")
		if !after.LastSeen.Equal(before.LastSeen) {
			t.Errorf("LastSeen mutated on failed heartbeat: before=%v after=%v",
				before.LastSeen, after.LastSeen)
		}
		if after.Status != "" || after.CurrentTask != "" || after.ContextUsage != 0 {
			t.Errorf("status/task/usage mutated on failed heartbeat: %+v", after)
		}
	})

	t.Run("end does not set Ended on append failure", func(t *testing.T) {
		reg := NewSessionRegistry()
		state := SessionState{
			SessionID: "bus-fail-endsess", Workspace: "w", Role: "r",
			RegisteredAt: now, LastSeen: now,
		}
		if _, _, err := reg.ApplyRegister(state, time.Minute, now, nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, known, err := reg.ApplyEnd("bus-fail-endsess", "because", "", now.Add(time.Second), failFnSimple)
		if !known {
			t.Fatal("end saw unknown session")
		}
		if err == nil {
			t.Fatal("expected err from failFn")
		}
		after, _ := reg.Get("bus-fail-endsess")
		if after.Ended {
			t.Error("session was marked Ended despite failed append")
		}
	})

	t.Run("offer does not install row on append failure", func(t *testing.T) {
		hreg := NewHandoffRegistry()
		h := HandoffState{
			HandoffID: "ho-fail-offer-1", FromSession: "a-b-c",
			TTLSeconds: 60, CreatedAt: now,
		}
		_, err := hreg.ApplyOffer(h, now, failFnSimple)
		if err == nil {
			t.Fatal("expected err from failFn")
		}
		if hreg.Len() != 0 {
			t.Errorf("handoff registry has %d rows after failed offer, want 0", hreg.Len())
		}
	})

	t.Run("claim does not transition state on append failure", func(t *testing.T) {
		hreg := NewHandoffRegistry()
		h := HandoffState{
			HandoffID: "ho-fail-claim-1", FromSession: "a-b-c",
			TTLSeconds: 60, CreatedAt: now,
		}
		if _, err := hreg.ApplyOffer(h, now, nil); err != nil {
			t.Fatalf("seed offer: %v", err)
		}
		result, err := hreg.ApplyClaim("ho-fail-claim-1", "claimant-x-y", now, failFnSimple)
		if err == nil {
			t.Fatal("expected appendErr from failFn")
		}
		if result.Rejection != "" {
			t.Errorf("unexpected rejection reason: %q", result.Rejection)
		}
		after, _ := hreg.Get("ho-fail-claim-1")
		if after.State != HandoffStateOpen {
			t.Errorf("handoff state = %q, want %q (unchanged on failed claim)",
				after.State, HandoffStateOpen)
		}
		if after.ClaimingSession != "" {
			t.Errorf("ClaimingSession set to %q on failed claim", after.ClaimingSession)
		}
	})

	t.Run("complete does not transition state on append failure", func(t *testing.T) {
		hreg := NewHandoffRegistry()
		h := HandoffState{
			HandoffID: "ho-fail-complete-1", FromSession: "a-b-c",
			TTLSeconds: 60, CreatedAt: now,
		}
		if _, err := hreg.ApplyOffer(h, now, nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, err := hreg.ApplyClaim("ho-fail-complete-1", "claimant-x-y", now, nil); err != nil {
			t.Fatalf("seed claim: %v", err)
		}
		_, reason, err := hreg.ApplyComplete("ho-fail-complete-1",
			"claimant-x-y", "ok", "notes", "", now.Add(time.Second), failFnSimple)
		if err == nil {
			t.Fatal("expected appendErr from failFn")
		}
		if reason != "" {
			t.Errorf("got rejection reason %q on append-failure path", reason)
		}
		after, _ := hreg.Get("ho-fail-complete-1")
		if after.State != HandoffStateClaimed {
			t.Errorf("handoff state = %q, want %q (unchanged on failed complete)",
				after.State, HandoffStateClaimed)
		}
		if after.CompletingSession != "" {
			t.Errorf("CompletingSession set on failed complete: %q", after.CompletingSession)
		}
	})
}

// ─── 17. TestHeartbeatOnEndedSessionDoesNotMutate ────────────────────────────
//
// Critical #2 from the codex review: a heartbeat against an ended session
// must 409 AND leave LastSeen + optional fields untouched. HTTP surface path.
func TestHeartbeatOnEndedSessionDoesNotMutate(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	// Register, then end.
	postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "ended-nohb-session", "workspace": "w", "role": "r",
	}).Body.Close()
	postJSON(t, ts.URL+"/v1/sessions/ended-nohb-session/end",
		map[string]any{"reason": "goodbye"}).Body.Close()

	before, ok := srv.sessionRegistry.Get("ended-nohb-session")
	if !ok {
		t.Fatal("post-end registry lookup failed")
	}
	lastSeenBefore := before.LastSeen

	// Count bus events before the heartbeat attempt so we can verify none
	// were appended.
	eventsBefore, _ := srv.busSessions.ReadEvents(BusSessions)
	heartbeatCountBefore := 0
	for _, e := range eventsBefore {
		if e.Type == EvtSessionHeartbeat {
			heartbeatCountBefore++
		}
	}

	// Heartbeat against the ended session.
	resp := postJSON(t, ts.URL+"/v1/sessions/ended-nohb-session/heartbeat",
		map[string]any{
			"status": "definitely-not-dead", "context_usage": 0.99,
			"current_task": "ghost-task",
		})
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}

	after, _ := srv.sessionRegistry.Get("ended-nohb-session")
	if !after.LastSeen.Equal(lastSeenBefore) {
		t.Errorf("LastSeen mutated on 409 heartbeat: before=%v after=%v",
			lastSeenBefore, after.LastSeen)
	}
	if after.Status == "definitely-not-dead" {
		t.Errorf("Status was mutated to %q on 409 heartbeat", after.Status)
	}
	if after.ContextUsage == 0.99 {
		t.Errorf("ContextUsage was mutated to %v on 409 heartbeat", after.ContextUsage)
	}
	if after.CurrentTask == "ghost-task" {
		t.Errorf("CurrentTask was mutated on 409 heartbeat")
	}

	// And no heartbeat event should have made it to the bus.
	eventsAfter, _ := srv.busSessions.ReadEvents(BusSessions)
	heartbeatCountAfter := 0
	for _, e := range eventsAfter {
		if e.Type == EvtSessionHeartbeat {
			heartbeatCountAfter++
		}
	}
	if heartbeatCountAfter != heartbeatCountBefore {
		t.Errorf("heartbeat event appended on 409 path: before=%d after=%d",
			heartbeatCountBefore, heartbeatCountAfter)
	}
}

// ─── 18. TestReplay_EmptyBuses ───────────────────────────────────────────────
//
// Edge: startup against a workspace whose bus_sessions and bus_handoffs
// files don't exist yet. Replay should succeed with empty registries and
// log "events=0" rather than crashing.
func TestReplay_EmptyBuses(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mgr := NewBusSessionManager(root)
	sreg := NewSessionRegistry()
	hreg := NewHandoffRegistry()

	if err := ReplaySessionRegistry(mgr, sreg); err != nil {
		t.Errorf("ReplaySessionRegistry on empty bus: %v", err)
	}
	if err := ReplayHandoffRegistry(mgr, hreg); err != nil {
		t.Errorf("ReplayHandoffRegistry on empty bus: %v", err)
	}
	if sreg.Len() != 0 {
		t.Errorf("session registry len = %d, want 0", sreg.Len())
	}
	if hreg.Len() != 0 {
		t.Errorf("handoff registry len = %d, want 0", hreg.Len())
	}
}

// ─── 19. TestReplay_UnknownEventType ─────────────────────────────────────────
//
// Edge: bus contains an event with a type the replay loop doesn't recognise
// (e.g. from a future schema). Replay must skip it without error and
// continue processing known types.
func TestReplay_UnknownEventType(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mgr := NewBusSessionManager(root)

	// Emit a known session.register event, a forward-compat unknown event,
	// then end the same session. Replay should produce a consistent row
	// with Ended=true, treating the unknown event as a no-op.
	_, err := mgr.AppendEvent(BusSessions, EvtSessionRegister, "repl-unk-session", map[string]interface{}{
		"session_id": "repl-unk-session", "workspace": "w", "role": "r",
	})
	if err != nil {
		t.Fatalf("append register: %v", err)
	}
	_, err = mgr.AppendEvent(BusSessions, "session.future.mystery-event", "repl-unk-session", map[string]interface{}{
		"session_id": "repl-unk-session", "mystery": "field",
	})
	if err != nil {
		t.Fatalf("append unknown: %v", err)
	}
	_, err = mgr.AppendEvent(BusSessions, EvtSessionEnd, "repl-unk-session", map[string]interface{}{
		"session_id": "repl-unk-session", "reason": "done",
	})
	if err != nil {
		t.Fatalf("append end: %v", err)
	}

	reg := NewSessionRegistry()
	if err := ReplaySessionRegistry(mgr, reg); err != nil {
		t.Errorf("replay: %v", err)
	}
	got, ok := reg.Get("repl-unk-session")
	if !ok {
		t.Fatal("registry did not rebuild session")
	}
	if !got.Ended {
		t.Error("session not marked ended after replay with intervening unknown event")
	}
}

// ─── 20. TestReplay_OutOfOrderSeq ────────────────────────────────────────────
//
// Substantive concern from the review: ReadEvents preserves file order.
// This test writes the events.jsonl file with lines in seq order [3,1,2]
// and verifies replay sorts by seq ascending so the final state matches
// what a normal monotonic append produces.
func TestReplay_OutOfOrderSeq(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mgr := NewBusSessionManager(root)

	// Hand-craft 3 events with explicit seqs and write them in reversed-ish
	// order to the jsonl file. Then invoke replay and verify the final
	// state is as though events had been consumed in seq order.
	if err := mgr.EnsureBus(BusSessions); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	evts := []BusBlock{
		{ // seq 3 — end
			V: 2, BusID: BusSessions, Seq: 3,
			Ts:   time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
			From: "order-test-session", Type: EvtSessionEnd,
			Payload: map[string]interface{}{"session_id": "order-test-session", "reason": "r3"},
		},
		{ // seq 1 — register
			V: 2, BusID: BusSessions, Seq: 1,
			Ts:   time.Now().UTC().Format(time.RFC3339Nano),
			From: "order-test-session", Type: EvtSessionRegister,
			Payload: map[string]interface{}{"session_id": "order-test-session", "workspace": "w", "role": "r"},
		},
		{ // seq 2 — heartbeat
			V: 2, BusID: BusSessions, Seq: 2,
			Ts:   time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
			From: "order-test-session", Type: EvtSessionHeartbeat,
			Payload: map[string]interface{}{"session_id": "order-test-session", "status": "working"},
		},
	}
	writeBusLinesForTest(t, mgr.EventsPath(BusSessions), evts)

	reg := NewSessionRegistry()
	if err := ReplaySessionRegistry(mgr, reg); err != nil {
		t.Fatalf("replay: %v", err)
	}
	got, ok := reg.Get("order-test-session")
	if !ok {
		t.Fatal("session not replayed")
	}
	// If replay consumed in file order (end before register), the row
	// wouldn't exist. Sort-by-seq means register is applied first, then
	// heartbeat updates status, then end marks Ended.
	if !got.Ended {
		t.Error("final state should be ended after sort-by-seq replay")
	}
	if got.Status != "working" {
		t.Errorf("status = %q, want %q (from heartbeat seq=2)", got.Status, "working")
	}
}

// ─── 21. TestReplay_DuplicateSeqWithConflictingPayload ───────────────────────
//
// Design decision being codified: ReadEvents de-dupes by seq using
// first-occurrence wins (see seen[block.Seq] check in bus_session.go).
// Even if the file contains two different payloads for the same seq,
// replay should consume exactly one of them — specifically the first.
func TestReplay_DuplicateSeqWithConflictingPayload(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mgr := NewBusSessionManager(root)

	if err := mgr.EnsureBus(BusSessions); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	evts := []BusBlock{
		{ // seq 1, first occurrence — wins per first-write-wins de-dup
			V: 2, BusID: BusSessions, Seq: 1,
			Ts:   time.Now().UTC().Format(time.RFC3339Nano),
			From: "dup-test-session", Type: EvtSessionRegister,
			Payload: map[string]interface{}{
				"session_id": "dup-test-session", "workspace": "w", "role": "r",
				"task": "FIRST",
			},
		},
		{ // seq 1, second occurrence with conflicting payload — should be ignored
			V: 2, BusID: BusSessions, Seq: 1,
			Ts:   time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
			From: "dup-test-session", Type: EvtSessionRegister,
			Payload: map[string]interface{}{
				"session_id": "dup-test-session", "workspace": "w2", "role": "r2",
				"task": "SECOND",
			},
		},
	}
	writeBusLinesForTest(t, mgr.EventsPath(BusSessions), evts)

	reg := NewSessionRegistry()
	if err := ReplaySessionRegistry(mgr, reg); err != nil {
		t.Fatalf("replay: %v", err)
	}
	got, _ := reg.Get("dup-test-session")
	if got == nil {
		t.Fatal("session not replayed")
	}
	if got.Task != "FIRST" {
		t.Errorf("duplicate-seq policy: task = %q, want FIRST (first-occurrence wins)", got.Task)
	}
	if got.Workspace != "w" {
		t.Errorf("duplicate-seq policy: workspace = %q, want w", got.Workspace)
	}
}

// ─── 22. TestReplay_CompleteWithoutClaim ─────────────────────────────────────
//
// Edge: bus_handoffs contains an orphaned complete with no prior claim
// (possible after a crash between claim and complete — bus append of claim
// landed, complete landed, but some intermediate state was truncated). The
// replay state machine should reject the complete (it sees state != claimed)
// and leave the handoff in its earlier state.
func TestReplay_CompleteWithoutClaim(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mgr := NewBusSessionManager(root)

	_, _ = mgr.AppendEvent(BusHandoffs, EvtHandoffOffer, "from-a-b-c", map[string]interface{}{
		"handoff_id":   "ho-orphan-1",
		"from_session": "from-a-b-c",
		"ttl_seconds":  600.0,
	})
	// Skip claim on purpose.
	_, _ = mgr.AppendEvent(BusHandoffs, EvtHandoffComplete, "completer-x-y-z", map[string]interface{}{
		"handoff_id":         "ho-orphan-1",
		"completing_session": "completer-x-y-z",
		"outcome":            "orphan",
	})

	reg := NewHandoffRegistry()
	if err := ReplayHandoffRegistry(mgr, reg); err != nil {
		t.Fatalf("replay: %v", err)
	}
	got, ok := reg.Get("ho-orphan-1")
	if !ok {
		t.Fatal("handoff not replayed at all")
	}
	if got.State != HandoffStateOpen {
		t.Errorf("state = %q, want %q (orphan complete should not transition)",
			got.State, HandoffStateOpen)
	}
	if got.CompletingSession != "" {
		t.Errorf("CompletingSession = %q, should be empty on orphan-complete skip",
			got.CompletingSession)
	}
}

// ─── 23. TestSessionRegister_TwoComponentIDRejected ──────────────────────────
//
// Spec drift fix: the regex must reject 2-component IDs like "a-b", per
// the design doc's ^[a-z0-9]+-[a-z0-9-]+-[a-z0-9-]+$.
func TestSessionRegister_TwoComponentIDRejected(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	resp := postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "a-b", "workspace": "w", "role": "r",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("2-component id: status = %d, want 400", resp.StatusCode)
	}

	// And the direct validator check, belt-and-suspenders.
	if err := ValidateSessionID("a-b"); err == nil {
		t.Error("ValidateSessionID(\"a-b\") returned nil; want error")
	}
	if err := ValidateSessionID("a-b-c"); err != nil {
		t.Errorf("ValidateSessionID(\"a-b-c\") returned err %v; want nil", err)
	}
	if err := ValidateSessionID("node-a-laptop-cogos-gap-closure"); err != nil {
		t.Errorf("ValidateSessionID on multi-component id: %v", err)
	}
}

// ─── 24. TestHandoffOffer_NegativeTTLRejected ────────────────────────────────
//
// Validation gap: ttl_seconds: -1 should return 400, not silently apply
// (which in the prior code path meant "no TTL enforcement" — a potential
// footgun where an offer never expires).
func TestHandoffOffer_NegativeTTLRejected(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	resp := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"from_session":     "neg-ttl-offer-a",
		"bootstrap_prompt": "bp",
		"ttl_seconds":      -1,
		"task": map[string]any{
			"title": "T", "goal": "G", "next_steps": []any{"s"},
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("negative ttl: status = %d, want 400", resp.StatusCode)
	}
}

// ─── 25. TestHandoffOffer_CallerSuppliedIDRejected ───────────────────────────
//
// Design-doc divergence fix: the kernel ALWAYS mints the handoff_id. A
// caller-supplied one is a 400 — this prevents malformed IDs landing in
// path-based claim/complete routes.
func TestHandoffOffer_CallerSuppliedIDRejected(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	resp := postJSON(t, ts.URL+"/v1/handoffs/offer", map[string]any{
		"handoff_id":       "callersupplied-12345",
		"from_session":     "caller-id-offer-a",
		"bootstrap_prompt": "bp",
		"task": map[string]any{
			"title": "T", "goal": "G", "next_steps": []any{"s"},
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("caller-supplied id: status = %d, want 400", resp.StatusCode)
	}
}

// ─── 26. TestRoutes_PresenceDoesNotShadowContext ─────────────────────────────
//
// Route-coexistence proof: POST /v1/sessions/register, GET
// /v1/sessions/presence, and the legacy GET /v1/sessions/{id}/context all
// coexist on the same mux. Fire them back-to-back and check each returns
// its expected semantics without cross-contamination.
func TestRoutes_PresenceDoesNotShadowContext(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	// 1. Register a session on the new route.
	reg := postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "route-coexist-session", "workspace": "w", "role": "r",
	})
	reg.Body.Close()
	if reg.StatusCode != http.StatusOK {
		t.Fatalf("register: %d", reg.StatusCode)
	}

	// 2. GET /v1/sessions/presence — the new route.
	respP, err := http.Get(ts.URL + "/v1/sessions/presence")
	if err != nil {
		t.Fatalf("GET presence: %v", err)
	}
	respP.Body.Close()
	if respP.StatusCode != http.StatusOK {
		t.Errorf("presence: %d, want 200", respP.StatusCode)
	}

	// 3. GET /v1/sessions/{id}/context — the legacy TAA-inference route
	//    registered by serve_bus.go / serve_sessions.go. This should still
	//    resolve to the legacy handler even though /presence sits on the
	//    same prefix. Expect 404 for an unknown session (context store is
	//    independent of the kernel-native registry) — the important thing
	//    is the handler RESPONDS rather than the path being shadowed by
	//    /presence.
	respC, err := http.Get(ts.URL + "/v1/sessions/route-coexist-session/context")
	if err != nil {
		t.Fatalf("GET context: %v", err)
	}
	defer respC.Body.Close()
	if respC.StatusCode >= 500 {
		t.Errorf("context route shadowed/broken: status = %d", respC.StatusCode)
	}
	// Accept any 2xx/4xx — the legacy context handler has its own body
	// format; we're only proving the route is still dispatched to it.
}

// writeBusLinesForTest writes hash-chained BusBlocks to the events file in
// the slice's order, computing each block's Hash so that ReadEvents will
// parse them. Used by replay tests that need to plant deliberately-
// out-of-order or duplicate-seq lines on disk.
func writeBusLinesForTest(t *testing.T, path string, evts []BusBlock) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	for i := range evts {
		evts[i].V = 2
		evts[i].Hash = computeBusBlockHash(&evts[i])
		line, err := json.Marshal(&evts[i])
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
}

// ─── 27. TestRegister_PreservesRegisteredAtAcrossReRegister (#44) ────────────
//
// Regression guard for issue #44: on re-register of an active session the
// bus event payload must carry the ORIGINAL registered_at (T1), not the
// current wall-clock time (T2). Before the fix, the payload was baked with
// `now` before ApplyRegister had a chance to preserve the lineage, so the
// bus and the in-memory SessionState disagreed. After a restart the registry
// would replay from the bus and lose T1.
//
// Asserts:
//  1. First registration → in-memory RegisteredAt = T1.
//  2. Re-registration → in-memory RegisteredAt still = T1 (preserved).
//  3. Re-registration → bus event seq=2 payload.registered_at = T1 (the fix).
//  4. Cold-restart replay → replayed RegisteredAt = T1 (no lineage loss).
func TestRegister_PreservesRegisteredAtAcrossReRegister(t *testing.T) {
	t.Parallel()
	srv, ts := newSessionsTestServer(t)

	// First register — capture the initial registered_at from the response.
	r1 := postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "lineage-test-session",
		"workspace":  "w",
		"role":       "r",
		"task":       "initial",
	})
	var resp1 sessionWriteResponse
	decodeJSON(t, r1, &resp1)
	if !resp1.OK {
		t.Fatalf("first register failed: %+v", resp1)
	}
	t1 := resp1.Session.RegisteredAt

	// Pause to ensure wall clock advances (sub-millisecond races on fast machines).
	time.Sleep(5 * time.Millisecond)

	// Re-register the same session.
	r2 := postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": "lineage-test-session",
		"workspace":  "w",
		"role":       "r",
		"task":       "updated",
	})
	var resp2 sessionWriteResponse
	decodeJSON(t, r2, &resp2)
	if !resp2.OK {
		t.Fatalf("re-register failed: %+v", resp2)
	}

	// Assert 1: in-memory RegisteredAt is preserved.
	if !resp2.Session.RegisteredAt.Equal(t1) {
		t.Errorf("in-memory RegisteredAt changed on re-register: was %v, got %v (want T1 preserved)",
			t1, resp2.Session.RegisteredAt)
	}

	// Assert 2: bus seq=2 payload.registered_at = T1 (the fix for #44).
	events, err := srv.busSessions.ReadEvents(BusSessions)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected >=2 bus events, got %d", len(events))
	}
	var reRegEvt *BusBlock
	for i := range events {
		if events[i].Seq == 2 {
			reRegEvt = &events[i]
			break
		}
	}
	if reRegEvt == nil {
		t.Fatalf("no seq=2 event on bus")
	}
	busRA, _ := reRegEvt.Payload["registered_at"].(string)
	if busRA == "" {
		t.Fatalf("seq=2 event payload missing registered_at field")
	}
	busRATime, parseErr := time.Parse(time.RFC3339Nano, busRA)
	if parseErr != nil {
		t.Fatalf("parse bus registered_at %q: %v", busRA, parseErr)
	}
	if !busRATime.Equal(t1) {
		t.Errorf("bus seq=2 registered_at = %v, want T1=%v (lineage not preserved in payload; refs #44)",
			busRATime, t1)
	}

	// Assert 3: cold-restart replay produces RegisteredAt = T1.
	root := srv.busSessions.WorkspaceRoot()
	freshMgr := NewBusSessionManager(root)
	freshReg := NewSessionRegistry()
	if replayErr := ReplaySessionRegistry(freshMgr, freshReg); replayErr != nil {
		t.Fatalf("replay: %v", replayErr)
	}
	replayed, ok := freshReg.Get("lineage-test-session")
	if !ok {
		t.Fatal("session not found after replay")
	}
	if !replayed.RegisteredAt.Equal(t1) {
		t.Errorf("replayed RegisteredAt = %v, want T1=%v (lineage lost after restart; refs #44)",
			replayed.RegisteredAt, t1)
	}
}

// ─── 28. TestRegisterThenListRoundTrip (#154) ─────────────────────────────────
//
// Regression guard for issue #154: POST /v1/sessions/register succeeds and the
// bus event lands correctly, but GET /v1/sessions and GET /v1/peer-awareness
// both returned empty because handleListSessions was reading s.sessions (the
// TAA inference-context store) instead of s.sessionRegistry (the bus-backed
// kernel-native registry). After the fix, GET /v1/sessions is wired to
// handleSessionPresence which reads s.sessionRegistry directly.
//
// Asserts:
//  1. Register returns 200 with created=true.
//  2. GET /v1/sessions immediately returns count=1 and includes the session.
//  3. GET /v1/sessions?include_ended=true also returns the session.
//  4. GET /v1/sessions?active_within_seconds=3600 also returns the session.
//  5. GET /v1/peer-awareness?sid=<id>&window=24h returns 200 (not a dead endpoint).
func TestRegisterThenListRoundTrip(t *testing.T) {
	t.Parallel()
	_, ts := newSessionsTestServer(t)

	sid := "repro-bug-154-abc"

	// 1. Register — must succeed.
	regResp := postJSON(t, ts.URL+"/v1/sessions/register", map[string]any{
		"session_id": sid,
		"workspace":  "/tmp/test-154",
		"role":       "claude-code",
		"status":     "active",
	})
	var regBody sessionWriteResponse
	decodeJSON(t, regResp, &regBody)
	if regResp.StatusCode != http.StatusOK || !regBody.OK || !regBody.Created {
		t.Fatalf("register: status=%d ok=%v created=%v", regResp.StatusCode, regBody.OK, regBody.Created)
	}

	// 2. GET /v1/sessions — must return the just-registered session.
	listResp, err := http.Get(ts.URL + "/v1/sessions")
	if err != nil {
		t.Fatalf("GET /v1/sessions: %v", err)
	}
	var listBody struct {
		Count    int `json:"count"`
		Sessions []struct {
			SessionID string `json:"session_id"`
			Active    bool   `json:"active"`
		} `json:"sessions"`
	}
	decodeJSON(t, listResp, &listBody)
	if listBody.Count != 1 {
		t.Fatalf("GET /v1/sessions count = %d, want 1 (fix #154)", listBody.Count)
	}
	if len(listBody.Sessions) == 0 || listBody.Sessions[0].SessionID != sid {
		t.Errorf("GET /v1/sessions returned unexpected sessions: %+v", listBody.Sessions)
	}
	if !listBody.Sessions[0].Active {
		t.Errorf("just-registered session should be active, got active=false")
	}

	// 3. include_ended=true — still returns the session.
	listEndedResp, err := http.Get(ts.URL + "/v1/sessions?include_ended=true")
	if err != nil {
		t.Fatalf("GET /v1/sessions?include_ended=true: %v", err)
	}
	var endedBody struct {
		Count int `json:"count"`
	}
	decodeJSON(t, listEndedResp, &endedBody)
	if endedBody.Count != 1 {
		t.Errorf("GET /v1/sessions?include_ended=true count = %d, want 1", endedBody.Count)
	}

	// 4. active_within_seconds=3600 — still returns the session.
	listWindowResp, err := http.Get(ts.URL + "/v1/sessions?active_within_seconds=3600")
	if err != nil {
		t.Fatalf("GET /v1/sessions?active_within_seconds=3600: %v", err)
	}
	var windowBody struct {
		Count int `json:"count"`
	}
	decodeJSON(t, listWindowResp, &windowBody)
	if windowBody.Count != 1 {
		t.Errorf("GET /v1/sessions?active_within_seconds=3600 count = %d, want 1", windowBody.Count)
	}

	// 5. GET /v1/peer-awareness?sid=<sid>&window=24h — must return 200 (not
	//    empty-endpoint) even when there are no activity events for this sid.
	//    The "anti_echo_mvp" note in the response is the canonical signal that
	//    the endpoint ran to completion.
	peerURL := ts.URL + "/v1/peer-awareness?sid=" + sid + "&window=24h&budget=1500"
	peerResp, err := http.Get(peerURL)
	if err != nil {
		t.Fatalf("GET /v1/peer-awareness: %v", err)
	}
	var peerBody struct {
		Packet string   `json:"packet"`
		Notes  []string `json:"notes"`
	}
	decodeJSON(t, peerResp, &peerBody)
	if peerResp.StatusCode != http.StatusOK {
		t.Errorf("peer-awareness status = %d, want 200", peerResp.StatusCode)
	}
	foundNote := false
	for _, n := range peerBody.Notes {
		if n == "anti_echo_mvp" {
			foundNote = true
		}
	}
	if !foundNote {
		t.Errorf("peer-awareness did not return anti_echo_mvp note: notes=%v", peerBody.Notes)
	}
}

// ─── 27. TestSessionRegistry_ReapStale ───────────────────────────────────────
//
// Regression guard for cogos#423: sessions that never get an explicit
// POST /v1/sessions/{id}/end (crash, kill -9, dropped client) must not
// persist with Ended=false forever. Exercises SessionRegistry.ReapStale
// directly against the four cases called out in the issue's fix sketch.
func TestSessionRegistry_ReapStale(t *testing.T) {
	t.Parallel()

	const ttl = time.Hour
	now := time.Now().UTC()

	seed := func(t *testing.T, reg *SessionRegistry, id string, lastSeen time.Time, ended bool) {
		t.Helper()
		state := SessionState{
			SessionID: id, Workspace: "w", Role: "r",
			RegisteredAt: lastSeen, LastSeen: lastSeen,
		}
		if _, _, err := reg.ApplyRegister(state, time.Minute, lastSeen, nil); err != nil {
			t.Fatalf("seed register %s: %v", id, err)
		}
		if ended {
			if _, _, err := reg.ApplyEnd(id, "test-seed", "", lastSeen, nil); err != nil {
				t.Fatalf("seed end %s: %v", id, err)
			}
		}
	}

	t.Run("past-TTL row is ended and emits a bus event", func(t *testing.T) {
		reg := NewSessionRegistry()
		seed(t, reg, "stale-zombie-a", now.Add(-2*ttl), false)

		var appended []string
		reaped := reg.ReapStale(ttl, now, func(id string) error {
			appended = append(appended, id)
			return nil
		})

		if len(reaped) != 1 || reaped[0] != "stale-zombie-a" {
			t.Fatalf("reaped = %v, want [stale-zombie-a]", reaped)
		}
		if len(appended) != 1 || appended[0] != "stale-zombie-a" {
			t.Fatalf("appendFn calls = %v, want one call for stale-zombie-a", appended)
		}
		row, ok := reg.Get("stale-zombie-a")
		if !ok {
			t.Fatal("row disappeared after reap")
		}
		if !row.Ended {
			t.Error("row.Ended = false, want true after reap")
		}
		if row.EndReason != sessionReapEndReason {
			t.Errorf("row.EndReason = %q, want %q", row.EndReason, sessionReapEndReason)
		}
	})

	t.Run("within-TTL row is left untouched", func(t *testing.T) {
		reg := NewSessionRegistry()
		seed(t, reg, "fresh-session-b", now.Add(-ttl/2), false)

		reaped := reg.ReapStale(ttl, now, func(string) error {
			t.Fatal("appendFn should not be called for a fresh row")
			return nil
		})

		if len(reaped) != 0 {
			t.Fatalf("reaped = %v, want none", reaped)
		}
		row, _ := reg.Get("fresh-session-b")
		if row.Ended {
			t.Error("fresh row was ended by the reaper")
		}
	})

	t.Run("already-ended row is skipped", func(t *testing.T) {
		reg := NewSessionRegistry()
		seed(t, reg, "already-done-c", now.Add(-2*ttl), true)

		reaped := reg.ReapStale(ttl, now, func(string) error {
			t.Fatal("appendFn should not be called for an already-ended row")
			return nil
		})

		if len(reaped) != 0 {
			t.Fatalf("reaped = %v, want none (already ended)", reaped)
		}
	})

	t.Run("appendFn failure leaves the row unmutated", func(t *testing.T) {
		reg := NewSessionRegistry()
		seed(t, reg, "bus-append-fails-d", now.Add(-2*ttl), false)
		before, _ := reg.Get("bus-append-fails-d")

		appendErr := errors.New("simulated bus append failure")
		reaped := reg.ReapStale(ttl, now, func(string) error { return appendErr })

		if len(reaped) != 0 {
			t.Fatalf("reaped = %v, want none on appendFn failure", reaped)
		}
		after, _ := reg.Get("bus-append-fails-d")
		if after.Ended {
			t.Error("row was ended despite appendFn failure")
		}
		if !after.LastSeen.Equal(before.LastSeen) {
			t.Errorf("LastSeen mutated on appendFn failure: before=%v after=%v",
				before.LastSeen, after.LastSeen)
		}
	})
}
