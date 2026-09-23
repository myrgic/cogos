package acp

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The two questions this file exists to answer, per the L1 spike brief:
//
//   - does SIGINT leave a RESUMABLE session (`claude --resume <id>` still
//     works afterward)?
//   - same question for stdin-close?
//
// Both require a live, authenticated `claude` subprocess. As of 2026-08-28
// these ran for real, after the operator ran `claude /login` to clear the
// dead-refresh-token OAuth condition that had every `claude --print`
// invocation failing earlier the same day. See testdata/README.md for the
// verdicts.

func skipUnlessLiveClaudeAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("claude"); err != nil {
		t.Skip("claude binary not on PATH")
	}
	// OAuth fixed 2026-08-28 (operator ran `claude /login`); the hard
	// skip that lived here is removed. Live resumability tests run for
	// real now.
}

func resumabilityCheck(t *testing.T, sessionID string) (resumable bool, replyText string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resumed, err := Spawn(ctx, SpawnOpts{SessionID: sessionID, ResumeExisting: true})
	if err != nil {
		t.Logf("resume spawn failed: %v", err)
		return false, ""
	}
	if err := resumed.Send(NewTextPrompt("Reply with exactly: RESUMED")); err != nil {
		t.Logf("resume send failed: %v", err)
		return false, ""
	}
	if err := resumed.CloseInput(); err != nil {
		t.Logf("resume close-input: %v", err)
	}

	var text string
	for ev := range resumed.Events() {
		if ev.Result != nil {
			text = ev.Result.Result
		}
		if ev.Assistant != nil {
			for _, c := range ev.Assistant.Message.Content {
				if c.Type == "text" && c.Text != "" {
					text = c.Text
				}
			}
		}
	}
	_ = resumed.Wait()
	return strings.Contains(strings.ToUpper(text), "RESUMED"), text
}

func TestCancel_SIGINT_ResumabilityAfter_LiveClaude(t *testing.T) {
	skipUnlessLiveClaudeAvailable(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pinned := freshSessionID()
	sp, err := Spawn(ctx, SpawnOpts{SessionID: pinned, ResumeExisting: false})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// A prompt shaped to force a long generation, so there is real
	// in-flight work to interrupt.
	if err := sp.Send(NewTextPrompt(
		"Write a very long, detailed 1500-word essay about the history of clocks. Do not stop early.",
	)); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Let real generation start before cancelling. Empirically load-bearing:
	// a 2s delay was flaky (3/4 trials landed the SIGINT during
	// subprocess startup / SessionStart-hook execution, before
	// system.init even flushed, producing a truncated ~6-frame capture
	// and an unresumable-looking session on --resume; the 1/4 that
	// happened to land after init resumed cleanly). 5s reliably lands
	// after init and into real generation — see testdata/README.md
	// "SIGINT resumability" for the full writeup.
	time.Sleep(5 * time.Second)
	if err := sp.Cancel(CancelSIGINT); err != nil {
		t.Fatalf("cancel(sigint): %v", err)
	}

	var trailing []Event
	for ev := range sp.Events() {
		trailing = append(trailing, ev)
	}
	t.Logf("frames arriving after SIGINT: %d", len(trailing))
	for _, ev := range trailing {
		if ev.Result != nil {
			t.Logf("  result: subtype=%s is_error=%v", ev.Result.Subtype, ev.Result.IsError)
		}
	}
	if waitErr := sp.Wait(); waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			t.Logf("exit code after SIGINT: %d", exitErr.ExitCode())
		}
	}

	resumable, reply := resumabilityCheck(t, pinned)
	t.Logf("VERDICT SIGINT resumability: resumable=%v reply=%q", resumable, reply)
	if !resumable {
		t.Logf("SIGINT did NOT leave a resumable session (this is a valid, informative result — not necessarily a test failure)")
	}
}

func TestCancel_StdinClose_ResumabilityAfter_LiveClaude(t *testing.T) {
	skipUnlessLiveClaudeAvailable(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pinned := freshSessionID()
	sp, err := Spawn(ctx, SpawnOpts{SessionID: pinned, ResumeExisting: false})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	if err := sp.Send(NewTextPrompt(
		"Write a very long, detailed 1500-word essay about the history of maps. Do not stop early.",
	)); err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(2 * time.Second)
	if err := sp.Cancel(CancelStdinClose); err != nil {
		t.Fatalf("cancel(stdin-close): %v", err)
	}

	var trailing []Event
	for ev := range sp.Events() {
		trailing = append(trailing, ev)
	}
	t.Logf("frames arriving after stdin-close: %d", len(trailing))
	for _, ev := range trailing {
		if ev.Result != nil {
			t.Logf("  result: subtype=%s is_error=%v", ev.Result.Subtype, ev.Result.IsError)
		}
	}
	if waitErr := sp.Wait(); waitErr != nil {
		t.Logf("exit after stdin-close: %v", waitErr)
	}

	resumable, reply := resumabilityCheck(t, pinned)
	t.Logf("VERDICT stdin-close resumability: resumable=%v reply=%q", resumable, reply)
	if !resumable {
		t.Logf("stdin-close did NOT leave a resumable session (this is a valid, informative result — not necessarily a test failure)")
	}
}
