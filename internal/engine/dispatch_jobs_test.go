// dispatch_jobs_test.go — coverage for the async job-handle registry
// (dispatch_jobs.go) and the async path through toolDispatchToHarness /
// toolPollDispatch.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- DispatchJobRegistry unit tests -----------------------------------------

func TestDispatchJobRegistry_LifecyclePendingToDone(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	jobID := reg.Create("cycle-1")

	rec, ok := reg.Get(jobID)
	if !ok {
		t.Fatalf("Get(%q): not found immediately after Create", jobID)
	}
	if rec.State != DispatchJobPending {
		t.Errorf("initial state = %q, want %q", rec.State, DispatchJobPending)
	}
	if rec.CycleID != "cycle-1" {
		t.Errorf("CycleID = %q, want %q", rec.CycleID, "cycle-1")
	}

	reg.MarkRunning(jobID)
	rec, _ = reg.Get(jobID)
	if rec.State != DispatchJobRunning {
		t.Errorf("state after MarkRunning = %q, want %q", rec.State, DispatchJobRunning)
	}

	result := &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: true, Content: "ok"}}}
	reg.Complete(jobID, result)
	rec, _ = reg.Get(jobID)
	if rec.State != DispatchJobDone {
		t.Errorf("state after Complete = %q, want %q", rec.State, DispatchJobDone)
	}
	if rec.Result == nil || len(rec.Result.Results) != 1 || rec.Result.Results[0].Content != "ok" {
		t.Errorf("Result not stored correctly: %+v", rec.Result)
	}
}

func TestDispatchJobRegistry_Fail(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	jobID := reg.Create("")
	reg.MarkRunning(jobID)
	reg.Fail(jobID, "boom")

	rec, ok := reg.Get(jobID)
	if !ok {
		t.Fatal("job not found after Fail")
	}
	if rec.State != DispatchJobFailed {
		t.Errorf("state = %q, want %q", rec.State, DispatchJobFailed)
	}
	if rec.Err != "boom" {
		t.Errorf("Err = %q, want %q", rec.Err, "boom")
	}
}

func TestDispatchJobRegistry_UnknownJobNotFound(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	if _, ok := reg.Get("does-not-exist"); ok {
		t.Fatal("expected unknown job id to be not-found")
	}
}

// TestDispatchJobRegistry_GetReturnsDefensiveCopy guards against a caller
// mutating the record it got back and corrupting the registry's internal
// state for other readers.
func TestDispatchJobRegistry_GetReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	jobID := reg.Create("")

	rec, _ := reg.Get(jobID)
	rec.State = DispatchJobFailed // mutate the caller's copy

	rec2, _ := reg.Get(jobID)
	if rec2.State != DispatchJobPending {
		t.Fatalf("registry state leaked caller mutation: got %q, want %q", rec2.State, DispatchJobPending)
	}
}

// TestDispatchJobRegistry_GetResultIsDeepCopy is the regression test for
// FINDING 4: TestDispatchJobRegistry_GetReturnsDefensiveCopy above only
// mutates the top-level State field of the returned record, which is a
// value type on DispatchJobRecord and was always safe (cp := *r copies it).
// It never exercised Result, a *DispatchBatchResult pointer — a plain
// `cp := *r` shallow copy leaves cp.Result pointing at the SAME
// DispatchBatchResult (and the same nested Results/ToolCalls/Notes slices)
// still stored in the registry, so the "defensive copy" claim the test name
// makes was never actually verified for the field most likely to be mutated
// by a real caller (e.g. a poll handler appending to Results or Notes).
//
// This test mutates the batch result returned by Get and confirms a second
// Get is unaffected, at every level: the Result pointer itself, the Results
// slice, an individual DispatchResult's ToolCalls slice, and the Notes
// slice.
func TestDispatchJobRegistry_GetResultIsDeepCopy(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	jobID := reg.Create("")

	original := &DispatchBatchResult{
		Results: []DispatchResult{
			{
				Index:   0,
				Success: true,
				Content: "original content",
				ToolCalls: []DispatchToolCallSummary{
					{Name: "cog_resolve_uri", ArgsDigest: "orig-args"},
				},
			},
		},
		TotalDurationSec: 1.5,
		Notes:            []string{"original note"},
	}
	reg.Complete(jobID, original)

	rec, ok := reg.Get(jobID)
	if !ok {
		t.Fatal("job not found after Complete")
	}

	// Mutate every level of the returned copy.
	if rec.Result == original {
		t.Fatal("Get returned the same *DispatchBatchResult pointer stored via Complete; not a defensive copy")
	}
	rec.Result.TotalDurationSec = 99
	rec.Result.Results[0].Content = "mutated content"
	rec.Result.Results[0].ToolCalls[0].ArgsDigest = "mutated-args"
	rec.Result.Notes[0] = "mutated note"
	rec.Result.Notes = append(rec.Result.Notes, "appended note")
	rec.Result.Results = append(rec.Result.Results, DispatchResult{Index: 1})

	rec2, ok := reg.Get(jobID)
	if !ok {
		t.Fatal("job not found on second Get")
	}
	if rec2.Result.TotalDurationSec != 1.5 {
		t.Errorf("TotalDurationSec leaked mutation: got %v, want 1.5", rec2.Result.TotalDurationSec)
	}
	if len(rec2.Result.Results) != 1 {
		t.Fatalf("Results length leaked mutation: got %d, want 1", len(rec2.Result.Results))
	}
	if rec2.Result.Results[0].Content != "original content" {
		t.Errorf("Results[0].Content leaked mutation: got %q, want %q", rec2.Result.Results[0].Content, "original content")
	}
	if rec2.Result.Results[0].ToolCalls[0].ArgsDigest != "orig-args" {
		t.Errorf("Results[0].ToolCalls[0].ArgsDigest leaked mutation: got %q, want %q", rec2.Result.Results[0].ToolCalls[0].ArgsDigest, "orig-args")
	}
	if len(rec2.Result.Notes) != 1 || rec2.Result.Notes[0] != "original note" {
		t.Errorf("Notes leaked mutation: got %v, want [%q]", rec2.Result.Notes, "original note")
	}

	// Also confirm the ORIGINAL passed into Complete was never touched —
	// guards against a future refactor that copies on write into the
	// registry instead of on read out of it (equally valid, but must
	// actually happen somewhere).
	if original.TotalDurationSec != 1.5 || original.Results[0].Content != "original content" {
		t.Error("the original DispatchBatchResult passed to Complete was mutated by a Get-side copy; snapshot must not alias it")
	}
}

// TestDispatchJobRegistry_LazyGCReclaimsExpiredTerminalJobs verifies the TTL
// sweep: a terminal job older than the TTL is removed on the next Create
// call (lazy GC, no background ticker).
func TestDispatchJobRegistry_LazyGCReclaimsExpiredTerminalJobs(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	reg.ttl = 10 * time.Millisecond

	fakeNow := time.Now()
	reg.now = func() time.Time { return fakeNow }

	oldJob := reg.Create("")
	reg.Complete(oldJob, &DispatchBatchResult{})

	// Advance fake time past the TTL, then create a new job — this should
	// trigger gcLocked and evict oldJob.
	fakeNow = fakeNow.Add(1 * time.Second)
	_ = reg.Create("")

	if _, ok := reg.Get(oldJob); ok {
		t.Fatal("expected expired terminal job to be GC'd, but it is still present")
	}
}

// TestDispatchJobRegistry_LazyGCDoesNotReclaimPending guards against GC
// evicting an in-flight (non-terminal) job just because it's old — only
// terminal (done/failed) records are TTL-eligible.
func TestDispatchJobRegistry_LazyGCDoesNotReclaimPending(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	reg.ttl = 10 * time.Millisecond

	fakeNow := time.Now()
	reg.now = func() time.Time { return fakeNow }

	pendingJob := reg.Create("")

	fakeNow = fakeNow.Add(1 * time.Second)
	_ = reg.Create("")

	if _, ok := reg.Get(pendingJob); !ok {
		t.Fatal("pending job was reclaimed by GC; only terminal jobs should be TTL-eligible")
	}
}

// --- detachedDispatchContext deadline sizing (FINDING 3) --------------------

// TestDetachedDispatchContext_DeadlineHasQueueHeadroom is the regression test
// for FINDING 3: detachedDispatchContext previously stamped the async
// goroutine's deadline as exactly req.TimeoutSeconds from "now" — the same
// instant startAsyncDispatch fires, BEFORE the dispatch has even reached
// DispatchToHarness's ollamaMu.Lock() (local_agent_harness.go), let alone
// acquired it. QueryDispatchToHarnessRouted -> DispatchToHarness -> the
// per-slot context.WithTimeout in dispatchSlot are all derived from this one
// outer deadline, so a busy kernel (metabolic cycle mid-run, holding
// ollamaMu for the documented 2-4 minute worst case — see
// dispatchTimeoutDefault's #432 forensics) burns the caller's entire
// requested budget just queueing for the lock. The per-slot work then starts
// with little-to-no time left and fails deadline-exceeded — precisely the
// busy-kernel case async dispatch exists to survive, while the equivalent
// synchronous call (whose caller supplies its own patience) succeeds once
// the lock frees up.
//
// This test only exercises the deadline arithmetic in isolation (no real
// ollamaMu contention — that needs a live LocalHarnessController and a
// concurrent metabolic cycle, out of scope for a fast unit test) but it
// directly catches a regression to the pre-fix "deadline == now +
// TimeoutSeconds" shape, which is the root cause the finding identifies.
func TestDetachedDispatchContext_DeadlineHasQueueHeadroom(t *testing.T) {
	t.Parallel()

	requestedSeconds := 30
	ctx, cancel := detachedDispatchContext(context.Background(), requestedSeconds)
	defer cancel()
	after := time.Now()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("detachedDispatchContext returned a context with no deadline")
	}

	// A plain context.WithTimeout(parent, requestedSeconds) deadline lands at
	// before/after + requestedSeconds give or take nanoseconds of scheduling
	// jitter — nowhere near a full minute of slack. Requiring at least 60s of
	// headroom (well under dispatchAsyncQueueHeadroomSeconds=240) is a bright
	// line that only a real headroom-add can cross, so this fails cleanly
	// against the pre-fix "no headroom at all" shape instead of passing on
	// jitter alone.
	const minHeadroomForTest = 60 * time.Second
	noHeadroomDeadline := after.Add(time.Duration(requestedSeconds) * time.Second)
	if !deadline.After(noHeadroomDeadline.Add(minHeadroomForTest)) {
		t.Fatalf(
			"deadline has no meaningful queueing headroom above the requested %ds budget: "+
				"deadline=%s, no-headroom deadline would be ~%s (delta=%s, want >= %s) — "+
				"a busy-kernel ollamaMu wait can now consume the entire per-slot "+
				"budget before any dispatch work starts",
			requestedSeconds, deadline, noHeadroomDeadline, deadline.Sub(noHeadroomDeadline), minHeadroomForTest)
	}

	// Sanity bound: the headroom shouldn't be unbounded either — confirm the
	// deadline still lands within [before, after] + requested + headroom, so
	// a future change can't silently balloon this into an effectively-infinite
	// timeout.
	maxExpected := after.Add(time.Duration(requestedSeconds+dispatchAsyncQueueHeadroomSeconds) * time.Second)
	if deadline.After(maxExpected) {
		t.Fatalf("deadline %s exceeds requested+headroom bound %s", deadline, maxExpected)
	}
}

// TestDetachedDispatchContext_DefaultTimeoutAlsoGetsHeadroom confirms the
// headroom applies on the no-explicit-timeout fallback path too (seconds<=0
// -> dispatchAsyncDefaultTimeoutSeconds), not just the caller-specified case.
func TestDetachedDispatchContext_DefaultTimeoutAlsoGetsHeadroom(t *testing.T) {
	t.Parallel()

	after := time.Now()
	ctx, cancel := detachedDispatchContext(context.Background(), 0)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("detachedDispatchContext returned a context with no deadline")
	}

	const minHeadroomForTest = 60 * time.Second
	noHeadroomDeadline := after.Add(time.Duration(dispatchAsyncDefaultTimeoutSeconds) * time.Second)
	if !deadline.After(noHeadroomDeadline.Add(minHeadroomForTest)) {
		t.Fatalf(
			"default-timeout deadline has no meaningful queueing headroom: deadline=%s, "+
				"no-headroom deadline would be ~%s (delta=%s, want >= %s)",
			deadline, noHeadroomDeadline, deadline.Sub(noHeadroomDeadline), minHeadroomForTest)
	}
}

// --- toolDispatchToHarness async path / toolPollDispatch --------------------

// TestToolDispatchToHarness_AsyncReturnsImmediateJobHandle is the test that
// fails without the change: without Async support, dispatching with
// Async=true would either be ignored (falling through to the synchronous
// path and blocking until fakeAgentDispatcher's canned result is available
// — which for a fast fake wouldn't distinguish the paths) or the field
// wouldn't exist at all. The meaningful assertion is that the response is a
// job handle shape ({job_id, status:"pending"}), not a DispatchBatchResult.
func TestToolDispatchToHarness_AsyncReturnsImmediateJobHandle(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))

	block := make(chan struct{})
	disp := &blockingDispatcher{
		release: block,
		canned:  &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: true, Content: "done"}}},
	}
	server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)

	result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{
		Task:  "do the thing",
		Async: true,
	})
	if err != nil {
		t.Fatalf("toolDispatchToHarness (async): %v", err)
	}

	var receipt dispatchJobReceipt
	decodeMCPJSONForAgentTests(t, result, &receipt)
	if receipt.JobID == "" {
		t.Fatal("expected non-empty job_id in async receipt")
	}
	if receipt.Status != string(DispatchJobPending) {
		t.Errorf("initial status = %q, want %q", receipt.Status, DispatchJobPending)
	}

	// Release the blocked dispatcher, then wait for the background goroutine
	// to actually reach a terminal state before the test returns — otherwise
	// t.TempDir()'s cleanup can race the goroutine's ledger write to
	// .cog/ledger/ under the workspace root (the goroutine outlives the
	// synchronous part of the test by design; this is the async path's whole
	// point, so the test must wait it out rather than fire-and-forget).
	close(block)
	waitForTerminalJob(t, server, receipt.JobID)
}

// TestToolDispatchToHarness_AsyncPollTransitionsToDone exercises the full
// async lifecycle: dispatch async, observe pending, unblock the fake
// dispatcher, poll until done, and confirm the polled result matches what a
// synchronous call would have returned for the same input.
func TestToolDispatchToHarness_AsyncPollTransitionsToDone(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))

	release := make(chan struct{})
	canned := &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: true, Content: "the answer"}}}
	disp := &blockingDispatcher{release: release, canned: canned}
	server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)

	result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{
		Task:  "do the thing",
		Async: true,
	})
	if err != nil {
		t.Fatalf("toolDispatchToHarness (async): %v", err)
	}
	var receipt dispatchJobReceipt
	decodeMCPJSONForAgentTests(t, result, &receipt)

	// Poll immediately: still pending/running since the fake dispatcher is
	// blocked on `release`.
	pollResult, _, err := server.toolPollDispatch(context.Background(), nil, pollDispatchInput{JobID: receipt.JobID})
	if err != nil {
		t.Fatalf("toolPollDispatch (pre-release): %v", err)
	}
	var status dispatchJobStatusResponse
	decodeMCPJSONForAgentTests(t, pollResult, &status)
	if status.Status != string(DispatchJobPending) && status.Status != string(DispatchJobRunning) {
		t.Fatalf("status before release = %q, want pending or running", status.Status)
	}

	close(release)

	final := waitForTerminalJob(t, server, receipt.JobID)
	if final.Status != string(DispatchJobDone) {
		t.Fatalf("final status = %q, want %q (error=%q)", final.Status, DispatchJobDone, final.Error)
	}
	if final.Result == nil || len(final.Result.Results) != 1 || final.Result.Results[0].Content != "the answer" {
		t.Fatalf("polled result does not match the synchronous canned result: %+v", final.Result)
	}
}

// TestToolPollDispatch_UnknownJobIDReturnsClearError guards the not-found
// path — an unknown or expired job_id must not panic or return an empty
// success response.
func TestToolPollDispatch_UnknownJobIDReturnsClearError(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))
	server := NewMCPServer(cfg, makeNucleus("Cog", "tester"), process)

	result, _, err := server.toolPollDispatch(context.Background(), nil, pollDispatchInput{JobID: "no-such-job"})
	if err != nil {
		t.Fatalf("toolPollDispatch: %v", err)
	}
	if result == nil || len(result.Content) == 0 {
		t.Fatal("expected a fallback error result for unknown job id")
	}
}

// TestStartAsyncDispatch_ReceiptAndLedgerCarryNonEmptyCycleID is the
// regression test for the confirmed cog-review finding on startAsyncDispatch
// (mcp_server.go): it used to call m.dispatchJobs.Create("") unconditionally,
// so the CycleID this PR's own docs describe (surfaced in the receipt +
// correlatable against harness.dispatch.start/end via the
// harness.dispatch.job.issued ledger event) was ALWAYS empty and never
// surfaced anywhere.
//
// This exercises the REAL call site — toolDispatchToHarness's async branch,
// which is what dispatchToHarnessAsync/startAsyncDispatch actually run
// behind — rather than calling reg.Create("cycle-1") directly (that unit
// test already exists in TestDispatchJobRegistry_LifecyclePendingToDone and
// would keep passing even with the mint-a-real-id fix reverted, since it
// never touches startAsyncDispatch).
func TestStartAsyncDispatch_ReceiptAndLedgerCarryNonEmptyCycleID(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))

	block := make(chan struct{})
	disp := &blockingDispatcher{
		release: block,
		canned:  &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: true, Content: "done"}}},
	}
	server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)

	result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{
		Task:  "do the thing",
		Async: true,
	})
	if err != nil {
		t.Fatalf("toolDispatchToHarness (async): %v", err)
	}

	var receipt dispatchJobReceipt
	decodeMCPJSONForAgentTests(t, result, &receipt)
	if receipt.JobID == "" {
		t.Fatal("expected non-empty job_id in async receipt")
	}
	if receipt.CycleID == "" {
		t.Fatal("receipt.CycleID is empty — startAsyncDispatch minted no correlation id (Create(\"\") regression)")
	}

	// The registry record itself must carry the same id (not just the
	// receipt) — this is what a poller sees via GET /v1/dispatch-jobs/{id}
	// or cog_poll_dispatch.
	rec, ok := server.dispatchJobs.Get(receipt.JobID)
	if !ok {
		t.Fatalf("job %q not found in registry immediately after creation", receipt.JobID)
	}
	if rec.CycleID != receipt.CycleID {
		t.Fatalf("registry CycleID %q does not match receipt CycleID %q", rec.CycleID, receipt.CycleID)
	}

	// The harness.dispatch.job.issued ledger event must carry the same
	// cycle_id — this is the correlation surface the PR's docs promise
	// (correlatable against harness.dispatch.start/end emitted later by
	// LocalHarnessController.DispatchToHarness for this same dispatch).
	ledgerResult, err := QueryLedger(root, LedgerQuery{EventType: "harness.dispatch.job.issued", Limit: 10})
	if err != nil {
		t.Fatalf("QueryLedger: %v", err)
	}
	found := false
	for _, ev := range ledgerResult.Events {
		// EmitLedgerEvent files everything under event["payload"] verbatim
		// (it only special-cases type/source/timestamp), so the fields set
		// on startAsyncDispatch's "payload" map land at ev.Data["payload"],
		// not ev.Data directly.
		payload, _ := ev.Data["payload"].(map[string]any)
		jobID, _ := payload["job_id"].(string)
		if jobID != receipt.JobID {
			continue
		}
		found = true
		cycleID, _ := payload["cycle_id"].(string)
		if cycleID == "" {
			t.Fatalf("harness.dispatch.job.issued event for job %q has empty cycle_id", jobID)
		}
		if cycleID != receipt.CycleID {
			t.Fatalf("ledger cycle_id %q does not match receipt CycleID %q", cycleID, receipt.CycleID)
		}
	}
	if !found {
		t.Fatalf("no harness.dispatch.job.issued ledger event found for job %q", receipt.JobID)
	}

	// Unblock the fake dispatcher and wait out the goroutine. waitForTerminalJob
	// only guarantees the REGISTRY has reached a terminal state
	// (registry.Complete/Fail) — startAsyncDispatch's goroutine emits the
	// harness.dispatch.job.completed ledger event (a second AppendEvent call
	// into this same t.TempDir() workspace) AFTER that, so a bare
	// waitForTerminalJob can still race t.TempDir()'s RemoveAll cleanup against
	// that trailing ledger write. Also wait for the completed ledger event
	// (root, not the registry) so this test doesn't reintroduce that flake.
	close(block)
	waitForTerminalJob(t, server, receipt.JobID)
	waitForLedgerEvent(t, root, "harness.dispatch.job.completed", receipt.JobID)
}

// waitForLedgerEvent polls the ledger at workspaceRoot until an event of the
// given type carrying data.payload.job_id == jobID appears, or a 5s deadline
// elapses (test failure on timeout). Use this after waitForTerminalJob when a
// test also needs to observe (or simply outlive) a ledger write that a
// background goroutine performs strictly after the registry reaches a
// terminal state. startAsyncDispatch now emits BEFORE the terminal
// transition, so after waitForTerminalJob this returns on its first read.
func waitForLedgerEvent(t *testing.T, workspaceRoot, eventType, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		res, err := QueryLedger(workspaceRoot, LedgerQuery{EventType: eventType, Limit: 20})
		if err == nil {
			for _, ev := range res.Events {
				payload, _ := ev.Data["payload"].(map[string]any)
				if id, _ := payload["job_id"].(string); id == jobID {
					return
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no %q ledger event observed for job %q within 5s", eventType, jobID)
}

// TestQueryDispatchToHarness_SyncPathUnaffectedByAsyncField is the flag-off
// mirror: Async=false (the zero value / default) must behave byte-for-byte
// like before this change — synchronous, returning a DispatchBatchResult
// directly rather than a job receipt.
func TestQueryDispatchToHarness_SyncPathUnaffectedByAsyncField(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))
	disp := &fakeAgentDispatcher{canned: &DispatchBatchResult{
		Results: []DispatchResult{{Index: 0, Success: true, Content: "sync-result"}},
	}}
	server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)

	result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{
		Task: "do the thing",
		// Async omitted -> false.
	})
	if err != nil {
		t.Fatalf("toolDispatchToHarness (sync): %v", err)
	}
	var batch DispatchBatchResult
	decodeMCPJSONForAgentTests(t, result, &batch)
	if len(batch.Results) != 1 || batch.Results[0].Content != "sync-result" {
		t.Fatalf("sync path result mismatch: %+v", batch)
	}
}

// waitForTerminalJob polls jobID via toolPollDispatch until it reaches a
// terminal state (done/failed) or a 5s deadline elapses (test failure on
// timeout). Callers that spawn a background dispatch goroutine (Async=true)
// MUST wait it out this way before returning — t.TempDir()'s cleanup runs as
// soon as the test function returns, and a still-running goroutine writing
// to the workspace's .cog/ledger/ races that cleanup (see the comment in
// TestToolDispatchToHarness_AsyncReturnsImmediateJobHandle).
// ledgerHasEvent reports whether the ledger already holds an event of the
// given type for jobID (one read, no waiting).
func ledgerHasEvent(t *testing.T, workspaceRoot, eventType, jobID string) bool {
	t.Helper()
	res, err := QueryLedger(workspaceRoot, LedgerQuery{EventType: eventType, Limit: 20})
	if err != nil {
		return false
	}
	for _, ev := range res.Events {
		payload, _ := ev.Data["payload"].(map[string]any)
		if id, _ := payload["job_id"].(string); id == jobID {
			return true
		}
	}
	return false
}

func waitForTerminalJob(t *testing.T, server *MCPServer, jobID string) dispatchJobStatusResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var status dispatchJobStatusResponse
	for time.Now().Before(deadline) {
		pollResult, _, err := server.toolPollDispatch(context.Background(), nil, pollDispatchInput{JobID: jobID})
		if err != nil {
			t.Fatalf("toolPollDispatch: %v", err)
		}
		decodeMCPJSONForAgentTests(t, pollResult, &status)
		if status.Status == string(DispatchJobDone) || status.Status == string(DispatchJobFailed) {
			return status
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("job %q did not reach a terminal state within 5s (last status=%q)", jobID, status.Status)
	return status
}

// --- test doubles ------------------------------------------------------------

// blockingDispatcher is an AgentController+AgentDispatcher fake whose
// DispatchToHarness call blocks on release before returning canned. Used to
// deterministically observe the "pending" window of an async dispatch
// before letting the underlying call complete.
type blockingDispatcher struct {
	fakeAgentController
	release chan struct{}
	canned  *DispatchBatchResult
	err     error // returned instead of canned when set
}

func (b *blockingDispatcher) DispatchToHarness(ctx context.Context, req DispatchRequest) (*DispatchBatchResult, error) {
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if b.err != nil {
		return nil, b.err
	}
	return b.canned, nil
}

// decodeMCPJSONForAgentTests is defined in agent_state_query_test.go (same
// package) and reused here rather than duplicated.

// TestStartAsyncDispatch_TimedOutSlotIsNotRecordedAsDone is the regression
// test for a live correctness bug found by adversarial review of the L2
// lifecycle design (2026-08-08).
//
// DispatchToHarness's contract is that it returns once every slot has
// "completed, errored, or timed out", with a non-nil batch whenever err is
// nil — so per-slot failures live in the BATCH, never in the returned error.
// A deadline-exceeded slot sets Error="timeout" and leaves Success=false
// (dispatchSlot, local_agent_harness.go). startAsyncDispatch used to branch
// only on `err != nil`, so a fully-timed-out dispatch took the success path
// and was recorded as status:"done" in BOTH the ledger and the job registry.
//
// The old code cannot pass this test: it called registry.Complete and emitted
// "done" for exactly this input.
func TestStartAsyncDispatch_TimedOutSlotIsNotRecordedAsDone(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))

	block := make(chan struct{})
	// The shape DispatchToHarness actually returns on a slot timeout:
	// a non-nil batch, a nil error, and a slot that did not succeed.
	disp := &blockingDispatcher{
		release: block,
		canned: &DispatchBatchResult{Results: []DispatchResult{
			{Index: 0, Success: false, Error: "timeout"},
		}},
	}
	server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)

	result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{
		Task:  "a task that times out",
		Async: true,
	})
	if err != nil {
		t.Fatalf("toolDispatchToHarness (async): %v", err)
	}
	var receipt dispatchJobReceipt
	decodeMCPJSONForAgentTests(t, result, &receipt)

	close(block)
	waitForTerminalJob(t, server, receipt.JobID)
	waitForLedgerEvent(t, root, "harness.dispatch.job.completed", receipt.JobID)

	// 1. The job registry — what GET /v1/dispatch-jobs/{id} and
	//    cog_poll_dispatch report to a caller.
	rec, ok := server.dispatchJobs.Get(receipt.JobID)
	if !ok {
		t.Fatalf("job %q not found in registry", receipt.JobID)
	}
	if rec.State != DispatchJobFailed {
		t.Fatalf("registry state = %q, want %q — a timed-out dispatch must not be reported as succeeded",
			rec.State, DispatchJobFailed)
	}

	// 2. The ledger — the durable record.
	ledgerResult, qerr := QueryLedger(root, LedgerQuery{EventType: "harness.dispatch.job.completed", Limit: 20})
	if qerr != nil {
		t.Fatalf("QueryLedger: %v", qerr)
	}
	found := false
	for _, ev := range ledgerResult.Events {
		payload, _ := ev.Data["payload"].(map[string]any)
		if jobID, _ := payload["job_id"].(string); jobID != receipt.JobID {
			continue
		}
		found = true
		if status, _ := payload["status"].(string); status != "failed" {
			t.Fatalf("ledger status = %q, want \"failed\" — a fully-timed-out dispatch was recorded as succeeded", status)
		}
		if timedOut, _ := payload["timed_out"].(bool); !timedOut {
			t.Error("ledger payload timed_out = false, want true — the timeout cause was not distinguishable")
		}
	}
	if !found {
		t.Fatalf("no harness.dispatch.job.completed ledger event for job %q", receipt.JobID)
	}
}

// TestSummarizeDispatchOutcome covers the verdict function directly, including
// the degenerate batches that must NOT read as success.
func TestSummarizeDispatchOutcome(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		batch    *DispatchBatchResult
		wantOK   bool
		wantTime bool
	}{
		{"nil batch is not success", nil, false, false},
		{"empty batch is not success", &DispatchBatchResult{}, false, false},
		{"all slots ok", &DispatchBatchResult{Results: []DispatchResult{
			{Index: 0, Success: true}, {Index: 1, Success: true}}}, true, false},
		{"timeout slot", &DispatchBatchResult{Results: []DispatchResult{
			{Index: 0, Success: false, Error: "timeout"}}}, false, true},
		{"partial failure", &DispatchBatchResult{Results: []DispatchResult{
			{Index: 0, Success: true}, {Index: 1, Success: false, Error: "boom"}}}, false, false},
		{"failure with no message", &DispatchBatchResult{Results: []DispatchResult{
			{Index: 0, Success: false}}}, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := summarizeDispatchOutcome(tc.batch)
			if got.ok != tc.wantOK {
				t.Errorf("ok = %v, want %v (errMsg=%q)", got.ok, tc.wantOK, got.errMsg)
			}
			if got.timedOut != tc.wantTime {
				t.Errorf("timedOut = %v, want %v", got.timedOut, tc.wantTime)
			}
			if !got.ok && got.errMsg == "" {
				t.Error("a non-ok outcome must carry a non-empty errMsg")
			}
		})
	}
}

// TestStartAsyncDispatch_PartialFailurePreservesSucceededSlots pins the
// regression that cog-review caught on the first cut of the timeout fix.
//
// Routing batch-derived failures through registry.Fail() marked the job
// "failed" correctly but stored ONLY the error string — Fail() never assigns
// rec.Result. On a partial failure (slots 0 and 1 succeed, slot 2 times out)
// that silently discarded the output of the slots that genuinely completed.
// The pre-fix bug at least kept the batch retrievable, mislabelled as "done";
// trading a wrong status for data loss is not a fix.
//
// The failure path must therefore use FailWithResult: correct status AND the
// batch preserved.
func TestStartAsyncDispatch_PartialFailurePreservesSucceededSlots(t *testing.T) {
	t.Parallel()
	root := makeWorkspace(t)
	cfg := makeConfig(t, root)
	process := NewProcess(cfg, makeNucleus("Cog", "tester"))

	block := make(chan struct{})
	disp := &blockingDispatcher{
		release: block,
		canned: &DispatchBatchResult{Results: []DispatchResult{
			{Index: 0, Success: true, Content: "slot zero real output"},
			{Index: 1, Success: true, Content: "slot one real output"},
			{Index: 2, Success: false, Error: "timeout"},
		}},
	}
	server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)

	result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{
		Task:  "three slots, one times out",
		Async: true,
	})
	if err != nil {
		t.Fatalf("toolDispatchToHarness (async): %v", err)
	}
	var receipt dispatchJobReceipt
	decodeMCPJSONForAgentTests(t, result, &receipt)

	close(block)
	waitForTerminalJob(t, server, receipt.JobID)
	waitForLedgerEvent(t, root, "harness.dispatch.job.completed", receipt.JobID)

	rec, ok := server.dispatchJobs.Get(receipt.JobID)
	if !ok {
		t.Fatalf("job %q not found in registry", receipt.JobID)
	}
	// Correct verdict...
	if rec.State != DispatchJobFailed {
		t.Fatalf("state = %q, want %q", rec.State, DispatchJobFailed)
	}
	// ...AND the work is not lost.
	if rec.Result == nil {
		t.Fatal("rec.Result is nil — the batch was discarded on the failure path, losing the output of the slots that succeeded")
	}
	if got := len(rec.Result.Results); got != 3 {
		t.Fatalf("rec.Result carries %d slots, want 3", got)
	}
	for _, idx := range []int{0, 1} {
		slot := rec.Result.Results[idx]
		if !slot.Success || slot.Content == "" {
			t.Errorf("slot %d lost its output on the failure path: success=%v content=%q",
				idx, slot.Success, slot.Content)
		}
	}
	if rec.Err == "" {
		t.Error("rec.Err is empty — the failure reason must still be recorded alongside the batch")
	}
}

// TestDispatchJobRegistry_FailWithResultKeepsBatch covers the registry method
// directly, including that it still records the error.
func TestDispatchJobRegistry_FailWithResultKeepsBatch(t *testing.T) {
	t.Parallel()
	reg := NewDispatchJobRegistry()
	jobID := reg.Create("cycle-1")
	batch := &DispatchBatchResult{Results: []DispatchResult{
		{Index: 0, Success: true, Content: "kept"},
		{Index: 1, Success: false, Error: "timeout"},
	}}
	reg.FailWithResult(jobID, "1/2 slots failed", batch)

	rec, ok := reg.Get(jobID)
	if !ok {
		t.Fatal("job not found")
	}
	if rec.State != DispatchJobFailed {
		t.Fatalf("state = %q, want %q", rec.State, DispatchJobFailed)
	}
	if rec.Err != "1/2 slots failed" {
		t.Fatalf("Err = %q, want %q", rec.Err, "1/2 slots failed")
	}
	if rec.Result == nil || len(rec.Result.Results) != 2 {
		t.Fatal("FailWithResult did not preserve the batch")
	}
	if rec.Result.Results[0].Content != "kept" {
		t.Errorf("succeeded slot content lost: %q", rec.Result.Results[0].Content)
	}
	// The defensive-copy contract must still hold on this path.
	rec.Result.Results[0].Content = "mutated"
	again, _ := reg.Get(jobID)
	if again.Result.Results[0].Content != "kept" {
		t.Error("Get() returned an aliased batch — mutating the caller's copy corrupted the registry")
	}
}

// Ordering contract: by the time a job is observable as terminal, its
// job.completed ledger event is already written, and the background
// goroutine does no further workspace I/O. The reverse order raced
// t.TempDir cleanup ("unlinkat ...: directory not empty") and let a poller
// see "done" before the ledger said so.
func TestStartAsyncDispatch_LedgerEventWrittenBeforeTerminal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		canned *DispatchBatchResult
		err    error
	}{
		{"done", &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: true, Content: "ok"}}}, nil},
		{"failed slot", &DispatchBatchResult{Results: []DispatchResult{{Index: 0, Success: false, Error: "timeout"}}}, nil},
		{"hard error", nil, errors.New("boom")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := makeWorkspace(t)
			cfg := makeConfig(t, root)
			process := NewProcess(cfg, makeNucleus("Cog", "tester"))
			block := make(chan struct{})
			disp := &blockingDispatcher{release: block, canned: tc.canned, err: tc.err}
			server := NewMCPServerWithAgentController(cfg, makeNucleus("Cog", "tester"), process, disp)
			result, _, err := server.toolDispatchToHarness(context.Background(), nil, dispatchToHarnessInput{Task: "t", Async: true})
			if err != nil {
				t.Fatal(err)
			}
			var receipt dispatchJobReceipt
			decodeMCPJSONForAgentTests(t, result, &receipt)
			close(block)
			waitForTerminalJob(t, server, receipt.JobID)
			if !ledgerHasEvent(t, root, "harness.dispatch.job.completed", receipt.JobID) {
				t.Fatal("job is terminal but its job.completed ledger event is not written yet")
			}
		})
	}
}
