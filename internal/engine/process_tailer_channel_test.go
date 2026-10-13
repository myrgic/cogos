package engine

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// fakeBusPublisher captures bus AppendEvent calls for assertion.
// Mimics BusSessionManager.AppendEvent's signature so it can be handed to
// Process.SetSessionActivityPublisher directly.
type fakeBusPublisher struct {
	mu    sync.Mutex
	calls []fakeBusCall
}

type fakeBusCall struct {
	BusID   string
	Type    string
	From    string
	Payload map[string]interface{}
}

func (f *fakeBusPublisher) Append(busID, eventType, from string, payload map[string]interface{}) (*BusBlock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeBusCall{
		BusID:   busID,
		Type:    eventType,
		From:    from,
		Payload: payload,
	})
	// Return a minimal non-nil BusBlock so callers that inspect the result
	// don't nil-deref. We don't need a real seq/hash here.
	return &BusBlock{V: 2, BusID: busID, Seq: len(f.calls), Type: eventType, From: from, Payload: payload}, nil
}

func (f *fakeBusPublisher) Calls() []fakeBusCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeBusCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// TestHandleTailerBlockPublishesToSessionChannel exercises the Phase 1A
// contract: every tailer block is recorded on the ledger AND, when the
// block carries a session id, fanned out to channel.<sid>.activity.
func TestHandleTailerBlockPublishesToSessionChannel(t *testing.T) {
	t.Parallel()

	type want struct {
		ledgerWritten bool
		busCallCount  int
		expectedBusID string // empty when no publish expected
	}

	claudeCodeSID := uuid.New().String()

	cases := []struct {
		name  string
		block CogBlock
		want  want
	}{
		{
			name: "claude-code block with sessionId fans to channel.<sid>.activity",
			block: CogBlock{
				ID:            uuid.New().String(),
				SessionID:     claudeCodeSID,
				SourceChannel: claudeCodeSourceChannel,
				Kind:          BlockMessage,
				Provenance: BlockProvenance{
					OriginSession: claudeCodeSID,
					OriginChannel: claudeCodeSourceChannel,
					NormalizedBy:  claudeCodeNormalizedBy,
				},
			},
			want: want{
				ledgerWritten: true,
				busCallCount:  1,
				expectedBusID: "channel." + claudeCodeSID + ".activity",
			},
		},
		{
			name: "block without session id writes ledger but skips publish",
			block: CogBlock{
				ID:            uuid.New().String(),
				SourceChannel: "headless",
				Kind:          BlockToolCall,
				Provenance: BlockProvenance{
					OriginChannel: "headless",
					NormalizedBy:  "tailer-headless",
				},
			},
			want: want{
				ledgerWritten: true,
				busCallCount:  0,
				expectedBusID: "",
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := makeConfig(t, t.TempDir())
			p := NewProcess(cfg, makeNucleus("T", "r"))

			fake := &fakeBusPublisher{}
			p.SetSessionActivityPublisher(fake.Append)

			// handleTailerBlock drops blocks when the process is not in a
			// receptive state. NewProcess initialises to StateReceptive, so
			// no explicit transition is required; if that contract ever
			// changes, add an explicit setState here so the test stays
			// targeted at the publish path.
			if got := p.State(); got != StateReceptive && got != StateActive {
				t.Fatalf("precondition: process state = %q; want receptive or active",
					got.String())
			}

			// Snapshot the per-session ledger file size before/after. The
			// ledger lives at
			//   WorkspaceRoot/.cog/ledger/<sessionID>/events.jsonl
			// (see RecordBlock → AppendEvent). When the incoming block has
			// a session id, RecordBlock preserves it; otherwise it uses the
			// kernel's own sessionID — we read both candidates so we
			// detect the append regardless of which bucket it lands in.
			ledgerSize := func() int64 {
				sids := []string{tc.block.SessionID, p.SessionID()}
				var total int64
				for _, sid := range sids {
					if sid == "" {
						continue
					}
					info, err := os.Stat(filepath.Join(
						cfg.WorkspaceRoot, ".cog", "ledger", sid, "events.jsonl"))
					if err == nil {
						total += info.Size()
					}
				}
				return total
			}

			before := ledgerSize()
			p.handleTailerBlock(tc.block)
			after := ledgerSize()
			if tc.want.ledgerWritten && after <= before {
				t.Fatalf("ledger: expected append (before=%d after=%d)",
					before, after)
			}

			// Bus publish assertion — must match expected shape.
			calls := fake.Calls()
			if len(calls) != tc.want.busCallCount {
				t.Fatalf("bus publish count = %d; want %d (calls=%+v)",
					len(calls), tc.want.busCallCount, calls)
			}
			if tc.want.busCallCount == 0 {
				return
			}

			call := calls[0]
			if call.BusID != tc.want.expectedBusID {
				t.Fatalf("bus_id = %q; want %q", call.BusID, tc.want.expectedBusID)
			}
			if call.Type != "tailer.block" {
				t.Fatalf("type = %q; want tailer.block", call.Type)
			}
			if call.From == "" {
				t.Fatalf("from is empty; want source_channel or kernel:tailer fallback")
			}
			if call.Payload == nil {
				t.Fatalf("payload is nil; want summary with kind/source_channel/timestamp/ref")
			}
			if got, _ := call.Payload["kind"].(string); got != string(tc.block.Kind) {
				t.Fatalf("payload.kind = %q; want %q", got, string(tc.block.Kind))
			}
			if got, _ := call.Payload["source_channel"].(string); got != tc.block.SourceChannel {
				t.Fatalf("payload.source_channel = %q; want %q", got, tc.block.SourceChannel)
			}
			if _, ok := call.Payload["timestamp"].(string); !ok {
				t.Fatalf("payload.timestamp missing or not a string; payload=%+v", call.Payload)
			}
			// ref is the ledger reference. RecordBlock returns the event
			// hash (may be empty in edge cases); we only assert the field
			// exists and is a string.
			if _, ok := call.Payload["ref"].(string); !ok {
				t.Fatalf("payload.ref missing or not a string; payload=%+v", call.Payload)
			}
		})
	}
}

// TestHandleTailerBlockNoPublisherWiredIsNoop confirms that a process
// constructed without a session-activity publisher still ingests tailer
// blocks (the ledger write is preserved; only the channel publish is
// skipped). This is the headless-benchmark path.
func TestHandleTailerBlockNoPublisherWiredIsNoop(t *testing.T) {
	t.Parallel()

	cfg := makeConfig(t, t.TempDir())
	p := NewProcess(cfg, makeNucleus("T", "r"))
	// Intentionally do NOT call SetSessionActivityPublisher.

	sid := uuid.New().String()
	block := CogBlock{
		ID:            uuid.New().String(),
		SessionID:     sid,
		SourceChannel: claudeCodeSourceChannel,
		Kind:          BlockMessage,
	}

	ledgerPath := filepath.Join(cfg.WorkspaceRoot, ".cog", "ledger", sid, "events.jsonl")
	var before int64
	if info, err := os.Stat(ledgerPath); err == nil {
		before = info.Size()
	}
	p.handleTailerBlock(block)
	var after int64
	if info, err := os.Stat(ledgerPath); err == nil {
		after = info.Size()
	}
	if after <= before {
		t.Fatalf("ledger: expected append even when publisher is nil (before=%d after=%d path=%s)",
			before, after, ledgerPath)
	}
}
