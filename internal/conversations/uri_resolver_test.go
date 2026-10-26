// uri_resolver_test.go — table-driven tests for the cog:conversations URI resolver.
//
// Covers (per RFC R1-R6):
//   - Path form parsing: whole observatory, source-scoped, session-scoped
//   - Every query param: q, role, since, until, limit, offset, order, res, fields
//   - Reserved param rejection: component=, ontology=
//   - Unknown param rejection
//   - Phrase vs term search (q= parsing)
//   - Fragment parsing: #id-<uuid>, #turn-N, #turn-N..M
//   - Bounded hash: refused on unbounded, computed on bounded (until= or fragment)
//   - URI-mixing error (uri + other params)
//   - Resolution levels: pointer, abstract, full
package conversations

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─── ParseConversationURI tests ──────────────────────────────────────────────

func TestParseConversationURI_Paths(t *testing.T) {
	tests := []struct {
		name      string
		uri       string
		wantSrc   string
		wantSess  string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "whole observatory",
			uri:     "cog:conversations",
			wantSrc: "", wantSess: "",
		},
		{
			name:    "source only",
			uri:     "cog:conversations/claude-code",
			wantSrc: "claude-code", wantSess: "",
		},
		{
			name:    "source and session",
			uri:     "cog:conversations/claude-code/abc123",
			wantSrc: "claude-code", wantSess: "abc123",
		},
		{
			name:    "hermes source",
			uri:     "cog:conversations/hermes-node-a/20260605_184354",
			wantSrc: "hermes-node-a", wantSess: "20260605_184354",
		},
		{
			name:      "wrong scheme",
			uri:       "cog:mem/semantic/foo",
			wantErr:   true,
			errSubstr: "not a conversations URI",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uq, err := ParseConversationURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error containing %q, got nil", tc.errSubstr)
				}
				if !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("want error containing %q, got %q", tc.errSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if uq.Source != tc.wantSrc {
				t.Errorf("Source: want %q, got %q", tc.wantSrc, uq.Source)
			}
			if uq.SessionID != tc.wantSess {
				t.Errorf("SessionID: want %q, got %q", tc.wantSess, uq.SessionID)
			}
		})
	}
}

func TestParseConversationURI_QueryParams(t *testing.T) {
	tests := []struct {
		name      string
		uri       string
		check     func(*testing.T, *URIQuery)
		wantErr   bool
		errSubstr string
	}{
		{
			name: "q term",
			uri:  "cog:conversations?q=harness",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Query != "harness" {
					t.Errorf("Query: want %q, got %q", "harness", uq.Query)
				}
			},
		},
		{
			name: "q phrase",
			uri:  `cog:conversations?q="harness attestation"`,
			check: func(t *testing.T, uq *URIQuery) {
				// The raw query string reaches parseSearchQuery later; the URI
				// parser stores the raw value including quotes.
				if uq.Query != `"harness attestation"` {
					t.Errorf("Query: want quoted phrase, got %q", uq.Query)
				}
				// Verify that parseSearchQuery extracts the phrase correctly.
				terms := parseSearchQuery(uq.Query)
				if len(terms) != 1 || terms[0] != "harness attestation" {
					t.Errorf("parseSearchQuery: want [harness attestation], got %v", terms)
				}
			},
		},
		{
			name: "role single",
			uri:  "cog:conversations?role=user",
			check: func(t *testing.T, uq *URIQuery) {
				if len(uq.Roles) != 1 || uq.Roles[0] != RoleUser {
					t.Errorf("Roles: want [user], got %v", uq.Roles)
				}
			},
		},
		{
			name: "role comma-list",
			uri:  "cog:conversations?role=user,assistant",
			check: func(t *testing.T, uq *URIQuery) {
				if len(uq.Roles) != 2 {
					t.Fatalf("Roles: want 2 elements, got %v", uq.Roles)
				}
				if uq.Roles[0] != RoleUser || uq.Roles[1] != RoleAssistant {
					t.Errorf("Roles: want [user assistant], got %v", uq.Roles)
				}
			},
		},
		{
			name:      "role invalid",
			uri:       "cog:conversations?role=operator",
			wantErr:   true,
			errSubstr: "invalid role",
		},
		{
			name: "since and until",
			uri:  "cog:conversations?since=2026-06-01T00:00:00Z&until=2026-06-10T00:00:00Z",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Since.IsZero() {
					t.Error("Since should be set")
				}
				if uq.Until.IsZero() {
					t.Error("Until should be set")
				}
				if uq.Until.Before(uq.Since) {
					t.Error("Until before Since")
				}
			},
		},
		{
			name:      "since invalid RFC3339",
			uri:       "cog:conversations?since=not-a-date",
			wantErr:   true,
			errSubstr: "invalid since",
		},
		{
			name:      "until invalid RFC3339",
			uri:       "cog:conversations?until=not-a-date",
			wantErr:   true,
			errSubstr: "invalid until",
		},
		{
			name: "limit default",
			uri:  "cog:conversations",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Limit != 20 {
					t.Errorf("default Limit: want 20, got %d", uq.Limit)
				}
			},
		},
		{
			name: "limit explicit",
			uri:  "cog:conversations?limit=50",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Limit != 50 {
					t.Errorf("Limit: want 50, got %d", uq.Limit)
				}
			},
		},
		{
			name: "limit server-capped at 200",
			uri:  "cog:conversations?limit=999",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Limit != 200 {
					t.Errorf("Limit: want 200 (capped), got %d", uq.Limit)
				}
			},
		},
		{
			name:      "limit negative",
			uri:       "cog:conversations?limit=-1",
			wantErr:   true,
			errSubstr: "invalid limit",
		},
		{
			name:      "limit non-integer",
			uri:       "cog:conversations?limit=abc",
			wantErr:   true,
			errSubstr: "invalid limit",
		},
		{
			name: "offset",
			uri:  "cog:conversations?offset=10",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Offset != 10 {
					t.Errorf("Offset: want 10, got %d", uq.Offset)
				}
			},
		},
		{
			name:      "offset negative",
			uri:       "cog:conversations?offset=-1",
			wantErr:   true,
			errSubstr: "invalid offset",
		},
		{
			name: "order asc",
			uri:  "cog:conversations?order=asc",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Order != "asc" {
					t.Errorf("Order: want asc, got %q", uq.Order)
				}
			},
		},
		{
			name: "order desc",
			uri:  "cog:conversations?order=desc",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Order != "desc" {
					t.Errorf("Order: want desc, got %q", uq.Order)
				}
			},
		},
		{
			name:      "order invalid",
			uri:       "cog:conversations?order=random",
			wantErr:   true,
			errSubstr: "invalid order",
		},
		{
			name: "res=pointer",
			uri:  "cog:conversations?res=pointer",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Res != ResPointer {
					t.Errorf("Res: want pointer, got %q", uq.Res)
				}
			},
		},
		{
			name: "res=abstract",
			uri:  "cog:conversations?res=abstract",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Res != ResAbstract {
					t.Errorf("Res: want abstract, got %q", uq.Res)
				}
			},
		},
		{
			name: "res=full (default)",
			uri:  "cog:conversations",
			check: func(t *testing.T, uq *URIQuery) {
				if uq.Res != ResFull {
					t.Errorf("Res: want full (default), got %q", uq.Res)
				}
			},
		},
		{
			name:      "res invalid",
			uri:       "cog:conversations?res=summary",
			wantErr:   true,
			errSubstr: "invalid res",
		},
		{
			name: "fields projection",
			uri:  "cog:conversations?fields=timestamp,role,text",
			check: func(t *testing.T, uq *URIQuery) {
				if len(uq.Fields) != 3 {
					t.Errorf("Fields: want 3, got %v", uq.Fields)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uq, err := ParseConversationURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error containing %q, got nil", tc.errSubstr)
				}
				if !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("want error %q, got %q", tc.errSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.check != nil {
				tc.check(t, uq)
			}
		})
	}
}

// ─── component= and ontology= are now active (not reserved) ─────────────────
//
// v0.2 groundwork activated these params. The old "reserved" check was removed
// so these tests now verify that the params parse without error.

func TestParseConversationURI_ReservedParams(t *testing.T) {
	// component= and ontology= were previously reserved but are now active params
	// as of v0.2 ontology-as-class enforcement. Verify they no longer error.
	for _, param := range []string{"component", "ontology"} {
		t.Run("now-active="+param, func(t *testing.T) {
			uri := fmt.Sprintf("cog:conversations?%s=foo", param)
			_, err := ParseConversationURI(uri)
			if err != nil {
				t.Errorf("param %q should be accepted (v0.2 active), got error: %v", param, err)
			}
		})
	}
}

func TestParseConversationURI_UnknownParam(t *testing.T) {
	tests := []string{
		"cog:conversations?foo=bar",
		"cog:conversations?ref=v1",        // ref= is ADR-067 address; not for conversations
		"cog:conversations?format=csv",
		"cog:conversations?cursor=abc",
	}
	for _, uri := range tests {
		t.Run(uri, func(t *testing.T) {
			_, err := ParseConversationURI(uri)
			if err == nil {
				t.Fatalf("want error for unknown param in %q, got nil", uri)
			}
			if !strings.Contains(err.Error(), "unknown query parameter") {
				t.Errorf("error should mention 'unknown query parameter', got %q", err.Error())
			}
		})
	}
}

// ─── Fragment tests ───────────────────────────────────────────────────────────

func TestParseConversationURI_Fragments(t *testing.T) {
	tests := []struct {
		name      string
		uri       string
		wantFrag  *FragmentSpec
		wantErr   bool
		errSubstr string
	}{
		{
			name: "id anchor",
			uri:  "cog:conversations/claude-code/sess1#id-abc-123-def",
			wantFrag: &FragmentSpec{ID: "abc-123-def", TurnM: -1},
		},
		{
			name:     "turn single",
			uri:      "cog:conversations#turn-5",
			wantFrag: &FragmentSpec{TurnN: 5, TurnM: -1},
		},
		{
			name: "turn range",
			uri:  "cog:conversations#turn-3..10",
			wantFrag: &FragmentSpec{TurnN: 3, TurnM: 10},
		},
		{
			name:      "turn range reversed",
			uri:       "cog:conversations#turn-10..3",
			wantErr:   true,
			errSubstr: "range end",
		},
		{
			name:      "unknown fragment form",
			uri:       "cog:conversations#section-foo",
			wantErr:   true,
			errSubstr: "must start with id- or turn-",
		},
		{
			name:      "empty id",
			uri:       "cog:conversations#id-",
			wantErr:   true,
			errSubstr: "empty id after id-",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uq, err := ParseConversationURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error containing %q, got nil", tc.errSubstr)
				}
				if !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("want error %q, got %q", tc.errSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantFrag == nil {
				if uq.Fragment != nil {
					t.Errorf("want no fragment, got %+v", uq.Fragment)
				}
				return
			}
			if uq.Fragment == nil {
				t.Fatal("want fragment, got nil")
			}
			if tc.wantFrag.ID != "" && uq.Fragment.ID != tc.wantFrag.ID {
				t.Errorf("Fragment.ID: want %q, got %q", tc.wantFrag.ID, uq.Fragment.ID)
			}
			if tc.wantFrag.ID == "" {
				if uq.Fragment.TurnN != tc.wantFrag.TurnN {
					t.Errorf("Fragment.TurnN: want %d, got %d", tc.wantFrag.TurnN, uq.Fragment.TurnN)
				}
				if uq.Fragment.TurnM != tc.wantFrag.TurnM {
					t.Errorf("Fragment.TurnM: want %d, got %d", tc.wantFrag.TurnM, uq.Fragment.TurnM)
				}
			}
		})
	}
}

// ─── Bounded hash tests ───────────────────────────────────────────────────────

func TestURIQuery_IsBounded(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		bounded bool
	}{
		{"no constraints", "cog:conversations", false},
		{"q only", "cog:conversations?q=foo", false},
		{"with until", "cog:conversations?until=2026-06-10T00:00:00Z", true},
		{"with since only", "cog:conversations?since=2026-06-01T00:00:00Z", false},
		{"since+until", "cog:conversations?since=2026-06-01T00:00:00Z&until=2026-06-10T00:00:00Z", true},
		{"id fragment", "cog:conversations#id-abc123", true},
		{"turn fragment", "cog:conversations#turn-5", true},
		{"turn range fragment", "cog:conversations#turn-3..10", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uq, err := ParseConversationURI(tc.uri)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if uq.isBounded() != tc.bounded {
				t.Errorf("isBounded: want %v, got %v", tc.bounded, uq.isBounded())
			}
		})
	}
}

func TestResolveConversationURI_BoundedHash(t *testing.T) {
	idx := buildTestIndex(t)

	t.Run("unbounded has no content_hash", func(t *testing.T) {
		slice, err := ResolveConversationURI("cog:conversations", idx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if slice.ContentHash != "" {
			t.Errorf("unbounded slice should not have content_hash, got %q", slice.ContentHash)
		}
		if slice.Bounded {
			t.Errorf("unbounded slice should have Bounded=false")
		}
	})

	t.Run("bounded by until has content_hash", func(t *testing.T) {
		slice, err := ResolveConversationURI("cog:conversations?until=2026-12-31T23:59:59Z", idx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if slice.ContentHash == "" {
			t.Error("bounded slice should have content_hash")
		}
		if !strings.HasPrefix(slice.ContentHash, "sha256:") {
			t.Errorf("content_hash should start with sha256:, got %q", slice.ContentHash)
		}
		if !slice.Bounded {
			t.Errorf("bounded slice should have Bounded=true")
		}
	})

	t.Run("bounded by fragment has content_hash", func(t *testing.T) {
		slice, err := ResolveConversationURI("cog:conversations#turn-0..1", idx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if slice.ContentHash == "" {
			t.Error("fragment-bounded slice should have content_hash")
		}
	})
}

// ─── Resolution level tests ────────────────────────────────────────────────────

func TestResolveConversationURI_ResLevels(t *testing.T) {
	idx := buildTestIndex(t)

	t.Run("res=full includes text", func(t *testing.T) {
		slice, err := ResolveConversationURI("cog:conversations?res=full", idx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, turn := range slice.Turns {
			if turn.Text == "" {
				t.Errorf("res=full: turn %d should have non-empty Text", turn.TurnIndex)
			}
		}
	})

	t.Run("res=pointer has no text", func(t *testing.T) {
		slice, err := ResolveConversationURI("cog:conversations?res=pointer", idx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, turn := range slice.Turns {
			if turn.Text != "" {
				t.Errorf("res=pointer: turn %d should have empty Text, got %q", turn.TurnIndex, turn.Text)
			}
		}
	})

	t.Run("res=abstract truncates text", func(t *testing.T) {
		// Build an index with a long turn.
		idx2 := buildIndexWithLongTurn(t)
		slice, err := ResolveConversationURI("cog:conversations?res=abstract", idx2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, turn := range slice.Turns {
			if len(turn.Text) > abstractMaxLen+3 { // +3 for ellipsis
				t.Errorf("res=abstract: turn %d text too long: %d chars", turn.TurnIndex, len(turn.Text))
			}
		}
	})
}

// ─── IDAnchor tests ───────────────────────────────────────────────────────────

func TestResolveConversationURI_IDAnchors(t *testing.T) {
	idx := buildTestIndex(t)
	slice, err := ResolveConversationURI("cog:conversations", idx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, turn := range slice.Turns {
		if !strings.HasPrefix(turn.IDAnchor, "#id-") {
			t.Errorf("turn %d: IDAnchor should start with #id-, got %q", turn.TurnIndex, turn.IDAnchor)
		}
		if turn.UUID == "" {
			t.Errorf("turn %d: UUID should not be empty", turn.TurnIndex)
		}
	}
}

// ─── Metadata fields tests ─────────────────────────────────────────────────────

func TestResolveConversationURI_Metadata(t *testing.T) {
	idx := buildTestIndex(t)
	slice, err := ResolveConversationURI("cog:conversations", idx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slice.ResolvedAt.IsZero() {
		t.Error("ResolvedAt should not be zero")
	}
	if slice.URI == "" {
		t.Error("URI should not be empty")
	}
	if len(slice.Sources) == 0 {
		t.Error("Sources should not be empty")
	}
}

// ─── Filter tests ─────────────────────────────────────────────────────────────

func TestResolveConversationURI_RoleFilter(t *testing.T) {
	idx := buildTestIndex(t)

	slice, err := ResolveConversationURI("cog:conversations?role=user", idx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, turn := range slice.Turns {
		if turn.Role != string(RoleUser) {
			t.Errorf("role filter: expected only user turns, got %q", turn.Role)
		}
	}
}

func TestResolveConversationURI_FragmentIDFilter(t *testing.T) {
	idx := buildTestIndex(t)

	// Find a real UUID from the index to use as fragment.
	var targetUUID string
	if turns := idx.SessionTurns("test-session-1"); len(turns) > 0 {
		targetUUID = turns[0].UUID
	}
	if targetUUID == "" {
		t.Skip("no turns in test index")
	}

	slice, err := ResolveConversationURI("cog:conversations#id-"+targetUUID, idx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slice.Count != 1 {
		t.Errorf("id fragment: want 1 result, got %d", slice.Count)
	}
	if len(slice.Turns) > 0 && slice.Turns[0].UUID != targetUUID {
		t.Errorf("id fragment: want UUID %q, got %q", targetUUID, slice.Turns[0].UUID)
	}
}

func TestResolveConversationURI_LimitOffset(t *testing.T) {
	idx := buildTestIndexNTurns(t, 10)

	// limit=3
	slice, err := ResolveConversationURI("cog:conversations?limit=3", idx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slice.Count != 3 {
		t.Errorf("limit=3: want 3 turns, got %d", slice.Count)
	}

	// offset=5, limit=3
	slice2, err := ResolveConversationURI("cog:conversations?offset=5&limit=3", idx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 10 total - 5 skipped = 5 remaining, limit to 3
	if slice2.Count != 3 {
		t.Errorf("offset=5,limit=3: want 3 turns, got %d", slice2.Count)
	}
	// First turn of offset slice should be turn index 5.
	if len(slice2.Turns) > 0 && slice2.Turns[0].TurnIndex != 5 {
		t.Errorf("offset=5: first result TurnIndex should be 5, got %d", slice2.Turns[0].TurnIndex)
	}
}

// ─── URI-mixing error test (from MCP layer) ───────────────────────────────────

func TestCheckURIMixing(t *testing.T) {
	// Simulate the MCP handler check: if URI is set alongside other filter
	// params, return an error.
	tests := []struct {
		name    string
		uri     string
		hasOtherParams bool
		wantErr bool
	}{
		{"uri alone", "cog:conversations?q=foo", false, false},
		{"uri + extra param", "cog:conversations?q=foo", true, true},
		{"no uri", "", false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkURIMixing(tc.uri, tc.hasOtherParams)
			if tc.wantErr && err == nil {
				t.Error("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("want no error, got %v", err)
			}
		})
	}
}

// checkURIMixing is the same logic used in the MCP handler.
func checkURIMixing(uri string, hasOtherParams bool) error {
	if uri != "" && hasOtherParams {
		return ErrURIMixedParams
	}
	return nil
}

// ─── Test helpers ─────────────────────────────────────────────────────────────

func buildTestIndex(t *testing.T) *Index {
	t.Helper()
	idx := newIndexForTest(t)
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	meta := SessionMeta{
		SessionID:   "test-session-1",
		Source:      "",
		TurnCount:   4,
		FirstTurnAt: now,
		LastTurnAt:  now.Add(4 * time.Minute),
	}
	turns := []Turn{
		{UUID: "uuid-0", SessionID: "test-session-1", TurnIndex: 0, Role: RoleUser, Timestamp: now, Text: "hello world"},
		{UUID: "uuid-1", SessionID: "test-session-1", TurnIndex: 1, Role: RoleAssistant, Timestamp: now.Add(time.Minute), Text: "hi there"},
		{UUID: "uuid-2", SessionID: "test-session-1", TurnIndex: 2, Role: RoleUser, Timestamp: now.Add(2 * time.Minute), Text: "what is harness attestation"},
		{UUID: "uuid-3", SessionID: "test-session-1", TurnIndex: 3, Role: RoleAssistant, Timestamp: now.Add(3 * time.Minute), Text: "harness attestation is about..."},
	}
	idx.setTurnsForTest("test-session-1", meta, turns)
	return idx
}

func buildIndexWithLongTurn(t *testing.T) *Index {
	t.Helper()
	idx := newIndexForTest(t)
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	longText := strings.Repeat("x", 1000)
	meta := SessionMeta{
		SessionID:   "long-session",
		TurnCount:   1,
		FirstTurnAt: now,
		LastTurnAt:  now,
	}
	turns := []Turn{
		{UUID: "uuid-long", SessionID: "long-session", TurnIndex: 0, Role: RoleUser, Timestamp: now, Text: longText},
	}
	idx.setTurnsForTest("long-session", meta, turns)
	return idx
}

func buildTestIndexNTurns(t *testing.T, n int) *Index {
	t.Helper()
	idx := newIndexForTest(t)
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	turns := make([]Turn, n)
	for i := 0; i < n; i++ {
		turns[i] = Turn{
			UUID:      fmt.Sprintf("uuid-%d", i),
			SessionID: "sess",
			TurnIndex: i,
			Role:      RoleUser,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Text:      fmt.Sprintf("turn %d text", i),
		}
	}
	idx.setTurnsForTest("sess", SessionMeta{
		SessionID:   "sess",
		TurnCount:   n,
		FirstTurnAt: now,
		LastTurnAt:  now.Add(time.Duration(n) * time.Minute),
	}, turns)
	return idx
}
