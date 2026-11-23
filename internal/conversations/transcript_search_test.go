// transcript_search_test.go — tests for Provider.SearchTurns (myrgic/cogos#650).
package conversations

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Index.Search walks sessions in lexical id order. SearchTurns must return
// newest first regardless of that order, and honour the limit.
func TestSearchTurns_NewestFirstAcrossSessions(t *testing.T) {
	p, root := newTestProvider(t)
	srcDir := t.TempDir()
	const needle = "stigmergy coordination"

	// Session "aaa..." is lexically first but older. "fff..." is newer.
	older := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	newer := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	writeJSONLFixture(t, srcDir, older, []string{
		makeUserRecord("u-old", "", older, "old note about "+needle, "2026-05-01T10:00:00Z"),
	})
	writeJSONLFixture(t, srcDir, newer, []string{
		makeUserRecord("u-new", "", newer, "new note about "+needle, "2026-05-20T10:00:00Z"),
	})
	writeObservatoryConfig(t, root, []string{srcDir})

	ctx := context.Background()
	cfgAny, _ := p.LoadConfig(root)
	liveAny, _ := p.FetchLive(ctx, cfgAny)
	plan, _ := p.ComputePlan(cfgAny, liveAny, nil)
	_, _ = p.ApplyPlan(ctx, plan)

	hits, err := p.SearchTurns(needle, 0)
	if err != nil {
		t.Fatalf("SearchTurns: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("want 2 hits, got %d", len(hits))
	}
	if hits[0].SessionID != newer {
		t.Errorf("first hit session = %s, want newest %s", hits[0].SessionID, newer)
	}

	capped, err := p.SearchTurns(needle, 1)
	if err != nil || len(capped) != 1 || capped[0].SessionID != newer {
		t.Errorf("limit=1 should return only the newest hit, got %+v err=%v", capped, err)
	}
}

// Before the index is loaded, SearchTurns must return ErrIndexNotInitialised,
// not an empty slice. The caller reports that as "unavailable".
func TestSearchTurns_UninitialisedIndexErrors(t *testing.T) {
	p, _ := newTestProvider(t)
	hits, err := p.SearchTurns("anything", 10)
	if !errors.Is(err, ErrIndexNotInitialised) {
		t.Fatalf("err = %v, want ErrIndexNotInitialised", err)
	}
	if hits != nil {
		t.Errorf("hits = %v, want nil", hits)
	}
}

// A blank query matches nothing and is not an error.
func TestSearchTurns_BlankQueryIsEmpty(t *testing.T) {
	p, root := newTestProvider(t)
	srcDir := t.TempDir()
	sid := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	writeJSONLFixture(t, srcDir, sid, []string{
		makeUserRecord("u1", "", sid, "some text", "2026-05-10T12:00:00Z"),
	})
	writeObservatoryConfig(t, root, []string{srcDir})
	ctx := context.Background()
	cfgAny, _ := p.LoadConfig(root)
	liveAny, _ := p.FetchLive(ctx, cfgAny)
	plan, _ := p.ComputePlan(cfgAny, liveAny, nil)
	_, _ = p.ApplyPlan(ctx, plan)

	hits, err := p.SearchTurns("   ", 10)
	if err != nil || len(hits) != 0 {
		t.Errorf("blank query: hits=%v err=%v, want empty and nil", hits, err)
	}
}

// A source-scoped transcript URI must resolve to exactly the turn it names. The
// index keys source sessions as "<source>/<sid>", and the URI must not double the
// source. The index is built directly with UpsertSession so the real key shape
// is exercised.
func TestSourceScopedURI_ResolvesToExactTurn(t *testing.T) {
	const source = "hermes-cog"
	const sid = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
	const uuid = "u-src-1"
	key := source + "/" + sid

	idx, err := NewIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	meta := SessionMeta{SessionID: key, Source: source, FirstTurnAt: ts, LastTurnAt: ts}
	turns := []Turn{
		{SessionID: key, TurnIndex: 0, UUID: uuid, Role: RoleUser,
			Text: "sourced transcript phrase quokka", Timestamp: ts},
		{SessionID: key, TurnIndex: 1, UUID: "a-src-1", Role: RoleAssistant,
			Text: "an unrelated reply", Timestamp: ts.Add(time.Minute)},
	}
	if err := idx.UpsertSession(meta, turns); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}

	hits := idx.Search("quokka", time.Time{}, time.Time{}, "", "", 10)
	if len(hits) != 1 {
		t.Fatalf("setup: want 1 hit, got %d", len(hits))
	}
	// This is what the adapter builds from a hit: bare sid after the source.
	bare := hits[0].SessionID[len(source)+1:]
	uri := "cog:conversations/" + source + "/" + bare + "#id-" + hits[0].UUID
	if want := "cog:conversations/" + source + "/" + sid + "#id-" + uuid; uri != want {
		t.Fatalf("URI = %q, want %q", uri, want)
	}

	resolved, err := ResolveConversationURI(uri, idx)
	if err != nil {
		t.Fatalf("ResolveConversationURI(%q): %v", uri, err)
	}
	if resolved.Count != 1 || len(resolved.Turns) != 1 || resolved.Turns[0].UUID != uuid {
		t.Fatalf("resolved %d turns, want exactly uuid %q: %+v", resolved.Count, uuid, resolved.Turns)
	}
}

// A Claude Code session has no source. Its URI uses the whole-observatory form
// and must resolve to the exact turn.
func TestCCURI_ResolvesToExactTurn(t *testing.T) {
	const sid = "cccccccc-1111-cccc-cccc-cccccccccccc"
	const uuid = "u-cc-1"
	idx, err := NewIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	meta := SessionMeta{SessionID: sid, FirstTurnAt: ts, LastTurnAt: ts}
	turns := []Turn{{SessionID: sid, TurnIndex: 0, UUID: uuid, Role: RoleUser,
		Text: "cc transcript phrase numbat", Timestamp: ts}}
	if err := idx.UpsertSession(meta, turns); err != nil {
		t.Fatalf("UpsertSession: %v", err)
	}
	uri := "cog:conversations#id-" + uuid
	resolved, err := ResolveConversationURI(uri, idx)
	if err != nil {
		t.Fatalf("ResolveConversationURI(%q): %v", uri, err)
	}
	if resolved.Count != 1 || resolved.Turns[0].UUID != uuid {
		t.Fatalf("resolved %+v, want exactly uuid %q", resolved.Turns, uuid)
	}
}
