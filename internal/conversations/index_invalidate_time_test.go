package conversations

import (
	"reflect"
	"testing"
	"time"
)

// TestInvalidate_RealisticTimesNoSpuriousInvalidation is the cog-review #718
// regression: metas carrying non-UTC timestamps (SourceMtime from
// fi.ModTime(), local zone) and an in-process time.Now() (monotonic reading)
// must not compare as "changed" after a disk round-trip. With `!=` every
// session was invalidated on every reload; with sessionMetaEqual only the
// session a peer actually changed is.
func TestInvalidate_RealisticTimesNoSpuriousInvalidation(t *testing.T) {
	raiseTurnsCacheEntries(t, 4096)
	dir := t.TempDir()
	ny := time.FixedZone("EDT", -4*3600)
	base := time.Date(2026, 10, 25, 19, 0, 0, 0, ny)

	// Writer A keeps its in-memory metas (time.Now() with monotonic reading,
	// local-zone mtimes) — exactly the daemon's own process state.
	a, err := NewIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	var items []SessionAndTurns
	for i := 0; i < 50; i++ {
		sid := sidFor(i)
		items = append(items, SessionAndTurns{
			Meta: SessionMeta{
				SessionID: sid, SourcePath: "/x/" + sid + ".jsonl", TurnCount: 1,
				FirstTurnAt: base, LastTurnAt: base.Add(time.Minute),
				IndexedAt: time.Now(), SourceMtime: base.Add(time.Duration(i) * time.Second), SourceSize: 100,
			},
			Turns: []Turn{{UUID: sid + "-u0", SessionID: sid, Role: RoleUser, Timestamp: base, Text: "t " + sid}},
		})
	}
	if _, err := a.UpsertSessions(items); err != nil {
		t.Fatal(err)
	}

	reads := countReadsBy(t)
	// Peer changes one session; A reloads (its in-memory metas vs decoded disk).
	c, err := NewIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Load(); err != nil {
		t.Fatal(err)
	}
	const changed = 13
	m := c.sessions[sidFor(changed)]
	m.IndexedAt = m.IndexedAt.Add(time.Hour)
	if err := c.UpsertSession(m, []Turn{{UUID: "n", SessionID: sidFor(changed), Text: "peer"}}); err != nil {
		t.Fatal(err)
	}
	if reloaded, err := a.LoadIfChanged(); err != nil || !reloaded {
		t.Fatalf("A LoadIfChanged = (%v, %v), want (true, nil)", reloaded, err)
	}
	*reads = 0
	for i := 0; i < 50; i++ {
		if _, ok := a.GetTurn(sidFor(i), 0); !ok {
			t.Fatalf("GetTurn(%d) failed", i)
		}
	}
	if *reads != 1 {
		t.Fatalf("writer-side reload: %d turns files re-read, want 1 (spurious invalidation from time.Time ==)", *reads)
	}

	// Second hop: a decoded-vs-decoded comparison (two independent Loads of
	// "-04:00" timestamps get distinct *Location values).
	b, err := NewIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Load(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		b.GetTurn(sidFor(i), 0)
	}
	m = c.sessions[sidFor(changed)]
	m.IndexedAt = m.IndexedAt.Add(time.Hour)
	if err := c.UpsertSession(m, []Turn{{UUID: "n2", SessionID: sidFor(changed), Text: "peer2"}}); err != nil {
		t.Fatal(err)
	}
	if reloaded, err := b.LoadIfChanged(); err != nil || !reloaded {
		t.Fatalf("B LoadIfChanged = (%v, %v), want (true, nil)", reloaded, err)
	}
	*reads = 0
	for i := 0; i < 50; i++ {
		if _, ok := b.GetTurn(sidFor(i), 0); !ok {
			t.Fatalf("B GetTurn(%d) failed", i)
		}
	}
	if *reads != 1 {
		t.Fatalf("decoded-vs-decoded reload: %d turns files re-read, want 1", *reads)
	}
}

// TestSessionMetaEqual_CoversEveryField fails when SessionMeta gains a field
// that sessionMetaEqual does not compare: each field is perturbed in turn and
// must make the metas unequal.
func TestSessionMetaEqual_CoversEveryField(t *testing.T) {
	base := SessionMeta{}
	rt := reflect.TypeOf(base)
	for i := 0; i < rt.NumField(); i++ {
		m := base
		f := reflect.ValueOf(&m).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString("x")
		case reflect.Int, reflect.Int64:
			f.SetInt(7)
		case reflect.Struct:
			if f.Type() != reflect.TypeOf(time.Time{}) {
				t.Fatalf("field %s: unhandled struct type %s — extend sessionMetaEqual and this test", rt.Field(i).Name, f.Type())
			}
			f.Set(reflect.ValueOf(time.Unix(1700000000, 0)))
		default:
			t.Fatalf("field %s: unhandled kind %s — extend sessionMetaEqual and this test", rt.Field(i).Name, f.Kind())
		}
		if sessionMetaEqual(base, m) {
			t.Errorf("sessionMetaEqual ignores field %s", rt.Field(i).Name)
		}
	}
	// Same instant, different Location / monotonic reading → equal.
	now := time.Now()
	a := SessionMeta{IndexedAt: now, SourceMtime: now.In(time.FixedZone("X", -4*3600))}
	b := SessionMeta{IndexedAt: now.Round(0).UTC(), SourceMtime: now.UTC()}
	if !sessionMetaEqual(a, b) {
		t.Error("sessionMetaEqual: same instants in different zones/monotonic compare unequal")
	}
}
