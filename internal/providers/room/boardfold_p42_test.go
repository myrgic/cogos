package room

import (
	"strings"
	"testing"
)

// Table-style tests for the board fold (RFC rooms-as-declared-resources
// §6.9.2, slice P4-2). One case per named fold rule. Every case builds a real
// world, evaluates it (so only accepted entries are folded), then folds with
// FoldBoard. Against the current skeleton these fail with the stub panic
// "unimplemented"; they are the acceptance bar for the implementation.

// foldCase is one row: a scenario that builds entries and asserts the fold.
type foldCase struct {
	name  string
	build func(t *testing.T, w *world) func(b *Board)
}

func runFoldCases(t *testing.T, cases []foldCase) {
	t.Helper()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t, nil)
			check := c.build(t, w)
			o := w.eval()
			b := FoldBoard(o, w.all)
			if b == nil {
				t.Fatal("FoldBoard returned nil")
			}
			check(b)
		})
	}
}

// §6.9.2 row "item.set": two sets of one field, neither in the other's deps,
// are both kept as a multi-value register with MarkFieldConflict.
func TestBoardFoldSetConcurrentFieldConflict(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "two concurrent sets keep both values and mark the field",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "title", "value": "from-a"})
			w.mk("node-b", KindItemSet, vroku, map[string]any{"item": id, "field": "title", "value": "from-b"})
			return func(b *Board) {
				it := b.Items[id]
				if it == nil {
					t.Fatalf("item %s not folded", id)
				}
				f := it.Fields["title"]
				if len(f.Values) != 2 {
					t.Fatalf("title values: got %d, want 2 (both kept)", len(f.Values))
				}
				if f.Mark != MarkFieldConflict {
					t.Fatalf("title mark: got %q, want %q", f.Mark, MarkFieldConflict)
				}
				authors := map[string]bool{}
				for _, v := range f.Values {
					authors[v.Author] = true
				}
				if !authors[chaz.String()] || !authors[vroku.String()] {
					t.Fatalf("value authors: got %v, want chaz and vroku", authors)
				}
			}
		},
	}, {
		name: "a later set whose deps cover both values replaces them and clears the mark",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "title", "value": "from-a"})
			w.mk("node-b", KindItemSet, vroku, map[string]any{"item": id, "field": "title", "value": "from-b"})
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "title", "value": "resolved"})
			return func(b *Board) {
				f := b.Items[id].Fields["title"]
				if len(f.Values) != 1 || f.Values[0].Value != "resolved" {
					t.Fatalf("title after resolve: got %+v, want single value resolved", f.Values)
				}
				if f.Mark != MarkNone {
					t.Fatalf("title mark after resolve: got %q, want none", f.Mark)
				}
			}
		},
	}, {
		name: "sequential sets (second cites first) show only the later value",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "title", "value": "first"})
			w.sync()
			w.mk("node-b", KindItemSet, vroku, map[string]any{"item": id, "field": "title", "value": "second"})
			return func(b *Board) {
				f := b.Items[id].Fields["title"]
				if len(f.Values) != 1 || f.Values[0].Value != "second" || f.Mark != MarkNone {
					t.Fatalf("title: got %+v mark %q, want single value second, no mark", f.Values, f.Mark)
				}
			}
		},
	}})
}

// §6.9.2 row "item.append": concurrent appends are both kept, ordered by
// (hlc, log, seq).
func TestBoardFoldAppendOrdering(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "concurrent appends to one list are both kept in (hlc, log, seq) order",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-b", KindItemAppend, vroku, map[string]any{"item": id, "field": "comments", "value": "b-first"})
			w.mk("node-a", KindItemAppend, chaz, map[string]any{"item": id, "field": "comments", "value": "a-second"})
			return func(b *Board) {
				l := b.Items[id].Lists["comments"]
				if len(l) != 2 {
					t.Fatalf("comments: got %d entries, want 2", len(l))
				}
				for i := 1; i < len(l); i++ {
					prev, cur := l[i-1], l[i]
					if prev.HLC > cur.HLC {
						t.Fatalf("comments out of hlc order: %q before %q", prev.HLC, cur.HLC)
					}
					if prev.HLC == cur.HLC && prev.Log > cur.Log {
						t.Fatalf("comments out of log order at equal hlc")
					}
				}
			}
		},
	}})
}

// §6.9.2 row "item.archive": an archive is kept with its closure; an archive
// concurrent with a set carries MarkConcurrentEdit and the set stays in the
// item's history.
func TestBoardFoldArchive(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "archive with closure done marks the item archived and keeps the closure",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": id, "closure": "done"})
			return func(b *Board) {
				it := b.Items[id]
				if !it.Archived {
					t.Fatalf("item not archived")
				}
				if it.Closure == nil || it.Closure.Kind != "done" {
					t.Fatalf("closure: got %+v, want kind done", it.Closure)
				}
				if it.Mark != MarkNone {
					t.Fatalf("archive mark: got %q, want none (no concurrent set)", it.Mark)
				}
			}
		},
	}, {
		name: "archive concurrent with a set is marked concurrent_edit and the set stays in history",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			set := w.mk("node-b", KindItemSet, vroku, map[string]any{"item": id, "field": "title", "value": "edited"})
			w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": id, "closure": "killed", "reason": "superseded"})
			setID, _ := set.ID()
			return func(b *Board) {
				it := b.Items[id]
				if !it.Archived || it.Mark != MarkConcurrentEdit {
					t.Fatalf("archived %v mark %q, want archived with %q", it.Archived, it.Mark, MarkConcurrentEdit)
				}
				found := false
				for _, h := range it.History {
					if h == setID {
						found = true
					}
				}
				if !found {
					t.Fatalf("concurrent set %s missing from History %v", setID, it.History)
				}
			}
		},
	}, {
		name: "archive with closure handed-off keeps owner and next",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": id, "closure": "handed-off", "owner": "cog", "next": "ship"})
			return func(b *Board) {
				c := b.Items[id].Closure
				if c == nil || c.Kind != "handed-off" || c.Owner != "cog" || c.Next != "ship" {
					t.Fatalf("closure: got %+v, want handed-off owner cog next ship", c)
				}
			}
		},
	}})
}

// §6.9.2 "An item's author": derived as the actor of the accepted item.create,
// immutable; an item.set of author is never folded.
func TestBoardFoldAuthorDerived(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "author is the actor of the accepted item.create and is not a settable field",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-b", vroku, nil)
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "author", "value": "person:chaz"})
			return func(b *Board) {
				it := b.Items[id]
				if it.Author != vroku.String() {
					t.Fatalf("author: got %q, want %q", it.Author, vroku.String())
				}
				if _, ok := it.Fields["author"]; ok {
					t.Fatalf("author field folded; it must never be settable")
				}
			}
		},
	}})
}

// §6.9.2 fold is a pure function of the SET of accepted entries: the same set
// in a different delivery order gives the same FoldHash.
func TestBoardFoldOrderIndependentHash(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "reversed delivery order gives an identical FoldHash",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "title", "value": "a"})
			w.mk("node-b", KindItemSet, vroku, map[string]any{"item": id, "field": "title", "value": "b"})
			w.mk("node-b", KindItemAppend, vroku, map[string]any{"item": id, "field": "comments", "value": "c"})
			return func(b *Board) {
				h1, err := FoldHash(b)
				if err != nil {
					t.Fatal(err)
				}
				rev := make([]*Entry, len(w.all))
				for i, e := range w.all {
					rev[len(w.all)-1-i] = e
				}
				b2 := FoldBoard(w.eval(), rev)
				h2, err := FoldHash(b2)
				if err != nil {
					t.Fatal(err)
				}
				if h1 == "" || h1 != h2 {
					t.Fatalf("fold hash differs by delivery order: %q vs %q", h1, h2)
				}
			}
		},
	}})
}

// §6.9.2: only accepted entries are folded. A rejected effect-bearing set by a
// guest must not appear in the item.
func TestBoardFoldSkipsRejected(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "a rejected item.set is not folded",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-b", guest, nil)
			w.sync()
			w.mk("node-b", KindItemSet, guest, map[string]any{"item": id, "field": "state", "value": "doing"})
			return func(b *Board) {
				if _, ok := b.Items[id].Fields["state"]; ok {
					t.Fatalf("rejected state set was folded")
				}
			}
		},
	}})
}

// §6.9.2 item.create row: two creates with one id, the lower entry id wins.
func TestBoardFoldCreateWinner(t *testing.T) {
	w := newWorld(t, nil)
	a, id := w.createItem("node-a", chaz, nil)
	w.sync()
	other := w.mk("node-b", KindItemCreate, vroku, map[string]any{"id": id, "text": "clash"})
	idA, _ := a.ID()
	idB, _ := other.ID()
	win := CreateWinner(a, other)
	if win == nil {
		t.Fatal("CreateWinner returned nil")
	}
	wantID := idA
	if strings.Compare(idB, idA) < 0 {
		wantID = idB
	}
	gotID, _ := win.ID()
	if gotID != wantID {
		t.Fatalf("CreateWinner: got %s, want lower entry id %s", gotID, wantID)
	}
}

// §6.9.2 state row: a conflicted state field reads as stalled.
func TestBoardFoldConflictedState(t *testing.T) {
	runFoldCases(t, []foldCase{{
		name: "concurrent running and done on state is a conflicted state",
		build: func(t *testing.T, w *world) func(b *Board) {
			_, id := w.createItem("node-a", chaz, nil)
			w.sync()
			w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "state", "value": "running"})
			w.mk("node-b", KindItemSet, vroku, map[string]any{"item": id, "field": "state", "value": "done"})
			return func(b *Board) {
				if !ConflictedState(b.Items[id]) {
					t.Fatalf("ConflictedState: got false, want true for concurrent state sets")
				}
			}
		},
	}})
}

// Layer 2 rule 2 and gate B3: a set whose deps cite an entry this replica does
// not hold is held, and the missing ids are returned.
func TestBoardFoldHoldForward(t *testing.T) {
	w := newWorld(t, nil)
	_, id := w.createItem("node-a", chaz, nil)
	w.sync()
	set := w.mk("node-a", KindItemSet, chaz, map[string]any{"item": id, "field": "title", "value": "x"})
	missing, held := HoldForward(set, map[string]*Entry{})
	if !held {
		t.Fatalf("HoldForward: got held=false, want true when deps are unknown")
	}
	if len(missing) == 0 {
		t.Fatalf("HoldForward: no missing ids reported")
	}
}

// §6.9.2 Concurrent: causality is deps only. Entries whose deps do not
// reference each other are concurrent; one citing the other is not.
func TestBoardFoldConcurrentIsDepsOnly(t *testing.T) {
	a := &Entry{Log: "A", Seq: 1}
	b := &Entry{Log: "B", Seq: 1}
	if !Concurrent(a, b, map[string]uint64{"A": 1}, map[string]uint64{"B": 1}) {
		t.Fatalf("Concurrent: got false for entries that do not cite each other")
	}
	if Concurrent(a, b, map[string]uint64{"A": 1}, map[string]uint64{"A": 1, "B": 1}) {
		t.Fatalf("Concurrent: got true although b cites a")
	}
}
