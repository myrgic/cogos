package room

import (
	"reflect"
	"testing"
)

// cog-review on 824b74b: Validate checked that the named owner holds the owner
// role but never that nobody else does, so a spec could make a second owner.
func TestBoardR7_OnlyTheNamedOwnerHoldsTheOwnerRole(t *testing.T) {
	w := newWorld(t, nil)
	s, err := SpecFromEntry(w.g)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(nil); err != nil {
		t.Fatal(err)
	}
	members := map[string]Member{}
	for id, m := range s.Members {
		members[id] = m
	}
	var other string
	for id, m := range members {
		if id != s.Owner && m.Type == TypePerson {
			m.Role = RoleOwner
			members[id] = m
			other = id
			break
		}
	}
	if other == "" {
		t.Fatal("fixture has no second person member")
	}
	s2 := *s
	s2.Members = members
	if err := s2.Validate(nil); err == nil {
		t.Fatalf("spec with a second owner (%s) accepted", other)
	}
}

// cog-review on 824b74b: an entry whose spec field names a real entry that is
// not a spec or genesis was held forever (spec_missing) instead of rejected.
func TestBoardR7_SpecFieldNamingANonSpecEntryIsRejected(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "a plain line")
	w.sync()
	x := w.mk("node-a", KindLine, chaz, map[string]any{"text": "x", "to": []any{}}, func(e *Entry) {
		e.Spec = idOfEntry(l)
	})
	w.sync()
	o := w.eval()
	w.rejected(o, x, CodeSpecLost)
}

// cog-review on 824b74b: equivocation events were appended in Go map order of
// the logs, so Outcome.Events differed across runs on one identical input.
func TestBoardR7_EquivocationEventsInStableOrder(t *testing.T) {
	w := newWorld(t, nil)
	fork := func(node string, actor Actor) []*Entry {
		a := w.line(node, actor, "one")
		b := *a
		b.Body = map[string]any{"text": "two", "to": []any{}}
		if err := b.Sign(testNodeSet[node].key); err != nil {
			t.Fatal(err)
		}
		return []*Entry{a, &b}
	}
	set := []*Entry{w.g}
	set = append(set, fork("node-a", chaz)...)
	set = append(set, fork("node-b", vroku)...)
	var first []Event
	for i := 0; i < 40; i++ {
		o := w.eval(set...)
		var ev []Event
		for _, e := range o.Events {
			if e.Type == EventLogEquivocation {
				ev = append(ev, e)
			}
		}
		if len(ev) != 2 {
			t.Fatalf("want 2 equivocation events, got %d", len(ev))
		}
		if i == 0 {
			first = ev
		} else if !reflect.DeepEqual(ev, first) {
			t.Fatalf("run %d: equivocation events in a different order", i)
		}
	}
}
