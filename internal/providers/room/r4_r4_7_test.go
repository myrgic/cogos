package room

import "testing"

// R4-7: a re-added member's accepted entries must not carry BeforeRemoval.
// Only entries at or below the removal cutoff are before the removal.
func TestBoardR4_R4_7(t *testing.T) {
	devA, devB := testNodeSet["node-a"].dev, testNodeSet["node-b"].dev
	w := newWorld(t, nil)
	before := w.line("node-b", vroku, "last words")
	w.sync()
	rm := defaultGenesisBody()
	rm["gen"] = 2
	delete(rm, "name")
	delete(rm["members"].(map[string]any), "vroku")
	rm["cutoffs"] = map[string]any{"vroku": map[string]any{devA: 1, devB: before.Seq}}
	removal := w.mk("node-a", KindSpec, chaz, rm)
	w.sync()
	readd := defaultGenesisBody()
	readd["gen"] = 3
	delete(readd, "name")
	readdE := w.mk("node-a", KindSpec, chaz, readd, func(e *Entry) { e.Spec = idOfEntry(removal) })
	w.sync()
	back := w.mk("node-b", KindLine, vroku, map[string]any{"text": "i am back", "to": []any{}}, func(e *Entry) { e.Spec = idOfEntry(readdE) })
	o := w.eval()
	w.accepted(o, removal)
	w.accepted(o, readdE)
	w.accepted(o, before)
	w.accepted(o, back)
	if v, _ := o.Verdict(idOfEntry(before)); !v.BeforeRemoval {
		t.Fatalf("control: entry at the cutoff must be BeforeRemoval: %+v", v)
	}
	if v, _ := o.Verdict(idOfEntry(back)); v.BeforeRemoval {
		t.Fatalf("re-added member's new entry marked BeforeRemoval: %+v", v)
	}
}
