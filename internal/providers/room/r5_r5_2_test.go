package room

import "testing"

// R5-2: a log that was not pinned by a removal's cutoffs (a device that joined
// after the removal) must not be marked BeforeRemoval when the member is re-added.
func TestBoardR5_R5_2(t *testing.T) {
	devA, devB := testNodeSet["node-a"].dev, testNodeSet["node-b"].dev
	phone := testNodeSet["phone"].dev
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
	readd["nodes"].(map[string]any)["phone"] = phone
	readd["members"].(map[string]any)["vroku"] = map[string]any{"type": "person", "role": "collaborator", "homes": []any{"node-b", "phone"}}
	readdE := w.mk("node-a", KindSpec, chaz, readd, func(e *Entry) { e.Spec = idOfEntry(removal) })
	w.sync()
	w.app("phone")
	back := w.mk("phone", KindLine, vroku, map[string]any{"text": "i am back from a new device", "to": []any{}}, func(e *Entry) { e.Spec = idOfEntry(readdE) })
	o := w.eval()
	w.accepted(o, removal)
	w.accepted(o, readdE)
	w.accepted(o, before)
	w.accepted(o, back)
	if v, _ := o.Verdict(idOfEntry(before)); !v.BeforeRemoval {
		t.Fatalf("control: entry at the cutoff must be BeforeRemoval: %+v", v)
	}
	if v, _ := o.Verdict(idOfEntry(back)); v.BeforeRemoval {
		t.Fatalf("new-device entry after re-add marked BeforeRemoval: %+v", v)
	}
}
