package room

import "testing"

// R4-2: a removal pins cutoffs for every log in the previous spec's nodes. A
// log absent from that map (a node dropped earlier, closed by its node
// cutoff) has no member cutoff; the missing key must not read as cutoff 0.
func TestBoardR4_R4_2(t *testing.T) {
	devA, devB := testNodeSet["node-a"].dev, testNodeSet["node-b"].dev
	devP := testNodeSet["phone"].dev
	w := newWorld(t, func(b map[string]any) {
		b["nodes"].(map[string]any)["phone"] = devP
		b["members"].(map[string]any)["vroku"].(map[string]any)["homes"] = []any{"phone"}
	})
	// Written on the phone under gen 1, before the phone is dropped.
	early := w.mk("phone", KindLine, vroku, map[string]any{"text": "from the phone", "to": []any{}})
	w.sync()
	drop := defaultGenesisBody()
	drop["gen"] = 2
	delete(drop, "name")
	drop["members"].(map[string]any)["vroku"].(map[string]any)["homes"] = []any{"node-b"}
	delete(drop["nodes"].(map[string]any), "phone")
	drop["node_cutoffs"] = map[string]any{devP: early.Seq}
	dropE := w.mk("node-a", KindSpec, chaz, drop)
	w.sync()
	rm := defaultGenesisBody()
	rm["gen"] = 3
	delete(rm, "name")
	delete(rm["members"].(map[string]any), "vroku")
	rm["cutoffs"] = map[string]any{"vroku": map[string]any{devA: 1, devB: 1}}
	removal := w.mk("node-a", KindSpec, chaz, rm, func(e *Entry) { e.Spec = idOfEntry(dropE) })
	w.sync()
	o := w.eval()
	w.accepted(o, dropE)
	w.accepted(o, removal)
	w.accepted(o, early)
}
