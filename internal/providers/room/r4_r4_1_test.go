package room

import "testing"

// R4-1 (round 4 queue): a successor of a Layer-2-rejected predecessor on the
// same log must be rejected, not accepted. The predecessor's deps regress
// (deps_regress), so the successor cites its prev, which is rejected.
func TestBoardR4_R4_1(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "anyone there")
	w.mk("node-b", KindLine, vroku, map[string]any{"text": "x", "to": []any{}})
	w.sync()
	p := w.mk("node-a", KindLine, cog, map[string]any{"text": "p", "to": []any{}})
	w.sync()
	// r drops the node-b dep its predecessor p carries: deps_regress.
	r := w.mk("node-a", KindLine, cog, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(l)},
		func(e *Entry) { delete(e.Deps, testNodeSet["node-b"].dev) })
	w.sync()
	// s is the successor of the rejected r on the same log.
	s := w.mk("node-a", KindLine, cog, map[string]any{"text": "s", "to": []any{}})
	w.sync()
	o := w.eval()
	w.accepted(o, p)
	w.rejected(o, r, CodeDepsRegress)
	w.rejected(o, s, CodePrevBad)
	assertOrderIndependent(t, w, 25)
}
