package room

import "testing"

// R5-3: defaultClaimant's "last" agent reply must be taken over the causal
// linearization (vec-sum, log, seq), not over HLC. Here cog's reply carries a
// later HLC but sage's reply causally follows it (sage observed it first), so
// sage is the last reply in L's causal past and must be the default claimant.
func TestBoardR5_R5_3(t *testing.T) {
	w := newWorld(t, nil)
	t1 := w.line("node-a", chaz, "first thread")
	t2 := w.line("node-a", chaz, "second thread")
	w.sync()
	// cog replies to t1 with an HLC far in the future.
	cogReply := w.mk("node-a", KindLine, cog, map[string]any{"text": "r1", "to": []any{}, "in_reply_to": idOfEntry(t1)},
		func(e *Entry) { e.HLC = "2026-10-15T23:59:59.000Z/0" })
	w.sync()
	// sage replies to t2 after observing cog's reply, so it is causally later.
	sageReply := w.mk("node-b", KindLine, sage, map[string]any{"text": "r2", "to": []any{}, "in_reply_to": idOfEntry(t2)})
	w.sync()
	l := w.line("node-a", chaz, "a question for the room")
	w.sync()
	body := func() map[string]any {
		return map[string]any{"line": idOfEntry(l), "default": true, "fallback": false, "name_hit": false}
	}
	sageDefault := w.mk("node-b", KindFloorClaim, sage, body())
	cogDefault := w.mk("node-a", KindFloorClaim, cog, body())
	o := w.eval()
	w.accepted(o, cogReply)
	w.accepted(o, sageReply)
	w.accepted(o, sageDefault)
	w.rejected(o, cogDefault, CodeClaimDefaultWrong)
	assertOrderIndependent(t, w, 25)
}
