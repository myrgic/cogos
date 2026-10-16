package room

import "testing"

// RFC §6.9.3 role table: a guest may create an item and set its non-effect
// fields, but an effect-bearing item.set is "no" for a guest, even on the
// item the guest created ("a guest ... steering work"). Pins the intent that
// cog-review on f31feae read as an omission.
func TestBoardR8_GuestCannotSteerOwnItem(t *testing.T) {
	w := newWorld(t, nil)
	create, id := w.createItem("node-b", guest, nil)
	w.sync()
	title := w.mk("node-b", KindItemSet, guest, map[string]any{"item": id, "field": "title", "value": "renamed"})
	state := w.mk("node-b", KindItemSet, guest, map[string]any{"item": id, "field": "state", "value": "doing"})
	w.sync()
	o := w.eval()
	w.accepted(o, create)
	w.accepted(o, title)
	w.rejected(o, state, CodeEffectFieldDenied)
	assertOrderIndependent(t, w, 25)
}
