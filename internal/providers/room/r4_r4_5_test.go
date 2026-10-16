package room

import (
	"strings"
	"testing"
)

// R4-5: an entry citing a spec that is held in this call must be held
// (spec_missing), never rejected (spec_lost). The spec below is held because
// its deps cite an entry on node-b's log that this replica does not hold.
func TestBoardR4_R4_5(t *testing.T) {
	w := newWorld(t, nil)
	b := defaultGenesisBody()
	b["gen"] = 2
	delete(b, "name")
	sp := w.mk("node-a", KindSpec, chaz, b, func(e *Entry) {
		e.Deps[testNodeSet["node-b"].dev] = Dep{Seq: 99, ID: strings.Repeat("a", 64)}
	})
	cite := w.mk("node-b", KindLine, vroku, map[string]any{"text": "cites a held spec"}, func(e *Entry) {
		e.Spec = idOfEntry(sp)
		e.Deps = map[string]Dep{}
	})
	o := w.eval()
	w.expect(o, cite, StatusHeld, CodeSpecMissing)
}
