package room

import (
	"strings"
	"testing"
)

// R5-1: a spec that cites a spec held in the same never-reached pass must be
// held (spec_missing), whichever DeviceID sorts first. Y is gen 3 with an
// unknown parent spec, so it is held by admit. X is gen 4 and cites Y. Under
// the old order X was evaluated before Y had a verdict and fell to spec_lost.
func TestBoardR5_R5_1(t *testing.T) {
	for _, order := range [][2]string{{"node-a", "node-b"}, {"node-b", "node-a"}} {
		t.Run(order[0]+"_first", func(t *testing.T) {
			w := newWorld(t, nil)
			yb := defaultGenesisBody()
			yb["gen"] = 3
			delete(yb, "name")
			y := w.mk(order[1], KindSpec, chaz, yb, func(e *Entry) {
				e.Spec = strings.Repeat("b", 64)
			})
			xb := defaultGenesisBody()
			xb["gen"] = 4
			delete(xb, "name")
			x := w.mk(order[0], KindSpec, chaz, xb, func(e *Entry) {
				e.Spec = idOfEntry(y)
			})
			o := w.eval()
			w.expect(o, y, StatusHeld, CodeSpecMissing)
			w.expect(o, x, StatusHeld, CodeSpecMissing)
		})
	}
}
